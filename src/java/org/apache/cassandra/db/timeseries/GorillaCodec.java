/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Lossless gorilla-style codec for (timestampMillis, double) series: delta-of-delta timestamps
 * and XOR-bitpacked values behind a fixed 21-byte header (version, count, first/last timestamp).
 * See docs/superpowers/plans/2026-07-31-gorilla-codec.md for the normative bit format.
 * Timestamps must be strictly increasing; values roundtrip bit-exactly (NaN payloads, -0.0).
 *
 * <p>Contract: {@code payload.position()..payload.limit()} must span EXACTLY one encoded chunk --
 * the sample count is trusted, so an over-long buffer will decode bytes belonging to whatever
 * follows in memory. The input buffer is never mutated (all access goes through a duplicate), so
 * independent cursors/peeks over the same payload are safe to use concurrently. Corruption
 * surfaces as {@link IllegalArgumentException}, {@link IndexOutOfBoundsException}, or
 * {@link java.nio.BufferUnderflowException} -- callers must treat all three as "corrupt chunk".
 * Buffers are read big-endian regardless of the caller's configured byte order.
 */
public final class GorillaCodec
{
    public static final byte VERSION = 1;
    public static final int HEADER_SIZE = 21;
    /** 2^24: keeps {@link BitWriter}'s int bit counter far from overflow (a 1h/1s window is 3600). */
    public static final int MAX_SAMPLES = 16_777_216;

    private GorillaCodec()
    {
    }

    /** Retained for source compatibility with existing {@code GorillaCodec.SampleCursor} references. */
    public interface SampleCursor extends org.apache.cassandra.db.timeseries.SampleCursor
    {
    }

    public static ByteBuffer encode(long[] timestamps, double[] values, int count)
    {
        if (count < 1)
            throw new IllegalArgumentException("count must be >= 1, got " + count);
        if (count > MAX_SAMPLES)
            throw new IllegalArgumentException("count " + count + " exceeds MAX_SAMPLES " + MAX_SAMPLES);
        if (timestamps.length < count || values.length < count)
            throw new IllegalArgumentException("arrays shorter than count " + count);

        BitWriter bits = new BitWriter();
        ValueEncoder valueEncoder = new ValueEncoder();
        valueEncoder.write(bits, values[0]);

        long previousTimestamp = timestamps[0];
        long previousDelta = 0;

        for (int i = 1; i < count; i++)
        {
            long timestamp = timestamps[i];
            if (timestamp <= previousTimestamp)
                throw new IllegalArgumentException("timestamps must be strictly increasing: " +
                                                   timestamp + " after " + previousTimestamp);
            long delta = timestamp - previousTimestamp;
            writeDod(bits, delta - previousDelta);
            previousTimestamp = timestamp;
            previousDelta = delta;

            valueEncoder.write(bits, values[i]);
        }

        ByteBuffer out = ByteBuffer.allocate(HEADER_SIZE + bits.sizeInBytes());
        out.order(ByteOrder.BIG_ENDIAN);   // allocate() already defaults to this; make the contract explicit
        out.put(VERSION);
        out.putInt(count);
        out.putLong(timestamps[0]);
        out.putLong(timestamps[count - 1]);
        bits.writeTo(out);
        out.flip();
        return out;
    }

    /**
     * Encodes only the value stream (first value raw, then XOR-bitpacked deltas) with no
     * timestamps interleaved -- the counterpart to the per-sample loop inside {@link #encode},
     * minus the {@link #writeDod} calls. Used by {@link ColumnarChunkCodec} (chunk format
     * version 3) to store a DOUBLE_GORILLA column section against the chunk's shared timestamp
     * axis instead of duplicating it per column. See {@link ValueDecoder} for the matching reader.
     */
    static void encodeValues(BitWriter bits, double[] values, int count)
    {
        ValueEncoder encoder = new ValueEncoder();
        for (int i = 0; i < count; i++)
            encoder.write(bits, values[i]);
    }

    public static int sampleCount(ByteBuffer payload)
    {
        ByteBuffer buffer = checkedHeader(payload);
        return buffer.getInt(buffer.position() + 1);
    }

    public static long firstTimestamp(ByteBuffer payload)
    {
        ByteBuffer buffer = checkedHeader(payload);
        return buffer.getLong(buffer.position() + 5);
    }

    public static long lastTimestamp(ByteBuffer payload)
    {
        ByteBuffer buffer = checkedHeader(payload);
        return buffer.getLong(buffer.position() + 13);
    }

    /**
     * Duplicates {@code payload}, pins the duplicate's byte order to big-endian, and verifies the
     * version byte at {@code position()} is {@link #VERSION}. Used by the header peeks so they are
     * independent of the caller's buffer byte order and reject foreign versions like {@link #cursor}.
     */
    private static ByteBuffer checkedHeader(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get(buffer.position());
        if (version != VERSION)
            throw new IllegalArgumentException("Unsupported gorilla chunk version: " + version);
        return buffer;
    }

    /**
     * Returns a cursor over the samples encoded in {@code payload}. See the class-level contract
     * for buffer bounds, mutation, corruption-signalling, and byte-order guarantees.
     */
    public static SampleCursor cursor(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get();
        if (version != VERSION)
            throw new IllegalArgumentException("Unsupported gorilla chunk version: " + version);
        int count = buffer.getInt();
        if (count < 1)
            throw new IllegalArgumentException("Corrupt gorilla chunk: count " + count);
        long firstTimestamp = buffer.getLong();
        buffer.getLong();   // last timestamp: header-only metadata
        BitReader bits = new BitReader(buffer);
        ValueDecoder valueDecoder = new ValueDecoder();

        return new SampleCursor()
        {
            private int index = -1;
            private long timestamp;
            private long delta;
            private long valueBits;

            @Override
            public boolean advance()
            {
                if (index + 1 >= count)
                    return false;
                index++;
                if (index == 0)
                {
                    timestamp = firstTimestamp;
                }
                else
                {
                    delta += readDod(bits);
                    timestamp += delta;
                }
                valueBits = valueDecoder.readBits(bits);
                return true;
            }

            @Override
            public long timestamp()
            {
                if (index < 0)
                    throw new IllegalStateException("advance() not called");
                return timestamp;
            }

            @Override
            public double value()
            {
                if (index < 0)
                    throw new IllegalStateException("advance() not called");
                return Double.longBitsToDouble(valueBits);
            }
        };
    }

    static void writeDod(BitWriter bits, long dod)
    {
        if (dod == 0)
        {
            bits.writeBit(false);
        }
        else if (dod >= -63 && dod <= 64)
        {
            bits.writeBits(0b10, 2);
            bits.writeBits(dod + 63, 7);
        }
        else if (dod >= -2047 && dod <= 2048)
        {
            bits.writeBits(0b110, 3);
            bits.writeBits(dod + 2047, 12);
        }
        else if (dod >= -65535 && dod <= 65536)
        {
            bits.writeBits(0b1110, 4);
            bits.writeBits(dod + 65535, 17);
        }
        else
        {
            bits.writeBits(0b1111, 4);
            bits.writeBits(dod, 64);
        }
    }

    static long readDod(BitReader bits)
    {
        if (!bits.readBit())
            return 0;
        if (!bits.readBit())
            return bits.readBits(7) - 63;
        if (!bits.readBit())
            return bits.readBits(12) - 2047;
        if (!bits.readBit())
            return bits.readBits(17) - 65535;
        return bits.readBits(64);
    }

    /**
     * Stateful writer for the gorilla value stream, one {@link #write} call per sample in order:
     * the first call emits the raw 64-bit value, later calls XOR against the previous value using
     * the same leading/trailing "window" bit-saving trick as the inline loop in {@link #encode}.
     * Shared by {@link #encode} (interleaved with per-sample {@link #writeDod} calls) and
     * {@link #encodeValues} (value stream only, no timestamps).
     */
    static final class ValueEncoder
    {
        private long previousBits;
        private int windowLeading = -1;
        private int windowTrailing;
        private int index = -1;

        void write(BitWriter bits, double value)
        {
            index++;
            long valueBits = Double.doubleToRawLongBits(value);
            if (index == 0)
            {
                bits.writeBits(valueBits, 64);
            }
            else
            {
                long xor = valueBits ^ previousBits;
                if (xor == 0)
                {
                    bits.writeBit(false);
                }
                else
                {
                    bits.writeBit(true);
                    int leading = Math.min(31, Long.numberOfLeadingZeros(xor));
                    int trailing = Long.numberOfTrailingZeros(xor);
                    if (windowLeading != -1 && leading >= windowLeading && trailing >= windowTrailing)
                    {
                        bits.writeBit(false);
                        bits.writeBits(xor >>> windowTrailing, 64 - windowLeading - windowTrailing);
                    }
                    else
                    {
                        bits.writeBit(true);
                        int meaningful = 64 - leading - trailing;
                        bits.writeBits(leading, 5);
                        bits.writeBits(meaningful - 1, 6);   // 1..64 stored as 0..63
                        bits.writeBits(xor >>> trailing, meaningful);
                        windowLeading = leading;
                        windowTrailing = trailing;
                    }
                }
            }
            previousBits = valueBits;
        }
    }

    /** Mirrors {@link ValueEncoder}: one {@link #readBits} call per sample, in the same order. */
    static final class ValueDecoder
    {
        private long previousBits;
        private int windowLeading = -1;
        private int windowTrailing;
        private int index = -1;

        long readBits(BitReader bits)
        {
            index++;
            if (index == 0)
            {
                previousBits = bits.readBits(64);
                return previousBits;
            }
            if (bits.readBit())
            {
                if (bits.readBit())
                {
                    windowLeading = (int) bits.readBits(5);
                    int meaningful = (int) bits.readBits(6) + 1;
                    if (windowLeading + meaningful > 64)
                        throw new IllegalArgumentException("Corrupt gorilla chunk: window " +
                                                           windowLeading + '+' + meaningful + " exceeds 64 bits");
                    windowTrailing = 64 - windowLeading - meaningful;
                    previousBits ^= bits.readBits(meaningful) << windowTrailing;
                }
                else
                {
                    int meaningful = 64 - windowLeading - windowTrailing;
                    previousBits ^= bits.readBits(meaningful) << windowTrailing;
                }
            }
            return previousBits;
        }
    }
}
