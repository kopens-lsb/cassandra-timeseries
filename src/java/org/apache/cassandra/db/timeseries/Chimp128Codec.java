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
import java.util.Arrays;

/**
 * Chunk codec version 2: timestamps use the same delta-of-delta scheme as version 1 (gorilla),
 * values use the Chimp128 scheme (Liakos et al., PVLDB'22): each value XORs against the most
 * similar of the previous 128 values (indexed by the 14 low bits of the raw representation)
 * when that XOR has more than 13 trailing zeros, else against the immediately previous value,
 * with a 2-bit flag tree and 3-bit bucketed leading-zero codes. Same 21-byte header and API
 * contract as {@link GorillaCodec} (exact-span payloads, big-endian, non-mutating cursors;
 * corruption surfaces as IllegalArgumentException / IndexOutOfBoundsException /
 * BufferUnderflowException). See docs/superpowers/specs/2026-07-31-chimp128-codec-design.md.
 */
public final class Chimp128Codec
{
    public static final byte VERSION = 2;
    public static final int HEADER_SIZE = 21;
    public static final int MAX_SAMPLES = GorillaCodec.MAX_SAMPLES;

    static final int RING_SIZE = 128;                    // 7-bit ring positions
    static final int INDEX_BITS = 14;                    // key = rawBits & 0x3FFF
    static final int KEY_MASK = (1 << INDEX_BITS) - 1;
    static final int TRAILING_THRESHOLD = 13;            // log2(128) + log2(64)
    static final int[] LEADING_BUCKETS = { 0, 8, 12, 16, 18, 20, 22, 24 };

    private Chimp128Codec()
    {
    }

    /** Rounds an actual leading-zero count DOWN to the nearest bucket; returns the 3-bit code. */
    static int leadingCode(int leadingZeros)
    {
        for (int code = LEADING_BUCKETS.length - 1; code >= 0; code--)
            if (leadingZeros >= LEADING_BUCKETS[code])
                return code;
        return 0;
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

        for (int j = 1; j < count; j++)
        {
            long timestamp = timestamps[j];
            if (timestamp <= previousTimestamp)
                throw new IllegalArgumentException("timestamps must be strictly increasing: " +
                                                   timestamp + " after " + previousTimestamp);
            long delta = timestamp - previousTimestamp;
            GorillaCodec.writeDod(bits, delta - previousDelta);
            previousTimestamp = timestamp;
            previousDelta = delta;

            valueEncoder.write(bits, values[j]);
        }

        ByteBuffer out = ByteBuffer.allocate(HEADER_SIZE + bits.sizeInBytes()).order(ByteOrder.BIG_ENDIAN);
        out.put(VERSION);
        out.putInt(count);
        out.putLong(timestamps[0]);
        out.putLong(timestamps[count - 1]);
        bits.writeTo(out);
        out.flip();
        return out;
    }

    /**
     * Encodes only the value stream (ring-XOR deltas) with no timestamps interleaved -- the
     * counterpart to the per-sample loop inside {@link #encode}, minus the
     * {@link GorillaCodec#writeDod} calls. Used by {@link ColumnarChunkCodec} (chunk format
     * version 3) to store a DOUBLE_CHIMP column section against the chunk's shared timestamp axis
     * instead of duplicating it per column. See {@link ValueDecoder} for the matching reader.
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

    private static ByteBuffer checkedHeader(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate().order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get(buffer.position());
        if (version != VERSION)
            throw new IllegalArgumentException("Unsupported chimp chunk version: " + version);
        return buffer;
    }

    public static SampleCursor cursor(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate().order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get();
        if (version != VERSION)
            throw new IllegalArgumentException("Unsupported chimp chunk version: " + version);
        int count = buffer.getInt();
        if (count < 1)
            throw new IllegalArgumentException("Corrupt chimp chunk: count " + count);
        long firstTimestamp = buffer.getLong();
        buffer.getLong();
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
                    delta += GorillaCodec.readDod(bits);
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

    /**
     * Stateful writer for the chimp128 value stream, one {@link #write} call per sample in order:
     * the first call emits the raw 64-bit value; later calls XOR against either an explicit ring
     * slot (left branch) or the immediately previous value (right branch), mirroring the inline
     * loop in {@link #encode}. Shared by {@link #encode} (interleaved with per-sample
     * {@link GorillaCodec#writeDod} calls) and {@link #encodeValues} (value stream only).
     */
    static final class ValueEncoder
    {
        private final long[] ring = new long[RING_SIZE];
        private final int[] indices = new int[1 << INDEX_BITS];
        private int previousLeadCode = -1;
        private int index = -1;

        ValueEncoder()
        {
            Arrays.fill(indices, -1);
        }

        void write(BitWriter bits, double value)
        {
            index++;
            long raw = Double.doubleToRawLongBits(value);
            int key = (int) (raw & KEY_MASK);
            if (index == 0)
            {
                bits.writeBits(raw, 64);
            }
            else
            {
                int candidateIndex = indices[key];
                boolean candidateUsable = candidateIndex >= 0 && index - candidateIndex < RING_SIZE;
                long candidateXor = 0;
                int candidateTrailing = 0;
                if (candidateUsable)
                {
                    candidateXor = raw ^ ring[candidateIndex % RING_SIZE];
                    candidateTrailing = candidateXor == 0 ? 64 : Long.numberOfTrailingZeros(candidateXor);
                }

                if (candidateUsable && candidateTrailing > TRAILING_THRESHOLD)
                {
                    // left branch: reference a ring value explicitly
                    bits.writeBit(false);
                    bits.writeBits(candidateIndex % RING_SIZE, 7);
                    if (candidateXor == 0)
                    {
                        bits.writeBit(false);
                        // spec decision: previousLeadCode intentionally NOT updated on an exact match
                    }
                    else
                    {
                        bits.writeBit(true);
                        int leadCode = leadingCode(Long.numberOfLeadingZeros(candidateXor));
                        int leadValue = LEADING_BUCKETS[leadCode];
                        int center = 64 - leadValue - candidateTrailing;
                        bits.writeBits(leadCode, 3);
                        bits.writeBits(center, 6);
                        bits.writeBits(candidateXor >>> candidateTrailing, center);
                        previousLeadCode = leadCode;
                    }
                }
                else
                {
                    // right branch: XOR against the immediately previous value
                    long xor = raw ^ ring[(index - 1) % RING_SIZE];
                    int leadCode = leadingCode(xor == 0 ? 64 : Long.numberOfLeadingZeros(xor));
                    int leadValue = LEADING_BUCKETS[leadCode];
                    bits.writeBit(true);
                    if (leadCode == previousLeadCode)
                    {
                        bits.writeBit(false);
                    }
                    else
                    {
                        bits.writeBit(true);
                        bits.writeBits(leadCode, 3);
                    }
                    bits.writeBits(xor, 64 - leadValue);
                    previousLeadCode = leadCode;
                }
            }
            ring[index % RING_SIZE] = raw;
            indices[key] = index;
        }
    }

    /** Mirrors {@link ValueEncoder}: one {@link #readBits} call per sample, in the same order. */
    static final class ValueDecoder
    {
        private final long[] ring = new long[RING_SIZE];
        private final int[] slotIndex = new int[RING_SIZE];
        private int previousLeadCode = -1;
        private int index = -1;

        ValueDecoder()
        {
            Arrays.fill(slotIndex, -1);
        }

        long readBits(BitReader bits)
        {
            index++;
            long valueBits;
            if (index == 0)
            {
                valueBits = bits.readBits(64);
            }
            else if (!bits.readBit())
            {
                // left branch: explicit ring reference
                int position = (int) bits.readBits(7);
                if (slotIndex[position] < 0)
                    throw new IllegalArgumentException("Corrupt chimp chunk: reference to unwritten ring slot " + position);
                long candidate = ring[position];
                if (!bits.readBit())
                {
                    valueBits = candidate;
                    // previousLeadCode intentionally unchanged (mirrors encoder)
                }
                else
                {
                    int leadCode = (int) bits.readBits(3);
                    int leadValue = LEADING_BUCKETS[leadCode];
                    int center = (int) bits.readBits(6);
                    int trailing = 64 - leadValue - center;
                    if (center < 1 || trailing <= TRAILING_THRESHOLD)
                        throw new IllegalArgumentException("Corrupt chimp chunk: center " + center +
                                                           " with lead " + leadValue);
                    valueBits = candidate ^ (bits.readBits(center) << trailing);
                    previousLeadCode = leadCode;
                }
            }
            else
            {
                // right branch: XOR against immediately previous value
                int leadCode;
                if (!bits.readBit())
                {
                    if (previousLeadCode < 0)
                        throw new IllegalArgumentException("Corrupt chimp chunk: lead reuse before any lead");
                    leadCode = previousLeadCode;
                }
                else
                {
                    leadCode = (int) bits.readBits(3);
                }
                int leadValue = LEADING_BUCKETS[leadCode];
                long xor = bits.readBits(64 - leadValue);
                valueBits = ring[(index - 1) % RING_SIZE] ^ xor;
                previousLeadCode = leadCode;
            }
            ring[index % RING_SIZE] = valueBits;
            slotIndex[index % RING_SIZE] = index;
            return valueBits;
        }
    }
}
