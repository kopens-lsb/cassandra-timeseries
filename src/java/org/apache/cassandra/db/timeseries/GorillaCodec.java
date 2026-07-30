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

/**
 * Lossless gorilla-style codec for (timestampMillis, double) series: delta-of-delta timestamps
 * and XOR-bitpacked values behind a fixed 21-byte header (version, count, first/last timestamp).
 * See docs/superpowers/plans/2026-07-31-gorilla-codec.md for the normative bit format.
 * Timestamps must be strictly increasing; values roundtrip bit-exactly (NaN payloads, -0.0).
 */
public final class GorillaCodec
{
    public static final byte VERSION = 1;
    static final int HEADER_SIZE = 21;

    private GorillaCodec()
    {
    }

    public interface SampleCursor
    {
        boolean advance();
        long timestamp();
        double value();
    }

    public static ByteBuffer encode(long[] timestamps, double[] values, int count)
    {
        if (count < 1)
            throw new IllegalArgumentException("count must be >= 1, got " + count);
        if (timestamps.length < count || values.length < count)
            throw new IllegalArgumentException("arrays shorter than count " + count);

        BitWriter bits = new BitWriter();
        long previousBits = Double.doubleToRawLongBits(values[0]);
        bits.writeBits(previousBits, 64);

        long previousTimestamp = timestamps[0];
        long previousDelta = 0;
        int windowLeading = -1;
        int windowTrailing = 0;

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

            long valueBits = Double.doubleToRawLongBits(values[i]);
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
            previousBits = valueBits;
        }

        ByteBuffer out = ByteBuffer.allocate(HEADER_SIZE + bits.sizeInBytes());
        out.put(VERSION);
        out.putInt(count);
        out.putLong(timestamps[0]);
        out.putLong(timestamps[count - 1]);
        bits.writeTo(out);
        out.flip();
        return out;
    }

    public static int sampleCount(ByteBuffer payload)
    {
        return payload.getInt(payload.position() + 1);
    }

    public static long firstTimestamp(ByteBuffer payload)
    {
        return payload.getLong(payload.position() + 5);
    }

    public static long lastTimestamp(ByteBuffer payload)
    {
        return payload.getLong(payload.position() + 13);
    }

    public static SampleCursor cursor(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        byte version = buffer.get();
        if (version != VERSION)
            throw new IllegalArgumentException("Unsupported gorilla chunk version: " + version);
        int count = buffer.getInt();
        if (count < 1)
            throw new IllegalArgumentException("Corrupt gorilla chunk: count " + count);
        long firstTimestamp = buffer.getLong();
        buffer.getLong();   // last timestamp: header-only metadata
        BitReader bits = new BitReader(buffer);

        return new SampleCursor()
        {
            private int index = -1;
            private long timestamp;
            private long delta;
            private long valueBits;
            private int windowLeading = -1;
            private int windowTrailing;

            @Override
            public boolean advance()
            {
                if (index + 1 >= count)
                    return false;
                index++;
                if (index == 0)
                {
                    timestamp = firstTimestamp;
                    valueBits = bits.readBits(64);
                    return true;
                }

                delta += readDod(bits);
                timestamp += delta;

                if (bits.readBit())
                {
                    if (bits.readBit())
                    {
                        windowLeading = (int) bits.readBits(5);
                        int meaningful = (int) bits.readBits(6) + 1;
                        windowTrailing = 64 - windowLeading - meaningful;
                        valueBits ^= bits.readBits(meaningful) << windowTrailing;
                    }
                    else
                    {
                        int meaningful = 64 - windowLeading - windowTrailing;
                        valueBits ^= bits.readBits(meaningful) << windowTrailing;
                    }
                }
                return true;
            }

            @Override
            public long timestamp()
            {
                return timestamp;
            }

            @Override
            public double value()
            {
                return Double.longBitsToDouble(valueBits);
            }
        };
    }

    private static void writeDod(BitWriter bits, long dod)
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

    private static long readDod(BitReader bits)
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
}
