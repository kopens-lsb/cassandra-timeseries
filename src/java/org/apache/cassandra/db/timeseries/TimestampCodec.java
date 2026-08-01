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

/**
 * The delta-of-delta timestamp encoding shared by every chunk format: a variable-length prefix code
 * over the second difference of the timestamp series, so a perfectly regular series costs one bit
 * per sample. Originally part of the gorilla codec (Pelt et al.); it outlived that codec's value
 * stream, which was removed when chimp128 became the only double codec, so it lives here on its own.
 *
 * <p>Prefix buckets, most to least common: {@code 0} (dod == 0), {@code 10} + 7 bits
 * ([-63, 64]), {@code 110} + 12 bits ([-2047, 2048]), {@code 1110} + 17 bits ([-65535, 65536]),
 * {@code 1111} + a raw 64-bit escape for anything else. Each bounded bucket stores
 * {@code dod + bias} so the stored value is non-negative.
 *
 * <p>{@link #readDod} must be called exactly as many times, in the same order, as
 * {@link #writeDod} was; a truncated or corrupt bitstream surfaces as
 * {@link IndexOutOfBoundsException} or {@link java.nio.BufferUnderflowException} out of the
 * underlying {@link BitReader}, which callers must treat as a corrupt chunk.
 */
final class TimestampCodec
{
    private TimestampCodec()
    {
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
}
