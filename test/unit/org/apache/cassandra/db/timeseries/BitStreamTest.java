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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BitStreamTest
{
    @Test
    public void singleBitsRoundtrip()
    {
        BitWriter writer = new BitWriter();
        boolean[] bits = { true, false, true, true, false, false, true, false, true };
        for (boolean bit : bits)
            writer.writeBit(bit);

        BitReader reader = readerOf(writer);
        for (boolean bit : bits)
            assertEquals(bit, reader.readBit());
    }

    @Test
    public void fullWidthValuesRoundtrip()
    {
        BitWriter writer = new BitWriter();
        long[] values = { 0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0xDEADBEEFCAFEBABEL };
        for (long value : values)
            writer.writeBits(value, 64);

        BitReader reader = readerOf(writer);
        for (long value : values)
            assertEquals(value, reader.readBits(64));
    }

    @Test
    public void unalignedMixedWidthsRoundtrip()
    {
        // widths deliberately misaligned so values straddle byte and word boundaries
        BitWriter writer = new BitWriter();
        int[] widths = { 1, 7, 12, 3, 64, 17, 5, 33, 9, 64, 2 };
        long[] values = new long[widths.length];
        Random random = new Random(42);
        for (int i = 0; i < widths.length; i++)
        {
            values[i] = random.nextLong() & (widths[i] == 64 ? -1L : (1L << widths[i]) - 1);
            writer.writeBits(values[i], widths[i]);
        }

        BitReader reader = readerOf(writer);
        for (int i = 0; i < widths.length; i++)
            assertEquals("width " + widths[i], values[i], reader.readBits(widths[i]));
    }

    @Test
    public void randomizedRoundtripAcrossSeeds()
    {
        for (long seed = 0; seed < 50; seed++)
        {
            Random random = new Random(seed);
            BitWriter writer = new BitWriter();
            List<long[]> written = new ArrayList<>(); // [value, width]
            int operations = 1 + random.nextInt(4000);   // forces capacity growth past 16 words
            for (int i = 0; i < operations; i++)
            {
                int width = 1 + random.nextInt(64);
                long value = random.nextLong() & (width == 64 ? -1L : (1L << width) - 1);
                writer.writeBits(value, width);
                written.add(new long[]{ value, width });
            }

            BitReader reader = readerOf(writer);
            for (long[] entry : written)
                assertEquals("seed " + seed, entry[0], reader.readBits((int) entry[1]));
        }
    }

    @Test
    public void sizeInBytesRoundsUp()
    {
        BitWriter writer = new BitWriter();
        writer.writeBits(0b101, 3);
        assertEquals(1, writer.sizeInBytes());
        writer.writeBits(0, 5);
        assertEquals(1, writer.sizeInBytes());
        writer.writeBit(true);
        assertEquals(2, writer.sizeInBytes());
    }

    private static BitReader readerOf(BitWriter writer)
    {
        ByteBuffer buffer = ByteBuffer.allocate(writer.sizeInBytes());
        writer.writeTo(buffer);
        buffer.flip();
        return new BitReader(buffer);
    }
}
