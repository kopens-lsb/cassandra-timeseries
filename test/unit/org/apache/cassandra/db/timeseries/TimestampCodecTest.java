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
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Direct round-trip coverage for the shared delta-of-delta timestamp encoding, which used to live
 * in the (now removed) gorilla codec and was only ever exercised through a full chunk encode. The
 * bucket-boundary case is the load-bearing one: a silent off-by-one in a bucket edge corrupts
 * timestamps without failing any round-trip test that happens to stay inside one bucket.
 */
public class TimestampCodecTest
{
    /** Every bucket edge and its just-outside neighbours, plus the 64-bit escape range. */
    private static final long[] BOUNDARY_DODS =
        { -65537, -65536, -65535, -2049, -2048, -2047, -65, -64, -63, -1, 0, 1,
          63, 64, 65, 2047, 2048, 2049, 65535, 65536, 65537,
          Long.MIN_VALUE, Long.MAX_VALUE, -1_000_000_000L, 1_000_000_000L };

    @Test
    public void bucketBoundariesRoundtrip()
    {
        assertRoundtrip(BOUNDARY_DODS);
    }

    @Test
    public void randomDodsRoundtrip()
    {
        Random random = new Random(19);
        long[] dods = new long[5000];
        for (int i = 0; i < dods.length; i++)
        {
            // mix of magnitudes so all five prefix buckets are hit repeatedly
            switch (i % 5)
            {
                case 0:  dods[i] = 0; break;
                case 1:  dods[i] = random.nextInt(128) - 63; break;
                case 2:  dods[i] = random.nextInt(4096) - 2047; break;
                case 3:  dods[i] = random.nextInt(131072) - 65535; break;
                default: dods[i] = random.nextLong(); break;
            }
        }
        assertRoundtrip(dods);
    }

    @Test
    public void zeroDodCostsOneBitPerSample()
    {
        // The whole point of the encoding: a perfectly regular series (constant delta => dod 0)
        // must cost exactly one bit per sample. Anything else is a regression in the prefix code.
        BitWriter writer = new BitWriter();
        for (int i = 0; i < 8000; i++)
            TimestampCodec.writeDod(writer, 0);
        assertEquals(1000, writer.sizeInBytes());
    }

    @Test
    public void bucketWidthsAreMonotonicInMagnitude()
    {
        // Each bucket must cost strictly more than the tighter one below it, else the prefix code
        // would be pointless -- and the escape must stay bounded at 4 + 64 bits.
        assertTrue(bits(0) < bits(64));
        assertTrue(bits(64) < bits(2048));
        assertTrue(bits(2048) < bits(65536));
        assertTrue(bits(65536) < bits(65537));
        assertEquals(68, bits(65537));
    }

    /**
     * Cost of one {@code dod} in bits. {@link BitWriter} only reports whole bytes, so the value is
     * written 1000 times and divided back out -- 1000 is a multiple of 8, so the total is always a
     * whole number of bytes and the division is exact for any per-value width.
     */
    private static int bits(long dod)
    {
        BitWriter writer = new BitWriter();
        for (int i = 0; i < 1000; i++)
            TimestampCodec.writeDod(writer, dod);
        return writer.sizeInBytes() * 8 / 1000;
    }

    private static void assertRoundtrip(long[] dods)
    {
        BitWriter writer = new BitWriter();
        for (long dod : dods)
            TimestampCodec.writeDod(writer, dod);

        ByteBuffer buffer = ByteBuffer.allocate(writer.sizeInBytes());
        writer.writeTo(buffer);
        buffer.flip();

        BitReader reader = new BitReader(buffer);
        for (int i = 0; i < dods.length; i++)
            assertEquals("dod " + i, dods[i], TimestampCodec.readDod(reader));
    }
}
