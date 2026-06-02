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
package org.apache.cassandra.db.aggregation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TimeBucketGapFillerTest
{
    private static final int PK = 0, BUCKET = 1, SUM = 2, COLS = 3;

    private static List<ByteBuffer> row(int pk, long bucketMillis, Integer sum)
    {
        return new ArrayList<>(Arrays.asList(Int32Type.instance.decompose(pk),
                                             ByteBufferUtil.bytes(bucketMillis),
                                             sum == null ? null : Int32Type.instance.decompose(sum)));
    }

    private static long bucketOf(List<ByteBuffer> r)
    {
        return ByteBufferUtil.toLong(r.get(BUCKET));
    }

    @Test
    public void testFillsMissingInteriorAndEdgeBuckets()
    {
        // Existing buckets 10s and 30s; range [0, 40s) step 10s -> expect 0,10,20,30 with 0 and 20 synthesized.
        List<List<ByteBuffer>> rows = new ArrayList<>(Arrays.asList(row(1, 10_000, 5), row(1, 30_000, 7)));

        List<List<ByteBuffer>> out = TimeBucketGapFiller.densify(rows, BUCKET, new int[]{ PK }, new int[0], new int[0],
                                                                 COLS, 0, 40_000, 10_000);

        assertEquals(4, out.size());
        long[] expectedBuckets = { 0, 10_000, 20_000, 30_000 };
        Integer[] expectedSums = { null, 5, null, 7 };
        for (int i = 0; i < 4; i++)
        {
            assertEquals(expectedBuckets[i], bucketOf(out.get(i)));
            assertEquals(Integer.valueOf(1), Int32Type.instance.compose(out.get(i).get(PK))); // series identity kept
            if (expectedSums[i] == null)
                assertNull(out.get(i).get(SUM));
            else
                assertEquals(expectedSums[i], Int32Type.instance.compose(out.get(i).get(SUM)));
        }
    }

    @Test
    public void testFillsPerPartitionIndependently()
    {
        // Two series; each densified over [0, 30s) step 10s independently.
        List<List<ByteBuffer>> rows = new ArrayList<>(Arrays.asList(row(1, 0, 1),
                                                                    row(1, 20_000, 2),
                                                                    row(2, 10_000, 9)));

        List<List<ByteBuffer>> out = TimeBucketGapFiller.densify(rows, BUCKET, new int[]{ PK }, new int[0], new int[0],
                                                                 COLS, 0, 30_000, 10_000);

        // pk1: 0(1),10(null),20(2) ; pk2: 0(null),10(9),20(null)
        assertEquals(6, out.size());
        assertEquals(1, (int) Int32Type.instance.compose(out.get(0).get(PK)));
        assertEquals(0, bucketOf(out.get(0)));
        assertEquals(10_000, bucketOf(out.get(1)));
        assertNull(out.get(1).get(SUM));
        assertEquals(20_000, bucketOf(out.get(2)));
        assertEquals(2, (int) Int32Type.instance.compose(out.get(3).get(PK)));
        assertEquals(0, bucketOf(out.get(3)));
        assertNull(out.get(3).get(SUM));
        assertEquals(9, (int) Int32Type.instance.compose(out.get(4).get(SUM)));
        assertEquals(20_000, bucketOf(out.get(5)));
    }

    @Test
    public void testNoGapsLeavesRowsUnchanged()
    {
        List<List<ByteBuffer>> rows = new ArrayList<>(Arrays.asList(row(1, 0, 1), row(1, 10_000, 2)));
        List<List<ByteBuffer>> out = TimeBucketGapFiller.densify(rows, BUCKET, new int[]{ PK }, new int[0], new int[0],
                                                                 COLS, 0, 20_000, 10_000);
        assertEquals(2, out.size());
        assertEquals(0, bucketOf(out.get(0)));
        assertEquals(10_000, bucketOf(out.get(1)));
    }

    @Test
    public void testLocfCarriesPreviousValueForward()
    {
        // Buckets 10s=5 and 30s=7 over [0,40s) step 10s; the SUM column (index 2) is locf.
        List<List<ByteBuffer>> rows = new ArrayList<>(Arrays.asList(row(1, 10_000, 5), row(1, 30_000, 7)));

        List<List<ByteBuffer>> out = TimeBucketGapFiller.densify(rows, BUCKET, new int[]{ PK },
                                                                 new int[]{ SUM }, new int[0], COLS,
                                                                 0, 40_000, 10_000);

        // bucket 0: nothing seen yet -> null; 10: 5; 20: carries 5; 30: 7
        assertEquals(4, out.size());
        assertNull(out.get(0).get(SUM));
        assertEquals(5, (int) Int32Type.instance.compose(out.get(1).get(SUM)));
        assertEquals(5, (int) Int32Type.instance.compose(out.get(2).get(SUM))); // carried forward
        assertEquals(7, (int) Int32Type.instance.compose(out.get(3).get(SUM)));
    }

    @Test
    public void testInterpolateLinearlyFillsBetweenValues()
    {
        // A double-valued column: bucket 0 = 0.0, bucket 30s = 30.0; 10s and 20s empty -> interpolate to 10.0, 20.0.
        List<List<ByteBuffer>> rows = new ArrayList<>(Arrays.asList(
            new ArrayList<>(Arrays.asList(Int32Type.instance.decompose(1), ByteBufferUtil.bytes(0L), ByteBufferUtil.bytes(0.0))),
            new ArrayList<>(Arrays.asList(Int32Type.instance.decompose(1), ByteBufferUtil.bytes(30_000L), ByteBufferUtil.bytes(30.0)))));

        List<List<ByteBuffer>> out = TimeBucketGapFiller.densify(rows, BUCKET, new int[]{ PK },
                                                                 new int[0], new int[]{ SUM },
                                                                 COLS, 0, 40_000, 10_000);

        assertEquals(4, out.size());
        assertEquals(0.0, ByteBufferUtil.toDouble(out.get(0).get(SUM)), 1e-9);
        assertEquals(10.0, ByteBufferUtil.toDouble(out.get(1).get(SUM)), 1e-9);
        assertEquals(20.0, ByteBufferUtil.toDouble(out.get(2).get(SUM)), 1e-9);
        assertEquals(30.0, ByteBufferUtil.toDouble(out.get(3).get(SUM)), 1e-9);
    }

    @Test
    public void testEmptyInputProducesNoRows()
    {
        List<List<ByteBuffer>> out = TimeBucketGapFiller.densify(new ArrayList<>(), BUCKET,
                                                                 new int[]{ PK }, new int[0], new int[0], COLS, 0, 30_000, 10_000);
        assertEquals(0, out.size());
    }

    @Test
    public void testInvalidRangeReturnsInput()
    {
        List<List<ByteBuffer>> rows = new ArrayList<>(Arrays.asList(row(1, 10_000, 5)));
        // finish <= start, and step <= 0: both should pass rows through untouched.
        assertEquals(rows, TimeBucketGapFiller.densify(rows, BUCKET, new int[]{ PK }, new int[0], new int[0], COLS, 40_000, 0, 10_000));
        assertEquals(rows, TimeBucketGapFiller.densify(rows, BUCKET, new int[]{ PK }, new int[0], new int[0], COLS, 0, 40_000, 0));
    }
}
