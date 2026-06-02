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
import java.util.List;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Densifies the rows of a {@code GROUP BY ... time_bucket_gapfill(...)} result: for every time bucket in the requested
 * {@code [start, finish)} range that has no row, a synthetic row is inserted so the bucket axis is continuous. This is
 * the pure, transport-agnostic core of gap-fill; wiring it into the query path supplies the parameters.
 *
 * <p>Input rows are expected to be ordered by their grouping columns — that is, grouped by partition (the columns at
 * {@code partitionKeyIndices}) and, within a partition, by ascending bucket. Bucket values are {@code timestamp}s
 * encoded as an 8-byte epoch-millis value (the {@code TimestampType} representation). Synthetic rows copy the partition
 * columns and the bucket column and leave every other column {@code null} (no fill policy yet — this is the G1
 * "densify only" step; {@code locf}/{@code interpolate} are layered on later).
 */
public final class TimeBucketGapFiller
{
    /**
     * @param rows the aggregated result rows, ordered by partition then ascending bucket
     * @param bucketIndex the column index holding the bucket timestamp
     * @param partitionKeyIndices the column indices that identify a partition (reset boundary); may be empty
     * @param columnCount the number of columns in each row
     * @param startMillis inclusive lower bound of the dense range (epoch millis, bucket-aligned)
     * @param finishMillis exclusive upper bound of the dense range (epoch millis)
     * @param stepMillis the bucket width in milliseconds (must be &gt; 0)
     * @return a new list of rows with missing in-range buckets filled
     */
    public static List<List<ByteBuffer>> densify(List<List<ByteBuffer>> rows,
                                                  int bucketIndex,
                                                  List<Integer> partitionKeyIndices,
                                                  List<Integer> locfColumnIndices,
                                                  int columnCount,
                                                  long startMillis,
                                                  long finishMillis,
                                                  long stepMillis)
    {
        if (stepMillis <= 0 || finishMillis <= startMillis)
            return rows;

        List<List<ByteBuffer>> out = new ArrayList<>(rows.size());

        int i = 0;
        int n = rows.size();
        while (i < n)
        {
            // Collect the next partition group (consecutive rows sharing the partition-key columns).
            List<ByteBuffer> first = rows.get(i);
            int groupEnd = i + 1;
            while (groupEnd < n && samePartition(first, rows.get(groupEnd), partitionKeyIndices))
                groupEnd++;

            fillGroup(rows, i, groupEnd, bucketIndex, partitionKeyIndices, locfColumnIndices, columnCount,
                      startMillis, finishMillis, stepMillis, out);
            i = groupEnd;
        }

        return out;
    }

    private static void fillGroup(List<List<ByteBuffer>> rows,
                                  int from,
                                  int to,
                                  int bucketIndex,
                                  List<Integer> partitionKeyIndices,
                                  List<Integer> locfColumnIndices,
                                  int columnCount,
                                  long startMillis,
                                  long finishMillis,
                                  long stepMillis,
                                  List<List<ByteBuffer>> out)
    {
        List<ByteBuffer> template = rows.get(from);
        // Last seen value per locf column within this partition, carried forward into synthesized empty buckets.
        ByteBuffer[] lastLocf = new ByteBuffer[columnCount];
        // Walk the expected bucket axis and the existing rows together; emit existing rows in place and synthesize the
        // missing ones. Existing rows outside [start, finish) are passed through unchanged.
        long bucket = startMillis;
        int r = from;

        while (r < to)
        {
            List<ByteBuffer> row = rows.get(r);
            long rowBucket = ByteBufferUtil.toLong(row.get(bucketIndex));

            // Pass through any existing rows that precede the current expected bucket (e.g. before start).
            if (rowBucket < bucket || bucket >= finishMillis)
            {
                rememberLocf(row, locfColumnIndices, lastLocf);
                out.add(row);
                r++;
                continue;
            }

            // Synthesize all empty buckets strictly before this row's bucket.
            while (bucket < rowBucket && bucket < finishMillis)
            {
                out.add(syntheticRow(template, bucketIndex, partitionKeyIndices, locfColumnIndices, lastLocf,
                                     columnCount, bucket));
                bucket += stepMillis;
            }

            // Emit the existing row for this bucket and advance both cursors past it.
            rememberLocf(row, locfColumnIndices, lastLocf);
            out.add(row);
            r++;
            if (bucket == rowBucket)
                bucket += stepMillis;
        }

        // Synthesize any remaining empty buckets up to finish.
        while (bucket < finishMillis)
        {
            out.add(syntheticRow(template, bucketIndex, partitionKeyIndices, locfColumnIndices, lastLocf,
                                 columnCount, bucket));
            bucket += stepMillis;
        }
    }

    private static void rememberLocf(List<ByteBuffer> row, List<Integer> locfColumnIndices, ByteBuffer[] lastLocf)
    {
        for (int idx : locfColumnIndices)
            lastLocf[idx] = row.get(idx);
    }

    private static List<ByteBuffer> syntheticRow(List<ByteBuffer> template,
                                                 int bucketIndex,
                                                 List<Integer> partitionKeyIndices,
                                                 List<Integer> locfColumnIndices,
                                                 ByteBuffer[] lastLocf,
                                                 int columnCount,
                                                 long bucketMillis)
    {
        List<ByteBuffer> row = new ArrayList<>(columnCount);
        for (int c = 0; c < columnCount; c++)
            row.add(null);
        // Carry the series identity (partition columns) and set the bucket; other columns stay null unless they are
        // locf columns, which carry the previous non-empty bucket's value forward.
        for (int idx : partitionKeyIndices)
            row.set(idx, template.get(idx));
        for (int idx : locfColumnIndices)
            row.set(idx, lastLocf[idx]);
        row.set(bucketIndex, ByteBufferUtil.bytes(bucketMillis));
        return row;
    }

    private static boolean samePartition(List<ByteBuffer> a, List<ByteBuffer> b, List<Integer> partitionKeyIndices)
    {
        for (int idx : partitionKeyIndices)
        {
            ByteBuffer x = a.get(idx);
            ByteBuffer y = b.get(idx);
            if (x == null ? y != null : !x.equals(y))
                return false;
        }
        return true;
    }

    private TimeBucketGapFiller()
    {
    }
}
