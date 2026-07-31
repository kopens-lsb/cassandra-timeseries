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

package org.apache.cassandra.db.compaction.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

/**
 * Routes the {@link Unfiltered}s of one partition into per-time-window buckets, keyed by window
 * start in milliseconds. This is the core primitive behind TSCS T3's window-boundary splits
 * (design spec section 4): every output bucket, written to its own sstable, is fully contained in
 * one time window on the <em>write-timestamp</em> axis — the same axis T1/T2 classify sstables by.
 *
 * Routing rules (plan D2):
 * <ul>
 *   <li>a {@link Row} routes by the maximum of its primary-key liveness timestamp, row-deletion
 *       timestamp and all cell/complex-deletion timestamps;</li>
 *   <li>a {@link RangeTombstoneBoundMarker} routes by its deletion timestamp — an open/close pair
 *       shares one deletion time and therefore travels together;</li>
 *   <li>a {@link RangeTombstoneBoundaryMarker} whose close- and open-deletions fall in different
 *       windows is decomposed into its corresponding close and open bound markers, each routed
 *       separately.</li>
 * </ul>
 *
 * Each bucket preserves the original clustering order (it is a subsequence of a sorted stream).
 * The partition-level deletion and static row are NOT routed — callers replicate them into every
 * output containing this partition (plan D3), so window-level whole-sstable drops cannot lose a
 * partition deletion covering other windows.
 */
public final class WindowRoutingIterator
{
    private WindowRoutingIterator()
    {
    }

    /**
     * @param partition            the partition's unfiltereds, in clustering order (forward iteration)
     * @param windowStartOfMillis  maps an epoch-millisecond timestamp to its window start
     * @param tableResolution      the table's timestamp resolution (cell timestamps → milliseconds)
     * @return window start (ms) → that window's unfiltereds, in original clustering order
     */
    public static NavigableMap<Long, List<Unfiltered>> route(UnfilteredRowIterator partition,
                                                             LongUnaryOperator windowStartOfMillis,
                                                             TimeUnit tableResolution)
    {
        NavigableMap<Long, List<Unfiltered>> buckets = new TreeMap<>();
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (unfiltered instanceof RangeTombstoneBoundaryMarker)
            {
                RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) unfiltered;
                long closeWindow = windowStartOfMillis.applyAsLong(toMillis(boundary.closeDeletionTime(false).markedForDeleteAt(), tableResolution));
                long openWindow = windowStartOfMillis.applyAsLong(toMillis(boundary.openDeletionTime(false).markedForDeleteAt(), tableResolution));
                if (closeWindow == openWindow)
                {
                    bucket(buckets, closeWindow).add(boundary);
                }
                else
                {
                    // The closing and opening deletions live in different windows: split the boundary
                    // so each window's sstable carries a self-contained marker.
                    bucket(buckets, closeWindow).add(boundary.createCorrespondingCloseMarker(false));
                    bucket(buckets, openWindow).add(boundary.createCorrespondingOpenMarker(false));
                }
            }
            else
            {
                long windowStart = windowStartOfMillis.applyAsLong(routingMillis(unfiltered, tableResolution));
                bucket(buckets, windowStart).add(unfiltered);
            }
        }
        return buckets;
    }

    private static List<Unfiltered> bucket(NavigableMap<Long, List<Unfiltered>> buckets, long windowStart)
    {
        return buckets.computeIfAbsent(windowStart, k -> new ArrayList<>());
    }

    /** The write-timestamp (converted to epoch millis) that decides an unfiltered's window. */
    @VisibleForTesting
    static long routingMillis(Unfiltered unfiltered, TimeUnit tableResolution)
    {
        if (unfiltered instanceof RangeTombstoneBoundMarker)
            return toMillis(((RangeTombstoneBoundMarker) unfiltered).deletionTime().markedForDeleteAt(), tableResolution);

        Row row = (Row) unfiltered;
        long max = Long.MIN_VALUE;
        if (!row.primaryKeyLivenessInfo().isEmpty())
            max = Math.max(max, row.primaryKeyLivenessInfo().timestamp());
        if (!row.deletion().isLive())
            max = Math.max(max, row.deletion().time().markedForDeleteAt());
        for (ColumnData cd : row)
        {
            if (cd instanceof ComplexColumnData)
            {
                ComplexColumnData complex = (ComplexColumnData) cd;
                if (!complex.complexDeletion().isLive())
                    max = Math.max(max, complex.complexDeletion().markedForDeleteAt());
                for (Cell<?> cell : complex)
                    max = Math.max(max, cell.timestamp());
            }
            else
            {
                max = Math.max(max, ((Cell<?>) cd).timestamp());
            }
        }
        if (max == Long.MIN_VALUE || max == LivenessInfo.NO_TIMESTAMP)
            throw new IllegalStateException("Row with no usable write timestamp cannot be window-routed: " + row);
        return toMillis(max, tableResolution);
    }

    private static long toMillis(long rawTimestamp, TimeUnit tableResolution)
    {
        return TimeUnit.MILLISECONDS.convert(rawTimestamp, tableResolution);
    }
}
