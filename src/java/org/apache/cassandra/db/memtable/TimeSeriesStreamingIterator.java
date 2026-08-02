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

package org.apache.cassandra.db.memtable;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.TimeSeriesColumnarPartition.Walk;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.utils.AbstractIterator;

/**
 * The streaming read over a {@link TimeSeriesColumnarPartition}: walks the partition's column
 * arrays directly instead of materialising the partition.
 *
 * <p><b>Cost model.</b> Opening consolidates the unsorted tail and captures the array references
 * and row count under the partition lock — the only locked moment of the read. Each slice's bounds
 * are then found by binary search over the clustering array (in comparator order, so DESC tables
 * work unchanged), and one {@code BTreeRow} is assembled per <em>pulled</em> row: a sliced or
 * LIMITed read assembles only what it returns, O(log n + rows), pinned by
 * {@link TimeSeriesColumnarPartition#rowsAssembled}. When the iterator closes, nothing survives it
 * — no snapshot, no cache, no retained state of any kind.
 *
 * <p><b>What it merges, and how.</b> Exactly what the rebuilt view merges, through the same
 * machinery: array rows and the overflow map's rows two-way merged in comparator order, then handed
 * — per slice — to {@link RowAndDeletionMergeIterator} together with the deletion info's range
 * tombstones, the partition deletion, the filtered static row and the column selection, with
 * {@code removeShadowedData} true, just as the {@code ImmutableBTreePartition} path does.
 *
 * <p><b>Concurrency.</b> Slots are append-only ({@link TimeSeriesColumnarPartition}), so a captured
 * slot is either live or superseded, never torn, and rows appended after the captured count are
 * invisible — the old snapshot's semantics. Because re-writes append, one clustering can occupy a
 * run of equal-clustering slots ordered oldest to newest, of which at most one is live; the walk
 * emits one row per run. The one race left is a promotion completing mid-read: after assembling a
 * live slot's row the walk re-checks the superseded flag, and if it flipped, discards the assembly
 * and takes the row from the overflow map instead — the double-check that keeps a reader from
 * returning a row the writer just moved. Where the overflow cursor already carries the promoted
 * clustering, the two-way merge prefers the overflow side on ties, so no clustering is ever emitted
 * twice.
 */
final class TimeSeriesStreamingIterator extends AbstractUnfilteredRowIterator
{
    /** Test hook, run at streaming open: lets a test force the open to fail and prove the fallback. */
    @VisibleForTesting
    static volatile Runnable openHook;

    /**
     * Test hook, run after a slot's row is assembled and before the superseded double-check: lets a
     * test promote that very clustering in the gap and prove the double-check closes it.
     */
    @VisibleForTesting
    static volatile Consumer<Clustering<?>> assembleHook;

    private final Walk walk;
    private final OverflowCursor cursor;
    private final ColumnFilter selection;
    private final Slices slices;

    private int idx;
    private Iterator<Unfiltered> currentSlice;

    private TimeSeriesStreamingIterator(Walk walk, OverflowCursor cursor, ColumnFilter selection,
                                        Slices slices, boolean reversed, Row staticRow)
    {
        super(walk.metadata, walk.key, walk.deletionInfo.getPartitionDeletion(),
              selection.fetchedColumns(), staticRow, reversed, walk.stats);
        this.walk = walk;
        this.cursor = cursor;
        this.selection = selection;
        this.slices = slices;
    }

    /**
     * The streaming iterator over {@code walk}, shaped exactly as the rebuilt path shapes its
     * iterators: no-rows for an empty slice set, a bare per-slice merge for one slice, an outer
     * slice-walking iterator for several.
     */
    static UnfilteredRowIterator open(Walk walk, ColumnFilter selection, Slices slices, boolean reversed)
    {
        Runnable hook = openHook;
        if (hook != null)
            hook.run();

        Row staticRow = filteredStaticRow(walk, selection);
        if (slices.size() == 0)
            return UnfilteredRowIterators.noRowsIterator(walk.metadata, walk.key, staticRow,
                                                         walk.deletionInfo.getPartitionDeletion(), reversed);

        OverflowCursor cursor = new OverflowCursor(walk, reversed);
        if (slices.size() == 1)
            return sliceIterator(walk, cursor, selection, slices.get(0), reversed, staticRow);
        return new TimeSeriesStreamingIterator(walk, cursor, selection, slices, reversed, staticRow);
    }

    @Override
    protected Unfiltered computeNext()
    {
        while (true)
        {
            if (currentSlice == null)
            {
                if (idx >= slices.size())
                    return endOfData();

                int sliceIdx = isReverseOrder ? slices.size() - idx - 1 : idx;
                currentSlice = sliceIterator(walk, cursor, selection, slices.get(sliceIdx), isReverseOrder,
                                             Rows.EMPTY_STATIC_ROW);
                idx++;
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
        }
    }

    /** One slice's rows and range tombstones, merged the same way the rebuilt path merges them. */
    private static UnfilteredRowIterator sliceIterator(Walk walk, OverflowCursor cursor, ColumnFilter selection,
                                                       Slice slice, boolean reversed, Row staticRow)
    {
        return new RowAndDeletionMergeIterator(walk.metadata, walk.key, walk.deletionInfo.getPartitionDeletion(),
                                               selection, staticRow, reversed, walk.stats,
                                               new SliceRows(walk, cursor, slice, reversed),
                                               walk.deletionInfo.rangeIterator(slice, reversed),
                                               true);
    }

    /** Mirrors {@code AbstractBTreePartition#staticRow(current, columns, false)}. */
    private static Row filteredStaticRow(Walk walk, ColumnFilter selection)
    {
        if (selection.fetchedColumns().statics.isEmpty()
            || (walk.staticRow.isEmpty() && walk.deletionInfo.getPartitionDeletion().isLive()))
            return Rows.EMPTY_STATIC_ROW;

        Row row = walk.staticRow.filter(selection, walk.deletionInfo.getPartitionDeletion(), false, walk.metadata);
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    /**
     * One slice's rows in query order: the array's [from, to) range — found by binary search against
     * the slice's <em>existing</em> bounds, never fabricated ones — two-way merged with the overflow
     * cursor. Ties between an array run and an overflow entry take the overflow side: an entry can
     * only share a run's clustering when the run was promoted, and then the overflow row is the
     * truth.
     */
    private static final class SliceRows extends AbstractIterator<Row>
    {
        private final Walk walk;
        private final OverflowCursor cursor;
        private final boolean reversed;

        /** Array bounds of the slice, ascending: rows [from, to) are inside it. */
        private final int from;
        private final int to;
        /** Next slot to visit, moving up (ascending) or down (reversed). */
        private int pos;

        SliceRows(Walk walk, OverflowCursor cursor, Slice slice, boolean reversed)
        {
            this.walk = walk;
            this.cursor = cursor;
            this.reversed = reversed;
            this.from = lowerBound(walk, slice.start());
            this.to = lowerBound(walk, slice.end());
            this.pos = reversed ? to - 1 : from;
            cursor.enterSlice(slice);
        }

        /**
         * The first slot whose clustering sorts after {@code bound}. A bound never compares equal to
         * a clustering (its kind breaks value ties), so this single search form places inclusive and
         * exclusive, start and end bounds alike: a slice's rows are exactly
         * [lowerBound(start), lowerBound(end)).
         */
        private static int lowerBound(Walk walk, ClusteringBound<?> bound)
        {
            int lo = 0;
            int hi = walk.size;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (walk.comparator.compare(walk.clusteringAt(mid), bound) > 0)
                    hi = mid;
                else
                    lo = mid + 1;
            }
            return lo;
        }

        @Override
        protected Row computeNext()
        {
            while (true)
            {
                Map.Entry<Clustering<?>, Row> overflowHead = cursor.peekWithinSlice();
                boolean arrayDone = reversed ? pos < from : pos >= to;
                if (arrayDone)
                {
                    if (overflowHead == null)
                        return endOfData();
                    cursor.consume();
                    return overflowHead.getValue();
                }

                int slot = pos;
                Clustering<?> clustering = walk.clusteringAt(slot);
                if (overflowHead != null)
                {
                    int cmp = queryCompare(overflowHead.getKey(), clustering);
                    if (cmp < 0)
                    {
                        cursor.consume();
                        return overflowHead.getValue();
                    }
                    if (cmp == 0)
                    {
                        // The overflow owns this clustering (a promotion): its row is the truth, the
                        // slots that share the clustering are only its headstones — skip them all.
                        cursor.consume();
                        if (reversed)
                        {
                            while (pos >= from && walk.equalSlots(slot, pos))
                                pos--;
                        }
                        else
                        {
                            while (pos < to && walk.equalSlots(slot, pos))
                                pos++;
                        }
                        return overflowHead.getValue();
                    }
                }

                pos = reversed ? pos - 1 : pos + 1;

                // The superseded check that keeps re-written rows from coming back stale or twice:
                // a live slot is its clustering's one truth in the arrays, and a superseded slot is
                // dead weight whose replacement is a newer slot, an overflow row, or — when both are
                // missing — a slot appended after this read captured its bounds.
                if (!walk.superseded(slot))
                {
                    Row assembled = walk.assembleRow(slot);
                    Consumer<Clustering<?>> hook = assembleHook;
                    if (hook != null)
                        hook.accept(assembled.clustering());
                    if (walk.superseded(slot))
                    {
                        // Double-check: a promotion landed mid-assembly and moved the truth to the
                        // overflow map — discard the assembly and take the promoted row instead.
                        Row replacement = walk.overflowGet(clustering);
                        if (replacement != null)
                            return replacement;
                    }
                    return assembled;
                }

                // Superseded, but a newer slot of the same clustering is visible to this read: that
                // slot (or the overflow) decides the run — in append order it can only be at a
                // higher index, which an ascending walk has yet to visit and a reversed walk already
                // emitted for.
                if (slot + 1 < to && walk.equalSlots(slot, slot + 1))
                    continue;

                // The newest visible member is superseded: a promotion put the truth in the
                // overflow map, or a re-write appended it past this read's captured bounds — in
                // which case this member, live at capture, is this read's truth.
                Row promoted = walk.overflowGet(clustering);
                if (promoted != null)
                    return promoted;
                return walk.assembleRow(slot);
            }
        }

        private int queryCompare(ClusteringPrefix<?> a, ClusteringPrefix<?> b)
        {
            int cmp = walk.comparator.compare(a, b);
            return reversed ? -cmp : cmp;
        }
    }

    /**
     * A single pass over the overflow map in query order, shared by every slice of one read so no
     * entry is visited twice. Entries behind the current slice's start are consumed silently (they
     * belonged to gaps between slices); the entry beyond its end is held, unconsumed, for the next
     * slice. The map is concurrent, so no lock is ever taken; entries inserted behind the cursor's
     * fetch position after the read opened are simply not seen, which the walk's double-check covers
     * for the one case where that matters.
     */
    private static final class OverflowCursor
    {
        private final Walk walk;
        private final boolean reversed;
        private final PeekingIterator<Map.Entry<Clustering<?>, Row>> entries;

        /** The current slice's bounds in walk order: walkStart is met first, walkEnd last. */
        private ClusteringBound<?> walkStart;
        private ClusteringBound<?> walkEnd;

        OverflowCursor(Walk walk, boolean reversed)
        {
            this.walk = walk;
            this.reversed = reversed;
            this.entries = walk.overflow == null
                           ? null
                           : Iterators.peekingIterator((reversed ? walk.overflow.descendingMap().entrySet()
                                                                 : walk.overflow.entrySet()).iterator());
        }

        void enterSlice(Slice slice)
        {
            walkStart = reversed ? slice.end() : slice.start();
            walkEnd = reversed ? slice.start() : slice.end();
        }

        /** The next entry inside the current slice, or null; never consumes past the slice's end. */
        Map.Entry<Clustering<?>, Row> peekWithinSlice()
        {
            if (entries == null)
                return null;
            while (entries.hasNext())
            {
                Clustering<?> clustering = entries.peek().getKey();
                if (queryCompare(clustering, walkStart) < 0)
                {
                    entries.next();
                    continue;
                }
                return queryCompare(clustering, walkEnd) > 0 ? null : entries.peek();
            }
            return null;
        }

        void consume()
        {
            entries.next();
        }

        private int queryCompare(ClusteringPrefix<?> a, ClusteringPrefix<?> b)
        {
            int cmp = walk.comparator.compare(a, b);
            return reversed ? -cmp : cmp;
        }
    }
}
