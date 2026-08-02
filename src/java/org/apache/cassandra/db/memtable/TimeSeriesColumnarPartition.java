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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;

import org.github.jamm.Unmetered;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * One window shard's version of one partition, stored column-wise in primitive arrays instead of as
 * a tree of {@code Row} and {@code Cell} objects.
 *
 * <p><b>Why.</b> In the object representation every ingested row costs a {@code BTreeRow}, a
 * {@code Cell} per column and a {@code ByteBuffer} per value; for a typical industrial tag table
 * (a timestamp clustering plus a handful of numeric columns) that overhead is several times the
 * data itself. Here the common case — every cell of the row written at one timestamp, which is what
 * a plain {@code INSERT} produces — is stored as one slot across a set of parallel arrays: the
 * clustering as a {@code long} where the clustering is a single timestamp/bigint column, one write
 * timestamp, one TTL and one local-deletion-time for the whole row, and each column's value in a
 * primitive array of its fixed width. Objects are built only when the partition is read or flushed.
 *
 * <p><b>Two tiers.</b> Anything the arrays cannot represent falls back to plain {@code Row} objects
 * in a per-partition overflow map, <em>one row at a time</em> — never the whole partition:
 * <ul>
 *   <li>a later write that touches an existing row at a different timestamp promotes that row (the
 *       merged result no longer has one uniform timestamp);</li>
 *   <li>rows with cell tombstones, row deletions, expired liveness or non-uniform TTLs;</li>
 *   <li>rows whose clustering does not fit the primitive clustering store.</li>
 * </ul>
 * Static rows, partition deletions and range tombstones keep their existing object representation in
 * dedicated fields, exactly as the reference memtables hold them, and are merged back in on read.
 * The fallback is total: an unrepresentable shape is a missed optimisation, never a refusal.
 *
 * <p><b>Ordering.</b> Rows are appended; a sorted prefix and an unsorted tail are tracked and the
 * tail is folded in lazily, on read or flush — in-order ingest never sorts. The tail is bounded
 * ({@link #MAX_UNSORTED_TAIL}) only so the duplicate-clustering probe a write performs stays cheap.
 *
 * <p><b>Concurrency: the slots are append-only.</b> Writes synchronise on this object; reads do not.
 * A slot's row data is immutable once written: a re-write of a clustering the arrays already hold
 * appends a <em>new</em> slot with the merged row and marks the old slot {@link #FLAG_SUPERSEDED} —
 * a one-shot 0&rarr;1 flag that is never cleared — and a promotion to the overflow tier inserts into
 * the concurrent overflow map and then supersedes the slot the same way. Consolidation and growth
 * replace the arrays wholesale instead of mutating them, so a reader that captured the array
 * references and the row count under the lock (see {@link #captureWalk()}) walks a frozen prefix
 * that no later write can tear. Reads therefore <b>retain nothing</b>: there is no snapshot and no
 * cache of any kind; the streaming iterator ({@link TimeSeriesStreamingIterator}) assembles one row
 * per pull and leaves nothing behind when it closes, and the full rebuild
 * ({@link #buildSnapshot()}) — the fail-open fallback and the flush view — builds, is consumed, and
 * is discarded.
 *
 * <p><b>The degradation valve.</b> Superseded slots are dead weight until flush. A pathologically
 * re-written partition would grow its arrays without bound, so when more than half of a partition's
 * slots are superseded (and the arrays are past their initial capacity — a handful of slots cannot
 * bloat), its next write demotes <em>that partition only</em> to the object tier: the arrays
 * freeze, an {@link TimeSeriesMemtable.ObjectShardPartition} sibling takes every later write, and
 * reads and flushes merge the two — demotion in the same spirit as fail-open, never refusal.
 */
public final class TimeSeriesColumnarPartition implements TimeSeriesMemtable.ShardPartition
{
    private static final int INITIAL_CAPACITY = 8;

    /**
     * Ceiling on the unsorted tail. A write below the current maximum clustering must check whether
     * that clustering already exists (binary search of the sorted prefix plus a linear scan of the
     * tail), so an unbounded tail would make heavily out-of-order ingest quadratic. When the tail
     * exceeds this, it is folded into the prefix during the write — which is the one case the
     * "sort lazily" rule allows sorting on the write path, and amortises to O(size / limit) per row.
     */
    private static final int MAX_UNSORTED_TAIL = 1024;

    private static final byte FLAG_HAS_LIVENESS = 1;
    /**
     * The slot's row is no longer the truth: it was either replaced by a later slot holding the
     * merged row (append-only re-write) or promoted to the overflow map. One-shot — set exactly once,
     * after its replacement is in place, and never cleared — so a lock-free reader sees each slot as
     * either live or superseded, never torn. The slot keeps its clustering for search order.
     */
    private static final byte FLAG_SUPERSEDED = 2;

    /**
     * Rows assembled from array slots into {@code BTreeRow}s, across every columnar partition. The
     * streaming read path's cost model is O(log n + rows returned); this is the counter that pins it
     * — a sliced read must move it by about the slice's row count, never by the partition's.
     */
    @VisibleForTesting
    public static final LongAdder rowsAssembled = new LongAdder();

    @Unmetered
    private final TableMetadataRef metadata;
    @Unmetered
    private final MemtableAllocator allocator;
    private final DecoratedKey key;

    /** Whether the clustering is a single (non-reversed) timestamp/bigint column, stored as a bare {@code long}. */
    private final boolean longClusterings;

    // ---- row-major parallel arrays; all mutation is guarded by 'this' ----
    private int capacity;
    private int size;
    /** Rows [0, sortedCount) are in clustering order; rows [sortedCount, size) arrived out of order. */
    private int sortedCount;
    private long[] clusteringKeys;
    private Object[] clusteringObjects;
    private long[] timestamps;
    private long[] localDeletionTimes;
    private int[] ttls;
    private byte[] flags;
    /** Per-column stores, kept sorted by column so a materialized row's cells are already in order. */
    private final List<ColumnStore> stores = new ArrayList<>(4);

    /** Slots carrying {@link #FLAG_SUPERSEDED}; feeds the degradation valve. Guarded by 'this'. */
    private int supersededCount;

    /** Greatest clustering held by the arrays, so in-order ingest can append without searching. */
    private long maxClusteringKey;
    private Object maxClusteringObject;

    /** Least clustering held by the arrays, maintained on append for {@link #mayContainRowsIn}. */
    private long minClusteringKey;
    private Object minClusteringObject;

    // ---- object-tier structures ----
    /**
     * Concurrent so the streaming read path can merge it without ever taking the lock; volatile so a
     * reader that captured a {@code null} before the map existed simply misses entries added after
     * its capture, which is the same visibility rule the arrays follow.
     */
    private volatile ConcurrentSkipListMap<Clustering<?>, Row> overflow;
    private Row staticRow = Rows.EMPTY_STATIC_ROW;
    private DeletionInfo deletionInfo = DeletionInfo.LIVE;
    private RegularAndStaticColumns columns = RegularAndStaticColumns.NONE;

    /**
     * Conservative stats for the streaming iterator, merged from every applied update. Only ever
     * widens, which {@link EncodingStats}'s contract allows: stats tune the serialization deltas and
     * a loose bound costs bytes, never correctness. The rebuilt views collect exact stats instead.
     */
    private volatile EncodingStats cumulativeStats = EncodingStats.NO_STATS;

    /**
     * The degradation valve's outcome: once more than half the slots are superseded, every later
     * write lands here and reads merge this partition's frozen state with it. Set exactly once,
     * under the lock; never cleared.
     */
    private volatile TimeSeriesMemtable.ObjectShardPartition demotedTo;

    TimeSeriesColumnarPartition(TableMetadataRef metadata, DecoratedKey key, MemtableAllocator allocator)
    {
        this.metadata = metadata;
        this.key = key;
        this.allocator = allocator;
        ClusteringComparator comparator = metadata.get().comparator;
        this.longClusterings = comparator.size() == 1
                               && (comparator.subtype(0) == TimestampType.instance
                                   || comparator.subtype(0) == LongType.instance);
    }

    /**
     * The fixed serialized width this column can be stored at, or 0 for the object fallback. Identity
     * comparison deliberately excludes {@code ReversedType} wrappers and anything not on this list —
     * an unknown type must land in the fallback, never fail.
     */
    private static int fixedWidthOf(AbstractType<?> type)
    {
        if (type == DoubleType.instance || type == LongType.instance || type == TimestampType.instance)
            return 8;
        if (type == FloatType.instance || type == Int32Type.instance || type == SimpleDateType.instance)
            return 4;
        if (type == BooleanType.instance)
            return 1;
        return 0;
    }

    // ------------------------------------------------------------------------------------- writes

    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, Cloner cloner, OpOrder.Group opGroup, AtomicLong liveDataSize)
    {
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        if (demoted == null)
        {
            Put put = new Put(cloner, indexer);
            synchronized (this)
            {
                demoted = demotedTo;
                if (demoted == null && size >= INITIAL_CAPACITY && supersededCount * 2 > size)
                {
                    // The degradation valve: over half the slots are dead weight, so this partition's
                    // re-write pattern defeats the arrays. Freeze them and give every later write to
                    // an object-tier sibling — including this one, applied below outside the lock.
                    // The floor exists because the valve is about array BLOAT: a partition of a few
                    // slots cannot bloat, and demoting it on its first promotion (1 of 1 slots
                    // superseded is over half) would throw healthy partitions to the object tier.
                    demoted = new TimeSeriesMemtable.ObjectShardPartition(metadata, key, allocator);
                    demotedTo = demoted;
                }
                if (demoted == null)
                {
                    cumulativeStats = cumulativeStats.mergeWith(update.stats());
                    mergeDeletionInfo(update.deletionInfo(), put);

                    RegularAndStaticColumns merged = update.columns().mergeTo(columns);
                    put.heapDelta += merged.unsharedHeapSize() - columns.unsharedHeapSize();
                    columns = merged;

                    Row updateStatic = update.staticRow();
                    if (!updateStatic.isEmpty())
                        mergeStatic(updateStatic, put);

                    for (Row row : update)
                    {
                        if (!row.isEmpty())
                            apply(row, put);
                    }
                }
            }
            if (demoted == null)
            {
                // Outside the lock: acquiring memtable space may block on a flush, and the flush path
                // takes this partition's lock to build its view.
                allocator.onHeap().adjust(put.heapDelta, opGroup);
                liveDataSize.addAndGet(put.dataDelta);
                return put.colUpdateTimeDelta;
            }
        }
        // Demoted: the object tier owns every write from here on. Also outside the lock, for the
        // same allocation-under-lock reason as above.
        return demoted.put(update, indexer, cloner, opGroup, liveDataSize);
    }

    /** Mirrors {@code BTreePartitionUpdater#merge(DeletionInfo, DeletionInfo)}, including the indexer events. */
    private void mergeDeletionInfo(DeletionInfo update, Put put)
    {
        if (update.isLive() || !update.mayModify(deletionInfo))
            return;

        if (!update.getPartitionDeletion().isLive())
            put.indexer.onPartitionDeletion(update.getPartitionDeletion());
        if (update.hasRanges())
            update.rangeIterator(false).forEachRemaining(put.indexer::onRangeTombstone);

        DeletionInfo merged = deletionInfo.mutableCopy().add(update.clone(HeapCloner.instance));
        put.heapDelta += merged.unsharedHeapSize() - deletionInfo.unsharedHeapSize();
        deletionInfo = merged;
    }

    private void mergeStatic(Row update, Put put)
    {
        if (staticRow.isEmpty())
        {
            Row cloned = update.clone(put.cloner);
            put.indexer.onInserted(update);
            put.heapDelta += cloned.unsharedHeapSizeExcludingData();
            put.dataDelta += cloned.dataSize();
            staticRow = cloned;
        }
        else
        {
            Row merged = Rows.merge(staticRow, update, put);
            put.indexer.onUpdated(staticRow, merged);
            Row cloned = merged.clone(put.cloner);
            put.heapDelta += cloned.unsharedHeapSizeExcludingData() - staticRow.unsharedHeapSizeExcludingData();
            put.dataDelta += cloned.dataSize() - staticRow.dataSize();
            staticRow = cloned;
        }
    }

    private void apply(Row row, Put put)
    {
        Clustering<?> clustering = row.clustering();

        // A clustering that was promoted (or never fit the arrays) lives in the overflow map, and the
        // overflow owns it exclusively, so this lookup must come before any array search or append.
        Row inOverflow = overflow == null ? null : overflow.get(clustering);
        if (inOverflow != null)
        {
            Row merged = Rows.merge(inOverflow, row, put);
            put.indexer.onUpdated(inOverflow, merged);
            Row cloned = merged.clone(put.cloner);
            put.dataDelta += cloned.dataSize() - inOverflow.dataSize();
            putOverflow(cloned, inOverflow, put);
            return;
        }

        RowShape shape = classify(row);

        long longKey = 0;
        boolean representable = !longClusterings;
        if (longClusterings)
        {
            ByteBuffer raw = clustering.bufferAt(0);
            representable = raw != null && raw.remaining() == 8;
            if (representable)
                longKey = raw.getLong(raw.position());
        }
        boolean arrayable = shape != null && representable;

        // In-order fast path: strictly above everything the arrays hold, and nothing in overflow that
        // could collide, means the clustering is certainly new — append without any lookup.
        boolean overflowEmpty = overflow == null || overflow.isEmpty();
        if (arrayable && overflowEmpty && (size == 0 || compareToMax(clustering, longKey) > 0))
        {
            append(row, shape, longKey, null, put);
            put.indexer.onInserted(row);
            put.dataDelta += row.dataSize();
            return;
        }

        int slot = representable ? findSlot(clustering, longKey) : -1;
        if (slot >= 0)
        {
            replaceSlot(slot, row, put);
            return;
        }

        if (arrayable)
        {
            append(row, shape, longKey, null, put);
            put.indexer.onInserted(row);
            put.dataDelta += row.dataSize();
        }
        else
        {
            Row cloned = row.clone(put.cloner);
            put.indexer.onInserted(row);
            put.dataDelta += cloned.dataSize();
            putOverflow(cloned, null, put);
        }
    }

    /**
     * The row's fast-path parameters, or {@code null} if the row needs the object tier: a row
     * deletion, a cell tombstone, an expired liveness, a cell whose value does not fit its column's
     * fixed width, a complex column, or cells that do not all share one (timestamp, ttl,
     * localDeletionTime).
     */
    private RowShape classify(Row row)
    {
        if (!row.deletion().isLive())
            return null;

        long ts = 0;
        int ttl = 0;
        long ldt = 0;
        boolean seen = false;
        boolean hasLiveness = false;

        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        if (!liveness.isEmpty())
        {
            if (liveness.ttl() == LivenessInfo.EXPIRED_LIVENESS_TTL)
                return null;
            hasLiveness = true;
            seen = true;
            ts = liveness.timestamp();
            ttl = liveness.ttl();
            ldt = liveness.localExpirationTime();
        }

        for (ColumnData data : row)
        {
            if (!data.column().isSimple())
                return null;
            Cell<?> cell = (Cell<?>) data;
            if (cell.isTombstone())
                return null;

            int width = fixedWidthOf(cell.column().type);
            if (width > 0)
            {
                ByteBuffer value = cell.buffer();
                if (value == null || value.remaining() != width)
                    return null;
            }

            if (!seen)
            {
                seen = true;
                ts = cell.timestamp();
                ttl = cell.ttl();
                ldt = cell.localDeletionTime();
            }
            else if (cell.timestamp() != ts || cell.ttl() != ttl || cell.localDeletionTime() != ldt)
            {
                return null;
            }
        }

        return seen ? new RowShape(ts, ttl, ldt, hasLiveness) : null;
    }

    /**
     * Appends {@code row} as a new slot. {@code reusedClustering} is non-null only for an append-only
     * re-write, whose clustering is already stored (and owned) by the superseded slot — sharing the
     * reference avoids a clone and, on a native allocator, avoids touching the value factory at all.
     */
    private void append(Row row, RowShape shape, long longKey, Clustering<?> reusedClustering, Put put)
    {
        ensureCapacity(put);
        int slot = size;

        Clustering<?> storedClustering = null;
        if (longClusterings)
        {
            clusteringKeys[slot] = longKey;
        }
        else if (reusedClustering != null)
        {
            storedClustering = reusedClustering;
            clusteringObjects[slot] = storedClustering;
        }
        else
        {
            storedClustering = put.cloner.clone(row.clustering());
            clusteringObjects[slot] = storedClustering;
            put.heapDelta += storedClustering.unsharedHeapSize();
        }
        timestamps[slot] = shape.timestamp;
        localDeletionTimes[slot] = shape.localDeletionTime;
        ttls[slot] = shape.ttl;
        flags[slot] = shape.hasLiveness ? FLAG_HAS_LIVENESS : 0;

        for (ColumnData data : row)
        {
            Cell<?> cell = (Cell<?>) data;
            storeFor(cell.column(), put).set(slot, cell, put);
        }

        // Non-strict comparison: an append-only re-write legitimately appends a clustering equal to
        // the previous slot's, and a non-decreasing prefix keeps every equal-clustering run in append
        // order — which is what lets a reader treat the run's last member as its newest.
        boolean inOrder = sortedCount == slot
                          && (slot == 0 || compareSlotTo(slot - 1, row.clustering(), longKey) <= 0);
        size++;
        if (inOrder)
            sortedCount = size;
        else if (size - sortedCount > MAX_UNSORTED_TAIL)
            consolidate();

        if (size == 1 || compareToMax(row.clustering(), longKey) > 0)
        {
            maxClusteringKey = longKey;
            maxClusteringObject = storedClustering;
        }
        if (size == 1 || compareToMin(row.clustering(), longKey) < 0)
        {
            minClusteringKey = longKey;
            minClusteringObject = storedClustering;
        }
    }

    /**
     * A write to a clustering the arrays already hold: materialize, reconcile through the standard
     * {@link Rows#merge} machinery, then replace <b>append-only</b> — the merged row goes to a new
     * slot (still one uniform timestamp) or to the overflow map (the design's cell slow path), and
     * only then is the old slot marked superseded. Slot data is never modified in place, which is
     * what lets readers walk the arrays without a lock.
     */
    private void replaceSlot(int slot, Row update, Put put)
    {
        Row existing = materializeRow(slot);
        Row merged = Rows.merge(existing, update, put);
        put.indexer.onUpdated(existing, merged);
        put.dataDelta += merged.dataSize() - existing.dataSize();

        RowShape shape = classify(merged);
        if (shape != null)
        {
            long longKey = longClusterings ? clusteringKeys[slot] : 0L;
            Clustering<?> reused = longClusterings ? null : (Clustering<?>) clusteringObjects[slot];
            append(merged, shape, longKey, reused, put);
        }
        else
        {
            Row cloned = merged.clone(put.cloner);
            putOverflow(cloned, null, put);
        }
        // Strictly after the replacement is in place, so no reader can ever observe the flag set
        // while the truth is nowhere.
        setSuperseded(slot);
    }

    /** One-shot: 0&rarr;1 only, never cleared. */
    private void setSuperseded(int slot)
    {
        if ((flags[slot] & FLAG_SUPERSEDED) == 0)
        {
            flags[slot] |= FLAG_SUPERSEDED;
            supersededCount++;
        }
    }

    private void putOverflow(Row cloned, Row replaced, Put put)
    {
        if (overflow == null)
            overflow = new ConcurrentSkipListMap<>(metadata.get().comparator);
        overflow.put(cloned.clustering(), cloned);
        put.heapDelta += cloned.unsharedHeapSizeExcludingData()
                         - (replaced == null ? 0 : replaced.unsharedHeapSizeExcludingData());
    }

    private ColumnStore storeFor(ColumnMetadata column, Put put)
    {
        int lo = 0;
        int hi = stores.size() - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cmp = stores.get(mid).column.compareTo(column);
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
                return stores.get(mid);
        }
        ColumnStore created = new ColumnStore(column, Math.max(capacity, INITIAL_CAPACITY));
        stores.add(lo, created);
        put.heapDelta += created.heapSize();
        return created;
    }

    private void ensureCapacity(Put put)
    {
        if (size < capacity)
            return;

        int newCapacity = Math.max(INITIAL_CAPACITY, capacity * 2);
        if (longClusterings)
            clusteringKeys = clusteringKeys == null ? new long[newCapacity] : Arrays.copyOf(clusteringKeys, newCapacity);
        else
            clusteringObjects = clusteringObjects == null ? new Object[newCapacity] : Arrays.copyOf(clusteringObjects, newCapacity);
        timestamps = timestamps == null ? new long[newCapacity] : Arrays.copyOf(timestamps, newCapacity);
        localDeletionTimes = localDeletionTimes == null ? new long[newCapacity] : Arrays.copyOf(localDeletionTimes, newCapacity);
        ttls = ttls == null ? new int[newCapacity] : Arrays.copyOf(ttls, newCapacity);
        flags = flags == null ? new byte[newCapacity] : Arrays.copyOf(flags, newCapacity);
        for (ColumnStore store : stores)
            store.grow(newCapacity);

        // 8 (clustering or reference) + 8 (timestamp) + 8 (ldt) + 4 (ttl) + 1 (flags) per new slot,
        // plus what each store reports for itself; array headers are noise at these growth steps.
        put.heapDelta += (newCapacity - capacity) * 29L;
        for (ColumnStore store : stores)
            put.heapDelta += (newCapacity - capacity) * (long) (store.width == 0 ? 9 : store.width + 1);
        capacity = newCapacity;
    }

    // ------------------------------------------------------------------------------------ ordering

    private int compareToMax(Clustering<?> clustering, long longKey)
    {
        return longClusterings
               ? Long.compare(longKey, maxClusteringKey)
               : metadata.get().comparator.compare(clustering, (Clustering<?>) maxClusteringObject);
    }

    private int compareToMin(Clustering<?> clustering, long longKey)
    {
        return longClusterings
               ? Long.compare(longKey, minClusteringKey)
               : metadata.get().comparator.compare(clustering, (Clustering<?>) minClusteringObject);
    }

    private int compareSlotTo(int slot, Clustering<?> clustering, long longKey)
    {
        return longClusterings
               ? Long.compare(clusteringKeys[slot], longKey)
               : metadata.get().comparator.compare((Clustering<?>) clusteringObjects[slot], clustering);
    }

    private int compareSlots(int a, int b)
    {
        return longClusterings
               ? Long.compare(clusteringKeys[a], clusteringKeys[b])
               : metadata.get().comparator.compare((Clustering<?>) clusteringObjects[a], (Clustering<?>) clusteringObjects[b]);
    }

    /**
     * @return the <b>live</b> slot holding {@code clustering}, or -1. Since re-writes append, a
     * clustering can occupy a run of slots of which at most one is live — the search must never hand
     * a superseded slot back to the write path, or a merge would resurrect stale data.
     */
    private int findSlot(Clustering<?> clustering, long longKey)
    {
        int lo = 0;
        int hi = sortedCount - 1;
        int hit = -1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cmp = compareSlotTo(mid, clustering, longKey);
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
            {
                hit = mid;
                break;
            }
        }
        if (hit >= 0)
        {
            for (int i = hit; i >= 0 && compareSlotTo(i, clustering, longKey) == 0; i--)
            {
                if ((flags[i] & FLAG_SUPERSEDED) == 0)
                    return i;
            }
            for (int i = hit + 1; i < sortedCount && compareSlotTo(i, clustering, longKey) == 0; i++)
            {
                if ((flags[i] & FLAG_SUPERSEDED) == 0)
                    return i;
            }
        }
        for (int i = sortedCount; i < size; i++)
        {
            if (compareSlotTo(i, clustering, longKey) == 0 && (flags[i] & FLAG_SUPERSEDED) == 0)
                return i;
        }
        return -1;
    }

    /**
     * Folds the unsorted tail into the sorted prefix. Never called by in-order ingest. Builds new
     * arrays rather than permuting in place: a reader that captured the old arrays keeps walking an
     * unchanging view.
     */
    private void consolidate()
    {
        if (sortedCount == size)
            return;

        int tailLength = size - sortedCount;
        Integer[] tail = new Integer[tailLength];
        for (int i = 0; i < tailLength; i++)
            tail[i] = sortedCount + i;
        // The sort is stable and the tie below prefers the prefix, so an equal-clustering run stays
        // in append order — oldest first, newest (the live or last-superseded one) last.
        Arrays.sort(tail, this::compareSlots);

        int[] permutation = new int[size];
        int prefix = 0;
        int t = 0;
        int out = 0;
        while (prefix < sortedCount && t < tailLength)
            permutation[out++] = compareSlots(prefix, tail[t]) <= 0 ? prefix++ : tail[t++];
        while (prefix < sortedCount)
            permutation[out++] = prefix++;
        while (t < tailLength)
            permutation[out++] = tail[t++];

        if (longClusterings)
        {
            long[] next = new long[capacity];
            for (int i = 0; i < size; i++)
                next[i] = clusteringKeys[permutation[i]];
            clusteringKeys = next;
        }
        else
        {
            Object[] next = new Object[capacity];
            for (int i = 0; i < size; i++)
                next[i] = clusteringObjects[permutation[i]];
            clusteringObjects = next;
        }
        long[] nextTimestamps = new long[capacity];
        long[] nextLdts = new long[capacity];
        int[] nextTtls = new int[capacity];
        byte[] nextFlags = new byte[capacity];
        for (int i = 0; i < size; i++)
        {
            nextTimestamps[i] = timestamps[permutation[i]];
            nextLdts[i] = localDeletionTimes[permutation[i]];
            nextTtls[i] = ttls[permutation[i]];
            nextFlags[i] = flags[permutation[i]];
        }
        timestamps = nextTimestamps;
        localDeletionTimes = nextLdts;
        ttls = nextTtls;
        flags = nextFlags;
        for (ColumnStore store : stores)
            store.applyPermutation(permutation, size, capacity);

        sortedCount = size;
    }

    // ------------------------------------------------------------------------------------- reads

    /** Rebuilds one slot as a real {@code BTreeRow}. Caller must hold the lock. */
    private Row materializeRow(int slot)
    {
        return currentWalk().assembleRow(slot);
    }

    private Clustering<?> clusteringAt(int slot)
    {
        return longClusterings
               ? Clustering.make(ByteBufferUtil.bytes(clusteringKeys[slot]))
               : (Clustering<?>) clusteringObjects[slot];
    }

    /** The arrays' current state as a walkable capture. Caller must hold the lock. */
    private Walk currentWalk()
    {
        ColumnWalk[] captured = new ColumnWalk[stores.size()];
        for (int i = 0; i < stores.size(); i++)
            captured[i] = stores.get(i).data;
        return new Walk(this, size, clusteringKeys, clusteringObjects,
                        timestamps, localDeletionTimes, ttls, flags, captured, overflow, deletionInfo,
                        staticRow, cumulativeStats);
    }

    /**
     * The overflow map as it is <em>now</em>, not as it was captured: the streaming double-check
     * must find a promotion that happened after the read opened — including the very first
     * promotion of the partition, which is what creates the map.
     */
    ConcurrentSkipListMap<Clustering<?>, Row> liveOverflow()
    {
        return overflow;
    }

    /**
     * What the streaming read path opens on: folds the unsorted tail (the one moment a read takes
     * the lock) and captures the array references and the row count. Everything captured is frozen —
     * later writes append past the captured count or replace the arrays wholesale — except the flag
     * bytes, whose only possible change is the one-shot supersede the reader double-checks for.
     */
    synchronized Walk captureWalk()
    {
        return captureWalkLocked();
    }

    /**
     * The full rebuild: every live slot plus the overflow, merged in comparator order. The read
     * path's fail-open fallback and the flush view — built on demand, consumed, discarded; never
     * retained anywhere. Caller must hold the lock.
     */
    private Snapshot buildSnapshot()
    {
        Walk walk = captureWalkLocked();
        TableMetadata tableMetadata = metadata.get();
        ClusteringComparator comparator = tableMetadata.comparator;

        try (BTree.FastBuilder<Row> builder = BTree.fastBuilder())
        {
            Iterator<Row> overflowRows = overflow == null
                                         ? Collections.emptyIterator()
                                         : overflow.values().iterator();
            Row pendingOverflow = overflowRows.hasNext() ? overflowRows.next() : null;

            for (int i = 0; i < walk.size; i++)
            {
                if (walk.superseded(i))
                    continue;
                Row row = walk.assembleRow(i);
                while (pendingOverflow != null && comparator.compare(pendingOverflow.clustering(), row.clustering()) < 0)
                {
                    builder.add(pendingOverflow);
                    pendingOverflow = overflowRows.hasNext() ? overflowRows.next() : null;
                }
                builder.add(row);
            }
            while (pendingOverflow != null)
            {
                builder.add(pendingOverflow);
                pendingOverflow = overflowRows.hasNext() ? overflowRows.next() : null;
            }

            Object[] tree = builder.build();
            EncodingStats stats = EncodingStats.Collector.collect(staticRow, BTree.iterator(tree), deletionInfo);
            return new Snapshot(tableMetadata, key, columns, staticRow, tree, deletionInfo, stats);
        }
    }

    /** {@link #captureWalk()} for callers already holding the lock. */
    private Walk captureWalkLocked()
    {
        consolidate();
        return currentWalk();
    }

    /**
     * A freshly built, immediately discarded object view for the read paths the streaming iterator
     * does not serve (point {@code getRow}, names filters, the no-argument iterator) and for the
     * streaming path's fail-open fallback. When the partition is demoted, the frozen columnar state
     * and the object-tier sibling are presented as one merged partition.
     */
    private Partition readView()
    {
        Partition base;
        synchronized (this)
        {
            base = buildSnapshot();
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted == null
               ? base
               : new TimeSeriesMemtable.MergedWindowPartition(metadata.get(), key, Arrays.<Partition>asList(base, demoted));
    }

    @Override
    public Partition flushView()
    {
        // Deliberately built afresh and never retained: the flush set walks every partition, and
        // pinning each one's materialized object form until the memtable is discarded would recreate
        // at flush time the very heap footprint the arrays exist to avoid.
        return readView();
    }

    // ------------------------------------------------------------------- Partition implementation

    @Override
    public TableMetadata metadata()
    {
        return metadata.get();
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return allocator.ensureOnHeap().applyToPartitionKey(key);
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        DeletionTime own;
        synchronized (this)
        {
            own = deletionInfo.getPartitionDeletion();
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        if (demoted != null)
        {
            DeletionTime theirs = demoted.partitionLevelDeletion();
            if (theirs.supersedes(own))
                return theirs;
        }
        return own;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        RegularAndStaticColumns own;
        synchronized (this)
        {
            own = columns;
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted == null ? own : own.mergeTo(demoted.columns());
    }

    @Override
    public EncodingStats stats()
    {
        EncodingStats own = cumulativeStats;
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted == null ? own : own.mergeWith(demoted.stats());
    }

    @Override
    public boolean isEmpty()
    {
        synchronized (this)
        {
            if (!deletionInfo.isLive() || !staticRow.isEmpty() || size > supersededCount
                || (overflow != null && !overflow.isEmpty()))
                return false;
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted == null || demoted.isEmpty();
    }

    @Override
    public boolean hasRows()
    {
        synchronized (this)
        {
            if (size > supersededCount || (overflow != null && !overflow.isEmpty()))
                return true;
        }
        TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
        return demoted != null && demoted.hasRows();
    }

    @Override
    public Row getRow(Clustering<?> clustering)
    {
        return allocator.ensureOnHeap().applyToRow(readView().getRow(clustering));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return allocator.ensureOnHeap().applyToPartition(readView().unfilteredIterator());
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(streamOrRebuild(selection, slices, reversed));
    }

    /**
     * The hot read path: stream straight off the arrays — O(log n) to find the slice bounds, one
     * {@code BTreeRow} assembled per pulled row, nothing retained when the iterator closes. Fails
     * open on the same principle as shard pruning ({@link TimeSeriesMemtable#pruneFailedOpen}): any
     * state or shape the streaming open cannot reason about — a demoted partition, a clustering-less
     * table, or any unexpected exception — falls back to the full rebuild, because a missed
     * optimisation is a slow read and an exception here is an outage.
     */
    private UnfilteredRowIterator streamOrRebuild(ColumnFilter selection, Slices slices, boolean reversed)
    {
        try
        {
            if (demotedTo == null && metadata.get().comparator.size() > 0)
                return TimeSeriesStreamingIterator.open(captureWalk(), selection, slices, reversed);
        }
        catch (RuntimeException e)
        {
            TimeSeriesMemtable.streamFailedOpen(e);
        }
        return readView().unfilteredIterator(selection, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(readView().unfilteredIterator(selection, clusteringsInQueryOrder, reversed));
    }

    /**
     * Whether a sliced read of this shard partition can return anything, answered from the
     * <b>clustering</b> values actually held — the arrays' maintained min/max plus the overflow
     * map's ends — and never from the shard's write-time window, because clustering time and write
     * time are independent. Fails open: any partition or range deletion, any static row, an empty
     * partition, a clustering-less table, and any state or slices shape the probe cannot handle all
     * answer {@code true} — pruning is an optimisation, and throwing here is a read outage.
     * Deliberately materialises nothing and never calls {@code Slice.make} on min/max: they are
     * memtable-owned clusterings, and fabricating bounds from them needs the value accessor's object
     * factory, which the native (offheap_objects) accessor refuses with
     * {@code UnsupportedOperationException} — the 2026-08-02 paged-read outage, hit on every page
     * after the first because only page one carries {@code Slices.ALL}.
     */
    @Override
    public synchronized boolean mayContainRowsIn(Slices slices)
    {
        try
        {
            TimeSeriesMemtable.ObjectShardPartition demoted = demotedTo;
            if (demoted != null && demoted.mayContainRowsIn(slices))
                return true;

            if (!deletionInfo.isLive() || !staticRow.isEmpty())
                return true;

            ClusteringComparator comparator = metadata.get().comparator;
            if (comparator.size() == 0)
                return true;

            Clustering<?> min = null;
            Clustering<?> max = null;
            if (size > 0)
            {
                min = longClusterings ? Clustering.make(ByteBufferUtil.bytes(minClusteringKey))
                                      : (Clustering<?>) minClusteringObject;
                max = longClusterings ? Clustering.make(ByteBufferUtil.bytes(maxClusteringKey))
                                      : (Clustering<?>) maxClusteringObject;
            }
            if (overflow != null && !overflow.isEmpty())
            {
                // Superseded slots keep their clustering in the arrays, so the array bounds already
                // cover them; this widens for the rows the arrays never fit.
                Clustering<?> first = overflow.firstKey();
                Clustering<?> last = overflow.lastKey();
                min = min == null || comparator.compare(first, min) < 0 ? first : min;
                max = max == null || comparator.compare(last, max) > 0 ? last : max;
            }
            return min == null || TimeSeriesMemtable.intersectsBounds(slices, comparator, min, max);
        }
        catch (RuntimeException e)
        {
            return TimeSeriesMemtable.pruneFailedOpen(e);
        }
    }

    // ------------------------------------------------------------------------------ observability

    /** Rows currently live in the primitive arrays (the fast path): slots not yet superseded. */
    @VisibleForTesting
    public synchronized int fastPathRowCount()
    {
        return size - supersededCount;
    }

    /** Rows in the object-tier overflow map — promoted rows plus rows the arrays never fit. */
    @VisibleForTesting
    public synchronized int overflowRowCount()
    {
        return overflow == null ? 0 : overflow.size();
    }

    /** Slots dead-weighted by an append-only re-write or a promotion; feeds the degradation valve. */
    @VisibleForTesting
    public synchronized int supersededSlotCount()
    {
        return supersededCount;
    }

    /** Whether the degradation valve moved this partition's future writes to the object tier. */
    @VisibleForTesting
    public boolean isDemoted()
    {
        return demotedTo != null;
    }

    /** Whether the clustering column is held as a bare {@code long} per row. */
    @VisibleForTesting
    public boolean usesLongClusterings()
    {
        return longClusterings;
    }

    // ------------------------------------------------------------------------------ inner classes

    private static final class RowShape
    {
        final long timestamp;
        final int ttl;
        final long localDeletionTime;
        final boolean hasLiveness;

        RowShape(long timestamp, int ttl, long localDeletionTime, boolean hasLiveness)
        {
            this.timestamp = timestamp;
            this.ttl = ttl;
            this.localDeletionTime = localDeletionTime;
            this.hasLiveness = hasLiveness;
        }
    }

    /**
     * One column's values. Fixed-width types (double, bigint, timestamp / float, int, date /
     * boolean) keep the serialized bit pattern in a primitive array; everything else keeps the
     * memtable-cloned {@code Cell} in an object array — total fallback, so no type can fail.
     *
     * <p>The arrays themselves live in a {@link ColumnWalk}, replaced wholesale on growth and
     * consolidation so a reader's captured reference is never mutated under it.
     */
    private static final class ColumnStore
    {
        final ColumnMetadata column;
        final int width;
        ColumnWalk data;

        ColumnStore(ColumnMetadata column, int capacity)
        {
            this.column = column;
            this.width = fixedWidthOf(column.type);
            this.data = new ColumnWalk(column, width, capacity);
        }

        long heapSize()
        {
            return data.present.length * (long) (width == 0 ? 9 : width + 1) + 32;
        }

        void grow(int newCapacity)
        {
            data = data.copyOf(newCapacity);
        }

        void applyPermutation(int[] permutation, int count, int capacity)
        {
            data = data.permuted(permutation, count, capacity);
        }

        void set(int slot, Cell<?> cell, Put put)
        {
            switch (width)
            {
                case 8:
                {
                    ByteBuffer value = cell.buffer();
                    data.longs[slot] = value.getLong(value.position());
                    break;
                }
                case 4:
                {
                    ByteBuffer value = cell.buffer();
                    data.ints[slot] = value.getInt(value.position());
                    break;
                }
                case 1:
                {
                    ByteBuffer value = cell.buffer();
                    data.bytes[slot] = value.get(value.position());
                    break;
                }
                default:
                {
                    Object previous = data.cells[slot];
                    if (previous != cell)
                    {
                        Cell<?> cloned = put.cloner.clone(cell);
                        put.heapDelta += cloned.unsharedHeapSizeExcludingData()
                                         - (previous == null ? 0 : ((Cell<?>) previous).unsharedHeapSizeExcludingData());
                        data.cells[slot] = cloned;
                    }
                    break;
                }
            }
            data.present[slot] = 1;
        }
    }

    /**
     * One column's value arrays, immutable from a reader's point of view: writes touch only slots
     * beyond any captured row count, and structural changes swap in a new instance.
     */
    static final class ColumnWalk
    {
        final ColumnMetadata column;
        final int width;
        final byte[] present;
        final long[] longs;
        final int[] ints;
        final byte[] bytes;
        final Object[] cells;

        ColumnWalk(ColumnMetadata column, int width, int capacity)
        {
            this.column = column;
            this.width = width;
            this.present = new byte[capacity];
            this.longs = width == 8 ? new long[capacity] : null;
            this.ints = width == 4 ? new int[capacity] : null;
            this.bytes = width == 1 ? new byte[capacity] : null;
            this.cells = width == 0 ? new Object[capacity] : null;
        }

        private ColumnWalk(ColumnMetadata column, int width, byte[] present, long[] longs, int[] ints, byte[] bytes, Object[] cells)
        {
            this.column = column;
            this.width = width;
            this.present = present;
            this.longs = longs;
            this.ints = ints;
            this.bytes = bytes;
            this.cells = cells;
        }

        ColumnWalk copyOf(int newCapacity)
        {
            return new ColumnWalk(column, width,
                                  Arrays.copyOf(present, newCapacity),
                                  longs == null ? null : Arrays.copyOf(longs, newCapacity),
                                  ints == null ? null : Arrays.copyOf(ints, newCapacity),
                                  bytes == null ? null : Arrays.copyOf(bytes, newCapacity),
                                  cells == null ? null : Arrays.copyOf(cells, newCapacity));
        }

        ColumnWalk permuted(int[] permutation, int count, int capacity)
        {
            ColumnWalk next = new ColumnWalk(column, width, capacity);
            for (int i = 0; i < count; i++)
            {
                next.present[i] = present[permutation[i]];
                if (longs != null)
                    next.longs[i] = longs[permutation[i]];
                if (ints != null)
                    next.ints[i] = ints[permutation[i]];
                if (bytes != null)
                    next.bytes[i] = bytes[permutation[i]];
                if (cells != null)
                    next.cells[i] = cells[permutation[i]];
            }
            return next;
        }

        Cell<?> cellAt(int slot, long timestamp, int ttl, long localDeletionTime)
        {
            switch (width)
            {
                case 8:
                {
                    ByteBuffer value = ByteBuffer.allocate(8);
                    value.putLong(0, longs[slot]);
                    return new BufferCell(column, timestamp, ttl, localDeletionTime, value, null);
                }
                case 4:
                {
                    ByteBuffer value = ByteBuffer.allocate(4);
                    value.putInt(0, ints[slot]);
                    return new BufferCell(column, timestamp, ttl, localDeletionTime, value, null);
                }
                case 1:
                    return new BufferCell(column, timestamp, ttl, localDeletionTime,
                                          ByteBuffer.wrap(new byte[]{ bytes[slot] }), null);
                default:
                    return (Cell<?>) cells[slot];
            }
        }
    }

    /**
     * Everything a reader needs to walk the partition without the lock, captured in one place under
     * it. The captured arrays are never mutated at any slot below {@link #size}: appends land beyond
     * it and structural changes replace the arrays. The single exception is {@link #flags}, whose
     * one-shot supersede bit may flip under the reader — which is exactly what the streaming
     * iterator's double-check reads.
     */
    static final class Walk
    {
        final TableMetadata metadata;
        final ClusteringComparator comparator;
        final DecoratedKey key;
        final int size;
        final boolean longClusterings;
        final long[] clusteringKeys;
        final Object[] clusteringObjects;
        final long[] timestamps;
        final long[] localDeletionTimes;
        final int[] ttls;
        final byte[] flags;
        final ColumnWalk[] stores;
        /** The overflow map as captured at open — what the merge cursor iterates. May be null. */
        final ConcurrentSkipListMap<Clustering<?>, Row> overflow;
        final DeletionInfo deletionInfo;
        final Row staticRow;
        final EncodingStats stats;
        private final TimeSeriesColumnarPartition owner;

        Walk(TimeSeriesColumnarPartition owner, int size,
             long[] clusteringKeys, Object[] clusteringObjects, long[] timestamps,
             long[] localDeletionTimes, int[] ttls, byte[] flags, ColumnWalk[] stores,
             ConcurrentSkipListMap<Clustering<?>, Row> overflow, DeletionInfo deletionInfo,
             Row staticRow, EncodingStats stats)
        {
            this.owner = owner;
            this.metadata = owner.metadata.get();
            this.comparator = metadata.comparator;
            this.key = owner.key;
            this.size = size;
            this.longClusterings = owner.longClusterings;
            this.clusteringKeys = clusteringKeys;
            this.clusteringObjects = clusteringObjects;
            this.timestamps = timestamps;
            this.localDeletionTimes = localDeletionTimes;
            this.ttls = ttls;
            this.flags = flags;
            this.stores = stores;
            this.overflow = overflow;
            this.deletionInfo = deletionInfo;
            this.staticRow = staticRow;
            this.stats = stats;
        }

        Clustering<?> clusteringAt(int slot)
        {
            return longClusterings
                   ? Clustering.make(ByteBufferUtil.bytes(clusteringKeys[slot]))
                   : (Clustering<?>) clusteringObjects[slot];
        }

        boolean equalSlots(int a, int b)
        {
            return longClusterings
                   ? clusteringKeys[a] == clusteringKeys[b]
                   : comparator.compare((Clustering<?>) clusteringObjects[a], (Clustering<?>) clusteringObjects[b]) == 0;
        }

        boolean superseded(int slot)
        {
            return (flags[slot] & FLAG_SUPERSEDED) != 0;
        }

        /**
         * A point lookup against the <b>live</b> overflow map, not the captured one: this is the
         * double-check's read, and the promotion it exists to catch may be the partition's first —
         * the one that creates the map after this walk captured {@code null}. No duplicate can
         * result: an entry found here but missed by the capture-time cursor was inserted behind
         * that cursor's fetch position, which the skip-list iterator will never revisit.
         */
        Row overflowGet(Clustering<?> clustering)
        {
            ConcurrentSkipListMap<Clustering<?>, Row> map = owner.liveOverflow();
            return map == null ? null : map.get(clustering);
        }

        /** One slot rebuilt as a real {@code BTreeRow} — the per-pull allocation of the streaming read. */
        Row assembleRow(int slot)
        {
            rowsAssembled.increment();
            long ts = timestamps[slot];
            int ttl = ttls[slot];
            long ldt = localDeletionTimes[slot];

            List<ColumnData> cellData = null;
            for (int s = 0; s < stores.length; s++)
            {
                ColumnWalk store = stores[s];
                if (store.present[slot] == 0)
                    continue;
                if (cellData == null)
                    cellData = new ArrayList<>(stores.length);
                cellData.add(store.cellAt(slot, ts, ttl, ldt));
            }

            LivenessInfo liveness = (flags[slot] & FLAG_HAS_LIVENESS) != 0
                                    ? LivenessInfo.withExpirationTime(ts, ttl, ldt)
                                    : LivenessInfo.EMPTY;
            Object[] tree = cellData == null ? BTree.empty() : BTree.build(cellData);
            return BTreeRow.create(clusteringAt(slot), liveness, Row.Deletion.LIVE, tree);
        }
    }

    /**
     * Accumulates one {@code put}'s accounting and stands in as the reconciliation callback so cell
     * merges report {@code colUpdateTimeDelta} exactly as {@code BTreePartitionUpdater} does.
     */
    private static final class Put implements ColumnData.PostReconciliationFunction
    {
        final Cloner cloner;
        final UpdateTransaction indexer;
        long heapDelta;
        long dataDelta;
        long colUpdateTimeDelta = Long.MAX_VALUE;

        Put(Cloner cloner, UpdateTransaction indexer)
        {
            this.cloner = cloner;
            this.indexer = indexer;
        }

        @Override
        public ColumnData insert(ColumnData insert)
        {
            return insert;
        }

        @Override
        public Cell<?> merge(Cell<?> previous, Cell<?> insert)
        {
            if (insert != previous)
            {
                long timeDelta = Math.abs(insert.timestamp() - previous.timestamp());
                if (timeDelta < colUpdateTimeDelta)
                    colUpdateTimeDelta = timeDelta;
            }
            return insert;
        }

        @Override
        public void delete(ColumnData existing)
        {
        }

        @Override
        public void onAllocatedOnHeap(long delta)
        {
        }
    }

    /**
     * The rebuilt view. {@code canHaveShadowedData} must be true: like any memtable partition,
     * the arrays keep rows that a later partition or range deletion shadows, and the iterator has to
     * filter them exactly as {@code AtomicBTreePartition} does.
     */
    private static final class Snapshot extends ImmutableBTreePartition
    {
        Snapshot(TableMetadata metadata,
                 DecoratedKey key,
                 RegularAndStaticColumns columns,
                 Row staticRow,
                 Object[] tree,
                 DeletionInfo deletionInfo,
                 EncodingStats stats)
        {
            super(metadata, key, columns, staticRow, tree, deletionInfo, stats);
        }

        @Override
        protected boolean canHaveShadowedData()
        {
            return true;
        }
    }
}
