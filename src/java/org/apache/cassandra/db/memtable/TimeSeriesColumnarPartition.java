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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.cassandra.utils.btree.UpdateFunction;
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
 * <p><b>Concurrency.</b> Writes and snapshot builds synchronise on this object (the design accepts a
 * partition-level lock; flush snapshots run after the write barrier). Reads use a cached, immutable
 * snapshot — a {@link ImmutableBTreePartition} built from the arrays — which every write invalidates,
 * so reads never see a torn state. Because ingest is append-mostly, invalidation is not discard: the
 * last built snapshot and the row count it covered are kept, and when nothing but pure appends
 * happened since, the next read materialises only the appended rows and merges them into the cached
 * tree — O(appended), not O(partition). Anything else — an in-place slot merge, a promotion to the
 * overflow tier, an overflow write, a deletion or static change, a consolidation that reordered the
 * arrays — invalidates fully and the next read rebuilds from scratch. The flush path uses
 * {@link #flushView()}, which does <em>not</em> retain the snapshot, so flushing a large memtable
 * does not re-materialise every partition's object form at once.
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
    /** The slot's row was promoted to the overflow map; the slot only keeps its clustering for search order. */
    private static final byte FLAG_PROMOTED = 2;

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

    /** Greatest clustering held by the arrays, so in-order ingest can append without searching. */
    private long maxClusteringKey;
    private Object maxClusteringObject;

    /** Least clustering held by the arrays, maintained on append for {@link #mayContainRowsIn}. */
    private long minClusteringKey;
    private Object minClusteringObject;

    // ---- object-tier structures ----
    private TreeMap<Clustering<?>, Row> overflow;
    private Row staticRow = Rows.EMPTY_STATIC_ROW;
    private DeletionInfo deletionInfo = DeletionInfo.LIVE;
    private RegularAndStaticColumns columns = RegularAndStaticColumns.NONE;

    /**
     * Cached read view. Non-null only while it reflects the partition's latest state: every write
     * clears it and {@link #snapshot()} re-caches it, so an unchanged partition's read returns it
     * without taking the lock or allocating anything.
     */
    private volatile Snapshot snapshot;

    /**
     * The most recently built read view, kept across writes so an append-only interleaving can
     * extend it with just the newly appended rows instead of rebuilding O(partition) per
     * write→read cycle. Guarded by 'this'.
     */
    private Snapshot lastSnapshot;

    /** Value of {@link #size} when {@link #lastSnapshot} was built. */
    private int lastSnapshotSize;

    /**
     * Whether anything other than a pure array append happened since {@link #lastSnapshot} was
     * built: an in-place slot merge, a promotion into the overflow tier, any overflow write, a
     * static or deletion-info change, or a consolidation that reordered the arrays. Any of these
     * makes {@link #lastSnapshot} unusable as an extension base — correctness first — and the next
     * read rebuilds from scratch.
     */
    private boolean changedBeyondAppend;

    /** Full rebuilds of the read view, so tests can pin that append interleavings extend instead. */
    private long fullSnapshotBuilds;

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
        Put put = new Put(cloner, indexer);
        synchronized (this)
        {
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

            snapshot = null;
        }
        // Outside the lock: acquiring memtable space may block on a flush, and the flush path takes
        // this partition's lock to build its view.
        allocator.onHeap().adjust(put.heapDelta, opGroup);
        liveDataSize.addAndGet(put.dataDelta);
        return put.colUpdateTimeDelta;
    }

    /** Mirrors {@code BTreePartitionUpdater#merge(DeletionInfo, DeletionInfo)}, including the indexer events. */
    private void mergeDeletionInfo(DeletionInfo update, Put put)
    {
        if (update.isLive() || !update.mayModify(deletionInfo))
            return;

        changedBeyondAppend = true;
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
        changedBeyondAppend = true;
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
            append(row, shape, longKey, put);
            put.indexer.onInserted(row);
            put.dataDelta += row.dataSize();
            return;
        }

        int slot = representable ? findSlot(clustering, longKey) : -1;
        if (slot >= 0)
        {
            mergeIntoSlot(slot, row, put);
            return;
        }

        if (arrayable)
        {
            append(row, shape, longKey, put);
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

    private void append(Row row, RowShape shape, long longKey, Put put)
    {
        ensureCapacity(put);
        int slot = size;

        Clustering<?> storedClustering = null;
        if (longClusterings)
        {
            clusteringKeys[slot] = longKey;
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

        boolean inOrder = sortedCount == slot
                          && (slot == 0 || compareSlotTo(slot - 1, row.clustering(), longKey) < 0);
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
     * {@link Rows#merge} machinery, and either write the result back in place (still one uniform
     * timestamp) or promote this one row to the overflow map (the design's cell slow path).
     */
    private void mergeIntoSlot(int slot, Row update, Put put)
    {
        // Both outcomes mutate a row the cached snapshot may already hold — in place or by
        // promotion — so neither is an append and the snapshot must be rebuilt from scratch.
        changedBeyondAppend = true;
        Row existing = materializeRow(slot);
        Row merged = Rows.merge(existing, update, put);
        put.indexer.onUpdated(existing, merged);
        put.dataDelta += merged.dataSize() - existing.dataSize();

        RowShape shape = classify(merged);
        if (shape != null)
        {
            timestamps[slot] = shape.timestamp;
            localDeletionTimes[slot] = shape.localDeletionTime;
            ttls[slot] = shape.ttl;
            flags[slot] = shape.hasLiveness ? FLAG_HAS_LIVENESS : 0;
            for (ColumnData data : merged)
            {
                Cell<?> cell = (Cell<?>) data;
                storeFor(cell.column(), put).set(slot, cell, put);
            }
        }
        else
        {
            flags[slot] |= FLAG_PROMOTED;
            Row cloned = merged.clone(put.cloner);
            putOverflow(cloned, null, put);
        }
    }

    private void putOverflow(Row cloned, Row replaced, Put put)
    {
        changedBeyondAppend = true;
        if (overflow == null)
            overflow = new TreeMap<>(metadata.get().comparator);
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

    /** @return the slot holding {@code clustering}, or -1. Searches the sorted prefix, then the tail. */
    private int findSlot(Clustering<?> clustering, long longKey)
    {
        int lo = 0;
        int hi = sortedCount - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cmp = compareSlotTo(mid, clustering, longKey);
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
                return mid;
        }
        for (int i = sortedCount; i < size; i++)
        {
            if (compareSlotTo(i, clustering, longKey) == 0)
                return i;
        }
        return -1;
    }

    /** Folds the unsorted tail into the sorted prefix. Never called by in-order ingest. */
    private void consolidate()
    {
        if (sortedCount == size)
            return;

        // Folding shifts rows across slots, so a snapshot's row count no longer names a suffix.
        changedBeyondAppend = true;

        int tailLength = size - sortedCount;
        Integer[] tail = new Integer[tailLength];
        for (int i = 0; i < tailLength; i++)
            tail[i] = sortedCount + i;
        Arrays.sort(tail, this::compareSlots);

        int[] permutation = new int[size];
        int prefix = 0;
        int t = 0;
        int out = 0;
        while (prefix < sortedCount && t < tailLength)
            permutation[out++] = compareSlots(prefix, tail[t]) < 0 ? prefix++ : tail[t++];
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
        long ts = timestamps[slot];
        int ttl = ttls[slot];
        long ldt = localDeletionTimes[slot];

        List<ColumnData> cells = null;
        for (int s = 0; s < stores.size(); s++)
        {
            ColumnStore store = stores.get(s);
            if (store.present[slot] == 0)
                continue;
            if (cells == null)
                cells = new ArrayList<>(stores.size());
            cells.add(store.cellAt(slot, ts, ttl, ldt));
        }

        LivenessInfo liveness = (flags[slot] & FLAG_HAS_LIVENESS) != 0
                                ? LivenessInfo.withExpirationTime(ts, ttl, ldt)
                                : LivenessInfo.EMPTY;
        Object[] tree = cells == null ? BTree.empty() : BTree.build(cells);
        return BTreeRow.create(clusteringAt(slot), liveness, Row.Deletion.LIVE, tree);
    }

    private Clustering<?> clusteringAt(int slot)
    {
        return longClusterings
               ? Clustering.make(ByteBufferUtil.bytes(clusteringKeys[slot]))
               : (Clustering<?>) clusteringObjects[slot];
    }

    private Snapshot snapshot()
    {
        Snapshot current = snapshot;
        if (current != null)
            return current;
        synchronized (this)
        {
            current = snapshot;
            if (current == null)
            {
                current = refreshSnapshot();
                snapshot = current;
                lastSnapshot = current;
                lastSnapshotSize = size;
                changedBeyondAppend = false;
            }
            return current;
        }
    }

    @Override
    public Partition flushView()
    {
        Snapshot current = snapshot;
        if (current != null)
            return current;
        synchronized (this)
        {
            current = snapshot;
            // Deliberately not cached: the flush set walks every partition, and pinning each one's
            // materialized object form until the memtable is discarded would recreate at flush time
            // the very heap footprint the arrays exist to avoid.
            return current != null ? current : refreshSnapshot();
        }
    }

    /**
     * The partition's current read view: {@link #lastSnapshot} extended with just the rows appended
     * since it was built when pure appends are all that happened, a full {@link #buildSnapshot()}
     * otherwise. Caller must hold the lock.
     */
    private Snapshot refreshSnapshot()
    {
        Snapshot base = lastSnapshot;
        if (base == null || changedBeyondAppend || size < lastSnapshotSize)
            return buildSnapshot();
        return extendSnapshot(base, lastSnapshotSize);
    }

    /**
     * Rebuilds the read view as {@code base} plus only the rows appended after it was built —
     * O(appended), not O(partition). Only sound when nothing but appends happened since: an appended
     * row's clustering is guaranteed distinct from everything the base holds (a write to an existing
     * clustering goes through {@link #mergeIntoSlot} or the overflow map, both of which invalidate
     * fully), so the merge is a pure union, ordered on both sides by the table's comparator — which
     * keeps reversed (DESC) clusterings exactly as a full rebuild would. Caller must hold the lock.
     */
    private Snapshot extendSnapshot(Snapshot base, int from)
    {
        if (from == size)
            return base;

        List<Row> appended = new ArrayList<>(size - from);
        for (int i = from; i < size; i++)
        {
            // Belt and braces: a promoted slot here means the invalidation bookkeeping failed.
            // Rebuild rather than serve a row the overflow map now owns.
            if ((flags[i] & FLAG_PROMOTED) != 0)
                return buildSnapshot();
            appended.add(materializeRow(i));
        }

        ClusteringComparator comparator = metadata.get().comparator;
        // The appended region may end in an unsorted tail; the base tree is never touched.
        appended.sort(comparator);

        Object[] tree = BTree.update(base.tree, BTree.build(appended, UpdateFunction.noOp()), comparator);
        EncodingStats stats = EncodingStats.Collector.collect(staticRow, appended.iterator(), deletionInfo)
                                                     .mergeWith(base.stats());
        return new Snapshot(metadata.get(), key, columns, staticRow, tree, deletionInfo, stats);
    }

    /** Caller must hold the lock. */
    private Snapshot buildSnapshot()
    {
        fullSnapshotBuilds++;
        consolidate();
        TableMetadata tableMetadata = metadata.get();
        ClusteringComparator comparator = tableMetadata.comparator;

        try (BTree.FastBuilder<Row> builder = BTree.fastBuilder())
        {
            Iterator<Row> overflowRows = overflow == null
                                         ? Collections.emptyIterator()
                                         : new ArrayList<>(overflow.values()).iterator();
            Row pendingOverflow = overflowRows.hasNext() ? overflowRows.next() : null;

            for (int i = 0; i < size; i++)
            {
                if ((flags[i] & FLAG_PROMOTED) != 0)
                    continue;
                Row row = materializeRow(i);
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
        return snapshot().partitionLevelDeletion();
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return snapshot().columns();
    }

    @Override
    public EncodingStats stats()
    {
        return snapshot().stats();
    }

    @Override
    public boolean isEmpty()
    {
        return snapshot().isEmpty();
    }

    @Override
    public boolean hasRows()
    {
        return snapshot().hasRows();
    }

    @Override
    public Row getRow(Clustering<?> clustering)
    {
        return allocator.ensureOnHeap().applyToRow(snapshot().getRow(clustering));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return allocator.ensureOnHeap().applyToPartition(snapshot().unfilteredIterator());
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(snapshot().unfilteredIterator(selection, slices, reversed));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(snapshot().unfilteredIterator(selection, clusteringsInQueryOrder, reversed));
    }

    /**
     * Whether a sliced read of this shard partition can return anything, answered from the
     * <b>clustering</b> values actually held — the arrays' maintained min/max plus the overflow
     * map's ends — and never from the shard's write-time window, because clustering time and write
     * time are independent. Fails open: any partition or range deletion, any static row, an empty
     * partition, a clustering-less table, and any state or slices shape the probe cannot handle all
     * answer {@code true} — pruning is an optimisation, and throwing here is a read outage.
     * Deliberately touches neither {@link #snapshot()} (pruning must not materialise anything) nor
     * {@code Slice.make} on min/max: they are memtable-owned clusterings, and fabricating bounds
     * from them needs the value accessor's object factory, which the native (offheap_objects)
     * accessor refuses with {@code UnsupportedOperationException} — the 2026-08-02 paged-read
     * outage, hit on every page after the first because only page one carries {@code Slices.ALL}.
     */
    @Override
    public synchronized boolean mayContainRowsIn(Slices slices)
    {
        try
        {
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
                // Promoted slots keep their clustering in the arrays, so the array bounds already
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

    /** Rows currently held by the primitive arrays (the fast path). */
    @VisibleForTesting
    public synchronized int fastPathRowCount()
    {
        int live = 0;
        for (int i = 0; i < size; i++)
        {
            if ((flags[i] & FLAG_PROMOTED) == 0)
                live++;
        }
        return live;
    }

    /** Rows in the object-tier overflow map — promoted rows plus rows the arrays never fit. */
    @VisibleForTesting
    public synchronized int overflowRowCount()
    {
        return overflow == null ? 0 : overflow.size();
    }

    /** Whether the clustering column is held as a bare {@code long} per row. */
    @VisibleForTesting
    public boolean usesLongClusterings()
    {
        return longClusterings;
    }

    /**
     * How many times the read view was rebuilt from scratch, as opposed to extended with only the
     * appended rows. An append-mostly write→read interleaving must keep this a small constant —
     * one full build per non-append mutation, not one per read.
     */
    @VisibleForTesting
    public synchronized long fullSnapshotBuilds()
    {
        return fullSnapshotBuilds;
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
     */
    private static final class ColumnStore
    {
        final ColumnMetadata column;
        final int width;
        byte[] present;
        long[] longs;
        int[] ints;
        byte[] bytes;
        Object[] cells;

        ColumnStore(ColumnMetadata column, int capacity)
        {
            this.column = column;
            this.width = fixedWidthOf(column.type);
            this.present = new byte[capacity];
            switch (width)
            {
                case 8:
                    longs = new long[capacity];
                    break;
                case 4:
                    ints = new int[capacity];
                    break;
                case 1:
                    bytes = new byte[capacity];
                    break;
                default:
                    cells = new Object[capacity];
                    break;
            }
        }

        long heapSize()
        {
            return present.length * (long) (width == 0 ? 9 : width + 1) + 32;
        }

        void grow(int newCapacity)
        {
            present = Arrays.copyOf(present, newCapacity);
            if (longs != null)
                longs = Arrays.copyOf(longs, newCapacity);
            if (ints != null)
                ints = Arrays.copyOf(ints, newCapacity);
            if (bytes != null)
                bytes = Arrays.copyOf(bytes, newCapacity);
            if (cells != null)
                cells = Arrays.copyOf(cells, newCapacity);
        }

        void applyPermutation(int[] permutation, int count, int capacity)
        {
            byte[] nextPresent = new byte[capacity];
            for (int i = 0; i < count; i++)
                nextPresent[i] = present[permutation[i]];
            present = nextPresent;

            if (longs != null)
            {
                long[] next = new long[capacity];
                for (int i = 0; i < count; i++)
                    next[i] = longs[permutation[i]];
                longs = next;
            }
            if (ints != null)
            {
                int[] next = new int[capacity];
                for (int i = 0; i < count; i++)
                    next[i] = ints[permutation[i]];
                ints = next;
            }
            if (bytes != null)
            {
                byte[] next = new byte[capacity];
                for (int i = 0; i < count; i++)
                    next[i] = bytes[permutation[i]];
                bytes = next;
            }
            if (cells != null)
            {
                Object[] next = new Object[capacity];
                for (int i = 0; i < count; i++)
                    next[i] = cells[permutation[i]];
                cells = next;
            }
        }

        void set(int slot, Cell<?> cell, Put put)
        {
            switch (width)
            {
                case 8:
                {
                    ByteBuffer value = cell.buffer();
                    longs[slot] = value.getLong(value.position());
                    break;
                }
                case 4:
                {
                    ByteBuffer value = cell.buffer();
                    ints[slot] = value.getInt(value.position());
                    break;
                }
                case 1:
                {
                    ByteBuffer value = cell.buffer();
                    bytes[slot] = value.get(value.position());
                    break;
                }
                default:
                {
                    Object previous = cells[slot];
                    if (previous != cell)
                    {
                        Cell<?> cloned = put.cloner.clone(cell);
                        put.heapDelta += cloned.unsharedHeapSizeExcludingData()
                                         - (previous == null ? 0 : ((Cell<?>) previous).unsharedHeapSizeExcludingData());
                        cells[slot] = cloned;
                    }
                    break;
                }
            }
            present[slot] = 1;
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
     * The read/flush view. {@code canHaveShadowedData} must be true: like any memtable partition,
     * the arrays keep rows that a later partition or range deletion shadows, and the iterator has to
     * filter them exactly as {@code AtomicBTreePartition} does.
     */
    private static final class Snapshot extends ImmutableBTreePartition
    {
        /** The row tree, re-exposed for incremental extension ({@code BTreePartitionData}'s copy is package-private). */
        final Object[] tree;

        Snapshot(TableMetadata metadata,
                 DecoratedKey key,
                 RegularAndStaticColumns columns,
                 Row staticRow,
                 Object[] tree,
                 DeletionInfo deletionInfo,
                 EncodingStats stats)
        {
            super(metadata, key, columns, staticRow, tree, deletionInfo, stats);
            this.tree = tree;
        }

        @Override
        protected boolean canHaveShadowedData()
        {
            return true;
        }
    }
}
