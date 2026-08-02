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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.github.jamm.Unmetered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategyOptions;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.timeseries.tiering.ColdWindowChunkFlush;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * Memtable for tables compacted by {@link TimeSeriesCompactionStrategy}, sharded by the compaction
 * window a row's write timestamp falls in.
 *
 * <p><b>Why shard by window at write time.</b> Splitting a flush across window boundaries already
 * works, but it costs: {@code SSTableWriter} consumes a partition in one pass and accepts its key
 * exactly once, so routing one across windows means buffering the whole partition on heap.
 * {@code WindowRoutingIterator} caps that at 64 MiB and, beyond it, gives up splitting and writes
 * the partition whole — which yields a window-spanning sstable, an unchanged shape on the next
 * split-refreeze, and a window the no-progress guard parks. Deciding the window when the row
 * arrives removes the buffering, and with it the ceiling: a memtable is a mutable map, so a row
 * goes into its window in O(1) no matter how large the partition grows.
 *
 * <p><b>Configuration.</b> Memtables are selected by configuration <em>key</em>, not class name, so
 * this needs an entry in {@code cassandra.yaml} on every node before any table can use it:
 * <pre>
 * memtable:
 *   configurations:
 *     timeseries:
 *       class_name: TimeSeriesMemtable
 * </pre>
 * and then per table:
 * <pre>
 * ALTER TABLE ks.tbl WITH memtable = 'timeseries';
 * </pre>
 * The window size is read from the table's compaction options rather than taken as a parameter here
 * — a window size configured in two places is one that can disagree with itself, and the flush would
 * then land in a window the strategy does not expect.
 *
 * <p><b>What it does not constrain.</b> The window comes from a <em>write timestamp</em>, which every
 * cell of every schema has, so the shape of the primary key is irrelevant: compound partition keys,
 * several clustering columns and non-timestamp clusterings are all fine. Counter and non-frozen
 * collection columns are fine too — splitting one logical row's cells over several sstables and
 * reconciling them on read is what Cassandra already does, and it is exactly what
 * {@code WindowRoutingIterator} does today on the flush path for these same tables. The only thing
 * {@link #unsupportedReason} declines is a table not using {@link TimeSeriesCompactionStrategy},
 * because then there is no {@code window_size} to shard by.
 *
 * <p><b>Unsupported schemas fall back, they do not fail.</b> A memtable is on the write path, so
 * refusing a schema by throwing would fail every write to that table and turn a missed optimisation
 * into an outage. {@link #unsupportedReason} decides, and {@link Factory#create} falls back to the
 * default memtable with one warning per table per hour.
 *
 * <p><b>Storage inside a shard is two-tier.</b> Tables whose regular columns are all simple (no
 * counters, no non-frozen collections or UDTs) store each partition column-wise in primitive arrays
 * — see {@link TimeSeriesColumnarPartition} — which is where the heap-per-row saving comes from.
 * Any other table keeps the reference representation, an {@link AtomicBTreePartition} per partition
 * ({@link ObjectShardPartition}). The choice is per table and invisible to reads and flushes, and it
 * is a storage decision only: it never widens what {@link #unsupportedReason} accepts.
 *
 * <p><b>Statistics are not inflated by sharding</b> — see {@link #partitionCount()}.
 */
public class TimeSeriesMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMemtable.class);

    /**
     * Window start (epoch ms) to that window's partitions. A {@link ConcurrentSkipListMap} rather than a
     * hash map because the flush path (task 3) wants the windows in order, and because a memtable that
     * ingests a backfill can hold many of them.
     */
    private final ConcurrentNavigableMap<Long, WindowShard> shards = new ConcurrentSkipListMap<>();

    /**
     * Every distinct partition key held by any shard, in token order, mapped to the one cloned key
     * instance all the shards share.
     *
     * <p>This index is what keeps {@link #partitionCount()} honest: the same key can live in several
     * window shards, and summing shard sizes would report one partition as many, which would make the
     * memtable look like it holds more partitions than it does and skew the flush-size estimate the
     * sstable writer is given. It also makes {@link #isClean()} and {@link #lastToken()} O(1)-ish
     * instead of a scan across every shard, and it lets a point read that misses entirely skip probing
     * each shard.
     */
    private final ConcurrentNavigableMap<PartitionPosition, DecoratedKey> keys = new ConcurrentSkipListMap<>();

    /**
     * Sum of the size deltas reported by every {@link AtomicBTreePartition#addAll}. A row's data is
     * stored in exactly one shard, so sharding does not double-count it; the only per-shard extra is
     * the fixed overhead of the additional (empty) partition object, which is memory that really was
     * allocated.
     */
    private final AtomicLong liveDataSize = new AtomicLong(0);

    /**
     * Read once from the table's compaction options and refreshed on {@link #metadataUpdated()}:
     * parsing the duration strings involves a regex, which is not something to do per write.
     */
    private volatile TimeSeriesCompactionStrategyOptions options;

    /**
     * Whether new shards store partitions column-wise ({@link TimeSeriesColumnarPartition}) or as
     * {@link AtomicBTreePartition}s. Refreshed on {@link #metadataUpdated()}; shards created before a
     * change keep their representation — a columnar partition handed something it cannot hold in its
     * arrays (say a complex column added by ALTER mid-memtable) demotes those rows to its object
     * overflow tier, so a stale flag is a lost optimisation, not a correctness problem.
     */
    private volatile boolean columnarStorage;

    TimeSeriesMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound,
                       TableMetadataRef metadataRef,
                       Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
        this.options = optionsOf(metadataRef.get());
        this.columnarStorage = columnarCapable(metadataRef.get());
    }

    /**
     * Whether every regular column of {@code metadata} is a simple (single-cell, non-counter) column,
     * which is what the columnar tier can hold. Counter cells need counter-context reconciliation and
     * complex columns are multi-cell, so such tables keep the reference object storage — inside the
     * shard, without narrowing {@link #unsupportedReason}. Static columns do not matter: static rows
     * are stored as object rows in either representation.
     */
    @VisibleForTesting
    static boolean columnarCapable(TableMetadata metadata)
    {
        if (metadata.isCounter())
            return false;
        for (ColumnMetadata column : metadata.regularColumns())
        {
            if (column.isComplex())
                return false;
        }
        return true;
    }

    private static TimeSeriesCompactionStrategyOptions optionsOf(TableMetadata metadata)
    {
        return new TimeSeriesCompactionStrategyOptions(metadata.params.compaction.options());
    }

    /**
     * Entry point {@code MemtableParams} finds by reflection, given a copy of the configured
     * parameters. It rejects the configuration if any are left unconsumed, which is how a typo in a
     * memtable parameter is caught when the node starts rather than at the first flush. This
     * memtable takes none.
     */
    public static Memtable.Factory factory(Map<String, String> options)
    {
        return Factory.INSTANCE;
    }

    /**
     * @return why this memtable cannot hold {@code metadata}, or {@code null} if it can.
     */
    public static String unsupportedReason(TableMetadata metadata)
    {
        Class<?> compaction = metadata.params.compaction.klass();
        if (!TimeSeriesCompactionStrategy.class.isAssignableFrom(compaction))
            return "table uses " + compaction.getSimpleName() + ", not TimeSeriesCompactionStrategy, "
                   + "so there is no window_size to shard by";

        return null;
    }

    @Override
    public void metadataUpdated()
    {
        super.metadataUpdated();
        this.options = optionsOf(metadata());
        this.columnarStorage = columnarCapable(metadata());
    }

    /** The window shards, for tests that need to see how a write sequence was distributed. */
    @VisibleForTesting
    public ConcurrentNavigableMap<Long, WindowShard> shards()
    {
        return shards;
    }

    // ------------------------------------------------------------------------------------- writes

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the
     * appropriate OpOrdering.
     */
    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, boolean assumeMissing)
    {
        Cloner cloner = allocator.cloner(opGroup);
        DecoratedKey key = internKey(update.partitionKey(), cloner, opGroup);
        TimeSeriesCompactionStrategyOptions opts = options;

        // An UpdateTransaction is scoped to one partition update and guarantees start() before
        // anything else and commit() at the end. Splitting the update into several addAll calls must
        // not turn that into several transactions, so the calls see a wrapper that swallows both and
        // the real pair is issued here, around the whole update.
        UpdateTransaction scoped = new SingleScopeTransaction(indexer);
        long colUpdateTimeDelta;
        indexer.start();
        try
        {
            Long window = singleWindowOf(update, opts);
            colUpdateTimeDelta = window != null
                                 ? shardFor(window).put(key, update, scoped, cloner, opGroup, assumeMissing, liveDataSize)
                                 : putSplitByWindow(key, update, scoped, cloner, opGroup, opts);
        }
        finally
        {
            indexer.commit();
        }

        // Collected from the update as a whole, exactly once, so that splitting it across shards
        // cannot inflate the operation count or widen the collected column set / stats.
        updateMin(minTimestamp, update.stats().minTimestamp);
        updateMin(minLocalDeletionTime, update.stats().minLocalDeletionTime);
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        return colUpdateTimeDelta;
    }

    /**
     * Applies an update whose contents do not all belong to one window, one element at a time. Nothing
     * is accumulated: each row (and each tombstone, and the static row) is wrapped on its own and
     * handed straight to its window's shard. That is the whole point of moving the split to write
     * time — the flush-time splitter has to buffer because the sstable writer takes a partition in one
     * pass, and a mutable map has no such constraint.
     */
    private long putSplitByWindow(DecoratedKey key,
                                  PartitionUpdate update,
                                  UpdateTransaction indexer,
                                  Cloner cloner,
                                  OpOrder.Group opGroup,
                                  TimeSeriesCompactionStrategyOptions opts)
    {
        TableMetadata metadata = update.metadata();
        long colUpdateTimeDelta = Long.MAX_VALUE;

        DeletionInfo deletionInfo = update.deletionInfo();
        DeletionTime partitionDeletion = deletionInfo.getPartitionDeletion();
        if (!partitionDeletion.isLive())
        {
            PartitionUpdate.Builder one = new PartitionUpdate.Builder(metadata, key, RegularAndStaticColumns.NONE, 1);
            one.addPartitionDeletion(partitionDeletion);
            colUpdateTimeDelta = Math.min(colUpdateTimeDelta,
                                          apply(windowOf(partitionDeletion.markedForDeleteAt(), opts),
                                                key, one.build(), indexer, cloner, opGroup));
        }

        if (deletionInfo.hasRanges())
        {
            for (Iterator<RangeTombstone> it = deletionInfo.rangeIterator(false); it.hasNext(); )
            {
                RangeTombstone tombstone = it.next();
                PartitionUpdate.Builder one = new PartitionUpdate.Builder(metadata, key, RegularAndStaticColumns.NONE, 1);
                one.add(tombstone);
                colUpdateTimeDelta = Math.min(colUpdateTimeDelta,
                                              apply(windowOf(tombstone.deletionTime().markedForDeleteAt(), opts),
                                                    key, one.build(), indexer, cloner, opGroup));
            }
        }

        Row staticRow = update.staticRow();
        if (!staticRow.isEmpty())
            colUpdateTimeDelta = Math.min(colUpdateTimeDelta,
                                          apply(windowOf(maxTimestamp(staticRow), opts),
                                                key, singleRow(metadata, key, staticRow), indexer, cloner, opGroup));

        for (Row row : update)
        {
            if (row.isEmpty())
                continue;
            colUpdateTimeDelta = Math.min(colUpdateTimeDelta,
                                          apply(windowOf(maxTimestamp(row), opts),
                                                key, singleRow(metadata, key, row), indexer, cloner, opGroup));
        }

        return colUpdateTimeDelta;
    }

    private long apply(long window,
                       DecoratedKey key,
                       PartitionUpdate update,
                       UpdateTransaction indexer,
                       Cloner cloner,
                       OpOrder.Group opGroup)
    {
        return shardFor(window).put(key, update, indexer, cloner, opGroup, false, liveDataSize);
    }

    /**
     * Wraps one row as an update of its own. Built through {@link PartitionUpdate.Builder} rather than
     * {@link PartitionUpdate#singleRowUpdate} because the builder recomputes {@link EncodingStats};
     * {@code singleRowUpdate} leaves {@code NO_STATS} on the update, and those stats are merged into
     * the stored partition and then used as the delta base when the partition is serialized.
     */
    private static PartitionUpdate singleRow(TableMetadata metadata, DecoratedKey key, Row row)
    {
        RegularAndStaticColumns columns = row.isStatic()
                                          ? new RegularAndStaticColumns(Columns.from(row), Columns.NONE)
                                          : new RegularAndStaticColumns(Columns.NONE, Columns.from(row));
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key, columns, 1);
        builder.add(row);
        return builder.build();
    }

    /**
     * @return the one window every element of {@code update} routes to, or {@code null} if they do not
     *         all agree and the update has to be split. The common case — a single row, or a batch of
     *         rows written at the same time — takes the non-null answer and is then applied whole, with
     *         no per-row wrapping at all.
     */
    private @Nullable Long singleWindowOf(PartitionUpdate update, TimeSeriesCompactionStrategyOptions opts)
    {
        long window = 0;
        boolean seen = false;

        DeletionInfo deletionInfo = update.deletionInfo();
        DeletionTime partitionDeletion = deletionInfo.getPartitionDeletion();
        if (!partitionDeletion.isLive())
        {
            window = windowOf(partitionDeletion.markedForDeleteAt(), opts);
            seen = true;
        }

        if (deletionInfo.hasRanges())
        {
            for (Iterator<RangeTombstone> it = deletionInfo.rangeIterator(false); it.hasNext(); )
            {
                long candidate = windowOf(it.next().deletionTime().markedForDeleteAt(), opts);
                if (seen && candidate != window)
                    return null;
                window = candidate;
                seen = true;
            }
        }

        Row staticRow = update.staticRow();
        if (!staticRow.isEmpty())
        {
            long candidate = windowOf(maxTimestamp(staticRow), opts);
            if (seen && candidate != window)
                return null;
            window = candidate;
            seen = true;
        }

        for (Row row : update)
        {
            if (row.isEmpty())
                continue;
            long candidate = windowOf(maxTimestamp(row), opts);
            if (seen && candidate != window)
                return null;
            window = candidate;
            seen = true;
        }

        // An update with nothing timestamped in it stores nothing; the window it nominally lands in is
        // arbitrary, so use the epoch one rather than reading a clock.
        return seen ? window : windowOf(0L, opts);
    }

    /**
     * The window a row belongs to, taken from its <b>maximum</b> write timestamp.
     *
     * <p>The maximum, not the minimum or the clustering value, because
     * {@link TimeSeriesCompactionStrategy} assigns an sstable to a window by the sstable's maximum
     * timestamp. Routing by any other rule would put flushed data in a window the strategy does not
     * expect it in, and the window would then never classify as frozen.
     */
    private static long maxTimestamp(Row row)
    {
        long max = Long.MIN_VALUE;

        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        if (!liveness.isEmpty())
            max = Math.max(max, liveness.timestamp());

        if (!row.deletion().isLive())
            max = Math.max(max, row.deletion().time().markedForDeleteAt());

        for (ColumnData data : row)
        {
            if (data.column().isSimple())
            {
                max = Math.max(max, ((Cell<?>) data).timestamp());
            }
            else
            {
                ComplexColumnData complex = (ComplexColumnData) data;
                if (!complex.complexDeletion().isLive())
                    max = Math.max(max, complex.complexDeletion().markedForDeleteAt());
                for (Cell<?> cell : complex)
                    max = Math.max(max, cell.timestamp());
            }
        }
        return max;
    }

    /**
     * Window arithmetic is delegated to {@link TimeSeriesCompactionStrategyOptions#windowStartFor} and
     * never open-coded: the memtable and the strategy have to agree on where a boundary is, and two
     * copies of the same division eventually stop agreeing.
     */
    private static long windowOf(long writeTimestamp, TimeSeriesCompactionStrategyOptions opts)
    {
        return opts.windowStartFor(TimeUnit.MILLISECONDS.convert(writeTimestamp, opts.timestampResolution));
    }

    private DecoratedKey internKey(DecoratedKey key, Cloner cloner, OpOrder.Group opGroup)
    {
        DecoratedKey existing = keys.get(key);
        if (existing != null)
            return existing;

        DecoratedKey cloned = cloner.clone(key);
        DecoratedKey raced = keys.putIfAbsent(cloned, cloned);
        if (raced != null)
            return raced;

        // Allocated after the fact, as the skip-list memtables do: it saves allocating and then having
        // to give back, at the cost of possibly overshooting the declared limit slightly.
        allocator.onHeap().allocate((int) (cloned.getToken().getHeapSize() + SkipListMemtable.ROW_OVERHEAD_HEAP_SIZE),
                                    opGroup);
        return cloned;
    }

    private WindowShard shardFor(long windowStart)
    {
        WindowShard shard = shards.get(windowStart);
        if (shard != null)
            return shard;

        WindowShard created = new WindowShard(windowStart, metadata, allocator, columnarStorage);
        WindowShard raced = shards.putIfAbsent(windowStart, created);
        return raced != null ? raced : created;
    }

    // -------------------------------------------------------------------------------------- reads

    /**
     * All shards holding anything in the range, merged. The merge is not an optimisation to skip: the
     * same partition key lives in as many shards as it has windows, and returning the shards
     * unmerged would hand the caller the same partition several times, each with only part of its
     * rows.
     */
    @Override
    public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter,
                                                         DataRange dataRange,
                                                         SSTableReadsListener readsListener)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        List<UnfilteredPartitionIterator> iterators = new ArrayList<>(shards.size());
        for (WindowShard shard : shards.values())
            iterators.add(new MemtableUnfilteredPartitionIterator(metadata(),
                                                                  shard.subMap(left, includeStart, right, includeStop)
                                                                       .values().iterator(),
                                                                  columnFilter,
                                                                  dataRange));

        if (iterators.isEmpty())
            return EmptyIterators.unfilteredPartition(metadata());
        if (iterators.size() == 1)
            return iterators.get(0);
        return UnfilteredPartitionIterators.merge(iterators, null);
        // readsListener is ignored as it only accepts sstable signals
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter selectedColumns,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        if (!keys.containsKey(key))
            return null;

        boolean sliced = slices != Slices.ALL;
        List<UnfilteredRowIterator> iterators = new ArrayList<>(2);
        for (WindowShard shard : shards.values())
        {
            ShardPartition partition = shard.partitions.get(key);
            if (partition == null)
                continue;
            // A sliced read skips a shard partition by the CLUSTERING range it actually holds, never
            // by the shard's write-time window: clustering time and write time are independent, and
            // window pruning would silently drop backfilled rows — old clusterings living in the
            // current write window's shard.
            if (sliced && !partition.mayContainRowsIn(slices))
                continue;
            iterators.add(partition.unfilteredIterator(selectedColumns, slices, reversed));
        }
        return mergeRows(iterators);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        if (!keys.containsKey(key))
            return null;

        List<UnfilteredRowIterator> iterators = new ArrayList<>(2);
        for (WindowShard shard : shards.values())
        {
            ShardPartition partition = shard.partitions.get(key);
            if (partition != null)
                iterators.add(partition.unfilteredIterator());
        }
        return mergeRows(iterators);
    }

    private static @Nullable UnfilteredRowIterator mergeRows(List<UnfilteredRowIterator> iterators)
    {
        switch (iterators.size())
        {
            case 0: return null;
            case 1: return iterators.get(0);
            default: return UnfilteredRowIterators.merge(iterators);
        }
    }

    // --------------------------------------------------------------------------------- statistics

    @Override
    public boolean isClean()
    {
        return keys.isEmpty();
    }

    @Override
    public Token lastToken()
    {
        Iterator<PartitionPosition> descending = keys.descendingKeySet().iterator();
        return descending.hasNext() ? descending.next().getToken() : null;
    }

    /**
     * The number of <b>distinct</b> partition keys, not the number of (key, window) pairs. A partition
     * spread over three windows is one partition; counting it three times would misreport the table's
     * partition count and hand the flush writer an estimate several times too large.
     */
    @Override
    public long partitionCount()
    {
        return keys.size();
    }

    @Override
    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    // ----------------------------------------------------------------------------------- flushing

    /**
     * The rows already encoded into durable chunks by {@link ColdWindowChunkFlush}, to be left out of
     * the base sstable. Decided exactly once per flush, before the first flush set is iterated;
     * {@link ColdWindowChunkFlush#NONE} whenever cold-window chunking does not apply (no tiering
     * policy, unsupported schema, or any failure -- rows are always the fallback).
     */
    private ColdWindowChunkFlush.RowOmissions flushOmissions;
    private boolean flushOmissionsDecided;

    /**
     * Encodes this memtable's cold windows into chunks (see {@link ColdWindowChunkFlush} for the
     * durability ordering) and remembers which rows that made omittable. Synchronized and sticky:
     * {@link #getFlushSet} is called once per disk range, and every range must see the one decision --
     * chunk writes are not to be repeated, and two ranges disagreeing on omissions would tear a
     * partition between chunk and sstable.
     */
    private synchronized ColdWindowChunkFlush.RowOmissions flushOmissions()
    {
        if (!flushOmissionsDecided)
        {
            flushOmissionsDecided = true;
            flushOmissions = ColdWindowChunkFlush.encode(this, () -> mergedPartitions(null, true, null, false));
        }
        return flushOmissions;
    }

    /**
     * The merged view over the range, exactly as an unsharded memtable would present it: one entry per
     * distinct partition key, with the shards' versions of that partition merged. Splitting the flush
     * into one sstable per window is a separate step and deliberately not done here, so that any
     * difference this memtable makes to a flushed sstable is a sharding defect and nothing else.
     *
     * <p>The one exception is stage 4's cold-window chunk flush: rows {@link #flushOmissions()} has
     * already made durable in the chunk table are filtered out of the returned partitions. Rows are
     * only ever omitted <em>after</em> their chunk is durable and the coverage ledger is widened --
     * the ordering rule {@link ColdWindowChunkFlush} enforces.
     */
    @Override
    public FlushablePartitionSet<Partition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        ColdWindowChunkFlush.RowOmissions omissions = flushOmissions();
        long keysSize = 0;
        long keyCount = 0;
        for (DecoratedKey key : keysSubMap(from, true, to, false).values())
        {
            keysSize += key.getKey().remaining();
            ++keyCount;
        }
        final long partitionKeysSize = keysSize;
        final long partitionCount = keyCount;
        TableMetadata currentTableMetadata = metadata();

        return new AbstractFlushablePartitionSet<Partition>()
        {
            private final TableMetadata tableMetadata = currentTableMetadata;

            @Override
            public Memtable memtable()
            {
                return TimeSeriesMemtable.this;
            }

            @Override
            public PartitionPosition from()
            {
                return from;
            }

            @Override
            public PartitionPosition to()
            {
                return to;
            }

            @Override
            public long partitionCount()
            {
                return partitionCount;
            }

            @Override
            public Iterator<Partition> iterator()
            {
                Iterator<Partition> merged = mergedPartitions(from, true, to, false);
                if (omissions.isEmpty())
                    return merged;
                return Iterators.transform(merged, partition -> omitChunkedRows(partition, omissions));
            }

            @Override
            public long partitionKeysSize()
            {
                return partitionKeysSize;
            }

            @Override
            public TableMetadata metadata()
            {
                return tableMetadata;
            }
        };
    }

    /**
     * Every shard's partitions in the range, in token order, with the runs that share a key collapsed
     * into one merged partition. A k-way merge rather than "walk the key index and probe every shard"
     * because the probing cost would be the key count times the window count, and a memtable that took
     * a backfill has a lot of windows.
     */
    private Iterator<Partition> mergedPartitions(PartitionPosition from,
                                                 boolean includeFrom,
                                                 PartitionPosition to,
                                                 boolean includeTo)
    {
        List<Iterator<ShardPartition>> perShard = new ArrayList<>(shards.size());
        for (WindowShard shard : shards.values())
            perShard.add(shard.subMap(from, includeFrom, to, includeTo).values().iterator());

        PeekingIterator<ShardPartition> merged =
            Iterators.peekingIterator(Iterators.mergeSorted(perShard,
                                                            (a, b) -> a.partitionKey().compareTo(b.partitionKey())));
        TableMetadata tableMetadata = metadata();

        return new AbstractIterator<Partition>()
        {
            @Override
            protected Partition computeNext()
            {
                if (!merged.hasNext())
                    return endOfData();

                ShardPartition first = merged.next();
                DecoratedKey key = first.partitionKey();
                if (!merged.hasNext() || !merged.peek().partitionKey().equals(key))
                    return first.flushView();

                List<Partition> sameKey = new ArrayList<>(2);
                sameKey.add(first.flushView());
                while (merged.hasNext() && merged.peek().partitionKey().equals(key))
                    sameKey.add(merged.next().flushView());
                return new MergedWindowPartition(tableMetadata, key, sameKey);
            }
        };
    }

    private Map<PartitionPosition, DecoratedKey> keysSubMap(PartitionPosition left,
                                                            boolean includeLeft,
                                                            PartitionPosition right,
                                                            boolean includeRight)
    {
        return subMap(keys, left, includeLeft, right, includeRight);
    }

    private static <T> Map<PartitionPosition, T> subMap(ConcurrentNavigableMap<PartitionPosition, T> map,
                                                        PartitionPosition left,
                                                        boolean includeLeft,
                                                        PartitionPosition right,
                                                        boolean includeRight)
    {
        if (left != null && left.isMinimum())
            left = null;
        if (right != null && right.isMinimum())
            right = null;

        try
        {
            if (left == null)
                return right == null ? map : map.headMap(right, includeRight);
            return right == null
                   ? map.tailMap(left, includeLeft)
                   : map.subMap(left, includeLeft, right, includeRight);
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Invalid range requested {} - {}", left, right);
            throw e;
        }
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1024L * 1024 * 1024 * 1024 * 1024);
    }

    // ------------------------------------------------------------------------------ inner classes

    /**
     * What a window shard stores per partition. Two implementations: {@link TimeSeriesColumnarPartition}
     * (primitive column arrays, the common case) and {@link ObjectShardPartition} (the reference
     * {@link AtomicBTreePartition}, for counter tables and tables with non-frozen collections/UDTs).
     * Both are ordinary {@link Partition}s to every read path.
     */
    public interface ShardPartition extends Partition
    {
        /**
         * Applies {@code update}, adding its data-size delta to {@code liveDataSize}.
         *
         * @return the minimum timestamp delta between replaced and replacing cells, as
         *         {@code BTreePartitionUpdater#colUpdateTimeDelta} reports it.
         */
        long put(PartitionUpdate update, UpdateTransaction indexer, Cloner cloner, OpOrder.Group opGroup, AtomicLong liveDataSize);

        /**
         * A fully-materialized, stable view for the flush path. Unlike the read path this must not
         * leave a cached materialization behind, or flushing a memtable would rebuild — and pin —
         * the object form of every partition at once.
         */
        Partition flushView();

        /**
         * Whether a read restricted to {@code slices} can find anything here. Implementations answer
         * from the <b>clustering</b> values actually present (plus any deletion or static row, which
         * every slice must see) — never from the shard's write-time window, because clustering time
         * and write time are independent: a backfilled row carries an old clustering but lives in
         * the current write window's shard. Any doubt must answer {@code true}; skipping is only an
         * optimisation, and this default declines it.
         */
        default boolean mayContainRowsIn(Slices slices)
        {
            return true;
        }
    }

    /** The reference representation: one {@link AtomicBTreePartition}, exactly as the default memtables store it. */
    public static final class ObjectShardPartition implements ShardPartition
    {
        private final AtomicBTreePartition partition;

        /**
         * Comparator-least/greatest row clustering ever written here, for {@link #mayContainRowsIn};
         * null until the first row. Cloned onto plain heap because an update's buffers belong to the
         * write and are not guaranteed stable afterwards. Guarded by 'this'.
         */
        private Clustering<?> minClustering;
        private Clustering<?> maxClustering;

        ObjectShardPartition(TableMetadataRef metadata, DecoratedKey key, MemtableAllocator allocator)
        {
            this.partition = new AtomicBTreePartition(metadata, key, allocator);
        }

        @Override
        public long put(PartitionUpdate update, UpdateTransaction indexer, Cloner cloner, OpOrder.Group opGroup, AtomicLong liveDataSize)
        {
            // Widened before the rows land, so a concurrent sliced read can never see a row whose
            // clustering the bounds do not yet cover — too-wide bounds only cost a skipped skip.
            widenClusteringBounds(update);
            BTreePartitionUpdater updater = partition.addAll(update, cloner, opGroup, indexer);
            liveDataSize.addAndGet(updater.dataSize);
            return updater.colUpdateTimeDelta;
        }

        private synchronized void widenClusteringBounds(PartitionUpdate update)
        {
            Row last = update.lastRow();
            if (last == null)
                return;
            Row first = update.iterator().next();
            ClusteringComparator comparator = update.metadata().comparator;
            if (minClustering == null || comparator.compare(first.clustering(), minClustering) < 0)
                minClustering = HeapCloner.instance.clone(first.clustering());
            if (maxClustering == null || comparator.compare(last.clustering(), maxClustering) > 0)
                maxClustering = HeapCloner.instance.clone(last.clustering());
        }

        /** Same contract as the columnar override: prune by held clusterings only, fail open otherwise. */
        @Override
        public boolean mayContainRowsIn(Slices slices)
        {
            if (!partition.deletionInfo().isLive() || !partition.staticRow().isEmpty())
                return true;

            ClusteringComparator comparator = partition.metadata().comparator;
            if (comparator.size() == 0)
                return true;

            Clustering<?> min;
            Clustering<?> max;
            synchronized (this)
            {
                min = minClustering;
                max = maxClustering;
            }
            return min == null || slices.intersects(Slice.make(min, max));
        }

        @Override
        public Partition flushView()
        {
            return partition;
        }

        @Override
        public TableMetadata metadata()
        {
            return partition.metadata();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partition.partitionKey();
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return partition.partitionLevelDeletion();
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return partition.columns();
        }

        @Override
        public EncodingStats stats()
        {
            return partition.stats();
        }

        @Override
        public boolean isEmpty()
        {
            return partition.isEmpty();
        }

        @Override
        public boolean hasRows()
        {
            return partition.hasRows();
        }

        @Override
        public Row getRow(Clustering<?> clustering)
        {
            return partition.getRow(clustering);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator()
        {
            return partition.unfilteredIterator();
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
        {
            return partition.unfilteredIterator(selection, slices, reversed);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection,
                                                        NavigableSet<Clustering<?>> clusteringsInQueryOrder,
                                                        boolean reversed)
        {
            return partition.unfilteredIterator(selection, clusteringsInQueryOrder, reversed);
        }
    }

    /** One compaction window's partitions. */
    public static final class WindowShard
    {
        private final long windowStart;

        /**
         * Indexed by {@link PartitionPosition} only so that a token bound can be used to select a range;
         * {@link #put} only ever stores {@link DecoratedKey}s.
         */
        final ConcurrentNavigableMap<PartitionPosition, ShardPartition> partitions = new ConcurrentSkipListMap<>();

        @Unmetered  // shared schema metadata is not memory this memtable owns
        private final TableMetadataRef metadata;

        @Unmetered  // total pool size should not be included in the memtable's deep size
        private final MemtableAllocator allocator;

        /** Whether new partitions use the columnar representation; fixed at shard creation. */
        private final boolean columnar;

        WindowShard(long windowStart, TableMetadataRef metadata, MemtableAllocator allocator, boolean columnar)
        {
            this.windowStart = windowStart;
            this.metadata = metadata;
            this.allocator = allocator;
            this.columnar = columnar;
        }

        public long windowStart()
        {
            return windowStart;
        }

        public int size()
        {
            return partitions.size();
        }

        long put(DecoratedKey key,
                 PartitionUpdate update,
                 UpdateTransaction indexer,
                 Cloner cloner,
                 OpOrder.Group opGroup,
                 boolean assumeMissing,
                 AtomicLong liveDataSize)
        {
            ShardPartition previous = assumeMissing ? null : partitions.get(key);

            if (previous == null)
            {
                // The key is already cloned and interned by the memtable, so every shard shares it.
                ShardPartition empty = columnar
                                       ? new TimeSeriesColumnarPartition(metadata, key, allocator)
                                       : new ObjectShardPartition(metadata, key, allocator);
                previous = partitions.putIfAbsent(key, empty);
                if (previous == null)
                {
                    previous = empty;
                    allocator.onHeap().allocate(SkipListMemtable.ROW_OVERHEAD_HEAP_SIZE, opGroup);
                    liveDataSize.addAndGet(8);
                }
            }

            return previous.put(update, indexer, cloner, opGroup, liveDataSize);
        }

        Map<PartitionPosition, ShardPartition> subMap(PartitionPosition left,
                                                      boolean includeLeft,
                                                      PartitionPosition right,
                                                      boolean includeRight)
        {
            return TimeSeriesMemtable.subMap(partitions, left, includeLeft, right, includeRight);
        }
    }

    /**
     * @return {@code partition} with the rows {@link ColdWindowChunkFlush} already chunked filtered
     * out, or the partition itself when it has nothing to omit.
     */
    private static Partition omitChunkedRows(Partition partition, ColdWindowChunkFlush.RowOmissions omissions)
    {
        return omissions.containsKey(partition.partitionKey())
               ? new ChunkOmittedPartition(partition, omissions)
               : partition;
    }

    /**
     * A flushing partition minus the rows a durable chunk already carries. Only rows are ever
     * filtered: partitions holding any tombstone are never chunked at all (see
     * {@link ColdWindowChunkFlush}), and static rows always flush normally, so deletions and static
     * content pass through untouched.
     */
    private static final class ChunkOmittedPartition implements Partition
    {
        private final Partition delegate;
        private final ColdWindowChunkFlush.RowOmissions omissions;

        ChunkOmittedPartition(Partition delegate, ColdWindowChunkFlush.RowOmissions omissions)
        {
            this.delegate = delegate;
            this.omissions = omissions;
        }

        @Override
        public TableMetadata metadata()
        {
            return delegate.metadata();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return delegate.partitionKey();
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return delegate.partitionLevelDeletion();
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            // The delegate's set may name columns only omitted rows carried; a superset is legal for
            // the serialization header, an undercount would not be.
            return delegate.columns();
        }

        @Override
        public EncodingStats stats()
        {
            return delegate.stats();
        }

        @Override
        public boolean isEmpty()
        {
            // fullyOmitted is only ever true for a partition with no deletion and no static row, so
            // nothing but the omitted rows could make it non-empty.
            return omissions.fullyOmitted(partitionKey()) || delegate.isEmpty();
        }

        @Override
        public boolean hasRows()
        {
            return !omissions.fullyOmitted(partitionKey()) && delegate.hasRows();
        }

        @Override
        public Row getRow(Clustering<?> clustering)
        {
            return omissions.shouldOmit(partitionKey(), clustering) ? null : delegate.getRow(clustering);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator()
        {
            return filter(delegate.unfilteredIterator());
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
        {
            return filter(delegate.unfilteredIterator(columns, slices, reversed));
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns,
                                                        NavigableSet<Clustering<?>> clusteringsInQueryOrder,
                                                        boolean reversed)
        {
            return filter(delegate.unfilteredIterator(columns, clusteringsInQueryOrder, reversed));
        }

        private UnfilteredRowIterator filter(UnfilteredRowIterator iterator)
        {
            DecoratedKey key = partitionKey();
            return Transformation.apply(iterator, new Transformation<UnfilteredRowIterator>()
            {
                @Override
                protected Row applyToRow(Row row)
                {
                    return omissions.shouldOmit(key, row.clustering()) ? null : row;
                }
            });
        }
    }

    /**
     * One partition key's versions from several window shards, presented as a single partition. Only
     * the flush path builds these — reads go through the iterator merges above.
     */
    private static final class MergedWindowPartition implements Partition
    {
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final List<Partition> sources;

        MergedWindowPartition(TableMetadata metadata, DecoratedKey key, List<Partition> sources)
        {
            this.metadata = metadata;
            this.key = key;
            this.sources = sources;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            DeletionTime max = DeletionTime.LIVE;
            for (Partition source : sources)
            {
                DeletionTime candidate = source.partitionLevelDeletion();
                if (candidate.supersedes(max))
                    max = candidate;
            }
            return max;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            RegularAndStaticColumns columns = RegularAndStaticColumns.NONE;
            for (Partition source : sources)
                columns = columns.mergeTo(source.columns());
            return columns;
        }

        @Override
        public EncodingStats stats()
        {
            EncodingStats stats = EncodingStats.NO_STATS;
            for (Partition source : sources)
                stats = stats.mergeWith(source.stats());
            return stats;
        }

        @Override
        public boolean isEmpty()
        {
            for (Partition source : sources)
                if (!source.isEmpty())
                    return false;
            return true;
        }

        @Override
        public boolean hasRows()
        {
            for (Partition source : sources)
                if (source.hasRows())
                    return true;
            return false;
        }

        @Override
        public Row getRow(Clustering<?> clustering)
        {
            try (UnfilteredRowIterator iterator = unfilteredIterator(ColumnFilter.selection(columns()),
                                                                     FBUtilities.singleton(clustering, metadata.comparator),
                                                                     false))
            {
                while (iterator.hasNext())
                {
                    Unfiltered unfiltered = iterator.next();
                    if (unfiltered.isRow())
                        return (Row) unfiltered;
                }
            }
            return null;
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator()
        {
            List<UnfilteredRowIterator> iterators = new ArrayList<>(sources.size());
            for (Partition source : sources)
                iterators.add(source.unfilteredIterator());
            return UnfilteredRowIterators.merge(iterators);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
        {
            List<UnfilteredRowIterator> iterators = new ArrayList<>(sources.size());
            for (Partition source : sources)
                iterators.add(source.unfilteredIterator(columns, slices, reversed));
            return UnfilteredRowIterators.merge(iterators);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns,
                                                        NavigableSet<Clustering<?>> clusteringsInQueryOrder,
                                                        boolean reversed)
        {
            List<UnfilteredRowIterator> iterators = new ArrayList<>(sources.size());
            for (Partition source : sources)
                iterators.add(source.unfilteredIterator(columns, clusteringsInQueryOrder, reversed));
            return UnfilteredRowIterators.merge(iterators);
        }
    }

    private static final class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<ShardPartition> iterator;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                            Iterator<ShardPartition> iterator,
                                            ColumnFilter columnFilter,
                                            DataRange dataRange)
        {
            this.metadata = metadata;
            this.iterator = iterator;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            ShardPartition partition = iterator.next();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(partition.partitionKey());
            return filter.getUnfilteredRowIterator(columnFilter, partition);
        }
    }

    /**
     * Passes every indexing event through but swallows {@code start} and {@code commit}, so that an
     * update split across several shards is still one index transaction. The real pair is issued by
     * {@link #put} around the whole update.
     */
    private static final class SingleScopeTransaction implements UpdateTransaction
    {
        private final UpdateTransaction delegate;

        SingleScopeTransaction(UpdateTransaction delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void start()
        {
        }

        @Override
        public void commit()
        {
        }

        @Override
        public void onPartitionDeletion(DeletionTime deletionTime)
        {
            delegate.onPartitionDeletion(deletionTime);
        }

        @Override
        public void onRangeTombstone(RangeTombstone rangeTombstone)
        {
            delegate.onRangeTombstone(rangeTombstone);
        }

        @Override
        public void onInserted(Row row)
        {
            delegate.onInserted(row);
        }

        @Override
        public void onUpdated(Row existing, Row updated)
        {
            delegate.onUpdated(existing, updated);
        }
    }

    public static class Factory implements Memtable.Factory
    {
        static final Factory INSTANCE = new Factory();

        @Override
        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadataRef,
                               Memtable.Owner owner)
        {
            TableMetadata metadata = metadataRef.get();
            String reason = unsupportedReason(metadata);
            if (reason != null)
            {
                // Keyed per table, not per memtable: a busy table flushes constantly, and one line
                // per flush for a condition that only changes on ALTER buries the line that matters.
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                 "timeseries-memtable-unsupported:" + metadata.keyspace + '.' + metadata.name,
                                 1, TimeUnit.HOURS,
                                 "memtable = 'timeseries' is set on {}.{} but cannot be used: {}. Falling " +
                                 "back to the default memtable — writes are unaffected, only the " +
                                 "window-sharding optimisation is lost.",
                                 metadata.keyspace, metadata.name, reason);
                return SkipListMemtableFactory.INSTANCE.create(commitLogLowerBound, metadataRef, owner);
            }

            return new TimeSeriesMemtable(commitLogLowerBound, metadataRef, owner);
        }

        @Override
        public boolean equals(Object o)
        {
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getClass());
        }
    }
}
