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
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
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

    TimeSeriesMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound,
                       TableMetadataRef metadataRef,
                       Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
        this.options = optionsOf(metadataRef.get());
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

        WindowShard created = new WindowShard(windowStart, metadata, allocator);
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

        List<UnfilteredRowIterator> iterators = new ArrayList<>(2);
        for (WindowShard shard : shards.values())
        {
            AtomicBTreePartition partition = shard.partitions.get(key);
            if (partition != null)
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
            AtomicBTreePartition partition = shard.partitions.get(key);
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
     * The merged view over the range, exactly as an unsharded memtable would present it: one entry per
     * distinct partition key, with the shards' versions of that partition merged. Splitting the flush
     * into one sstable per window is a separate step and deliberately not done here, so that any
     * difference this memtable makes to a flushed sstable is a sharding defect and nothing else.
     */
    @Override
    public FlushablePartitionSet<Partition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
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
                return mergedPartitions(from, true, to, false);
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
        List<Iterator<AtomicBTreePartition>> perShard = new ArrayList<>(shards.size());
        for (WindowShard shard : shards.values())
            perShard.add(shard.subMap(from, includeFrom, to, includeTo).values().iterator());

        PeekingIterator<AtomicBTreePartition> merged =
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

                AtomicBTreePartition first = merged.next();
                DecoratedKey key = first.partitionKey();
                if (!merged.hasNext() || !merged.peek().partitionKey().equals(key))
                    return first;

                List<AtomicBTreePartition> sameKey = new ArrayList<>(2);
                sameKey.add(first);
                while (merged.hasNext() && merged.peek().partitionKey().equals(key))
                    sameKey.add(merged.next());
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

    /** One compaction window's partitions, stored exactly as the default memtable stores them. */
    public static final class WindowShard
    {
        private final long windowStart;

        /**
         * Indexed by {@link PartitionPosition} only so that a token bound can be used to select a range;
         * {@link #put} only ever stores {@link DecoratedKey}s.
         */
        final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();

        private final TableMetadataRef metadata;

        @Unmetered  // total pool size should not be included in the memtable's deep size
        private final MemtableAllocator allocator;

        WindowShard(long windowStart, TableMetadataRef metadata, MemtableAllocator allocator)
        {
            this.windowStart = windowStart;
            this.metadata = metadata;
            this.allocator = allocator;
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
            AtomicBTreePartition previous = assumeMissing ? null : partitions.get(key);

            long initialSize = 0;
            if (previous == null)
            {
                // The key is already cloned and interned by the memtable, so every shard shares it.
                AtomicBTreePartition empty = new AtomicBTreePartition(metadata, key, allocator);
                previous = partitions.putIfAbsent(key, empty);
                if (previous == null)
                {
                    previous = empty;
                    allocator.onHeap().allocate(SkipListMemtable.ROW_OVERHEAD_HEAP_SIZE, opGroup);
                    initialSize = 8;
                }
            }

            BTreePartitionUpdater updater = previous.addAll(update, cloner, opGroup, indexer);
            liveDataSize.addAndGet(initialSize + updater.dataSize);
            return updater.colUpdateTimeDelta;
        }

        Map<PartitionPosition, AtomicBTreePartition> subMap(PartitionPosition left,
                                                            boolean includeLeft,
                                                            PartitionPosition right,
                                                            boolean includeRight)
        {
            return TimeSeriesMemtable.subMap(partitions, left, includeLeft, right, includeRight);
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
        private final List<AtomicBTreePartition> sources;

        MergedWindowPartition(TableMetadata metadata, DecoratedKey key, List<AtomicBTreePartition> sources)
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
            for (AtomicBTreePartition source : sources)
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
            for (AtomicBTreePartition source : sources)
                columns = columns.mergeTo(source.columns());
            return columns;
        }

        @Override
        public EncodingStats stats()
        {
            EncodingStats stats = EncodingStats.NO_STATS;
            for (AtomicBTreePartition source : sources)
                stats = stats.mergeWith(source.stats());
            return stats;
        }

        @Override
        public boolean isEmpty()
        {
            for (AtomicBTreePartition source : sources)
                if (!source.isEmpty())
                    return false;
            return true;
        }

        @Override
        public boolean hasRows()
        {
            for (AtomicBTreePartition source : sources)
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
            for (AtomicBTreePartition source : sources)
                iterators.add(source.unfilteredIterator());
            return UnfilteredRowIterators.merge(iterators);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
        {
            List<UnfilteredRowIterator> iterators = new ArrayList<>(sources.size());
            for (AtomicBTreePartition source : sources)
                iterators.add(source.unfilteredIterator(columns, slices, reversed));
            return UnfilteredRowIterators.merge(iterators);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns,
                                                        NavigableSet<Clustering<?>> clusteringsInQueryOrder,
                                                        boolean reversed)
        {
            List<UnfilteredRowIterator> iterators = new ArrayList<>(sources.size());
            for (AtomicBTreePartition source : sources)
                iterators.add(source.unfilteredIterator(columns, clusteringsInQueryOrder, reversed));
            return UnfilteredRowIterators.merge(iterators);
        }
    }

    private static final class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<AtomicBTreePartition> iterator;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                            Iterator<AtomicBTreePartition> iterator,
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
            AtomicBTreePartition partition = iterator.next();
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
