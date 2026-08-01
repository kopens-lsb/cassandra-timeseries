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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeUUID;

/**
 * An {@link SSTableMultiWriter} that splits incoming partitions at time-window boundaries so every
 * output sstable is fully contained in one window on the write-timestamp axis (TSCS T3, design spec
 * section 4). Used for both memtable flush and streaming, which share the
 * {@code ColumnFamilyStore.createSSTableMultiWriter} hook.
 *
 * Per-window writers are created lazily in the flush directory. The partition-level deletion and the
 * static row are NOT replicated into every window: each is routed exactly once, to the window its own
 * write timestamp names (static cells individually) — see {@link WindowRoutingIterator} for why, and
 * for why that is safe against whole-window retention drops. Unlike
 * {@link org.apache.cassandra.db.compaction.unified.ShardedMultiWriter} this splits WITHIN
 * partitions (routing every timestamped element), not just between them.
 */
public class TimeWindowSplittingMultiWriter implements SSTableMultiWriter
{
    private static final Logger logger = LoggerFactory.getLogger(TimeWindowSplittingMultiWriter.class);

    /**
     * Cap on concurrently-open per-window writers. Each writer holds data, index, filter and CRC file
     * descriptors plus write buffers, so an unbounded fan-out (a backfill flush or a legacy spanning
     * stream covering a year at {@code window_size=1h} wants ~8,760) exhausts file descriptors or heap.
     *
     * <p>Beyond the cap, further windows are <em>snapped</em> onto an already-open writer instead of
     * failing: routing is asked for the snapped window key, so each partition still yields at most one
     * slice per writer and nothing has to be merged. The resulting sstable spans windows and classifies
     * FREEZING, and {@code SplitRefreezeCompactionTask} re-splits it later from a smaller input — each
     * pass peels off up to {@code maxWindowWriters} windows, so the process converges rather than
     * looping. Failing the write instead was rejected: this writer is on the memtable-flush and
     * bootstrap-stream paths, where an exception blocks writes or aborts a bootstrap.
     */
    @VisibleForTesting
    static volatile int maxWindowWriters = 1000;

    private final ColumnFamilyStore cfs;
    private final Descriptor descriptor;
    private final long keyCount;
    private final long repairedAt;
    private final TimeUUID pendingRepair;
    private final boolean isTransient;
    private final IntervalSet<CommitLogPosition> commitLogPositions;
    private final int sstableLevel;
    private final SerializationHeader header;
    private final Collection<Index.Group> indexGroups;
    private final ILifecycleTransaction txn;
    private final LongUnaryOperator windowStartOfMillis;
    private final TimeUnit tableResolution;
    private final NavigableMap<Long, SSTableWriter> writers = new TreeMap<>();

    public TimeWindowSplittingMultiWriter(ColumnFamilyStore cfs,
                                          Descriptor descriptor,
                                          long keyCount,
                                          long repairedAt,
                                          TimeUUID pendingRepair,
                                          boolean isTransient,
                                          IntervalSet<CommitLogPosition> commitLogPositions,
                                          int sstableLevel,
                                          SerializationHeader header,
                                          Collection<Index.Group> indexGroups,
                                          ILifecycleTransaction txn,
                                          LongUnaryOperator windowStartOfMillis,
                                          TimeUnit tableResolution)
    {
        this.cfs = cfs;
        this.descriptor = descriptor;
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.isTransient = isTransient;
        this.commitLogPositions = commitLogPositions;
        this.sstableLevel = sstableLevel;
        this.header = header;
        this.indexGroups = indexGroups;
        this.txn = txn;
        this.windowStartOfMillis = windowStartOfMillis;
        this.tableResolution = tableResolution;
    }

    @Override
    public void append(UnfilteredRowIterator partition)
    {
        if (partition.isReverseOrder())
            throw new IllegalStateException("Window-splitting writer only supports forward iteration");

        // Header placement (deletion and static cells routed by their own timestamps) is
        // WindowRoutingIterator's contract - see slices() for the rationale.
        for (Map.Entry<Long, UnfilteredRowIterator> entry : WindowRoutingIterator.slices(partition, routingFunction(), tableResolution).entrySet())
            writerFor(entry.getKey()).append(entry.getValue());
    }

    /**
     * The window function routing is asked to use. Below the writer cap this is the strategy's own
     * window arithmetic; at the cap it additionally snaps unknown windows onto an open writer, so a
     * partition can never produce two slices targeting one writer (which would append the same
     * partition key twice).
     */
    private LongUnaryOperator routingFunction()
    {
        if (writers.size() < maxWindowWriters)
            return windowStartOfMillis;

        NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, "window-writer-cap", 1, TimeUnit.MINUTES,
                         "{}.{} reached the cap of {} concurrent window writers while writing {}; further " +
                         "windows are folded onto open writers, producing window-spanning sstables that a " +
                         "later split-refreeze has to break up. window_size is very likely far too small for " +
                         "the write-timestamp spread of this data.",
                         cfs.getKeyspaceName(), cfs.getTableName(), maxWindowWriters, descriptor);

        return millis -> {
            long window = windowStartOfMillis.applyAsLong(millis);
            if (writers.containsKey(window))
                return window;
            Map.Entry<Long, SSTableWriter> floor = writers.floorEntry(window);
            return floor != null ? floor.getKey() : writers.firstKey();
        };
    }

    private SSTableWriter writerFor(long windowStart)
    {
        SSTableWriter writer = writers.get(windowStart);
        if (writer == null)
        {
            Descriptor desc = writers.isEmpty() ? descriptor : cfs.newSSTableDescriptor(descriptor.directory);
            writer = createWriter(desc, writers.size() + 1);
            writers.put(windowStart, writer);
        }
        return writer;
    }

    /**
     * @param writerOrdinal 1-based index of the writer being created, used to taper the per-writer key
     *        estimate. Unlike {@link org.apache.cassandra.db.compaction.unified.ShardedMultiWriter},
     *        whose shards partition the key space so that {@code keyCount / shards} is exact, windows
     *        do <em>not</em>: the same partition key can legitimately appear in every window, so
     *        dividing by the final writer count would under-size every bloom filter. Tapering as
     *        {@code keyCount / ordinal} keeps the first (overwhelmingly the largest, in steady state)
     *        writer at the full estimate while bounding the aggregate at {@code keyCount * ln(N)}
     *        instead of {@code keyCount * N}.
     */
    private SSTableWriter createWriter(Descriptor desc, int writerOrdinal)
    {
        MetadataCollector metadataCollector = new MetadataCollector(cfs.metadata().comparator)
                                              .sstableLevel(sstableLevel)
                                              .commitLogIntervals(commitLogPositions != null ? commitLogPositions : IntervalSet.empty());
        return desc.getFormat().getWriterFactory().builder(desc)
                   .setKeyCount(Math.max(1, keyCount / writerOrdinal))
                   .setRepairedAt(repairedAt)
                   .setPendingRepair(pendingRepair)
                   .setTransientSSTable(isTransient)
                   .setTableMetadataRef(cfs.metadata)
                   .setMetadataCollector(metadataCollector)
                   .setSerializationHeader(header)
                   .addDefaultComponents(indexGroups)
                   .setSecondaryIndexGroups(indexGroups)
                   .setCompressionDictionaryManager(cfs.compressionDictionaryManager())
                   .build(txn, cfs);
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult)
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.size());
        for (SSTableWriter writer : writers.values())
            sstables.add(writer.finish(openResult));
        return sstables;
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        List<SSTableReader> sstables = new ArrayList<>(writers.size());
        for (SSTableWriter writer : writers.values())
            sstables.add(writer.finished());
        return sstables;
    }

    @Override
    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        for (SSTableWriter writer : writers.values())
            writer.setOpenResult(openResult);
        return this;
    }

    @Override
    public String getFilename()
    {
        return writers.isEmpty() ? descriptor.baseFile().toString() : writers.firstEntry().getValue().getFilename();
    }

    @Override
    public long getBytesWritten()
    {
        long bytes = 0;
        for (SSTableWriter writer : writers.values())
            bytes += writer.getFilePointer();
        return bytes;
    }

    @Override
    public long getOnDiskBytesWritten()
    {
        long bytes = 0;
        for (SSTableWriter writer : writers.values())
            bytes += writer.getEstimatedOnDiskBytesWritten();
        return bytes;
    }

    @Override
    public long getTotalRows()
    {
        long rows = 0;
        for (SSTableWriter writer : writers.values())
            rows += writer.getTotalRows();
        return rows;
    }

    @Override
    public TableId getTableId()
    {
        return cfs.metadata().id;
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        Throwable t = accumulate;
        for (SSTableWriter writer : writers.values())
            t = writer.commit(t);
        return t;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        Throwable t = accumulate;
        for (SSTableWriter writer : writers.values())
        {
            // Untrack before aborting, exactly as SimpleSSTableMultiWriter and ShardedMultiWriter do:
            // RangeAwareSSTableWriter can abort a zero-byte per-directory writer while its siblings
            // commit on the same transaction, and a deleted file that keeps its ADD record in the
            // LogFile turns into leftover-verification noise (or a failure) on the next restart.
            txn.untrackNew(writer);
            t = writer.abort(t);
        }
        return t;
    }

    @Override
    public void prepareToCommit()
    {
        for (SSTableWriter writer : writers.values())
            writer.prepareToCommit();
    }

    @Override
    public void close()
    {
        for (SSTableWriter writer : writers.values())
            writer.close();
    }

}
