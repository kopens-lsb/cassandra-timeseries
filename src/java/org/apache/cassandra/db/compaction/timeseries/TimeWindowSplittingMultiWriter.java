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
import org.apache.cassandra.utils.TimeUUID;

/**
 * An {@link SSTableMultiWriter} that splits incoming partitions at time-window boundaries so every
 * output sstable is fully contained in one window on the write-timestamp axis (TSCS T3, design spec
 * section 4). Used for both memtable flush and streaming, which share the
 * {@code ColumnFamilyStore.createSSTableMultiWriter} hook.
 *
 * Per-window writers are created lazily in the flush directory; the partition-level deletion and
 * static row are replicated into every window output containing the partition (plan D3), so a
 * whole-window drop can never lose deletion metadata covering other windows. Unlike
 * {@link org.apache.cassandra.db.compaction.unified.ShardedMultiWriter} this splits WITHIN
 * partitions (routing every row/marker), not just between them.
 */
public class TimeWindowSplittingMultiWriter implements SSTableMultiWriter
{
    private final ColumnFamilyStore cfs;
    private final Descriptor descriptor;
    private final long keyCount;
    private final long repairedAt;
    private final TimeUUID pendingRepair;
    private final boolean isTransient;
    private final IntervalSet<CommitLogPosition> commitLogPositions;
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

        // Header placement (deletion/static once, in its own window) is WindowRoutingIterator's
        // contract - see slices() for the rationale (plan D3, revised).
        for (Map.Entry<Long, UnfilteredRowIterator> entry : WindowRoutingIterator.slices(partition, windowStartOfMillis, tableResolution).entrySet())
            writerFor(entry.getKey()).append(entry.getValue());
    }

    private SSTableWriter writerFor(long windowStart)
    {
        SSTableWriter writer = writers.get(windowStart);
        if (writer == null)
        {
            Descriptor desc = writers.isEmpty() ? descriptor : cfs.newSSTableDescriptor(descriptor.directory);
            writer = createWriter(desc);
            writers.put(windowStart, writer);
        }
        return writer;
    }

    private SSTableWriter createWriter(Descriptor desc)
    {
        MetadataCollector metadataCollector = new MetadataCollector(cfs.metadata().comparator)
                                              .commitLogIntervals(commitLogPositions != null ? commitLogPositions : IntervalSet.empty());
        return desc.getFormat().getWriterFactory().builder(desc)
                   .setKeyCount(keyCount)
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
            t = writer.abort(t);
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
