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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.FBUtilities.nowInSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end tests for TSCS T3's flush-time window splitting: a memtable whose write timestamps
 * span multiple time windows must flush into one sstable per window, each fully contained in its
 * window, with partition-level deletions preserved across the split. Exercises the real flush path
 * (Flushing → ColumnFamilyStore.createSSTableMultiWriter → strategy override).
 */
public class TimeWindowSplittingMultiWriterTest extends SchemaLoader
{
    private static final String KEYSPACE1 = "TimeWindowSplittingMultiWriterTest";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STATIC = "StandardStatic";
    private static final long MINUTE_MS = 60_000L;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        ServerTestUtils.prepareServer();
        // TSCS must be the SCHEMA-level strategy: cfs.setCompactionParameters is only a local override
        // that a CompactionStrategyManager reload (triggered e.g. by the first flush's disk-boundary
        // change) silently reverts to the schema params - later flushes would stop splitting.
        // timestamp_resolution=MILLISECONDS: raw millisecond write timestamps below are read directly,
        // same convention as the other TSCS tests. 1-minute windows keep the numbers small.
        CompactionParams tscs = CompactionParams.create(TimeSeriesCompactionStrategy.class,
                                                        ImmutableMap.of("timestamp_resolution", "MILLISECONDS",
                                                                        "window_size", "1m",
                                                                        "freeze_after", "1m"));
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1).compaction(tscs),
                                    SchemaLoader.staticCFMD(KEYSPACE1, CF_STATIC).compaction(tscs));
    }

    private static ColumnFamilyStore prepare()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        return cfs;
    }

    private static long windowOf(long ms)
    {
        return ms - Math.floorMod(ms, MINUTE_MS);
    }

    private static void write(ColumnFamilyStore cfs, String key, String clustering, long writeTsMillis)
    {
        new RowUpdateBuilder(cfs.metadata(), writeTsMillis, key)
            .clustering(clustering)
            .add("val", ByteBuffer.wrap(new byte[8]))
            .build()
            .applyUnsafe();
    }

    @Test
    public void spanningFlushSplitsPerWindow()
    {
        ColumnFamilyStore cfs = prepare();
        long now = System.currentTimeMillis();
        long oldWindowTs = now - 10 * MINUTE_MS;       // backfill written with an old timestamp
        write(cfs, "tag-1", "c1", oldWindowTs);
        write(cfs, "tag-1", "c2", now);                // same partition, current window
        write(cfs, "tag-2", "c1", now);                // different partition, current window
        Util.flush(cfs);

        assertEquals(2, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            // timestamp_resolution=MILLISECONDS: raw metadata timestamps are already milliseconds
            assertEquals("sstable must be fully contained in one window: " + sstable,
                         windowOf(sstable.getMinTimestamp()),
                         windowOf(sstable.getMaxTimestamp()));
        }
    }

    @Test
    public void singleWindowFlushStaysSingle()
    {
        ColumnFamilyStore cfs = prepare();
        long now = System.currentTimeMillis();
        long base = windowOf(now - 5 * MINUTE_MS);
        write(cfs, "tag-1", "c1", base + 1);
        write(cfs, "tag-1", "c2", base + 2);
        write(cfs, "tag-2", "c1", base + 3);
        Util.flush(cfs);

        assertEquals(1, cfs.getLiveSSTables().size());
    }

    @Test
    public void partitionDeletionSurvivesTheSplit()
    {
        ColumnFamilyStore cfs = prepare();
        long now = System.currentTimeMillis();
        long oldWindowTs = now - 10 * MINUTE_MS;
        DecoratedKey key = Util.dk("tag-del");

        write(cfs, "tag-del", "old", oldWindowTs);
        // Partition deletion stamped AFTER the old row but BEFORE the new one: the old row must stay
        // deleted and the new row visible. The deletion is routed to the window of its OWN timestamp,
        // exactly once - not replicated into every window, which would stamp an old deletion timestamp
        // into newer windows' sstable metadata and leave them permanently spanning. Read-time partition
        // merge applies it across windows anyway, and whole-window retention drops proceed oldest-first,
        // so by the time the deletion's window is dropped every window it could shadow is already gone.
        // The memtable may legitimately drop the shadowed old row before flush, so the sstable count is
        // not asserted, only window containment and read semantics.
        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata(), key, oldWindowTs + 1, nowInSeconds())).applyUnsafe();
        write(cfs, "tag-del", "new", now);
        Util.flush(cfs);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertEquals(windowOf(sstable.getMinTimestamp()), windowOf(sstable.getMaxTimestamp()));
        assertEquals(1, Util.getOnlyPartition(Util.cmd(cfs, key).build()).rowCount());
    }

    /**
     * The C1 reproduction, end to end and in ordinary CQL terms:
     * <pre>
     *   INSERT INTO t (key, name, val)  VALUES (...) USING TIMESTAMP &lt;old&gt;;
     *   UPDATE t USING TIMESTAMP &lt;new&gt; SET val0 = ... WHERE key = ... AND name = ...;
     * </pre>
     * The memtable merges the two statements into ONE row whose cell timestamps straddle a window
     * boundary. Routing the whole row by its max timestamp put the old cell's timestamp into the new
     * window's sstable, so that sstable's min and max fell in different windows: it classified FREEZING
     * for ever, {@code nextSplitRefreezeCandidate} re-selected it on every background round, and each
     * rewrite produced an identical still-spanning output under a new generation. Routing per cell must
     * produce contained sstables, and the next background round must have nothing to do.
     */
    @Test
    public void straddlingRowSplitsPerCellAndLeavesNothingToDo()
    {
        ColumnFamilyStore cfs = prepare();
        long now = System.currentTimeMillis();
        long oldWindowTs = now - 10 * MINUTE_MS;
        DecoratedKey key = Util.dk("tag-straddle");

        new RowUpdateBuilder(cfs.metadata(), oldWindowTs, "tag-straddle")
            .clustering("c1").add("val", ByteBuffer.wrap(new byte[8])).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), now, "tag-straddle")
            .clustering("c1").add("val0", ByteBuffer.wrap(new byte[8])).build().applyUnsafe();
        Util.flush(cfs);

        assertEquals(2, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertEquals("sstable must be fully contained in one window: " + sstable,
                         windowOf(sstable.getMinTimestamp()), windowOf(sstable.getMaxTimestamp()));

        // Both cells of the split row are still readable as one row.
        assertEquals(1, Util.getOnlyPartition(Util.cmd(cfs, key).build()).rowCount());

        // Every window is now a single contained sstable, so the strategy has no work: no split-refreeze,
        // no freeze. This is the assertion the loop used to fail.
        assertTrue(cfs.getCompactionStrategyManager().getNextBackgroundTasks(nowInSeconds()).isEmpty());
    }

    /**
     * A partition deletion carrying a static row written in a different window: the second path into the
     * same loop. The header used to be placed at max(partitionDeletion, staticRow), writing the old
     * deletion timestamp into the new window's sstable.
     */
    @Test
    public void partitionDeletionAndStaticRowRouteIndependently()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STATIC);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        long now = System.currentTimeMillis();
        long oldWindowTs = now - 10 * MINUTE_MS;
        DecoratedKey key = Util.dk("tag-static");

        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata(), key, oldWindowTs, nowInSeconds())).applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), now, "tag-static").add("val", "unit").build().applyUnsafe();
        Util.flush(cfs);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertEquals("sstable must be fully contained in one window: " + sstable,
                         windowOf(sstable.getMinTimestamp()), windowOf(sstable.getMaxTimestamp()));
        assertTrue(cfs.getCompactionStrategyManager().getNextBackgroundTasks(nowInSeconds()).isEmpty());
    }

    /**
     * The failure path neither this writer nor {@code ShardedMultiWriter} had a test for: abort must
     * untrack every per-window writer on the transaction before aborting it. RangeAwareSSTableWriter
     * aborts a zero-byte per-directory writer while its siblings commit on the same transaction, so a
     * deleted file that keeps its ADD record in the LogFile becomes leftover-verification noise (or a
     * failure) on the next restart.
     */
    @Test
    public void abortUntracksEveryWindowWriter()
    {
        ColumnFamilyStore cfs = prepare();
        long now = System.currentTimeMillis();
        ILifecycleTransaction txn = Mockito.mock(ILifecycleTransaction.class);
        // The writer reads these off the transaction while building; a bare mock returns null and the
        // builder refuses ("The requested resource has not been initialized yet").
        Mockito.when(txn.opType()).thenReturn(OperationType.FLUSH);
        Mockito.when(txn.opId()).thenReturn(TimeUUID.Generator.nextTimeUUID());

        TimeWindowSplittingMultiWriter writer =
            new TimeWindowSplittingMultiWriter(cfs,
                                               cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables()),
                                               2, 0, null, false, null, 0,
                                               SerializationHeader.makeWithoutStats(cfs.metadata()),
                                               List.of(), txn,
                                               TimeWindowSplittingMultiWriterTest::windowOf,
                                               TimeUnit.MILLISECONDS);
        try
        {
            writer.append(twoWindowPartition(cfs, now - 10 * MINUTE_MS, now));
            assertEquals(2, writer.finished().size());
            writer.abort(null);
            Mockito.verify(txn, Mockito.times(2)).untrackNew(Mockito.any(SSTableWriter.class));
        }
        finally
        {
            writer.close();
        }
    }

    /** One partition with two rows whose write timestamps fall in different windows. */
    private static UnfilteredRowIterator twoWindowPartition(ColumnFamilyStore cfs, long oldTs, long newTs)
    {
        ColumnMetadata val = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));
        Row older = BTreeRow.singleCellRow(Clustering.make(AsciiType.instance.decompose("c1")),
                                           BufferCell.live(val, oldTs, ByteBuffer.wrap(new byte[8])));
        Row newer = BTreeRow.singleCellRow(Clustering.make(AsciiType.instance.decompose("c2")),
                                           BufferCell.live(val, newTs, ByteBuffer.wrap(new byte[8])));
        return new Util.UnfilteredSource(cfs.metadata(), Util.dk("tag-abort"), null,
                                         List.<Unfiltered>of(older, newer).iterator());
    }

    /**
     * C2: the writer cap has to hold <em>inside</em> one partition.
     * <p>
     * It used to be evaluated once per {@code append()} - that is, once per partition - after which
     * {@code writerFor} created writers with no further check. So the first partition of a flush opened
     * one writer per window, unbounded: a 10 MB partition holding one row an hour for ten years wants
     * ~87,600 concurrent {@code SSTableWriter}s (each with data, index, filter and CRC descriptors plus
     * buffers), on the memtable-flush path, and the routing-buffer budget never trips because the
     * partition is nowhere near 64 MiB. Here one partition spans ten windows with a cap of three; before
     * the fix this flushed ten sstables.
     */
    @Test
    public void oneOversizedPartitionCannotExceedTheWriterCap()
    {
        ColumnFamilyStore cfs = prepare();
        int saved = TimeWindowSplittingMultiWriter.maxWindowWriters;
        try
        {
            TimeWindowSplittingMultiWriter.maxWindowWriters = 3;
            long now = System.currentTimeMillis();
            for (int i = 0; i < 10; i++)                 // ONE partition, ten distinct write-timestamp windows
                write(cfs, "tag-cap", "c" + i, now - i * MINUTE_MS);
            Util.flush(cfs);

            assertEquals("one partition must not open more writers than the cap", 3, cfs.getLiveSSTables().size());
            // Folding windows together costs containment, not data: every row is still readable.
            assertEquals(10, Util.getOnlyPartition(Util.cmd(cfs, Util.dk("tag-cap")).build()).rowCount());
        }
        finally
        {
            TimeWindowSplittingMultiWriter.maxWindowWriters = saved;
        }
    }

    @Test
    public void allRowsReadableAfterSplit()
    {
        ColumnFamilyStore cfs = prepare();
        long now = System.currentTimeMillis();
        for (int i = 0; i < 6; i++)
            write(cfs, "tag-r", "c" + i, now - i * 3 * MINUTE_MS);
        Util.flush(cfs);

        assertEquals(6, cfs.getLiveSSTables().size());
        assertEquals(6, Util.getOnlyPartition(Util.cmd(cfs, Util.dk("tag-r")).build()).rowCount());
    }
}
