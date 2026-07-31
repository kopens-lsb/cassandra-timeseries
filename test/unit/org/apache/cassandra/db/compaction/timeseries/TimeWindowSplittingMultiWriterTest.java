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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.utils.FBUtilities.nowInSeconds;
import static org.junit.Assert.assertEquals;

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
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1)
                                                .compaction(CompactionParams.create(TimeSeriesCompactionStrategy.class,
                                                                                    ImmutableMap.of("timestamp_resolution", "MILLISECONDS",
                                                                                                    "window_size", "1m",
                                                                                                    "freeze_after", "1m"))));
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
        // deleted and the new row visible, regardless of how the split distributes the partition
        // header (the deletion is replicated into every output window - plan D3). The memtable may
        // legitimately drop the shadowed old row before flush, so the sstable count is not asserted,
        // only window containment and read semantics.
        new Mutation(PartitionUpdate.fullPartitionDelete(cfs.metadata(), key, oldWindowTs + 1, nowInSeconds())).applyUnsafe();
        write(cfs, "tag-del", "new", now);
        Util.flush(cfs);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertEquals(windowOf(sstable.getMinTimestamp()), windowOf(sstable.getMaxTimestamp()));
        assertEquals(1, Util.getOnlyPartition(Util.cmd(cfs, key).build()).rowCount());
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
