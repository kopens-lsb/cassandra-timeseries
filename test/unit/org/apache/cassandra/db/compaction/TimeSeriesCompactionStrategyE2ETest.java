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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.utils.FBUtilities.nowInSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end coverage for the retention-driven whole-window drop, mirroring
 * {@link TimeWindowCompactionStrategyTest#testDropExpiredSSTables()}: real schema, real flushed sstables,
 * and (via {@link ColumnFamilyStore#setCompactionParameters}) the real, non-mocked
 * {@link TimeSeriesCompactionStrategy} two-arg constructor - the one reflection instantiates in production,
 * which the Mockito-based {@link TimeSeriesCompactionStrategyTest} never exercises.
 * <p>
 * This lives in its own class (rather than alongside {@link TimeSeriesCompactionStrategyTest}) because a
 * SchemaLoader-based fixture (real keyspace/schema init) and the Mockito-only fixture don't share static
 * init cleanly in one JUnit class.
 */
public class TimeSeriesCompactionStrategyE2ETest extends SchemaLoader
{
    private static final String KEYSPACE1 = "TimeSeriesCompactionStrategyE2ETest";
    private static final String CF_STANDARD1 = "Standard1";
    private static final int TTL_SECONDS = 10;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CassandraRelevantProperties.STREAMING_HISTOGRAM_ROUND_SECONDS.setInt(1);
        ServerTestUtils.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testDropExpiredWindowWhole() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // This sstable's window closed roughly an hour ago - well past the 2-minute retention cutoff
        // configured below. timestamp_resolution=MILLISECONDS below makes the strategy read this raw
        // millisecond value directly as the cell timestamp, same convention as TimeWindowCompactionStrategyTest.
        long expiredWriteMillis = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        DecoratedKey expiredKey = Util.dk("expired-window");
        new RowUpdateBuilder(cfs.metadata(), expiredWriteMillis, TTL_SECONDS, expiredKey.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();
        Util.flush(cfs);
        SSTableReader expiredSSTable = cfs.getLiveSSTables().iterator().next();

        // This one lands in the current window and must survive the drop.
        DecoratedKey liveKey = Util.dk("live-window");
        new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), liveKey.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();
        Util.flush(cfs);
        assertEquals(2, cfs.getLiveSSTables().size());

        // Wait for the expiring row's TTL to elapse, mirroring TimeWindowCompactionStrategyTest -
        // not required by the retention check itself (which only looks at write timestamps), but it
        // confirms the whole-window drop and ordinary TTL/tombstone expiry coexist without interference.
        Thread.sleep(TimeUnit.SECONDS.toMillis(TTL_SECONDS + 1));

        // window_size=1m, freeze_after=1m, retention=2m (the configuration minimum, since retention must
        // be >= window_size + freeze_after): the expired-window sstable is an hour past that cutoff, the
        // live one is in the current window.
        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeSeriesCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "window_size", "1m",
                                                    "freeze_after", "1m",
                                                    "retention", "2m"));

        TimeSeriesCompactionStrategy tscs =
            (TimeSeriesCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(expiredSSTable);

        long gcBefore = nowInSeconds();
        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasks(gcBefore);
        AbstractCompactionTask task = Iterables.getOnlyElement(tasks, null);
        assertNotNull(task);
        assertTrue(task instanceof TimeSeriesCompactionTask);
        assertEquals(1, Iterables.size(task.transaction.originals()));
        assertEquals(expiredSSTable, task.transaction.originals().iterator().next());

        task.execute(ActiveCompactionsTracker.NOOP);

        assertFalse(cfs.getLiveSSTables().contains(expiredSSTable));
        assertEquals(1, cfs.getLiveSSTables().size());
    }
}
