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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.NativePool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The production condition the heap-based memtable tests cannot reach: {@code
 * memtable_allocation_type: offheap_objects} clones clusterings into {@code NativeClustering}s,
 * whose value accessor has no object factory — {@code NativeAccessor.factory()} throws
 * {@code UnsupportedOperationException}. Fabricating a {@code ClusteringBound} from a
 * memtable-owned clustering ({@code Slice.make} / {@code ClusteringBound.create}) therefore throws
 * on a native memtable, and the shard-pruning probe of the 2026-08-02 outage did exactly that.
 *
 * <p>Paging is what put the probe on the hot path: page one of a paged read carries
 * {@code Slices.ALL}, which skips pruning, but every later page carries the pager's
 * {@code Slices.forPaging} rewrite — never {@code ALL} — so every paged read of the tag table
 * failed from its second page while unpaged reads stayed green. The in-process
 * {@code CQLTester.execute()} path neither pages nor allocates natively, which is why the original
 * battery missed it; paged native-protocol reads over offheap_objects are therefore a required
 * part of this memtable's coverage.
 */
public class TimeSeriesMemtableOffheapReadPathTest extends CQLTester
{
    private static final long HOUR_MS = 3_600_000L;

    /** 2026-08-02T00:00:00Z — exactly on an hour boundary, so {@code +k hours} is window {@code k}. */
    private static final long BASE_MS = 1785628800000L;

    private static final String TSCS =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}";

    /** Microsecond write timestamp {@code hours} windows after {@link #BASE_MS}. */
    private static long at(int hours)
    {
        return (BASE_MS + hours * HOUR_MS) * 1000L;
    }

    /**
     * Overrides {@link CQLTester#setUpClass()} so the allocation type is switched after
     * {@code daemonInitialization} but before the server — and with it the first memtable pool —
     * exists. Same pattern as {@code MemtableSizeTestBase#setup}.
     */
    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            ((Config) confField.get(null)).memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        prepareServer();
    }

    /**
     * A DESC clustering declines the bare-long clustering store, so every stored clustering is a
     * {@code NativeClustering} — the shape the production tag table hit. Before the fix, page two
     * of the plain paged read threw {@code UnsupportedOperationException} out of the pruning probe.
     */
    @Test
    public void pagedReadsSurviveNativeClusteringsOnDescTable() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH CLUSTERING ORDER BY (ts DESC) AND compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 40; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(i < 20 ? 0 : 1) + i);
        assertOffheapTimeSeriesMemtable();

        assertEquals(40, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1'", 7)));
        assertEquals(40, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' ORDER BY ts ASC", 7)));
        assertEquals(25, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' AND ts >= " +
                                                    (BASE_MS + 5000L) + " LIMIT 25", 7)));
    }

    /**
     * An ASC timestamp clustering keeps the bare-long store (heap bounds), but overflow rows are
     * cloned whole — native clusterings. A row tombstone below every array clustering makes the
     * overflow map's native clustering the min bound, which is the other way production could
     * throw: any promoted or unrepresentable row whose clustering extends the array bounds.
     */
    @Test
    public void pagedReadsSurviveNativeOverflowClusteringsOnAscTable() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 30; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);
        // Promote one row (later write timestamp, same window)...
        execute("UPDATE %s USING TIMESTAMP ? SET v = ? WHERE series = ? AND ts = ?",
                at(0) + 60_000_000L, 7.5, "S1", BASE_MS + 7000L);
        // ...and leave a row tombstone BELOW every array clustering: overflow-only native bound.
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE series = ? AND ts = ?",
                at(0) + 61_000_000L, "S1", BASE_MS - 60_000L);
        execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                "S1", BASE_MS + 40_000L, 40.0, at(1));
        assertOffheapTimeSeriesMemtable();

        assertEquals(31, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1'", 7)));
        assertEquals(10, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' AND ts >= " +
                                                    BASE_MS + " AND ts <= " + (BASE_MS + 9000L), 3)));
    }

    /**
     * The unit-level pin of the outage: the bounds probe of a native partition, fed the pager's
     * rewritten slices directly. Before the fix every covering probe here threw
     * {@code UnsupportedOperationException}; after it, covering shapes keep the shard and
     * exhausted shapes prune it.
     */
    @Test
    public void nativeBoundsProbeHandlesPagerSlices() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH CLUSTERING ORDER BY (ts DESC) AND compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);
        assertOffheapTimeSeriesMemtable();

        TimeSeriesMemtable memtable = (TimeSeriesMemtable) getCurrentColumnFamilyStore().getCurrentMemtable();
        TimeSeriesMemtable.ShardPartition partition = solePartition(memtable);
        ClusteringComparator comparator = getCurrentColumnFamilyStore().metadata().comparator;

        Clustering<?> mid = Clustering.make(ByteBufferUtil.bytes(BASE_MS + 5000L));
        assertTrue(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator, mid, false, false)));
        assertTrue(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator, mid, false, true)));
        assertTrue(partition.mayContainRowsIn(Slices.with(comparator, Slice.make(mid))));

        // Pruning must still prune: past the comparator-last row (ts BASE_MS on DESC) is nothing.
        assertFalse(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator,
                                                                    Clustering.make(ByteBufferUtil.bytes(BASE_MS)),
                                                                    false, false)));
        assertFalse(partition.mayContainRowsIn(Slices.NONE));
        assertTrue(partition.mayContainRowsIn(Slices.ALL));
    }

    private void assertOffheapTimeSeriesMemtable()
    {
        assertTrue("this test must run on a NativePool (offheap_objects), or it proves nothing",
                   AbstractAllocatorMemtable.MEMORY_POOL instanceof NativePool);
        Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
        assertTrue("expected a TimeSeriesMemtable, got " + current.getClass().getName(),
                   current instanceof TimeSeriesMemtable);
    }

    private static TimeSeriesMemtable.ShardPartition solePartition(TimeSeriesMemtable memtable)
    {
        List<TimeSeriesMemtable.ShardPartition> partitions = new ArrayList<>();
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
            partitions.addAll(shard.partitions.values());
        assertEquals(1, partitions.size());
        return partitions.get(0);
    }

    private static int count(ResultSet resultSet)
    {
        int rows = 0;
        for (Row ignored : resultSet)
            rows++;
        return rows;
    }
}
