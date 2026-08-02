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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * The read-path optimisations of {@link TimeSeriesMemtable}: the incrementally extended snapshot of
 * {@link TimeSeriesColumnarPartition}, and the clustering-bounds shard pruning in
 * {@code rowIterator}.
 *
 * <p><b>Why the reads are interleaved with the writes.</b> The production symptom motivating the
 * snapshot work was a tag ingesting one row per second: every write invalidated the cached snapshot
 * just before the next read, so each read rebuilt O(partition). The fix extends the last snapshot
 * when nothing but appends happened — which means the dangerous state is precisely
 * write→read→write→read, where a read caches a view and the following write decides whether that
 * cache may be extended or must be discarded. Every differential case here therefore reads
 * <em>between</em> writes, and the whole transcript of reads — not just the final state — must be
 * byte-identical to {@code memtable = 'skiplist'}. A stale-cache bug shows up as a mid-sequence
 * read returning yesterday's row and cannot hide behind a correct final read.
 *
 * <p><b>Why the backfill case exists.</b> Shard pruning must use the CLUSTERING values a shard
 * partition holds, never the shard's write-time window: clustering time and write time are
 * independent, and a backfilled row (old clustering, current write timestamp) lives in the current
 * window's shard. Pruning by window silently drops it — a confusion hit in production once already,
 * which {@link #backfilledRowsStayVisibleToSlicedReads} pins.
 */
public class TimeSeriesMemtableReadPathTest extends CQLTester
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

    /** The reads a workload performs are appended here, forming the transcript the harness compares. */
    private StringBuilder transcript;

    @FunctionalInterface
    private interface Workload
    {
        void run() throws Throwable;
    }

    @FunctionalInterface
    private interface StructuralCheck
    {
        void check(TimeSeriesMemtable memtable) throws Throwable;
    }

    // -------------------------------------------------------- problem 1: the incremental snapshot

    /** Pure appends with a read after every write, across three windows: the extension path, live throughout. */
    @Test
    public void matchesReferenceOnInterleavedAppendsAndReads() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 24; i++)
                                  {
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.5, at(i % 3) + i);
                                      observe("SELECT * FROM %s WHERE series = 'S1'");
                                  }
                              },
                              memtable -> assertTrue("the workload was meant to span several windows",
                                                     memtable.shards().size() > 1));
    }

    /**
     * Read, promote, read again: the first read caches a snapshot; the later-timestamp UPDATE of an
     * existing row in the <b>same</b> window promotes that row to the overflow tier without changing
     * the array row count, so an incremental path that skipped the promotion invalidation would keep
     * serving the cached, pre-update row. The mid-sequence reads pin exactly that.
     */
    @Test
    public void matchesReferenceOnReadsAroundPromotion() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 5; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT q FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 2000L));
                                  // 60 s later in write time — same window, different timestamp: promotion.
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0) + 60_000_000L, 222, "S1", BASE_MS + 2000L);
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT q FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 2000L),
                                          "SELECT writetime(v), writetime(q) FROM %s WHERE series = 'S1' AND ts = "
                                          + (BASE_MS + 2000L));
                                  // Appends after the promotion must read back cleanly too.
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 9000L, 9.0, 9, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertEquals("exactly the updated row should have been promoted",
                                               1, overflowRows(memtable));
                              });
    }

    /** Read, overwrite an existing row in place (same write timestamp, so no promotion), read again. */
    @Test
    public void matchesReferenceOnReadsAroundInPlaceOverwrite() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS, 1.0, at(0));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                                  // Same clustering, same write timestamp: reconciled in place, no promotion —
                                  // but the cached snapshot must still not be extended past it.
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS, 2.0, at(0));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                                  // An older-timestamp overwrite must lose, and reads must show that too.
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS, 3.0, at(0) - 1_000_000L);
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 1000L, 4.0, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> assertTrue(memtable.shards().size() > 1));
    }

    /** Reads interleaved with a row deletion, a delete of a clustering never written, and a re-insert. */
    @Test
    public void matchesReferenceOnReadsAroundRowDeletion() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 5; i++)
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                                  // Same window (50 s later), so the tombstone lands in the same shard
                                  // partition that holds the row and must invalidate its snapshot.
                                  execute("DELETE FROM %s USING TIMESTAMP ? WHERE series = ? AND ts = ?",
                                          at(0) + 50_000_000L, "S1", BASE_MS + 1000L);
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                                  // A tombstone for a clustering the arrays never held.
                                  execute("DELETE FROM %s USING TIMESTAMP ? WHERE series = ? AND ts = ?",
                                          at(0) + 51_000_000L, "S1", BASE_MS + 777_000L);
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                                  // Re-insert over the tombstone, then read again.
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 1000L, 99.0, at(0) + 55_000_000L);
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT * FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 1000L));
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 8000L, 8.0, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertEquals("the deleted row and the never-written tombstone should both " +
                                               "sit in the overflow tier", 2, overflowRows(memtable));
                              });
    }

    /** Descending arrival: every append lands in the unsorted tail, and reads interleave with the sorting. */
    @Test
    public void matchesReferenceOnInterleavedOutOfOrderArrival() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 30; i >= 0; i--)
                                  {
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + (30 - i));
                                      if (i % 5 == 0)
                                          observe("SELECT * FROM %s WHERE series = 'S1'",
                                                  "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 10_000L)
                                                  + " AND ts <= " + (BASE_MS + 20_000L));
                                  }
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 40_000L, 40.0, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> assertTrue(memtable.shards().size() > 1));
    }

    /** The same interleavings on a DESC table: the merged view must respect the reversed comparator exactly. */
    @Test
    public void matchesReferenceOnDescClusteringInterleaved() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH CLUSTERING ORDER BY (ts DESC) AND compaction = " + TSCS,
                              () -> {
                                  // Ascending ts is comparator-descending on a DESC table: the tail path.
                                  for (int i = 0; i < 10; i++)
                                  {
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);
                                      if (i % 3 == 0)
                                          observe("SELECT * FROM %s WHERE series = 'S1'");
                                  }
                                  // Descending ts is comparator-ascending: the in-order append path.
                                  for (int i = 30; i > 20; i--)
                                  {
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + 100 + i);
                                      observe("SELECT * FROM %s WHERE series = 'S1'");
                                  }
                                  // A promotion mid-sequence, then reads in both directions and a slice.
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0) + 60_000_000L, 555, "S1", BASE_MS + 5000L);
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT * FROM %s WHERE series = 'S1' ORDER BY ts ASC",
                                          "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 3000L)
                                          + " AND ts <= " + (BASE_MS + 25_000L));
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 50_000L, 50.0, 50, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertEquals(1, overflowRows(memtable));
                              });
    }

    // ------------------------------------------------------------ problem 2: shard pruning bounds

    /**
     * ⚠ Pins the write-window/clustering-time confusion: rows written NOW whose clusterings lie
     * several windows in the past live in the CURRENT write window's shard, and the inverse holds
     * too. Sliced reads over either clustering range must return them; pruning shards by their
     * write-time window silently drops them. A slice over a clustering range nothing covers must
     * still come back empty — that is the pruning actually working.
     */
    @Test
    public void backfilledRowsStayVisibleToSlicedReads() throws Throwable
    {
        String oldRange = "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + BASE_MS +
                          " AND ts < " + (BASE_MS + HOUR_MS);
        String newRange = "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 5 * HOUR_MS);
        String emptyRange = "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 2 * HOUR_MS) +
                            " AND ts < " + (BASE_MS + 3 * HOUR_MS);

        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  // The backfill: written in window 5, clusterings from window 0's time range.
                                  for (int i = 0; i < 10; i++)
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 60_000L, i * 1.0, at(5) + i);
                                  // The inverse: written in window 0, clusterings from window 5's time range.
                                  for (int i = 0; i < 10; i++)
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + 5 * HOUR_MS + i * 60_000L, 100.0 + i, at(0) + i);
                                  observe(oldRange, newRange, emptyRange);
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  // Independent of the reference, so this cannot pass vacuously.
                                  assertEquals("the backfilled rows must be found in the old clustering range",
                                               10, execute(oldRange).size());
                                  assertEquals("the forward-filled rows must be found in the new clustering range",
                                               10, execute(newRange).size());
                                  assertEquals("a range nothing covers must come back empty",
                                               0, execute(emptyRange).size());
                              });
    }

    // -------------------------------------------------- every Slices shape a real read can produce

    /**
     * {@code mayContainRowsIn} is fed by real reads, and real reads do not only produce the shapes
     * a plain {@code SELECT} does: a <b>paged</b> read rewrites its slices per page via
     * {@code Slices.forPaging}, producing rewritten {@code ArrayBackedSlices} and, once a page
     * exhausts the query's range, {@code Slices.NONE}. The 2026-08-02 production outage was this
     * method throwing on a paged read's page-two slices — page one is {@code Slices.ALL} and skips
     * pruning entirely, which is exactly why unpaged testing missed it. This probes every shape,
     * on an ASC and a DESC table: never throw, prune only what really cannot match.
     */
    @Test
    public void mayContainRowsInHandlesEverySlicesShapeAPagerCanProduce() throws Throwable
    {
        probeSlicesShapes(false);
        probeSlicesShapes(true);
    }

    private void probeSlicesShapes(boolean desc) throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH CLUSTERING ORDER BY (ts " + (desc ? "DESC" : "ASC") + ") " +
                    "AND compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);

        TimeSeriesMemtable memtable = (TimeSeriesMemtable) getCurrentColumnFamilyStore().getCurrentMemtable();
        TimeSeriesMemtable.ShardPartition partition = solePartition(memtable);
        ClusteringComparator comparator = getCurrentColumnFamilyStore().metadata().comparator;

        // The trivial shapes.
        assertTrue(partition.mayContainRowsIn(Slices.ALL));
        assertFalse("NONE selects nothing; pruning it is correct", partition.mayContainRowsIn(Slices.NONE));

        // Plain ranges, built in comparator order. On DESC the comparator-lesser end is the LATER
        // time, so a bounds probe accidentally built in time order fails exactly this case.
        assertTrue("a slice covering held rows must not be pruned (desc=" + desc + ')',
                   partition.mayContainRowsIn(rangeSlices(comparator, BASE_MS + 3000L, BASE_MS + 7000L)));
        assertFalse("a slice above every held clustering must be pruned (desc=" + desc + ')',
                    partition.mayContainRowsIn(rangeSlices(comparator, BASE_MS + 60_000L, BASE_MS + 90_000L)));
        assertFalse("a slice below every held clustering must be pruned (desc=" + desc + ')',
                    partition.mayContainRowsIn(rangeSlices(comparator, BASE_MS - 90_000L, BASE_MS - 60_000L)));

        // What the pager actually feeds this method: forPaging rewrites from a mid-partition row,
        // in both paging directions, inclusive and not.
        Clustering<?> mid = Clustering.make(ByteBufferUtil.bytes(BASE_MS + 5000L));
        assertTrue(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator, mid, false, false)));
        assertTrue(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator, mid, true, false)));
        assertTrue(partition.mayContainRowsIn(Slices.ALL.forPaging(comparator, mid, false, true)));

        // Paging past the comparator-last row leaves nothing: prune, never throw.
        long comparatorLastTs = desc ? BASE_MS : BASE_MS + 9000L;
        Clustering<?> comparatorLast = Clustering.make(ByteBufferUtil.bytes(comparatorLastTs));
        assertFalse("paging past the final row leaves nothing to read (desc=" + desc + ')',
                    partition.mayContainRowsIn(Slices.ALL.forPaging(comparator, comparatorLast, false, false)));

        // A single-slice query paged past its slice collapses to Slices.NONE — the selects-nothing
        // implementation whose get(int) throws; the probe must neither call it nor keep the shard.
        Slices exhausted = Slices.with(comparator, Slice.make(mid)).forPaging(comparator, mid, false, false);
        assertSame(Slices.NONE, exhausted);
        assertFalse(partition.mayContainRowsIn(exhausted));
    }

    /** A single [tsA, tsB] slice with start/end put in COMPARATOR order, whichever order that is. */
    private static Slices rangeSlices(ClusteringComparator comparator, long tsA, long tsB)
    {
        Clustering<?> a = Clustering.make(ByteBufferUtil.bytes(tsA));
        Clustering<?> b = Clustering.make(ByteBufferUtil.bytes(tsB));
        boolean aFirst = comparator.compare(a, b) <= 0;
        return Slices.with(comparator, Slice.make(aFirst ? a : b, aFirst ? b : a));
    }

    private static TimeSeriesMemtable.ShardPartition solePartition(TimeSeriesMemtable memtable)
    {
        List<TimeSeriesMemtable.ShardPartition> partitions = new ArrayList<>();
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
            partitions.addAll(shard.partitions.values());
        assertEquals(1, partitions.size());
        return partitions.get(0);
    }

    /**
     * Paged native-protocol reads over multi-page partitions, against the unpaged answer. The
     * in-process {@code execute()} path never pages, so without this the pager's slice rewrites
     * reach {@code mayContainRowsIn} for the first time in production.
     */
    @Test
    public void pagedNativeReadsMatchUnpagedReads() throws Throwable
    {
        for (boolean desc : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                        "WITH CLUSTERING ORDER BY (ts " + (desc ? "DESC" : "ASC") + ") " +
                        "AND compaction = " + TSCS + " AND memtable = 'timeseries'");
            for (int i = 0; i < 40; i++)
                execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        "S1", BASE_MS + i * 1000L, i * 1.0, at(i % 2) + i);

            assertEquals(40, execute("SELECT * FROM %s WHERE series = 'S1'").size());
            assertEquals(40, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1'", 7)));
            assertEquals(40, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' ORDER BY ts " +
                                                        (desc ? "ASC" : "DESC"), 7)));
            assertEquals(20, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' AND ts >= " +
                                                        (BASE_MS + 20_000L), 7)));
            assertEquals(15, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' LIMIT 15", 4)));
        }
    }

    private static int count(com.datastax.driver.core.ResultSet resultSet)
    {
        int rows = 0;
        for (com.datastax.driver.core.Row ignored : resultSet)
            rows++;
        return rows;
    }

    // --------------------------------------------------------------------------------- perf guard

    // ------------------------------------------------------------------------------------ harness

    /**
     * Applies {@code work} to a fresh table on the reference memtable and to a fresh table on
     * {@link TimeSeriesMemtable}. Every {@link #observe} the workload performs — i.e. every read
     * interleaved between its writes — is appended to a transcript, and the two transcripts must be
     * byte-identical, before and after a flush. {@code structural} runs against the live
     * {@code TimeSeriesMemtable} only, so a test also fails when the bytes were right but the
     * storage did not do what the scenario was built to make it do.
     */
    private void assertSameAsReference(String schema, Workload work, StructuralCheck structural) throws Throwable
    {
        String reference = runTranscribed(schema, "skiplist", work, null);
        String sharded = runTranscribed(schema, "timeseries", work, structural);
        assertEquals(reference, sharded);
    }

    private String runTranscribed(String schema, String memtableConfig, Workload work, StructuralCheck structural)
    throws Throwable
    {
        createTable(schema + " AND memtable = '" + memtableConfig + "'");
        transcript = new StringBuilder();
        work.run();

        if (structural != null)
        {
            Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
            assertTrue("expected a TimeSeriesMemtable, got " + current.getClass().getName(),
                       current instanceof TimeSeriesMemtable);
            structural.check((TimeSeriesMemtable) current);
        }

        observe("SELECT * FROM %s");
        flush();
        transcript.append("== flushed\n");
        observe("SELECT * FROM %s");
        return transcript.toString();
    }

    /** Runs {@code selects} now and appends their hex dumps to the transcript. */
    private void observe(String... selects) throws Throwable
    {
        for (String select : selects)
        {
            transcript.append("-- ").append(select).append('\n');
            transcript.append(dump(execute(select)));
        }
    }

    /** Rows in the object-tier overflow maps over every columnar partition of every shard. */
    private static int overflowRows(TimeSeriesMemtable memtable)
    {
        int rows = 0;
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
            for (TimeSeriesMemtable.ShardPartition partition : shard.partitions.values())
                rows += ((TimeSeriesColumnarPartition) partition).overflowRowCount();
        return rows;
    }

    /** Hex, column by column, so nothing a typed accessor would normalise away can hide a difference. */
    private static String dump(UntypedResultSet resultSet)
    {
        StringBuilder out = new StringBuilder();
        for (UntypedResultSet.Row row : resultSet)
        {
            for (ColumnSpecification column : row.getColumns())
            {
                String name = column.name.toString();
                ByteBuffer value = row.has(name) ? row.getBlob(name) : null;
                out.append(name).append('=')
                   .append(value == null ? "null" : ByteBufferUtil.bytesToHex(value))
                   .append(' ');
            }
            out.append('\n');
        }
        return out.toString();
    }
}
