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
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The append-only slot store and the streaming read path of {@link TimeSeriesColumnarPartition}:
 * re-writes append a new slot and supersede the old, promotions supersede into the concurrent
 * overflow map, and {@link TimeSeriesStreamingIterator} walks the arrays directly — so the shapes
 * that newly exist to get wrong are superseded-slot runs (re-write storms), the two-way merge with
 * the overflow (promotion storms, ties), the slice binary search (ASC and DESC), the degradation
 * valve (a partition crossing 50% superseded demotes to the object tier), and the
 * promotion-vs-reader race the double-check closes.
 *
 * <p>Differential cases follow the house rule: the same operation sequence against {@code memtable
 * = 'skiplist'} and {@code memtable = 'timeseries'}, every interleaved read transcribed, and the two
 * transcripts byte-identical before and after a flush. The cost model — a sliced read assembles
 * only the slice's rows — is pinned by the {@link TimeSeriesColumnarPartition#rowsAssembled}
 * counter, never by wall-clock. The race and fail-open cases are pinned deterministically through
 * the test hooks on {@link TimeSeriesStreamingIterator}, because a timing-window assertion would
 * only be red when the scheduler felt like it.
 */
public class TimeSeriesMemtableStreamingReadTest extends CQLTester
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

    @After
    public void clearStreamingHooks()
    {
        TimeSeriesStreamingIterator.openHook = null;
        TimeSeriesStreamingIterator.assembleHook = null;
    }

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

    // ------------------------------------------------------------------ re-write (supersede) storms

    /**
     * Whole-row re-writes at later timestamps in the same window: each one appends a new slot and
     * supersedes the old, so the arrays accumulate equal-clustering runs whose dead members the
     * streaming walk must skip — a stale or duplicated row here is exactly the supersede check not
     * being load-bearing. Kept below the 50% valve so the partition stays columnar and the streaming
     * path, not the demoted fallback, is what answers every read.
     */
    @Test
    public void matchesReferenceOnRewriteStormAcrossManyRows() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 60; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                                  // First re-write round: full rows, later timestamps, same window.
                                  for (int i = 10; i < 30; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 10.0, 100 + i, at(0) + 1_000_000L + i);
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 5000L)
                                          + " AND ts <= " + (BASE_MS + 25_000L));
                                  // Second round over a subset: three-deep runs.
                                  for (int i = 10; i < 20; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 100.0, 200 + i, at(0) + 2_000_000L + i);
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT * FROM %s WHERE series = 'S1' ORDER BY ts DESC",
                                          "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 8000L)
                                          + " AND ts < " + (BASE_MS + 22_000L),
                                          "SELECT q FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 15_000L));
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 90_000L, 90.0, 90, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertEquals("whole-row re-writes must not touch the overflow tier",
                                               0, totalOverflowRows(memtable));
                                  assertEquals("each of the 30 re-writes supersedes exactly one slot",
                                               30, totalSupersededSlots(memtable));
                                  assertFalse("below the 50% valve nothing may demote", anyDemoted(memtable));
                              });
    }

    /**
     * The pathological shape the degradation valve exists for: the same cell re-written 100 times.
     * The first re-writes supersede slot after slot; once superseded slots exceed half, the next
     * write demotes the partition to the object tier, and every read — mid-storm, at the end, and
     * after the flush — must still match the reference exactly.
     */
    @Test
    public void matchesReferenceOnSameCellRewriteStormAndDemotes() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 100; i++)
                                  {
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS, i * 1.0, at(0) + i);
                                      if (i % 10 == 0)
                                          observe("SELECT * FROM %s WHERE series = 'S1'");
                                  }
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT v FROM %s WHERE series = 'S1' AND ts = " + BASE_MS);
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 1000L, 7.0, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertTrue("a 100x same-cell storm must cross the 50% valve and demote",
                                             anyDemoted(memtable));
                              });
    }

    // ------------------------------------------------------------------------- promotion storms

    /**
     * Promotions below the valve: a third of the rows move to the overflow tier, so the streaming
     * two-way merge crosses array and overflow rows constantly — including the equal-clustering tie
     * the merge must resolve to the overflow side — in both read directions.
     */
    @Test
    public void matchesReferenceOnPromotionStormBelowValve() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 30; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);
                                  for (int i = 0; i < 30; i += 3)
                                  {
                                      execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                              at(0) + 60_000_000L + i, 500 + i, "S1", BASE_MS + i * 1000L);
                                      observe("SELECT * FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + i * 1000L));
                                  }
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT * FROM %s WHERE series = 'S1' ORDER BY ts DESC",
                                          "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 2000L)
                                          + " AND ts <= " + (BASE_MS + 20_000L));
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 50_000L, 50.0, 50, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertEquals(10, totalOverflowRows(memtable));
                                  assertFalse("10 of 30 superseded is below the valve", anyDemoted(memtable));
                              });
    }

    /**
     * A promotion storm that crosses the valve: of 15 promoting updates, exactly 11 land in the
     * overflow before superseded slots exceed half of 20, and the 12th demotes the partition — its
     * update and the rest land in the object-tier sibling, whose cells the read merge must reconcile
     * with the frozen arrays.
     */
    @Test
    public void matchesReferenceOnPromotionStormAcrossTheValve() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 20; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);
                                  for (int i = 0; i < 15; i++)
                                  {
                                      execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                              at(0) + 60_000_000L + i, 700 + i, "S1", BASE_MS + i * 1000L);
                                      if (i % 4 == 0)
                                          observe("SELECT * FROM %s WHERE series = 'S1'");
                                  }
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT q FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 14_000L),
                                          "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 8000L));
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 40_000L, 40.0, 40, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertTrue("15 promotions of 20 rows must cross the valve", anyDemoted(memtable));
                                  assertEquals("exactly 11 promotions fit before the valve fires " +
                                               "(the 12th write finds 11 of 20 slots superseded)",
                                               11, totalOverflowRows(memtable));
                              });
    }

    // --------------------------------------------------------- mixed, DESC and out-of-order shapes

    /**
     * A DESC table declines the bare-long clustering store, so the binary search and the run walk
     * operate on object clusterings with the reversed comparator — with out-of-order arrival,
     * re-writes and a promotion layered on top, read in both directions and sliced.
     */
    @Test
    public void matchesReferenceOnDescRewriteAndPromotionMix() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH CLUSTERING ORDER BY (ts DESC) AND compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 20; i++)
                                  {
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + (i * 7) % 20 * 1000L, i * 1.0, i, at(0) + i);
                                      if (i % 6 == 0)
                                          observe("SELECT * FROM %s WHERE series = 'S1'");
                                  }
                                  // Whole-row re-writes (append-only supersede) of five clusterings.
                                  for (int i = 0; i < 5; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 10.0, 300 + i, at(0) + 1_000_000L + i);
                                  // One promotion (partial update, later timestamp, same window).
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0) + 60_000_000L, 999, "S1", BASE_MS + 7000L);
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT * FROM %s WHERE series = 'S1' ORDER BY ts ASC",
                                          "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 2000L)
                                          + " AND ts <= " + (BASE_MS + 12_000L),
                                          "SELECT q FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 3000L));
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 50_000L, 50.0, 50, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertTrue(totalSupersededSlots(memtable) >= 6);
                                  assertFalse(anyDemoted(memtable));
                              });
    }

    /**
     * Descending arrival keeps every append in the unsorted tail, so re-writes land while runs are
     * split between prefix and tail and the open-time consolidation is what makes them contiguous —
     * with reads interleaved throughout.
     */
    @Test
    public void matchesReferenceOnOutOfOrderArrivalWithRewrites() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 40; i >= 0; i--)
                                  {
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + (40 - i));
                                      if (i % 3 == 0)
                                      {
                                          // Re-write a clustering that is already held, mid-arrival.
                                          execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                                  "S1", BASE_MS + i * 1000L, i * 100.0, at(0) + 500_000L + i);
                                      }
                                      if (i % 8 == 0)
                                          observe("SELECT * FROM %s WHERE series = 'S1'",
                                                  "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 10_000L)
                                                  + " AND ts <= " + (BASE_MS + 30_000L));
                                  }
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 60_000L, 60.0, at(1));
                                  observe("SELECT * FROM %s WHERE series = 'S1'",
                                          "SELECT * FROM %s WHERE series = 'S1' ORDER BY ts DESC");
                              },
                              memtable -> {
                                  assertTrue(memtable.shards().size() > 1);
                                  assertEquals(14, totalSupersededSlots(memtable));
                                  assertFalse(anyDemoted(memtable));
                              });
    }

    // ------------------------------------------------------------------- paged native-protocol reads

    /**
     * Paged native reads over the streaming path, on an ASC and a DESC table holding superseded
     * runs and promoted rows: the pager's per-page {@code Slices.forPaging} rewrites hit the binary
     * search with every bound shape a real client produces — the exact surface the in-process
     * {@code execute()} path cannot reach.
     */
    @Test
    public void pagedNativeReadsMatchUnpagedOverRewritesAndPromotions() throws Throwable
    {
        for (boolean desc : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (series text, ts timestamp, v double, q int, PRIMARY KEY (series, ts)) " +
                        "WITH CLUSTERING ORDER BY (ts " + (desc ? "DESC" : "ASC") + ") " +
                        "AND compaction = " + TSCS + " AND memtable = 'timeseries'");
            for (int i = 0; i < 40; i++)
                execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        "S1", BASE_MS + i * 1000L, i * 1.0, i, at(i % 2) + i);
            // Ten whole-row re-writes and five promotions, all in window 0.
            for (int i = 0; i < 10; i++)
                execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                        "S1", BASE_MS + i * 2000L, i * 10.0, 100 + i, at(0) + 1_000_000L + i);
            for (int i = 0; i < 5; i++)
                execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                        at(0) + 60_000_000L + i, 900 + i, "S1", BASE_MS + (20 + i * 2) * 1000L);

            assertEquals(40, execute("SELECT * FROM %s WHERE series = 'S1'").size());
            assertEquals(40, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1'", 7)));
            assertEquals(40, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' ORDER BY ts " +
                                                        (desc ? "ASC" : "DESC"), 7)));
            assertEquals(20, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' AND ts >= " +
                                                        (BASE_MS + 20_000L), 7)));
            assertEquals(15, count(executeNetWithPaging("SELECT * FROM %s WHERE series = 'S1' LIMIT 15", 4)));
        }
    }

    // ----------------------------------------------------------------- the assembly-count cost pin

    /**
     * The streaming cost model, pinned by counting, never by clock: a slice covering 10% of a
     * 10,000-row partition must assemble about 1,000 rows — the slice — and a full read must
     * assemble them all. Before the streaming path, the sliced read rebuilt the whole partition;
     * an off-by-one in the slice binary search moves the boundary counts; a quiet fall-back to the
     * rebuild path moves the sliced count to 10,000. All three would be red here.
     */
    @Test
    public void slicedReadsAssembleOnlyTheSlicedRows() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 10_000; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);

        long before = TimeSeriesColumnarPartition.rowsAssembled.sum();
        UntypedResultSet slice = execute("SELECT * FROM %s WHERE series = 'S1' AND ts >= " +
                                         (BASE_MS + 1_000_000L) + " AND ts < " + (BASE_MS + 2_000_000L));
        long slicedAssemblies = TimeSeriesColumnarPartition.rowsAssembled.sum() - before;

        assertEquals(1000, slice.size());
        assertTrue("a 10% slice of a 10k-row partition must assemble ~1k rows, not the partition; assembled "
                   + slicedAssemblies, slicedAssemblies >= 1000 && slicedAssemblies <= 1100);

        before = TimeSeriesColumnarPartition.rowsAssembled.sum();
        assertEquals(10_000, execute("SELECT * FROM %s WHERE series = 'S1'").size());
        long fullAssemblies = TimeSeriesColumnarPartition.rowsAssembled.sum() - before;
        assertTrue("a full read really does assemble the partition; assembled " + fullAssemblies,
                   fullAssemblies >= 10_000);
        System.out.println("rowsAssembled: 10% slice of 10k rows -> " + slicedAssemblies
                           + "; full read -> " + fullAssemblies);
    }

    // ------------------------------------------------------------------------ the promotion race

    /**
     * The double-check, pinned deterministically: the reader assembles a live slot's row, and in
     * the gap before the flag re-check — held open by the test hook — a promoting write supersedes
     * that very slot and moves the merged row to the overflow map. The reader must discard its
     * assembly and return the promoted row; returning the stale assembly is exactly the bug the
     * re-check exists to close, and removing the re-check turns this red.
     */
    @Test
    public void promotionLandingMidAssemblyIsCaughtByTheDoubleCheck() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, q int, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);

        long targetTs = BASE_MS + 2000L;
        AtomicBoolean promoted = new AtomicBoolean();
        AtomicReference<Throwable> hookFailure = new AtomicReference<>();
        TimeSeriesStreamingIterator.assembleHook = clustering -> {
            try
            {
                ByteBuffer raw = clustering.bufferAt(0);
                if (raw != null && raw.remaining() == 8 && raw.getLong(raw.position()) == targetTs
                    && promoted.compareAndSet(false, true))
                {
                    // Promote the clustering whose assembly is in flight: later write timestamp,
                    // same window — the row moves to the overflow map and the slot is superseded.
                    execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                            at(0) + 60_000_000L, 222, "S1", targetTs);
                }
            }
            catch (Throwable t)
            {
                hookFailure.compareAndSet(null, t);
            }
        };

        UntypedResultSet rows = execute("SELECT * FROM %s WHERE series = 'S1'");

        assertNull(hookFailure.get());
        assertTrue("the hook must actually have promoted the target row", promoted.get());
        assertEquals(5, rows.size());
        boolean found = false;
        for (UntypedResultSet.Row row : rows)
        {
            if (row.getTimestamp("ts").getTime() != targetTs)
                continue;
            found = true;
            assertEquals("the reader must return the promoted row, not its stale pre-promotion assembly",
                         222, row.getInt("q"));
            assertEquals(2.0, row.getDouble("v"), 0.0);
        }
        assertTrue(found);
    }

    /**
     * The same race free-running: a writer thread promotes 90 of 200 rows (below the valve) while
     * the reader loops full-partition reads. Safety, not timing, is asserted: every read sees every
     * clustering exactly once — no duplicates from a promotion the merge saw on both sides, no
     * losses from a superseded slot whose overflow row a reader missed — and every value is either
     * the original or the promoted one, never anything torn.
     */
    @Test
    public void concurrentPromotionsNeverDuplicateOrDropRows() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, q int, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        int rowCount = 200;
        int promotions = 90;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);

        String promote = formatQuery("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?");
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try
            {
                start.await();
                for (int i = 0; i < promotions; i++)
                    QueryProcessor.executeInternal(promote, at(0) + 60_000_000L + i, 1000 + i, "S1",
                                                   new Date(BASE_MS + i * 1000L));
            }
            catch (Throwable t)
            {
                writerFailure.compareAndSet(null, t);
            }
            finally
            {
                done.set(true);
            }
        }, "promoting-writer");
        writer.start();
        start.countDown();

        int reads = 0;
        while (!done.get() || reads < 5)
        {
            UntypedResultSet rows = execute("SELECT ts, q FROM %s WHERE series = 'S1'");
            Set<Long> seen = new HashSet<>();
            for (UntypedResultSet.Row row : rows)
            {
                long ts = row.getTimestamp("ts").getTime();
                assertTrue("clustering " + ts + " returned twice in one read", seen.add(ts));
                int i = (int) ((ts - BASE_MS) / 1000L);
                int q = row.getInt("q");
                assertTrue("row " + i + " read back q=" + q + ", neither original nor promoted",
                           q == i || q == 1000 + i);
            }
            assertEquals("a read must see every clustering exactly once", rowCount, seen.size());
            reads++;
        }
        writer.join(30_000);
        assertFalse(writer.isAlive());
        assertNull(writerFailure.get());

        UntypedResultSet rows = execute("SELECT ts, q FROM %s WHERE series = 'S1'");
        for (UntypedResultSet.Row row : rows)
        {
            int i = (int) ((row.getTimestamp("ts").getTime() - BASE_MS) / 1000L);
            assertEquals(i < promotions ? 1000 + i : i, row.getInt("q"));
        }
        assertFalse("90 of 200 superseded stays below the valve", anyDemoted(currentTimeSeriesMemtable()));
    }

    // ---------------------------------------------------------------------------------- fail-open

    /**
     * The fail-open contract, forced: the open hook throws out of the streaming open, and the read
     * must silently take the full-rebuild fallback — right answers, no exception. The assembly
     * counter proves the fallback really ran (a rebuild assembles the whole partition where the
     * slice would assemble five rows), so this cannot pass by the hook quietly not firing. A
     * streaming path that throws instead of falling back turns this red — the 2026-08-02 paged-read
     * outage was exactly a read-path optimisation that threw.
     */
    @Test
    public void streamingOpenFailureFallsBackToTheRebuildPath() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, q int, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 30; i++)
            execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);
        execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                at(0) + 60_000_000L, 555, "S1", BASE_MS + 7000L);

        String full = "SELECT * FROM %s WHERE series = 'S1'";
        String sliced = "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 10_000L)
                        + " AND ts < " + (BASE_MS + 15_000L);
        String expectedFull = dump(execute(full));
        String expectedSliced = dump(execute(sliced));

        TimeSeriesStreamingIterator.openHook = () -> {
            throw new RuntimeException("injected streaming-open failure");
        };
        try
        {
            long before = TimeSeriesColumnarPartition.rowsAssembled.sum();
            assertEquals("the fallback must give the same answer", expectedSliced, dump(execute(sliced)));
            long assembled = TimeSeriesColumnarPartition.rowsAssembled.sum() - before;
            assertTrue("the rebuild fallback materialises the partition (~30 rows), proving it ran; assembled "
                       + assembled, assembled >= 29);
            assertEquals(expectedFull, dump(execute(full)));
        }
        finally
        {
            TimeSeriesStreamingIterator.openHook = null;
        }

        // And with the hook gone, the streaming path serves the same bytes again.
        assertEquals(expectedSliced, dump(execute(sliced)));
        assertEquals(expectedFull, dump(execute(full)));
    }

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
            structural.check(currentTimeSeriesMemtable());

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

    private TimeSeriesMemtable currentTimeSeriesMemtable()
    {
        Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
        assertTrue("expected a TimeSeriesMemtable, got " + current.getClass().getName(),
                   current instanceof TimeSeriesMemtable);
        return (TimeSeriesMemtable) current;
    }

    private static List<TimeSeriesColumnarPartition> columnarPartitions(TimeSeriesMemtable memtable)
    {
        List<TimeSeriesColumnarPartition> partitions = new ArrayList<>();
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
            for (TimeSeriesMemtable.ShardPartition partition : shard.partitions.values())
                partitions.add((TimeSeriesColumnarPartition) partition);
        return partitions;
    }

    private static int totalOverflowRows(TimeSeriesMemtable memtable)
    {
        int rows = 0;
        for (TimeSeriesColumnarPartition partition : columnarPartitions(memtable))
            rows += partition.overflowRowCount();
        return rows;
    }

    private static int totalSupersededSlots(TimeSeriesMemtable memtable)
    {
        int slots = 0;
        for (TimeSeriesColumnarPartition partition : columnarPartitions(memtable))
            slots += partition.supersededSlotCount();
        return slots;
    }

    private static boolean anyDemoted(TimeSeriesMemtable memtable)
    {
        for (TimeSeriesColumnarPartition partition : columnarPartitions(memtable))
        {
            if (partition.isDemoted())
                return true;
        }
        return false;
    }

    private static int count(com.datastax.driver.core.ResultSet resultSet)
    {
        int rows = 0;
        for (com.datastax.driver.core.Row ignored : resultSet)
            rows++;
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
