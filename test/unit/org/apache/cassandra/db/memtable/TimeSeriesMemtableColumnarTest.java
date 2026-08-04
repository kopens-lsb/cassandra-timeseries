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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * What the two-tier columnar storage of {@link TimeSeriesColumnarPartition} newly makes possible to
 * get wrong, checked differentially against {@code memtable = 'skiplist'} exactly like
 * {@link TimeSeriesMemtableDifferentialTest} — plus structural assertions that the intended tier
 * really held the rows, because a columnar bug that silently dumped everything into the object
 * overflow (or, worse, silently fell back to object partitions) would leave every byte-comparison
 * green while deleting the entire point of the feature.
 *
 * <p>The differential cases target the seams of the design:
 * <ul>
 *   <li>INSERT then a partial UPDATE at a later timestamp <em>in the same window</em> — the row
 *       promotion path (per-cell timestamps), which the window-sharding tests never hit because
 *       their updates always landed in different shards;</li>
 *   <li>a row where only some cells carry a TTL — the non-uniform (ttl, localDeletionTime) case;</li>
 *   <li>a row of every supported primitive type — the bit-pattern round trip through the arrays;</li>
 *   <li>a row mixing primitive columns with fallback (text/blob/uuid/frozen) columns;</li>
 *   <li>a partition where some rows are promoted and the rest stay in the arrays;</li>
 *   <li>a reversed clustering order, which must decline the long-clustering store and still sort
 *       correctly through the object clustering store.</li>
 * </ul>
 *
 * <p>The <b>write path</b> section at the end guards the ingest optimisations, which are invisible to
 * a differential byte comparison — a broken one is a lost throughput win, a wrong one is a torn read
 * or a lost partition, and neither shows up in a SELECT. It pins that the partition's mutual
 * exclusion is one lock that reads and writes share (and that its contention counters move), that
 * concurrent writers creating one key intern exactly one instance of it and never publish a partition
 * ahead of its key, and that the row size fused into {@code classify()} is the row's own
 * {@code dataSize()} to the byte.
 */
public class TimeSeriesMemtableColumnarTest extends CQLTester
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

    // ------------------------------------------------------------------------ differential cases

    /**
     * INSERT then partial UPDATEs at later timestamps in the <b>same</b> window: the merged row no
     * longer has one uniform write timestamp, so that row — and only that row — must be promoted to
     * the object overflow tier. A static column rides along to check the static row's object tier.
     */
    @Test
    public void matchesReferenceOnPromotionByLaterUpdateInSameWindow() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, flag text static, ts timestamp, v double, q int, note text, " +
                              "PRIMARY KEY (series, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  execute("INSERT INTO %s (series, flag, ts, v, q, note) VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", "raised", BASE_MS, 1.5, 10, "first", at(0));
                                  // One second later in write time — same window, different timestamp.
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0) + 1_000_000L, 20, "S1", BASE_MS);
                                  execute("UPDATE %s USING TIMESTAMP ? SET note = ? WHERE series = ? AND ts = ?",
                                          at(0) + 2_000_000L, "second", "S1", BASE_MS);
                                  // Untouched sibling row in the same window stays in the arrays.
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 1000L, 2.5, 11, at(0) + 3_000_000L);
                                  // And a second window, so the write sequence is really sharded.
                                  execute("INSERT INTO %s (series, flag, ts, v) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", "lowered", BASE_MS + 2000L, 3.5, at(1));
                              },
                              memtable -> {
                                  int[] counts = columnarCounts(memtable);
                                  assertEquals("exactly the updated row should have been promoted", 1, counts[1]);
                                  assertTrue("the untouched rows must stay in the arrays", counts[0] >= 2);
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT q, note FROM %s WHERE series = 'S1' AND ts = " + BASE_MS,
                              // Per-cell write times are what promotion exists to preserve; a bug that
                              // collapsed them to one row timestamp would leave every value identical.
                              "SELECT writetime(v), writetime(q), writetime(note) FROM %s WHERE series = 'S1' AND ts = " + BASE_MS);
    }

    /**
     * A row where only some cells carry a TTL, written at one timestamp. The fast path stores one
     * (ttl, localDeletionTime) per row, so this row must take the overflow tier and keep each cell's
     * own expiration exactly; a row whose TTL is uniform must stay in the arrays.
     */
    @Test
    public void matchesReferenceOnPartiallyTtldRow() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS, 1.0, 1, at(0));
                                  // Same write timestamp, but now with a TTL: only q expires.
                                  execute("UPDATE %s USING TTL 86400 AND TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0), 2, "S1", BASE_MS);
                                  // A row whose TTL is uniform across every cell stays on the fast path.
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ? AND TTL 86400",
                                          "S1", BASE_MS + 1000L, 2.0, 2, at(0) + 1_000_000L);
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 2000L, 3.0, 3, at(1));
                              },
                              memtable -> {
                                  int[] counts = columnarCounts(memtable);
                                  assertEquals("the mixed-TTL row should be the only promoted one", 1, counts[1]);
                                  assertTrue("the uniform-TTL row must stay in the arrays", counts[0] >= 2);
                                  // ttl() is relative to "now", so it cannot go into the byte-for-byte
                                  // reference comparison without flaking; assert presence/absence here
                                  // instead. A storage bug that leaked the row TTL onto v, or dropped
                                  // q's, changes exactly this.
                                  UntypedResultSet ttls = execute("SELECT ttl(v), ttl(q) FROM %s " +
                                                                  "WHERE series = 'S1' AND ts = " + BASE_MS);
                                  UntypedResultSet.Row row = ttls.one();
                                  String ttlV = row.getColumns().get(0).name.toString();
                                  String ttlQ = row.getColumns().get(1).name.toString();
                                  assertTrue("v was written without a TTL and must stay that way", !row.has(ttlV));
                                  assertTrue("q was written with a TTL and must keep it", row.has(ttlQ));
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT writetime(v), writetime(q) FROM %s WHERE series = 'S1' AND ts = " + BASE_MS);
    }

    /** Every type the primitive arrays support, with edge-case values, plus rows with absent columns. */
    @Test
    public void matchesReferenceOnEveryPrimitiveType() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (k text, ts timestamp, d double, f float, i int, b bigint, " +
                              "bo boolean, t timestamp, dt date, PRIMARY KEY (k, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  execute("INSERT INTO %s (k, ts, d, f, i, b, bo, t, dt) " +
                                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, '2026-08-02') USING TIMESTAMP ?",
                                          "k1", BASE_MS, 1.25, 2.5f, 42, 9_000_000_000L, true, BASE_MS + 5L, at(0));
                                  execute("INSERT INTO %s (k, ts, d, f, i, b, bo, t, dt) " +
                                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, '1969-12-31') USING TIMESTAMP ?",
                                          "k1", BASE_MS + 1000L, -0.0, Float.NaN, Integer.MIN_VALUE,
                                          Long.MIN_VALUE + 1, false, 0L, at(0) + 1000L);
                                  execute("INSERT INTO %s (k, ts, d, f, i, b, bo, t, dt) " +
                                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, '2262-04-11') USING TIMESTAMP ?",
                                          "k2", BASE_MS, Double.NaN, -Float.MAX_VALUE, -1, Long.MAX_VALUE,
                                          true, -BASE_MS, at(1));
                                  // Absent columns: presence tracking, not zero-filling, must decide what a read sees.
                                  execute("INSERT INTO %s (k, ts, d) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "k2", BASE_MS + 1000L, 7.75, at(1) + 1000L);
                                  execute("INSERT INTO %s (k, ts, bo) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "k2", BASE_MS + 2000L, false, at(2));
                              },
                              memtable -> {
                                  int[] counts = columnarCounts(memtable);
                                  assertEquals("every one of these rows fits the fast path", 5, counts[0]);
                                  assertEquals("nothing here should have been promoted", 0, counts[1]);
                              },
                              "SELECT * FROM %s WHERE k = 'k1'",
                              "SELECT * FROM %s WHERE k = 'k2'",
                              "SELECT d, bo, dt FROM %s WHERE k = 'k2' AND ts = " + (BASE_MS + 1000L));
    }

    /** Primitive and fallback (text, blob, uuid, frozen collection) columns side by side in one row. */
    @Test
    public void matchesReferenceOnMixedPrimitiveAndFallbackColumns() throws Throwable
    {
        UUID id1 = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        UUID id2 = UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
        ByteBuffer payload = ByteBufferUtil.hexToBytes("00ff10a7c3bf");

        assertSameAsReference("CREATE TABLE %s (k text, ts timestamp, v double, name text, payload blob, id uuid, " +
                              "props frozen<map<text, text>>, PRIMARY KEY (k, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  execute("INSERT INTO %s (k, ts, v, name, payload, id, props) " +
                                          "VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
                                          "k1", BASE_MS, 1.0, "sensor-a", payload, id1, map("unit", "bar"), at(0));
                                  execute("INSERT INTO %s (k, ts, v, name, id) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                                          "k1", BASE_MS + 1000L, 2.0, "", id2, at(0) + 1000L);
                                  // Overwrite a fallback column at the same timestamp (in-place reconcile),
                                  // and a primitive column at a later one (promotion of a mixed row).
                                  execute("UPDATE %s USING TIMESTAMP ? SET name = ? WHERE k = ? AND ts = ?",
                                          at(0), "sensor-A", "k1", BASE_MS);
                                  execute("UPDATE %s USING TIMESTAMP ? SET v = ? WHERE k = ? AND ts = ?",
                                          at(0) + 5_000_000L, 9.0, "k1", BASE_MS + 1000L);
                                  execute("INSERT INTO %s (k, ts, v, name) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "k1", BASE_MS + 2000L, 3.0, "sensor-c", at(1));
                              },
                              memtable -> {
                                  int[] counts = columnarCounts(memtable);
                                  assertEquals("only the cross-timestamp update should promote", 1, counts[1]);
                                  assertTrue(counts[0] >= 2);
                              },
                              "SELECT * FROM %s WHERE k = 'k1'",
                              "SELECT name, payload, props FROM %s WHERE k = 'k1' AND ts = " + BASE_MS,
                              "SELECT writetime(v), writetime(name) FROM %s WHERE k = 'k1' AND ts = " + (BASE_MS + 1000L));
    }

    /** One partition, ten fast-path rows, exactly one of them promoted by a later same-window update. */
    @Test
    public void matchesReferenceWhenSomeRowsPromoteAndOthersStay() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 10; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0) + 60_000_000L, 333, "S1", BASE_MS + 3000L);
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 20_000L, 20.0, 20, at(1));
                              },
                              memtable -> {
                                  int[] counts = columnarCounts(memtable);
                                  assertEquals("nine untouched rows plus the window-1 row stay in the arrays",
                                               10, counts[0]);
                                  assertEquals("exactly the updated row is promoted", 1, counts[1]);
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT q FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 3000L),
                              "SELECT writetime(v), writetime(q) FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 3000L));
    }

    /**
     * A reversed clustering order declines the long-clustering store (the raw {@code long} order
     * would be backwards), so this exercises the object clustering store with out-of-order arrival
     * and a same-window promotion on top.
     */
    @Test
    public void matchesReferenceOnReversedClusteringOrder() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH CLUSTERING ORDER BY (ts DESC) AND compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 20; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + (i * 7919) % 20 * 1000L, i * 1.0, i, at(0) + i);
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0) + 60_000_000L, 999, "S1", BASE_MS + 5000L);
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 50_000L, 50.0, at(1));
                              },
                              memtable -> {
                                  for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
                                      for (TimeSeriesMemtable.ShardPartition partition : shard.partitions.values())
                                          assertTrue("a reversed timestamp clustering must use the long store " +
                                                     "(raw value stored, comparisons negated)",
                                                     ((TimeSeriesColumnarPartition) partition).usesLongClusterings());
                                  assertEquals(1, columnarCounts(memtable)[1]);
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT * FROM %s WHERE series = 'S1' ORDER BY ts ASC");
    }

    /**
     * Ascending-time ingest into a DESC-clustered table — the production {@code tm_tag_point} write
     * pattern. In comparator order every new sample lands strictly below everything the arrays hold,
     * so the certainly-new bounds shortcut must admit it without a slot search. A rewrite of the
     * newest sample (equal to the comparator minimum, not below it) and a rewrite of an older sample
     * must still merge through the slot search, and appends made while a promoted row occupies the
     * overflow must not take the shortcut at all. The counter assertion is what fails if the
     * min-side guard is lost — the differential dump alone would stay green, because the shortcut is
     * a pure duplicate-detection optimisation.
     */
    @Test
    public void ascendingIngestIntoReversedClusteringTakesCertainNewShortcut() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, " +
                              "PRIMARY KEY (series, ts)) WITH CLUSTERING ORDER BY (ts DESC) AND compaction = " + TSCS,
                              () -> {
                                  // 50 samples in ascending time order: below the comparator minimum every time.
                                  for (int i = 0; i < 50; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i);
                                  // Rewrite of the newest sample: EQUAL to the comparator minimum — must
                                  // merge through the slot search, never shortcut-append a duplicate. The
                                  // later write time in the same window promotes the row to overflow.
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0) + 60_000_000L, 999, "S1", BASE_MS + 49_000L);
                                  // Rewrite of an old sample while overflow is non-empty: slot-search path.
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(0) + 61_000_000L, 888, "S1", BASE_MS + 10_000L);
                                  // More ascending samples with the overflow occupied: correct, but the
                                  // shortcut must decline (a colliding clustering could live there).
                                  for (int i = 50; i < 60; i++)
                                      execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, i, at(0) + i + 100);
                                  // A second write-time window, so the sequence is really sharded.
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 100_000L, 100.0, at(1));
                              },
                              memtable -> {
                                  long shortcuts = 0;
                                  for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
                                      for (TimeSeriesMemtable.ShardPartition partition : shard.partitions.values())
                                          shortcuts += ((TimeSeriesColumnarPartition) partition).certainNewAppends();
                                  // Window 0: the 50 ascending samples (one first-row admit + 49 by the
                                  // min-side bound); the ten written while the overflow was occupied do
                                  // not count. Window 1: its single first row.
                                  assertEquals(51, shortcuts);
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT * FROM %s WHERE series = 'S1' ORDER BY ts ASC",
                              "SELECT q FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 49_000L),
                              "SELECT q FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 10_000L));
    }

    // ------------------------------------------------------------------------- storage-tier gates

    /** An eligible schema must actually get columnar partitions — quiet fallback would void the feature. */
    @Test
    public void columnarStorageIsUsedForEligibleSchema() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", BASE_MS + i * 1000L, i * 1.0, at(0) + i);

        TimeSeriesMemtable memtable = (TimeSeriesMemtable) getCurrentColumnFamilyStore().getCurrentMemtable();
        int partitions = 0;
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
        {
            for (TimeSeriesMemtable.ShardPartition partition : shard.partitions.values())
            {
                partitions++;
                assertTrue("eligible schema should store columnar, got " + partition.getClass().getSimpleName(),
                           partition instanceof TimeSeriesColumnarPartition);
                TimeSeriesColumnarPartition columnar = (TimeSeriesColumnarPartition) partition;
                assertTrue("a single timestamp clustering should use the long store",
                           columnar.usesLongClusterings());
                assertEquals(10, columnar.fastPathRowCount());
                assertEquals(0, columnar.overflowRowCount());
            }
        }
        assertEquals(1, partitions);
    }

    /**
     * Counter tables and tables with non-frozen collections must keep the reference object storage
     * inside the shard — and must never be refused: the schema gate is TSCS-or-not, nothing else.
     */
    @Test
    public void objectStorageIsUsedForCounterAndMultiCellSchemas() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, hits counter, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        execute("UPDATE %s SET hits = hits + 5 WHERE k = 'a' AND c = 'x'");
        assertAllPartitionsUseObjectStorage();

        createTable("CREATE TABLE %s (k text, c timestamp, tags map<text, text>, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        execute("INSERT INTO %s (k, c, tags) VALUES (?, ?, ?) USING TIMESTAMP ?",
                "k1", BASE_MS, map("a", "1"), at(0));
        assertAllPartitionsUseObjectStorage();
    }

    private void assertAllPartitionsUseObjectStorage()
    {
        TimeSeriesMemtable memtable = (TimeSeriesMemtable) getCurrentColumnFamilyStore().getCurrentMemtable();
        int partitions = 0;
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
        {
            for (TimeSeriesMemtable.ShardPartition partition : shard.partitions.values())
            {
                partitions++;
                assertTrue("this schema cannot use the primitive arrays and must fall back to " +
                           "object storage, got " + partition.getClass().getSimpleName(),
                           partition instanceof TimeSeriesMemtable.ObjectShardPartition);
            }
        }
        assertTrue("the write should have created at least one partition", partitions > 0);
    }

    // ------------------------------------------------------------------------------- write path

    /**
     * The uncontended write counter must move once per write, and the contended one must not move at
     * all when there is only one writer.
     *
     * <p>This is the test that fails if the {@code ReentrantLock} is reverted to {@code synchronized}
     * — a monitor cannot be asked about contention, so both counters would sit at zero and the whole
     * point of the conversion (telling lock cost from CPU cost after 6d5f2a3156) would be gone with
     * every other test still green.
     */
    @Test
    public void writesCountAsUncontendedWhenSingleThreaded() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts bigint, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?", "S1", 0L, 0.0, at(0));

        long uncontendedBefore = TimeSeriesColumnarPartition.uncontendedWrites.sum();
        long contendedBefore = TimeSeriesColumnarPartition.contendedWrites.sum();

        int writes = 64;
        for (int i = 1; i <= writes; i++)
            execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    "S1", (long) i, i * 1.0, at(0) + i);

        assertEquals("every write takes the partition lock exactly once, and an unlocked lock must be "
                     + "acquired by the tryLock() fast path",
                     writes, TimeSeriesColumnarPartition.uncontendedWrites.sum() - uncontendedBefore);
        assertEquals("nothing else was writing, so no acquisition can have been contended",
                     0, TimeSeriesColumnarPartition.contendedWrites.sum() - contendedBefore);
    }

    /**
     * <b>One lock, not two.</b> Every write and every read-side probe that touches writer-owned state
     * must exclude each other, so each call below must block while another thread holds the
     * partition's write lock and complete once it is released. A probe left on the old object monitor
     * would sail straight through the held lock and pass every other test in this file — while
     * walking arrays a concurrent {@code consolidate()} is replacing underneath it.
     *
     * <p>It doubles as the deterministic contention case: the write probe is guaranteed to find the
     * lock taken, so {@code contendedWrites} and {@code writeContentionNanos} must both move. Racing
     * N writer threads and hoping for a collision would test the same code less reliably.
     */
    @Test
    public void writeLockExcludesWritesAndReadSideProbes() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts bigint, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        rowMutation(metadata, "S1", 0L, at(0), 0.0).applyUnsafe();

        TimeSeriesMemtable memtable = currentTimeSeriesMemtable();
        TimeSeriesColumnarPartition partition = onlyColumnarPartition(memtable);

        long contendedBefore = TimeSeriesColumnarPartition.contendedWrites.sum();
        long nanosBefore = TimeSeriesColumnarPartition.writeContentionNanos.sum();

        // One entry per site that used to be `synchronized` on the partition. Adding a site to the
        // class means adding it here; that is the point.
        assertBlocksWhileLockHeld(partition, "put",
                                  () -> rowMutation(metadata, "S1", 1L, at(0) + 1, 1.0).applyUnsafe());
        assertBlocksWhileLockHeld(partition, "captureWalk", partition::captureWalk);
        assertBlocksWhileLockHeld(partition, "mayContainRowsIn", () -> partition.mayContainRowsIn(Slices.ALL));
        assertBlocksWhileLockHeld(partition, "partitionLevelDeletion", partition::partitionLevelDeletion);
        assertBlocksWhileLockHeld(partition, "columns", partition::columns);
        assertBlocksWhileLockHeld(partition, "isEmpty", partition::isEmpty);
        assertBlocksWhileLockHeld(partition, "hasRows", partition::hasRows);
        assertBlocksWhileLockHeld(partition, "fastPathRowCount", partition::fastPathRowCount);
        assertBlocksWhileLockHeld(partition, "certainNewAppends", partition::certainNewAppends);
        assertBlocksWhileLockHeld(partition, "overflowRowCount", partition::overflowRowCount);
        assertBlocksWhileLockHeld(partition, "supersededSlotCount", partition::supersededSlotCount);
        // readView(), reached by the rebuild paths the streaming iterator does not serve.
        assertBlocksWhileLockHeld(partition, "unfilteredIterator", () -> {
            try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
            {
                while (iterator.hasNext())
                    iterator.next();
            }
        });

        assertTrue("a write that found the lock held must be counted as contended",
                   TimeSeriesColumnarPartition.contendedWrites.sum() - contendedBefore >= 1);
        assertTrue("a contended write must report the nanoseconds it spent blocked",
                   TimeSeriesColumnarPartition.writeContentionNanos.sum() - nanosBefore > 0);
    }

    /**
     * N threads writing the <b>same new key</b> into one window: the key index must end up with
     * exactly one entry, and every shard's partition must hold that one instance.
     *
     * <p>The race this pins is the one the single-probe write path (probe the shard, intern only on a
     * miss) creates: every thread misses the shard, so every thread interns, and only
     * {@code putIfAbsent} decides which clone wins. Two live instances of one key would mean the
     * memtable counts one partition as two and stores the key twice; a partition published into a
     * shard before its key reached {@code keys} would be worse — {@link TimeSeriesMemtable#isClean()}
     * answers from {@code keys} alone and {@code ColumnFamilyStore} throws a clean memtable away
     * without flushing it, so the window between the two writes is a data-loss window and this test
     * hammers it.
     */
    @Test
    public void concurrentWritersToOneNewKeyInternExactlyOneKey() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts bigint, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        TimeSeriesMemtable memtable = currentTimeSeriesMemtable();
        assertTrue("the race is about creating the key, so nothing may exist yet", memtable.isClean());

        int threadCount = 8;
        int perThread = 250;
        CyclicBarrier startTogether = new CyclicBarrier(threadCount);
        List<Thread> threads = new ArrayList<>(threadCount);
        AtomicReference<Throwable> error = new AtomicReference<>();
        for (int t = 0; t < threadCount; t++)
        {
            int base = t * perThread;
            Thread thread = new Thread(() -> {
                try
                {
                    startTogether.await(30, TimeUnit.SECONDS);
                    for (int i = 0; i < perThread; i++)
                        rowMutation(metadata, "S1", base + i, at(0) + base + i, i * 1.0).applyUnsafe();
                }
                catch (Throwable e)
                {
                    error.compareAndSet(null, e);
                }
            }, "ts-concurrent-writer-" + t);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads)
            thread.join(TimeUnit.MINUTES.toMillis(2));
        if (error.get() != null)
            throw new AssertionError("a concurrent writer failed", error.get());

        assertSame("the test is void if the memtable was flushed under it",
                   memtable, getCurrentColumnFamilyStore().getCurrentMemtable());
        assertFalse("a memtable holding rows must never report itself clean — a clean memtable is "
                    + "discarded without being flushed", memtable.isClean());
        assertEquals("one key, however many threads created it", 1, memtable.keys().size());
        assertEquals("partitionCount() counts distinct keys", 1, memtable.partitionCount());

        DecoratedKey interned = memtable.keys().values().iterator().next();
        int rows = 0;
        int partitions = 0;
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
        {
            for (TimeSeriesMemtable.ShardPartition stored : shard.partitions.values())
            {
                partitions++;
                assertSame("a shard partition must hold the one interned key instance, not a second "
                           + "clone of it", interned, storedKeyOf(stored));
                rows += ((TimeSeriesColumnarPartition) stored).fastPathRowCount();
            }
        }
        assertEquals("all the writes fell in one window", 1, partitions);
        assertEquals("no write may be lost to the create race", threadCount * perThread, rows);
    }

    /** The same key written to three windows is three partitions but still one key, counted once. */
    @Test
    public void oneKeyAcrossThreeWindowsIsInternedOnce() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts bigint, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        TimeSeriesMemtable memtable = currentTimeSeriesMemtable();

        for (int window = 0; window < 3; window++)
            rowMutation(metadata, "S1", window, at(window), window * 1.0).applyUnsafe();

        assertEquals("one window per write timestamp", 3, memtable.shards().size());
        assertEquals("three shard partitions, one key", 1, memtable.keys().size());
        assertEquals(1, memtable.partitionCount());

        DecoratedKey interned = memtable.keys().values().iterator().next();
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
        {
            for (TimeSeriesMemtable.ShardPartition stored : shard.partitions.values())
                assertSame("every window's partition shares the one key instance — the reason the "
                           + "hit path must not take the key from partitionKey(), which copies under "
                           + "a native allocator", interned, storedKeyOf(stored));
        }
    }

    /**
     * {@code liveDataSize} must equal the sum of the applied rows' own {@code dataSize()}, to the
     * byte, now that the columnar fast path computes that size inside {@code classify()} instead of
     * asking the row for it.
     *
     * <p>Nothing else pins this: {@code MemtableSizeTestBase} is parameterised over skiplist,
     * skiplist_sharded and trie only, so no existing test would notice the fused sum drifting. The
     * classic way to get it wrong is to drop {@code Row.Deletion.dataSize()} because the row is not
     * deleted — it is {@code time.dataSize() + 1} even when LIVE, and omitting it would understate
     * every single row by a constant and quietly shrink the flush size estimate.
     */
    @Test
    public void liveDataSizeMatchesTheRowsOwnDataSize() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts bigint, v double, q int, tag text, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        TimeSeriesMemtable memtable = currentTimeSeriesMemtable();
        long before = memtable.getLiveDataSize();

        // Distinct clusterings only: a re-write goes through replaceSlot, which legitimately accounts
        // a merged-minus-existing delta and not a row size. Mixed shapes otherwise — every fixed
        // width the arrays know, a missing cell, an object-store (text) cell, a uniform TTL, and a
        // clustering below the minimum so the min-side certainly-new shortcut is exercised too.
        List<PartitionUpdate> updates = new ArrayList<>();
        updates.add(rowUpdate(metadata, "S1", 100L, at(0) + 1, 1.5, 7, "alpha", 0));
        updates.add(rowUpdate(metadata, "S1", 101L, at(0) + 2, 2.5, null, null, 0));
        updates.add(rowUpdate(metadata, "S1", 102L, at(0) + 3, null, 9, "a much longer tag value", 0));
        updates.add(rowUpdate(metadata, "S1", 103L, at(0) + 4, 4.5, 11, "gamma", 3600));
        updates.add(rowUpdate(metadata, "S1", 1L, at(0) + 5, 5.5, 13, "below-the-minimum", 0));
        updates.add(rowUpdate(metadata, "S1", 104L, at(0) + 6, Double.NaN, Integer.MIN_VALUE, "", 0));

        long expected = 0;
        for (PartitionUpdate update : updates)
        {
            for (Row row : update)
                expected += row.dataSize();
            new Mutation(update).applyUnsafe();
        }

        TimeSeriesColumnarPartition partition = onlyColumnarPartition(memtable);
        assertEquals("the workload must stay on the fused fast path or it proves nothing",
                     updates.size(), partition.fastPathRowCount());
        assertEquals(0, partition.overflowRowCount());
        // The 8 bytes are the shard's fixed charge for creating the partition object itself.
        assertEquals("the fused size must be the row's own dataSize(), byte for byte",
                     expected + 8, memtable.getLiveDataSize() - before);
    }

    // ------------------------------------------------------------------- write-path test helpers

    private TimeSeriesMemtable currentTimeSeriesMemtable()
    {
        Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
        assertTrue("expected a TimeSeriesMemtable, got " + current.getClass().getName(),
                   current instanceof TimeSeriesMemtable);
        return (TimeSeriesMemtable) current;
    }

    private static TimeSeriesColumnarPartition onlyColumnarPartition(TimeSeriesMemtable memtable)
    {
        TimeSeriesColumnarPartition only = null;
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
        {
            for (TimeSeriesMemtable.ShardPartition stored : shard.partitions.values())
            {
                assertTrue("this schema should be held columnar, got " + stored.getClass().getSimpleName(),
                           stored instanceof TimeSeriesColumnarPartition);
                assertTrue("expected exactly one partition", only == null);
                only = (TimeSeriesColumnarPartition) stored;
            }
        }
        assertNotNull("expected exactly one partition", only);
        return only;
    }

    /** The key the partition really holds, never the {@code ensureOnHeap()} copy the accessor returns. */
    private static DecoratedKey storedKeyOf(TimeSeriesMemtable.ShardPartition stored)
    {
        return stored instanceof TimeSeriesColumnarPartition
               ? ((TimeSeriesColumnarPartition) stored).storedKey()
               : stored.partitionKey();
    }

    private static Mutation rowMutation(TableMetadata metadata, String series, long ts, long writeTimestamp, double v)
    {
        return new Mutation(new RowUpdateBuilder(metadata, writeTimestamp, series).clustering(ts)
                                                                                  .add("v", v)
                                                                                  .buildUpdate());
    }

    private static PartitionUpdate rowUpdate(TableMetadata metadata, String series, long ts, long writeTimestamp,
                                             Double v, Integer q, String tag, int ttl)
    {
        // The five-argument constructor, whose arity is unique: (metadata, timestamp, ttl, key) and
        // (metadata, localDeletionTime, timestamp, key) are both four longs-and-an-object away from
        // each other, and picking the wrong one silently changes what is being tested.
        RowUpdateBuilder builder = new RowUpdateBuilder(metadata, FBUtilities.nowInSeconds(), writeTimestamp, ttl, series)
                                   .clustering(ts);
        if (v != null)
            builder.add("v", v);
        if (q != null)
            builder.add("q", q);
        if (tag != null)
            builder.add("tag", tag);
        return builder.buildUpdate();
    }

    /**
     * Runs {@code action} on another thread while this one holds {@code partition}'s write lock, and
     * requires it to be still running after {@link #LOCK_PROBE_MS} and to finish once the lock is
     * released. The wait is what distinguishes "took the lock" from "did not": an action that never
     * touches the lock finishes in microseconds.
     */
    private static void assertBlocksWhileLockHeld(TimeSeriesColumnarPartition partition,
                                                  String what,
                                                  Runnable action) throws Exception
    {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            started.countDown();
            try
            {
                action.run();
            }
            catch (Throwable e)
            {
                error.set(e);
            }
            finally
            {
                finished.countDown();
            }
        }, "ts-lock-probe-" + what);

        partition.writeLock.lock();
        try
        {
            thread.start();
            assertTrue(what + " probe thread never started", started.await(30, TimeUnit.SECONDS));
            assertFalse(what + " completed while another thread held the partition's write lock: it is "
                        + "not taking the same lock the write path takes, so it can read arrays a "
                        + "concurrent write is replacing",
                        finished.await(LOCK_PROBE_MS, TimeUnit.MILLISECONDS));
        }
        finally
        {
            partition.writeLock.unlock();
        }
        assertTrue(what + " did not complete after the write lock was released",
                   finished.await(60, TimeUnit.SECONDS));
        thread.join(TimeUnit.SECONDS.toMillis(60));
        if (error.get() != null)
            throw new AssertionError(what + " threw", error.get());
    }

    /**
     * How long a probe must stay blocked to count as blocked. The only way this flakes is a probe
     * that does <em>not</em> take the lock being stalled past it — a half-second GC pause landing
     * inside the window — so it is set well above what any of these calls costs (microseconds) and
     * can be raised freely: a genuinely blocked thread stays blocked however slow the machine is,
     * and the cost of raising it is only this test's runtime, half a second per probe.
     */
    private static final long LOCK_PROBE_MS = 500;

    // ------------------------------------------------------------------------------------ harness

    /**
     * Applies {@code work} to a fresh table on the reference memtable and to a fresh table on
     * {@link TimeSeriesMemtable}, requires both to read back identically before and after a flush,
     * and runs {@code structural} against the live {@code TimeSeriesMemtable} so a test also fails
     * when the data was right but sat in the wrong storage tier.
     */
    private void assertSameAsReference(String schema, Workload work, StructuralCheck structural, String... selects)
    throws Throwable
    {
        String reference = runAndDump(schema, "skiplist", work, null, selects);
        String sharded = runAndDump(schema, "timeseries", work, structural, selects);
        assertEquals(reference, sharded);
    }

    private String runAndDump(String schema, String memtable, Workload work, StructuralCheck structural, String[] selects)
    throws Throwable
    {
        createTable(schema + " AND memtable = '" + memtable + "'");
        work.run();

        if (structural != null)
        {
            Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
            assertTrue("expected a TimeSeriesMemtable, got " + current.getClass().getName(),
                       current instanceof TimeSeriesMemtable);
            TimeSeriesMemtable timeSeriesMemtable = (TimeSeriesMemtable) current;
            assertTrue("the workload was meant to span several windows", timeSeriesMemtable.shards().size() > 1);
            structural.check(timeSeriesMemtable);
        }

        StringBuilder dump = new StringBuilder();
        dump.append("== live\n").append(readAll(selects));
        flush();
        dump.append("== flushed\n").append(readAll(selects));
        return dump.toString();
    }

    private String readAll(String[] selects) throws Throwable
    {
        StringBuilder out = new StringBuilder();
        out.append("-- SELECT * FROM %s\n").append(dump(execute("SELECT * FROM %s")));
        for (String select : selects)
            out.append("-- ").append(select).append('\n').append(dump(execute(select)));
        return out.toString();
    }

    /** Fast-path and overflow row totals over every columnar partition of every shard. */
    private static int[] columnarCounts(TimeSeriesMemtable memtable)
    {
        int fastPath = 0;
        int overflowRows = 0;
        for (TimeSeriesMemtable.WindowShard shard : memtable.shards().values())
        {
            for (TimeSeriesMemtable.ShardPartition partition : shard.partitions.values())
            {
                assertTrue("this schema should be held columnar, got " + partition.getClass().getSimpleName(),
                           partition instanceof TimeSeriesColumnarPartition);
                fastPath += ((TimeSeriesColumnarPartition) partition).fastPathRowCount();
                overflowRows += ((TimeSeriesColumnarPartition) partition).overflowRowCount();
            }
        }
        return new int[]{ fastPath, overflowRows };
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
