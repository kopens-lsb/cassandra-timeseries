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
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
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
                                          assertTrue("reversed clusterings must not use the long store",
                                                     !((TimeSeriesColumnarPartition) partition).usesLongClusterings());
                                  assertEquals(1, columnarCounts(memtable)[1]);
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT * FROM %s WHERE series = 'S1' ORDER BY ts ASC");
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
