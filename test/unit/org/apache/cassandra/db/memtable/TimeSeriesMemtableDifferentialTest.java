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
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Whether {@link TimeSeriesMemtable} gives the same answers as the memtable it replaces.
 *
 * <p>A memtable defect shows up as silent data loss, not as an exception, so "it ran" proves nothing.
 * Every case here applies one operation sequence twice — once to a table on {@code memtable =
 * 'skiplist'} and once to a table on {@code memtable = 'timeseries'} — and requires the two to read
 * back byte-identical, both out of the memtable and again after a flush.
 *
 * <p>The sequences are built so that the pieces of a single row land in <em>different</em> window
 * shards, because that is what this memtable does that the reference does not: a row's window is
 * decided by its write timestamp, so an insert and a later partial update of the same row live in
 * separate shards and only the read merge puts them back together. Each case asserts that the
 * sharding actually happened ({@link #assertShardedOverWindows}) — without that, a bug that quietly
 * fell back to a single shard would leave every comparison green.
 *
 * <p>Every case reads through <b>both</b> read paths, because they are separate code in the memtable
 * and a merge can be broken in one and not the other: a full-table {@code SELECT} goes through
 * {@code partitionIterator}, a {@code WHERE <partition key> = ...} through {@code rowIterator}. The
 * post-flush read covers the third, {@code getFlushSet}.
 */
public class TimeSeriesMemtableDifferentialTest extends CQLTester
{
    private static final long HOUR_MS = 3_600_000L;

    /** 2026-08-02T00:00:00Z — exactly on an hour boundary, so {@code +k hours} is window {@code k}. */
    private static final long BASE_MS = 1785628800000L;

    private static final String TSCS =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}";

    /**
     * A counter cell cannot carry a custom timestamp (CQL rejects {@code USING TIMESTAMP} on counter
     * updates), so the only way to move counter writes across a window boundary is to make boundaries
     * happen quickly in wall-clock terms. Declaring the resolution as milliseconds while the cells are
     * really microseconds scales time by 1000, so a one-minute window is 60 ms of real time.
     */
    private static final String TSCS_FAST_WINDOWS =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1m','timestamp_resolution':'MILLISECONDS'}";

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

    // ------------------------------------------------------------------------------------- cases

    /** Writes scattered over six windows, in no particular order. */
    @Test
    public void matchesReferenceAcrossWindowBoundaries() throws Throwable
    {
        List<Object[]> ops = randomOperations(400, 20260802L);
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (Object[] op : ops)
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              op[0], op[1], op[2], op[3]);
                              },
                              "SELECT * FROM %s WHERE series = 'S0'",
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT * FROM %s WHERE series = 'S2'",
                              "SELECT * FROM %s WHERE series = 'S3'",
                              "SELECT * FROM %s WHERE series = 'S4'",
                              "SELECT * FROM %s WHERE series = 'S0' ORDER BY ts DESC");
    }

    /** The same (partition, clustering) written repeatedly across windows: the last write must win. */
    @Test
    public void matchesReferenceOnRepeatedWritesToTheSameClustering() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 6; i++)
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS, i * 1.5, at(i));
                              },
                              "SELECT * FROM %s WHERE series = 'S1'");
    }

    /** Rows arriving newest-first, so every shard is created out of order. */
    @Test
    public void matchesReferenceOnOutOfOrderArrival() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 200; i >= 0; i--)
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S" + (i % 4), BASE_MS + i * 60_000L, i * 1.5, at(i % 6));
                              },
                              "SELECT * FROM %s WHERE series = 'S0'",
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT * FROM %s WHERE series = 'S2'",
                              "SELECT * FROM %s WHERE series = 'S3'");
    }

    /**
     * An insert and a later single-column update of the same row. This is the case the sharding makes
     * hardest: the row exists in several shards, each holding a different subset of its cells, and
     * only the read merge can produce the whole row.
     */
    @Test
    public void matchesReferenceOnPartialColumnUpdate() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, note text, " +
                              "PRIMARY KEY (series, ts)) WITH compaction = " + TSCS,
                              () -> {
                                  execute("INSERT INTO %s (series, ts, v, q, note) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS, 1.0, 10, "first", at(0));
                                  execute("UPDATE %s USING TIMESTAMP ? SET q = ? WHERE series = ? AND ts = ?",
                                          at(1), 20, "S1", BASE_MS);
                                  execute("UPDATE %s USING TIMESTAMP ? SET note = ? WHERE series = ? AND ts = ?",
                                          at(2), "third", "S1", BASE_MS);
                                  execute("UPDATE %s USING TIMESTAMP ? SET v = ? WHERE series = ? AND ts = ?",
                                          at(3), 4.0, "S1", BASE_MS);
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT q, note FROM %s WHERE series = 'S1' AND ts = " + BASE_MS);
    }

    /** A row deleted in a later window than the one that holds it. */
    @Test
    public void matchesReferenceOnRowDeletion() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 5; i++)
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, at(0));
                                  execute("DELETE FROM %s USING TIMESTAMP ? WHERE series = ? AND ts = ?",
                                          at(2), "S1", BASE_MS + 2000L);
                                  // Re-inserted after the deletion, into a third window.
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 2000L, 99.0, at(4));
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT * FROM %s WHERE series = 'S1' AND ts = " + (BASE_MS + 2000L));
    }

    /** Range and partition deletions, whose window comes from their own deletion timestamp. */
    @Test
    public void matchesReferenceOnRangeAndPartitionDeletion() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 10; i++)
                                  {
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S1", BASE_MS + i * 1000L, i * 1.0, at(0));
                                      execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "S2", BASE_MS + i * 1000L, i * 1.0, at(1));
                                  }
                                  execute("DELETE FROM %s USING TIMESTAMP ? WHERE series = ? AND ts >= ? AND ts <= ?",
                                          at(3), "S1", BASE_MS + 3000L, BASE_MS + 6000L);
                                  execute("DELETE FROM %s USING TIMESTAMP ? WHERE series = ?", at(4), "S2");
                                  // Written after the partition delete, in a still later window.
                                  execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "S2", BASE_MS, 7.0, at(5));
                              },
                              "SELECT * FROM %s WHERE series = 'S1'",
                              "SELECT * FROM %s WHERE series = 'S2'",
                              "SELECT * FROM %s WHERE series = 'S1' AND ts >= " + (BASE_MS + 2000L) +
                              " AND ts <= " + (BASE_MS + 7000L));
    }

    /** TTLs, whose local expiration time is carried on the cell and must survive the split. */
    @Test
    public void matchesReferenceOnTtl() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (series text, ts timestamp, v double, q int, PRIMARY KEY (series, ts)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ? AND TTL 86400",
                                          "S1", BASE_MS, 1.0, 1, at(0));
                                  execute("UPDATE %s USING TIMESTAMP ? AND TTL 86400 SET q = ? WHERE series = ? AND ts = ?",
                                          at(2), 2, "S1", BASE_MS);
                                  execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                                          "S1", BASE_MS + 1000L, 5.0, 5, at(3));
                              },
                              "SELECT * FROM %s WHERE series = 'S1'");
    }

    // ---------------------------------------------------- shapes the schema gate used to turn away

    /** A compound partition key with three clustering columns, none of them a timestamp. */
    @Test
    public void matchesReferenceOnCompoundKeyWithThreeClusterings() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (site_id text, year int, month int, " +
                              "day int, hour int, minute int, v double, " +
                              "PRIMARY KEY ((site_id, year, month), day, hour, minute)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int day = 1; day <= 3; day++)
                                      for (int hour = 0; hour < 4; hour++)
                                          for (int minute = 0; minute < 3; minute++)
                                              execute("INSERT INTO %s (site_id, year, month, day, hour, minute, v) " +
                                                      "VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
                                                      "site-" + (day % 2), 2026, 8, day, hour, minute,
                                                      day * 100.0 + hour * 10 + minute,
                                                      at((day + hour) % 6));
                                  execute("UPDATE %s USING TIMESTAMP ? SET v = ? " +
                                          "WHERE site_id = ? AND year = ? AND month = ? AND day = ? AND hour = ? AND minute = ?",
                                          at(5), -1.0, "site-1", 2026, 8, 1, 0, 0);
                                  execute("DELETE FROM %s USING TIMESTAMP ? " +
                                          "WHERE site_id = ? AND year = ? AND month = ? AND day = ? AND hour = ?",
                                          at(4), "site-0", 2026, 8, 2, 1);
                              },
                              "SELECT * FROM %s WHERE site_id = 'site-0' AND year = 2026 AND month = 8",
                              "SELECT * FROM %s WHERE site_id = 'site-1' AND year = 2026 AND month = 8",
                              "SELECT * FROM %s WHERE site_id = 'site-1' AND year = 2026 AND month = 8 AND day = 1");
    }

    /** A plain text clustering column: the window never came from the clustering value anyway. */
    @Test
    public void matchesReferenceOnNonTimestampClustering() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (k text, c text, v double, PRIMARY KEY (k, c)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  for (int i = 0; i < 40; i++)
                                      execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                              "k" + (i % 3), "c" + i, i * 1.0, at(i % 6));
                                  execute("UPDATE %s USING TIMESTAMP ? SET v = ? WHERE k = ? AND c = ?",
                                          at(5), 123.0, "k0", "c0");
                              },
                              "SELECT * FROM %s WHERE k = 'k0'",
                              "SELECT * FROM %s WHERE k = 'k1'",
                              "SELECT * FROM %s WHERE k = 'k2'");
    }

    /**
     * A non-frozen {@code map}: additions, a removal and a single-key overwrite, each at a timestamp
     * that puts it in a different window, so the collection's cells end up spread over four shards
     * and only the merge reassembles it.
     */
    @Test
    public void matchesReferenceOnNonFrozenCollection() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (k text, c timestamp, tags map<text,text>, PRIMARY KEY (k, c)) " +
                              "WITH compaction = " + TSCS,
                              () -> {
                                  execute("INSERT INTO %s (k, c, tags) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "k1", BASE_MS, map("a", "1", "b", "2"), at(0));
                                  execute("UPDATE %s USING TIMESTAMP ? SET tags = tags + ? WHERE k = ? AND c = ?",
                                          at(1), map("c", "3"), "k1", BASE_MS);
                                  execute("UPDATE %s USING TIMESTAMP ? SET tags = tags - ? WHERE k = ? AND c = ?",
                                          at(2), set("a"), "k1", BASE_MS);
                                  execute("UPDATE %s USING TIMESTAMP ? SET tags['b'] = ? WHERE k = ? AND c = ?",
                                          at(3), "22", "k1", BASE_MS);
                                  // A second row whose whole collection is replaced in a later window.
                                  execute("INSERT INTO %s (k, c, tags) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "k1", BASE_MS + 1000L, map("x", "9"), at(1));
                                  execute("INSERT INTO %s (k, c, tags) VALUES (?, ?, ?) USING TIMESTAMP ?",
                                          "k1", BASE_MS + 1000L, map("y", "8"), at(4));
                              },
                              "SELECT * FROM %s WHERE k = 'k1'",
                              "SELECT tags FROM %s WHERE k = 'k1' AND c = " + BASE_MS,
                              "SELECT tags FROM %s WHERE k = 'k1' AND c = " + (BASE_MS + 1000L));
    }

    /**
     * Counter increments spread over several windows. Counters are read-modify-write, so a shard split
     * is only invisible if the read that feeds the next increment merges the shards correctly.
     */
    @Test
    public void matchesReferenceOnCounterIncrementsAcrossWindows() throws Throwable
    {
        assertSameAsReference("CREATE TABLE %s (k text, c text, hits counter, PRIMARY KEY (k, c)) " +
                              "WITH compaction = " + TSCS_FAST_WINDOWS,
                              () -> {
                                  for (int i = 0; i < 3; i++)
                                  {
                                      execute("UPDATE %s SET hits = hits + 5 WHERE k = 'a' AND c = 'x'");
                                      execute("UPDATE %s SET hits = hits + 3 WHERE k = 'a' AND c = 'y'");
                                      execute("UPDATE %s SET hits = hits - 1 WHERE k = 'b' AND c = 'x'");
                                      // One window is 60 ms of real time under this table's declared
                                      // timestamp resolution, so 80 ms guarantees the next batch lands
                                      // in a different shard.
                                      Thread.sleep(80);
                                  }
                              },
                              "SELECT k, c, hits FROM %s WHERE k = 'a'",
                              "SELECT k, c, hits FROM %s WHERE k = 'b'",
                              "SELECT k, c, hits FROM %s WHERE k = 'a' AND c = 'x'");
    }

    // ------------------------------------------------------------------------------------ harness

    /**
     * Applies {@code work} to a fresh table on the reference memtable and to a fresh table on
     * {@link TimeSeriesMemtable}, and requires both to read back identically before and after a flush.
     *
     * <p>{@code selects} are read in addition to a full-table scan; they exist to exercise the
     * single-partition read path, which is separate code from the range scan in a memtable.
     */
    private void assertSameAsReference(String schema, Workload work, String... selects) throws Throwable
    {
        String reference = runAndDump(schema, "skiplist", work, selects, false);
        String sharded = runAndDump(schema, "timeseries", work, selects, true);
        assertEquals(reference, sharded);
    }

    private String runAndDump(String schema, String memtable, Workload work, String[] selects, boolean expectSharded)
    throws Throwable
    {
        createTable(schema + " AND memtable = '" + memtable + "'");
        work.run();

        if (expectSharded)
            assertShardedOverWindows();

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

    /**
     * The memtable in use really is this one and it really did split the writes. Without this the
     * comparison would still pass if {@code create} had fallen back to the default memtable, or if a
     * window-assignment bug had funnelled every write into one shard.
     */
    private void assertShardedOverWindows()
    {
        Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
        assertTrue("expected a TimeSeriesMemtable, got " + current.getClass().getName(),
                   current instanceof TimeSeriesMemtable);
        int windows = ((TimeSeriesMemtable) current).shards().size();
        assertTrue("the workload was meant to span several windows but produced " + windows, windows > 1);
    }

    /**
     * Renders a result set as hex, column by column. Hex rather than a typed rendering because the
     * point is a byte-for-byte comparison of what the two memtables produced, including collection
     * element order and anything a typed accessor would normalise away.
     */
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

    /** series, clustering millis, value, write timestamp in microseconds. */
    private static List<Object[]> randomOperations(int count, long seed)
    {
        Random random = new Random(seed);
        List<Object[]> operations = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            int window = random.nextInt(6);
            operations.add(new Object[]{ "S" + random.nextInt(5),
                                         BASE_MS + window * HOUR_MS + random.nextInt(3600) * 1000L,
                                         Math.round(random.nextGaussian() * 1000) / 100.0,
                                         at(window) + random.nextInt(1000) });
        }
        return operations;
    }
}
