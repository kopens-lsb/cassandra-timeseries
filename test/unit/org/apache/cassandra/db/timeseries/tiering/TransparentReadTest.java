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

package org.apache.cassandra.db.timeseries.tiering;

import java.util.Date;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SP3 end-to-end: after the re-encoder moves closed windows into the chunk table and range-deletes
 * the base rows, a plain SELECT on the base table must still return every row - hot and cold merged
 * transparently (design spec section 3.3.1).
 *
 * Timestamps follow the TieredStorageServiceTest convention: small epoch-millis values as event
 * times (all far below the real "now", so every closed window is cold), synthetic runOnce clock.
 */
public class TransparentReadTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }

    private void loadTwoWindowsAndReencode() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        // Window [0,1h): 3 samples; window [1h,2h): 2 samples. Written with explicit writetimes.
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 3.0) USING TIMESTAMP 103", new Date(30 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 4.0) USING TIMESTAMP 104", new Date(HOUR + 10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 5.0) USING TIMESTAMP 105", new Date(HOUR + 20 * 60_000L));
        // Hot row (stays as a base row: its window is inside hot_window of the synthetic now=5h).
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 6.0) USING TIMESTAMP 106", new Date(4 * HOUR + 10 * 60_000L));

        TieredStorageService.TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(2L, stats.windowsEncoded);
    }

    @Test
    public void fullRangeSelectSeesAllRowsAfterReencoding() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet rows = execute("SELECT ts, value FROM %s WHERE tag = 't1'");
        assertEquals(6, rows.size());
        double[] expected = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
        int i = 0;
        for (UntypedResultSet.Row row : rows)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    @Test
    public void coldOnlyRangeSelect() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ?",
                                        new Date(0), new Date(HOUR));
        assertEquals(3, rows.size());
    }

    @Test
    public void pointLookupOnColdTimestamp() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts = ?", new Date(20 * 60_000L));
        assertEquals(1, rows.size());
        assertEquals(2.0, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void aggregateSpansHotAndCold() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet rows = execute("SELECT count(value) AS c, avg(value) AS a FROM %s WHERE tag = 't1'");
        assertEquals(6L, rows.one().getLong("c"));
        assertEquals(3.5, rows.one().getDouble("a"), 0.0001);
    }

    @Test
    public void limitAndDescOrder() throws Throwable
    {
        loadTwoWindowsAndReencode();

        UntypedResultSet desc = execute("SELECT value FROM %s WHERE tag = 't1' ORDER BY ts DESC LIMIT 3");
        assertEquals(3, desc.size());
        double[] expected = { 6.0, 5.0, 4.0 };
        int i = 0;
        for (UntypedResultSet.Row row : desc)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    @Test
    public void lateRowInsertedAfterSweepWinsOverChunk() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // A correction written AFTER the window was encoded: newer writetime than the chunk's
        // max_row_writetime, so the merge must prefer it over the encoded 2.0.
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 9.9) USING TIMESTAMP 200", new Date(20 * 60_000L));

        assertEquals(9.9, execute("SELECT value FROM %s WHERE tag = 't1' AND ts = ?", new Date(20 * 60_000L))
                          .one().getDouble("value"), 0.0);
        assertEquals(6, execute("SELECT value FROM %s WHERE tag = 't1'").size());
    }

    @Test
    public void gapFillSpansHotAndCold() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Buckets over [0, 5h): cold windows 0 and 1h come from chunks, 2h/3h are empty (densified
        // with null), 4h is the hot base row. Do not alias the bucket column (gap-fill v1 rule).
        String gf = "time_bucket_gapfill(1h, ts, '1970-01-01 00:00:00+0000', '1970-01-01 05:00:00+0000')";
        assertRows(execute("SELECT " + gf + ", avg(value) FROM %s WHERE tag = 't1' GROUP BY tag, " + gf),
                   row(new Date(0L), 2.0),
                   row(new Date(HOUR), 4.5),
                   row(new Date(2 * HOUR), null),
                   row(new Date(3 * HOUR), null),
                   row(new Date(4 * HOUR), 6.0));
    }

    /** Overwrites the [0,1h) window's chunk payload with {@code hexPayload} (a 0x... CQL blob literal). */
    private void overwriteFirstChunkPayload(String hexPayload) throws Throwable
    {
        execute("UPDATE " + KEYSPACE + ".\"" + ChunkTables.chunkTableName(currentTable()) + "\" " +
                "SET payload = " + hexPayload + " WHERE tag = 't1' AND window_start = ?", new Date(0L));
    }

    private void assertOnlySecondWindowAndHotRowServed() throws Throwable
    {
        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals(3, rows.size());
        double[] expected = { 4.0, 5.0, 6.0 };
        int i = 0;
        for (UntypedResultSet.Row row : rows)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    @Test
    public void corruptChunkSkippedWithRemainingDataServed() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // A first byte naming no chunk format at all -- what a scrambled header looks like.
        overwriteFirstChunkPayload("0xdeadbeef");

        // Window [0,1h) is unreadable and skipped; window [1h,2h) and the hot row still serve.
        assertOnlySecondWindowAndHotRowServed();
    }

    @Test
    public void corruptChunkWithASupportedVersionIsStillSkipped() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Version byte 3 (the format this build writes), but a header claiming 0 rows: corrupt
        // CONTENT, not a format this build cannot read. Availability wins here -- skip the one bad
        // chunk, serve the rest.
        overwriteFirstChunkPayload("0x03" + "0".repeat(40));

        assertOnlySecondWindowAndHotRowServed();
    }

    @Test
    public void chunkFromARemovedCodecFailsTheReadInsteadOfTruncatingIt() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Version byte 2 = the removed single-column (chimp128) format; byte 1 was gorilla. Unlike
        // corruption this is systematic (every chunk an older build wrote carries it), so skipping
        // would make this SELECT -- and every future one -- succeed while silently omitting the
        // [0,1h) window's history.
        overwriteFirstChunkPayload("0x02" + "0".repeat(40));

        try
        {
            execute("SELECT value FROM %s WHERE tag = 't1'");
            fail("expected the read to fail rather than silently drop the unreadable window");
        }
        catch (UnsupportedChunkFormatException e)
        {
            // must name the table, the window that proved it, and what the operator has to do
            assertTrue(e.getMessage(), e.getMessage().contains(currentTable()));
            assertTrue(e.getMessage(), e.getMessage().contains("older build"));
            assertTrue(e.getMessage(), e.getMessage().contains(ChunkTables.chunkTableName(currentTable())));
            assertTrue(e.getMessage(), e.getMessage().contains("not recoverable"));
        }
    }

    @Test
    public void multiPageMergedReadReturnsEveryRow() throws Throwable
    {
        // SP4: the merge moved inside the read machinery and now runs on each PAGE's own read
        // command, so chunk rows flow through the same limits and counters as hot rows and paging
        // composes with the merge. This used to fail with "spans multiple pages" -- a v1 restriction
        // that only existed because the merge was bolted on after the pager had already counted.
        // Also the only coverage of the COORDINATOR hook (DigestResolver.getData); every other test
        // here goes through the local path.
        requireNetwork();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, ?) USING TIMESTAMP ?",
                    new Date(i * 600_000L), i * 1.0, 100L + i);
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 99.0) USING TIMESTAMP 200", new Date(4 * HOUR));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        // 6 rows (5 chunked + 1 hot) at page size 2 = three pages.
        java.util.List<com.datastax.driver.core.Row> rows =
            executeNetWithPaging("SELECT ts, value FROM %s WHERE tag = 't1'", 2).all();
        assertEquals(6, rows.size());
        for (int i = 0; i < 5; i++)
            assertEquals(i * 1.0, rows.get(i).getDouble("value"), 0.0);
        assertEquals(99.0, rows.get(5).getDouble("value"), 0.0);
    }

    @Test
    public void worksWithAnyPartitionKeyColumnName() throws Throwable
    {
        // Regression: the chunk lookup must use the base table's actual pk column name (the docker
        // canonical schema uses tag_id, the tests above use tag - hardcoding either breaks the other).
        createTable("CREATE TABLE %s (tag_id text, ts timestamp, value double, PRIMARY KEY (tag_id, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        execute("INSERT INTO %s (tag_id, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(10 * 60_000L));
        execute("INSERT INTO %s (tag_id, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * 60_000L));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        assertEquals(2, execute("SELECT value FROM %s WHERE tag_id = 't1'").size());
    }

    @Test
    public void nonTieredTableUnaffected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0)", new Date(1000));
        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals(1, rows.size());

        // The row count above is satisfied even if every read is fully wrapped and issues a chunk
        // query per partition -- it asserts nothing about the fast path. The actual requirement is
        // that a table with no tiering policy is not wrapped AT ALL, i.e. maybeWrap hands back the
        // very iterator it was given.
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        UnfilteredPartitionIterator hot = EmptyIterators.unfilteredPartition(metadata);
        assertSame("a table with no tiering policy must not be wrapped", hot, maybeWrapFullPartitionRead(metadata, hot));
    }

    /**
     * (a) Raising {@code hot_window} -- an ordinary tuning change -- must not hide data that was
     * encoded under the old, shorter one. The rows below are inside the NEW hot window but their base
     * rows were deleted when they were chunked, so a merge gate that asks the current policy takes the
     * hot-only fast path and silently returns nothing.
     */
    @Test
    public void raisingHotWindowStillServesRowsEncodedUnderTheOldOne() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\"}");

        // A closed, hour-aligned window ~3h ago: unambiguously cold under hot_window=1h whenever the
        // test runs, and unambiguously hot under hot_window=24h.
        long now = System.currentTimeMillis();
        long window = now / HOUR * HOUR - 3 * HOUR;
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(window + 10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(window + 20 * 60_000L));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), now).windowsEncoded);
        assertEquals("the rows must live only in the chunk table now",
                     2, execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ?", new Date(window)).size());

        // Months later, an operator widens the hot window. Nothing about the already-encoded data changed.
        setPolicy("{\"hot_window\":\"24h\",\"chunk_window\":\"1h\"}");

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ?", new Date(window));
        assertEquals("raising hot_window must not hide already-encoded history", 2, rows.size());
    }

    /**
     * (b) Removing the {@code timeseries_tiering} extension stops new encoding; it must not make every
     * already-encoded window disappear from every {@code SELECT}. Removing a configuration option is
     * not an instruction to hide data whose only copy is the chunk table.
     */
    @Test
    public void droppingTheExtensionDoesNotHideEncodedHistory() throws Throwable
    {
        loadTwoWindowsAndReencode();

        alterTable("ALTER TABLE %s WITH extensions = {};");

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals("dropping the extension must not hide the chunk table's contents", 6, rows.size());
        double[] expected = { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
        int i = 0;
        for (UntypedResultSet.Row row : rows)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    /**
     * (c) Shrinking {@code chunk_window} must not orphan the wider chunks already written. The
     * merge's look-back has to use the widest width any existing chunk was written with; one
     * <em>current</em> window is not far enough back to find a 24h chunk whose {@code window_start}
     * sits 20h before the query range.
     */
    @Test
    public void shrinkingChunkWindowStillFindsWiderChunks() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"48h\",\"chunk_window\":\"24h\"}");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(HOUR));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * HOUR));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 120 * HOUR).windowsEncoded);

        // The one chunk covers [0, 24h). Shrink the window: its window_start is now 20 hours -- far
        // more than one chunk_window -- before the range asked for below.
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ? AND ts < ?",
                                        new Date(20 * HOUR), new Date(21 * HOUR));
        assertEquals("a chunk wider than the current chunk_window must still be found", 1, rows.size());
        assertEquals(2.0, rows.one().getDouble("value"), 0.0);
    }

    /**
     * The fast path still exists and is still free: a query whose range is entirely above everything
     * the chunk table covers must not read the chunk table at all. Proved behaviourally -- the one
     * chunk is rewritten as a format this build refuses to read, so any chunk read would fail the
     * query rather than return.
     */
    @Test
    public void queryAboveChunkCoverageDoesNotReadChunksAtAll() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\"}");

        long now = System.currentTimeMillis();
        long window = now / HOUR * HOUR - 3 * HOUR;
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(window + 10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 7.0) USING TIMESTAMP 103", new Date(now + 1000L));
        assertEquals(1L, new TieredStorageService().runOnce(KEYSPACE, currentTable(), now).windowsEncoded);

        // Version byte 2 = a removed codec: reading this chunk throws rather than returning anything.
        execute("UPDATE " + KEYSPACE + ".\"" + ChunkTables.chunkTableName(currentTable()) + "\" " +
                "SET payload = 0x02" + "0".repeat(40) + " WHERE tag = 't1' AND window_start = ?", new Date(window));

        // Above the top of coverage: served from hot rows only, without touching the chunk table.
        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ?", new Date(now));
        assertEquals(1, rows.size());
        assertEquals(7.0, rows.one().getDouble("value"), 0.0);

        // ...and a query that DOES reach into coverage reads the chunk, and fails on it.
        try
        {
            execute("SELECT value FROM %s WHERE tag = 't1' AND ts >= ?", new Date(window));
            fail("a query reaching into chunk coverage must read the chunk table");
        }
        catch (UnsupportedChunkFormatException expected)
        {
            // the chunk read happened, which is the point
        }
    }

    /**
     * A chunk read that fails must fail the {@code SELECT}. Degrading to hot rows only turns a
     * routine availability fault -- {@code ReadTimeoutException}, {@code UnavailableException},
     * {@code TombstoneOverwhelmingException}, all normal at QUORUM -- into a <em>successful</em>
     * query that is silently missing every cold row, behind a client warning most drivers never
     * surface.
     */
    @Test
    public void chunkReadFailureFailsTheQueryInsteadOfOmittingColdData() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Break the chunk SELECT itself. A dropped column is the only chunk-read failure a
        // single-node unit test can provoke deterministically; the failures that matter in production
        // (timeout, unavailable replica) arrive at exactly this catch by exactly the same route.
        execute("ALTER TABLE " + KEYSPACE + ".\"" + ChunkTables.chunkTableName(currentTable()) + "\" DROP payload");

        try
        {
            UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
            fail("expected the failed chunk read to fail the query; got " + rows.size() + " hot-only row(s)");
        }
        catch (RuntimeException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("payload"));
        }
    }

    /** @return what {@link TransparentReads#maybeWrap} does to {@code hot} for a full-partition read of 't1'. */
    private static UnfilteredPartitionIterator maybeWrapFullPartitionRead(TableMetadata metadata,
                                                                          UnfilteredPartitionIterator hot)
    {
        DecoratedKey key = metadata.partitioner.decorateKey(UTF8Type.instance.decompose("t1"));
        SinglePartitionReadCommand command =
            SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), key);
        return TransparentReads.maybeWrap(metadata, command, hot, null);
    }
}
