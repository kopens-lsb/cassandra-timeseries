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
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
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

        // Version byte 2 (supported), but a header claiming 0 samples: corrupt CONTENT, not a format
        // this build cannot read. Availability wins here -- skip the one bad chunk, serve the rest.
        overwriteFirstChunkPayload("0x02" + "0".repeat(40));

        assertOnlySecondWindowAndHotRowServed();
    }

    @Test
    public void chunkFromARemovedCodecFailsTheReadInsteadOfTruncatingIt() throws Throwable
    {
        loadTwoWindowsAndReencode();

        // Version byte 1 = the removed gorilla format. Unlike corruption this is systematic (every
        // chunk the old build wrote carries it), so skipping would make this SELECT -- and every
        // future one -- succeed while silently omitting the [0,1h) window's history.
        overwriteFirstChunkPayload("0x01" + "0".repeat(40));

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
    public void multiPageMergedReadFailsWithHint() throws Throwable
    {
        requireNetwork();
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0)", new Date(1000L * (i + 1)));

        // A full-range query on a tiering table activates the merge; needing a second page must fail
        // with a remedy instead of silently duplicating or losing chunk rows (v1 scope).
        try
        {
            executeNetWithPaging("SELECT * FROM %s WHERE tag = 't1'", 2).all();
            fail("expected InvalidQueryException for a multi-page transparent tiered read");
        }
        catch (com.datastax.driver.core.exceptions.InvalidQueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("spans multiple pages"));
        }
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
    }
}
