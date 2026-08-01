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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService.TierRunStats;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SP4 Task 3: the re-encoder chunks <b>every</b> regular column, not a designated value column.
 * <p>
 * These are the correctness tests for the code that deletes production rows after encoding them, so
 * they assert on the physical chunk (every column of every row, nulls included), on the physical
 * base table (what the range delete actually removed), and on the merged SELECT (what a client sees
 * afterwards) -- not on any one of the three alone.
 */
public class TieredStorageColumnsTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;
    private static final MapType<String, String> ATTRIBUTE_TYPE =
        MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false);

    // ---- the production shape: tm_tag_point ----

    /**
     * pp.tm_tag_point's shape, with pp's measured sparsity: a column that is always null
     * ({@code value_numeric}), columns that are constant ({@code quality}, {@code error_code}), a
     * high-entropy one ({@code latency}), a low-cardinality opaque one (the frozen map
     * {@code attribute}), and text/boolean columns with holes in them.
     */
    private String createProductionShapedTable() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (" +
                                   "tag_id text, timestamp timestamp, " +
                                   "area_id text static, asset_id text static, line_id text static, " +
                                   "opc_id text static, site_id text static, tag_name text static, type text static, " +
                                   "attribute frozen<map<text,text>>, error_code int, latency int, quality int, " +
                                   "value text, value_boolean boolean, value_numeric double, " +
                                   "PRIMARY KEY (tag_id, timestamp)) " +
                                   "WITH CLUSTERING ORDER BY (timestamp DESC) AND default_time_to_live = 5356800");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        return table;
    }

    private static final long[] TS = { 0L, 600_000L, 1_200_000L, 1_800_000L, 2_400_000L, 3_000_000L };
    private static final Integer[] LATENCY = { 12, 40, 7, 999, 3, 12 };
    private static final String[] VALUE = { "a", "b", "a", "c", null, "e" };
    private static final Boolean[] VALUE_BOOLEAN = { true, false, null, true, true, false };
    private static final String[] UNIT = { "C", "C", "F", "C", "F", "C" };

    private void loadProductionShapedRows() throws Throwable
    {
        execute("INSERT INTO %s (tag_id, area_id, asset_id, line_id, opc_id, site_id, tag_name, type) " +
                "VALUES ('t1', 'a', 'as', 'l', 'o', 's', 'tn', 'ty') USING TIMESTAMP 100");
        for (int i = 0; i < TS.length; i++)
        {
            // Columns that are null on this row are OMITTED rather than bound as null: binding null
            // writes a cell tombstone, and tombstoning a cold clustering is refused outright now
            // (TieredWrites). Omitting is also what a real writer does.
            StringBuilder columns = new StringBuilder("tag_id, timestamp, attribute, error_code, latency, quality");
            StringBuilder markers = new StringBuilder("?, ?, ?, 0, ?, 192");
            List<Object> binds = new ArrayList<>(List.of("t1", new Date(TS[i]), attribute(UNIT[i]), LATENCY[i]));
            if (VALUE[i] != null)
            {
                columns.append(", value");
                markers.append(", ?");
                binds.add(VALUE[i]);
            }
            if (VALUE_BOOLEAN[i] != null)
            {
                columns.append(", value_boolean");
                markers.append(", ?");
                binds.add(VALUE_BOOLEAN[i]);
            }
            binds.add(101L + i);
            execute("INSERT INTO %s (" + columns + ") VALUES (" + markers + ") USING TIMESTAMP ?", binds.toArray());
        }
        // Hot row (window [4h,5h) against the synthetic now = 5h): must survive untouched.
        execute("INSERT INTO %s (tag_id, timestamp, latency) VALUES ('t1', ?, 1) USING TIMESTAMP 200",
                new Date(4 * HOUR));
    }

    @Test
    public void everyColumnOfEveryRowRoundTripsThroughTheChunk() throws Throwable
    {
        createProductionShapedTable();
        loadProductionShapedRows();

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, stats.windowsEncoded);
        assertEquals(TS.length, stats.rowsEncoded);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t1", new Date(0L)).one();
        assertEquals(TS.length, chunkRow.getInt("samples"));
        assertEquals(ColumnarChunkCodec.VERSION, chunkRow.getByte("codec"));

        // 1) the physical chunk carries every column of every row, nulls included
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        for (int i = 0; i < TS.length; i++)
        {
            assertTrue("expected sample " + i, cursor.advance());
            assertEquals(TS[i], cursor.timestamp());
            assertEquals(attribute(UNIT[i]), cursor.getBytes("attribute"));
            assertEquals(0, (int) Int32Type.instance.compose(cursor.getBytes("error_code")));
            assertEquals(192, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
            assertEquals((int) LATENCY[i], (int) Int32Type.instance.compose(cursor.getBytes("latency")));
            if (VALUE[i] == null)
                assertTrue("value must stay null at sample " + i, cursor.isNull("value"));
            else
                assertEquals(VALUE[i], UTF8Type.instance.compose(cursor.getBytes("value")));
            if (VALUE_BOOLEAN[i] == null)
                assertTrue("value_boolean must stay null at sample " + i, cursor.isNull("value_boolean"));
            else
                assertEquals(VALUE_BOOLEAN[i], BooleanType.instance.compose(cursor.getBytes("value_boolean")));
            // Never written by anyone: an all-null column must not materialise a default.
            assertTrue("value_numeric must stay null at sample " + i, cursor.isNull("value_numeric"));
        }
        assertFalse(cursor.advance());

        // 2) the source rows are physically gone, but the statics are not
        assertEquals(0, raw("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp < ?", new Date(HOUR)).size());
        UntypedResultSet statics = raw("SELECT area_id, asset_id, line_id, opc_id, site_id, tag_name, type, " +
                                       "WRITETIME(site_id) AS wt FROM %s WHERE tag_id = 't1'");
        assertEquals(1, statics.size());
        assertEquals("s", statics.one().getString("site_id"));
        assertEquals("ty", statics.one().getString("type"));
        assertEquals(100L, statics.one().getLong("wt"));

        // 3) a plain SELECT returns every row again, every column exactly as written
        UntypedResultSet merged = execute("SELECT timestamp, attribute, error_code, latency, quality, value, " +
                                          "value_boolean, value_numeric FROM %s WHERE tag_id = 't1' " +
                                          "AND timestamp < ? ORDER BY timestamp ASC", new Date(HOUR));
        assertEquals(TS.length, merged.size());
        int i = 0;
        for (UntypedResultSet.Row row : merged)
        {
            assertEquals(new Date(TS[i]), row.getTimestamp("timestamp"));
            assertEquals(attribute(UNIT[i]), row.getBytes("attribute"));
            assertEquals(0, row.getInt("error_code"));
            assertEquals(192, row.getInt("quality"));
            assertEquals((int) LATENCY[i], row.getInt("latency"));
            assertEquals(VALUE[i], VALUE[i] == null ? null : row.getString("value"));
            assertEquals(VALUE[i] != null, row.has("value"));
            assertEquals(VALUE_BOOLEAN[i] != null, row.has("value_boolean"));
            if (VALUE_BOOLEAN[i] != null)
                assertEquals(VALUE_BOOLEAN[i], (Boolean) row.getBoolean("value_boolean"));
            assertFalse("value_numeric was never written and must read back as null",
                        row.has("value_numeric"));
            i++;
        }

        // 4) the hot row is untouched
        assertEquals(1, raw("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp = ?",
                            new Date(4 * HOUR)).size());
    }

    @Test
    public void descClusteredMergedReadsAreCompleteAndInOrder() throws Throwable
    {
        // Regression: on a table declared WITH CLUSTERING ORDER BY (ts DESC) -- the shape the
        // production table uses -- Slices arrive in DESCENDING timestamp order, so the comparator's
        // start bound is the range's UPPER time bound. Reading them as if ascending turned
        // `WHERE timestamp < X` into `[X+1, +inf)` and served ZERO cold rows, silently.
        createProductionShapedTable();
        loadProductionShapedRows();
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        // Natural (DESC) order over everything: 6 cold rows + the hot one, newest first.
        UntypedResultSet all = execute("SELECT timestamp FROM %s WHERE tag_id = 't1'");
        assertEquals(TS.length + 1, all.size());
        long previous = Long.MAX_VALUE;
        for (UntypedResultSet.Row row : all)
        {
            long ts = row.getTimestamp("timestamp").getTime();
            assertTrue("DESC table must return newest first, got " + ts + " after " + previous, ts < previous);
            previous = ts;
        }

        // A bounded slice: the half-open upper bound must land on the right side of the range.
        assertEquals(3, execute("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp < ?",
                                new Date(1_800_000L)).size());
        assertEquals(4, execute("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp <= ?",
                                new Date(1_800_000L)).size());
        assertEquals(3, execute("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp > ? AND timestamp < ?",
                                new Date(0L), new Date(2_400_000L)).size());

        // ORDER BY ASC on a DESC table (the "reversed" read) must come back oldest first.
        UntypedResultSet ascending = execute("SELECT timestamp FROM %s WHERE tag_id = 't1' " +
                                             "AND timestamp < ? ORDER BY timestamp ASC", new Date(HOUR));
        assertEquals(TS.length, ascending.size());
        int i = 0;
        for (UntypedResultSet.Row row : ascending)
            assertEquals(new Date(TS[i++]), row.getTimestamp("timestamp"));

        // A point lookup (names filter, not a slice) on a cold timestamp.
        assertEquals(1, execute("SELECT timestamp FROM %s WHERE tag_id = 't1' AND timestamp = ?",
                                new Date(1_200_000L)).size());
    }

    @Test
    public void aSecondCycleIsAByteIdenticalNoOp() throws Throwable
    {
        createProductionShapedTable();
        loadProductionShapedRows();

        TieredStorageService service = new TieredStorageService();
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        UntypedResultSet.Row before = execute(chunkPayloadQuery(), "t1", new Date(0L)).one();
        ByteBuffer firstPayload = ByteBufferUtil.clone(before.getBytes("payload"));
        long firstWritetime = before.getLong("chunk_wt");

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(0, second.windowsEncoded);
        assertEquals(0, second.rowsEncoded);
        assertEquals(0, second.lateMerges);
        assertEquals(0, second.bytesWritten);

        UntypedResultSet.Row after = execute(chunkPayloadQuery(), "t1", new Date(0L)).one();
        assertEquals("the chunk payload must be byte-identical after a second cycle",
                     firstPayload, after.getBytes("payload"));
        assertEquals("the chunk row must not even be rewritten", firstWritetime, after.getLong("chunk_wt"));
    }

    // ---- late-row merge semantics ----

    @Test
    public void aLateUpdateOfOneColumnKeepsTheOtherColumnsOfThatRow() throws Throwable
    {
        // `UPDATE t SET quality = ? WHERE ...` writes ONE cell. Merging must be PER COLUMN: the
        // columns the update did not mention keep the values already in the chunk. Replacing the
        // whole chunked row with the (mostly-null) base row would blank them.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, note text, " +
                    "PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts, value, quality, note) VALUES ('t', ?, 1.5, 192, 'ok') " +
                "USING TIMESTAMP 100", new Date(0L));
        execute("INSERT INTO %s (tag, ts, value, quality, note) VALUES ('t', ?, 2.5, 192, 'ok') " +
                "USING TIMESTAMP 101", new Date(600_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));

        TieredStorageService service = new TieredStorageService();
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());

        // Late correction touching ONLY quality, on a row that now exists only inside the chunk.
        execute("UPDATE %s USING TIMESTAMP 300 SET quality = 7 WHERE tag = 't' AND ts = ?", new Date(600_000L));

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, second.windowsEncoded);
        assertEquals(1, second.lateMerges);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t", new Date(0L)).one();
        assertEquals(2, chunkRow.getInt("samples"));         // merged, not appended
        assertEquals(300L, chunkRow.getLong("max_row_writetime"));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(chunkRow.getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(0L, cursor.timestamp());
        assertEquals(1.5, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertEquals(192, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertEquals("ok", UTF8Type.instance.compose(cursor.getBytes("note")));
        assertTrue(cursor.advance());
        assertEquals(600_000L, cursor.timestamp());
        assertEquals("the update must win on the column it touched",
                     7, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertEquals("an untouched column must keep the value the chunk already held",
                     2.5, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertEquals("ok", UTF8Type.instance.compose(cursor.getBytes("note")));
        assertFalse(cursor.advance());

        // ...and the same through a plain SELECT.
        UntypedResultSet merged = execute("SELECT value, quality, note FROM %s WHERE tag = 't' AND ts = ?",
                                          new Date(600_000L));
        assertEquals(1, merged.size());
        assertEquals(2.5, merged.one().getDouble("value"), 0.0);
        assertEquals(7, merged.one().getInt("quality"));
        assertEquals("ok", merged.one().getString("note"));
    }

    @Test
    public void aLateRowAtANewTimestampAddsAWholeRow() throws Throwable
    {
        // The other half of the merge rule: a late row at a timestamp the chunk does NOT hold is a
        // new row, and the columns it leaves out are null (there is nothing to inherit).
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 1.5, 192) USING TIMESTAMP 100",
                new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));

        TieredStorageService service = new TieredStorageService();
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        execute("INSERT INTO %s (tag, ts, quality) VALUES ('t', ?, 5) USING TIMESTAMP 300", new Date(600_000L));
        assertEquals(1, service.runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(execute(chunkSelectQuery(), "t", new Date(0L))
                                                          .one().getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(0L, cursor.timestamp());
        assertEquals(1.5, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertTrue(cursor.advance());
        assertEquals(600_000L, cursor.timestamp());
        assertEquals(5, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertTrue("a brand-new row's unwritten column must be null, not inherited from another row",
                   cursor.isNull("value"));
        assertFalse(cursor.advance());
    }

    // ---- cold data is immutable: writes that would tombstone it are rejected ----

    private void chunkOneRow() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 1.5, 192) USING TIMESTAMP 100",
                new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());
    }

    /** Asserts {@code query} is refused as an attempt to mutate chunked (cold) data. */
    private void assertRejectedAsColdWrite(String query, Object... values) throws Throwable
    {
        try
        {
            execute(query, values);
            fail("expected the write to be rejected: " + query);
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("immutable"));
            assertTrue(e.getMessage(), e.getMessage().contains("cold_window"));
        }
    }

    @Test
    public void deletingOneCellOfAChunkedRowIsRejected() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE value FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts = ?", new Date(0L));
        // ...and nothing changed: the row still reads back complete.
        UntypedResultSet rows = execute("SELECT value, quality FROM %s WHERE tag = 't' AND ts = ?", new Date(0L));
        assertEquals(1, rows.size());
        assertEquals(1.5, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void deletingAChunkedRowIsRejected() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts = ?", new Date(0L));
    }

    @Test
    public void deletingAChunkedRangeIsRejected() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts >= ? AND ts < ?",
                                  new Date(0L), new Date(HOUR));
    }

    @Test
    public void deletingAWholePartitionOfATieredTableIsRejected() throws Throwable
    {
        // No clustering bound at all, so it necessarily covers chunked data.
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE FROM %s USING TIMESTAMP 300 WHERE tag = 't'");
    }

    @Test
    public void updatingAChunkedColumnToNullIsRejected() throws Throwable
    {
        // `SET col = null` writes a cell tombstone, which is the same hazard as a DELETE -- and the
        // guard inspects the built mutation, so it is caught without special-casing the statement.
        chunkOneRow();
        assertRejectedAsColdWrite("UPDATE %s USING TIMESTAMP 300 SET value = null WHERE tag = 't' AND ts = ?",
                                  new Date(0L));
    }

    @Test
    public void insertingANullValueIntoAChunkedClusteringIsRejected() throws Throwable
    {
        // Same tombstone by another route: an INSERT that binds null for a column deletes that cell.
        chunkOneRow();
        assertRejectedAsColdWrite("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, ?) USING TIMESTAMP 300",
                                  new Date(0L), null);
    }

    @Test
    public void conditionalDeleteOfChunkedDataIsRejected() throws Throwable
    {
        // LWT/CAS builds its update in CQL3CasRequest.makeUpdates and never goes through
        // ModificationStatement.getMutations, so without a guard there `IF` was a way straight past
        // the immutability rule. The condition must be one that PASSES against a non-existent base
        // row (chunked data is invisible to LWT conditions -- see below), or the statement no-ops
        // before it ever builds an update.
        chunkOneRow();
        assertRejectedAsColdWrite("DELETE FROM %s WHERE tag = 't' AND ts = ? IF value = null", new Date(0L));
        assertRejectedAsColdWrite("DELETE value FROM %s WHERE tag = 't' AND ts = ? IF quality = null", new Date(0L));
    }

    @Test
    public void conditionalUpdateToNullOfChunkedDataIsRejected() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("UPDATE %s SET value = null WHERE tag = 't' AND ts = ? IF quality = null",
                                  new Date(0L));
    }

    @Test
    public void aConditionalBatchCannotSmuggleAColdDeleteThrough() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("BEGIN BATCH " +
                                  "DELETE value FROM %s WHERE tag = 't' AND ts = ? IF quality = null " +
                                  "APPLY BATCH", new Date(0L));
    }

    @Test
    public void aConditionalWriteOfARealValueToAColdClusteringStillSucceeds() throws Throwable
    {
        // The rule is about UN-writing cold data; a conditional late correction is still legal.
        chunkOneRow();
        execute("UPDATE %s SET quality = 7 WHERE tag = 't' AND ts = ? IF value = null", new Date(0L));
        UntypedResultSet rows = execute("SELECT value, quality FROM %s WHERE tag = 't' AND ts = ?", new Date(0L));
        assertEquals(7, rows.one().getInt("quality"));
        assertEquals("the columns the UPDATE did not name still come from the chunk",
                     1.5, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void lwtConditionsDoNotSeeChunkedData() throws Throwable
    {
        // Documented limitation, and the reason the conditions above are written against null: a CAS
        // precondition read deliberately bypasses the hot+chunk merge, because Paxos can only
        // linearize data it owns and a chunk is a blob in another table that no ballot orders. So a
        // chunked row reads as ABSENT to `IF`, and `IF EXISTS` simply does not apply -- it writes
        // nothing, which is safe, rather than deleting cold data.
        chunkOneRow();
        UntypedResultSet applied = execute("DELETE FROM %s WHERE tag = 't' AND ts = ? IF EXISTS", new Date(0L));
        assertFalse("IF EXISTS must not apply against a chunked row", applied.one().getBoolean("[applied]"));
        // ...and the row is untouched.
        assertEquals(1.5, execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(0L))
                          .one().getDouble("value"), 0.0);
    }

    // ---- end-to-end: a tombstone written while the row was hot must survive re-encoding ----

    /**
     * Real-world sequence, and the only one the immutability rule leaves legal: delete something,
     * it ages out, tiering runs over its window. The delete must hold both before and after the
     * chunk is written, and the chunk must not contain the deleted data.
     */
    private String loadRecentWindowForDeletion() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, " +
                                   "PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        return table;
    }

    /** Hour-aligned start of the CURRENT window, so the rows below are unambiguously hot right now. */
    private static long currentWindowStart()
    {
        return (System.currentTimeMillis() / HOUR) * HOUR;
    }

    @Test
    public void aCellDeletedWhileHotStaysDeletedAfterReEncoding() throws Throwable
    {
        loadRecentWindowForDeletion();
        long base = currentWindowStart();
        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, ?, 192)",
                    new Date(base + i * 60_000L), i + 1.0);

        // Legal: still inside hot_window.
        execute("DELETE value FROM %s WHERE tag = 't' AND ts = ?", new Date(base + 60_000L));
        assertFalse(execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(base + 60_000L))
                    .one().has("value"));

        // Now let the window age out and re-encode it.
        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), base + 5 * HOUR).windowsEncoded);

        UntypedResultSet rows = execute("SELECT ts, value, quality FROM %s WHERE tag = 't'");
        assertEquals(3, rows.size());
        UntypedResultSet.Row[] all = rows.stream().toArray(UntypedResultSet.Row[]::new);
        assertEquals(1.0, all[0].getDouble("value"), 0.0);
        assertFalse("the cell deleted while hot must not come back from the chunk", all[1].has("value"));
        assertEquals(192, all[1].getInt("quality"));
        assertEquals(3.0, all[2].getDouble("value"), 0.0);

        // ...and the chunk itself never held it.
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(execute(chunkSelectQuery(), "t", new Date(base))
                                                          .one().getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertTrue(cursor.advance());
        assertEquals(base + 60_000L, cursor.timestamp());
        assertTrue("the chunk must not carry the deleted cell", cursor.isNull("value"));
    }

    @Test
    public void aRowDeletedWhileHotStaysDeletedAfterReEncoding() throws Throwable
    {
        loadRecentWindowForDeletion();
        long base = currentWindowStart();
        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, ?)", new Date(base + i * 60_000L), i + 1.0);

        execute("DELETE FROM %s WHERE tag = 't' AND ts = ?", new Date(base + 60_000L));
        assertEquals(2, execute("SELECT ts FROM %s WHERE tag = 't'").size());

        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), base + 5 * HOUR).windowsEncoded);

        UntypedResultSet rows = execute("SELECT ts, value FROM %s WHERE tag = 't'");
        assertEquals("the row deleted while hot must not come back from the chunk", 2, rows.size());
        UntypedResultSet.Row[] all = rows.stream().toArray(UntypedResultSet.Row[]::new);
        assertEquals(new Date(base), all[0].getTimestamp("ts"));
        assertEquals(new Date(base + 120_000L), all[1].getTimestamp("ts"));
        assertEquals(2, execute(chunkSelectQuery(), "t", new Date(base)).one().getInt("samples"));
    }

    @Test
    public void aRangeDeletedWhileHotStaysDeletedAfterReEncoding() throws Throwable
    {
        loadRecentWindowForDeletion();
        long base = currentWindowStart();
        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, ?)", new Date(base + i * 60_000L), i + 1.0);

        // Range delete over the first two, while all four are hot. The range tombstone's own
        // timestamp is newer than every row's, so the re-encoder's delete does NOT remove it: it is
        // still in the base partition when the merged read runs against the chunk.
        execute("DELETE FROM %s WHERE tag = 't' AND ts >= ? AND ts < ?",
                new Date(base), new Date(base + 120_000L));
        assertEquals(2, execute("SELECT ts FROM %s WHERE tag = 't'").size());

        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), base + 5 * HOUR).windowsEncoded);

        UntypedResultSet rows = execute("SELECT ts, value FROM %s WHERE tag = 't'");
        assertEquals("the range deleted while hot must not come back from the chunk", 2, rows.size());
        UntypedResultSet.Row[] all = rows.stream().toArray(UntypedResultSet.Row[]::new);
        assertEquals(3.0, all[0].getDouble("value"), 0.0);
        assertEquals(4.0, all[1].getDouble("value"), 0.0);
    }

    // ---- the documented tie at exactly max_row_writetime + 1 ----

    @Test
    public void aLateRowAtExactlyMaxWritetimePlusOneTiesWithTheChunk() throws Throwable
    {
        // Chunk rows are reconstructed at max_row_writetime + 1 (ChunkReadSupport), so a late row
        // written at exactly that microsecond TIES with the reconstruction, and Cassandra breaks cell
        // ties by comparing the serialized values (larger wins). This test pins that documented
        // hazard in both directions -- it is the price of (A), not a desirable behaviour.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('bigger', ?, 1.0) USING TIMESTAMP 100", new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('smaller', ?, 1.0) USING TIMESTAMP 100", new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('bigger', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('smaller', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));
        assertEquals(2, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        // maxWt is 100, so the reconstruction sits at 101; write both late rows at exactly 101.
        execute("INSERT INTO %s (tag, ts, value) VALUES ('bigger', ?, 2.0) USING TIMESTAMP 101", new Date(0L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('smaller', ?, 0.5) USING TIMESTAMP 101", new Date(0L));

        // 2.0 sorts above 1.0 as big-endian IEEE-754 bytes, so the late row wins...
        assertEquals(2.0, execute("SELECT value FROM %s WHERE tag = 'bigger' AND ts = ?", new Date(0L))
                          .one().getDouble("value"), 0.0);
        // ...and 0.5 sorts below 1.0, so the STALE CHUNK VALUE wins. This is the documented hazard:
        // a late correction to a smaller value, written at exactly maxWt + 1, is lost.
        assertEquals(1.0, execute("SELECT value FROM %s WHERE tag = 'smaller' AND ts = ?", new Date(0L))
                          .one().getDouble("value"), 0.0);
    }

    @Test
    public void writingRealValuesToColdClusteringsIsStillAllowed() throws Throwable
    {
        // Only UN-writing cold data is refused. A late correction is a supported operation: the
        // re-encoder merges it into the chunk per column on its next cycle.
        chunkOneRow();
        execute("UPDATE %s USING TIMESTAMP 300 SET quality = 7 WHERE tag = 't' AND ts = ?", new Date(0L));
        UntypedResultSet rows = execute("SELECT value, quality FROM %s WHERE tag = 't' AND ts = ?", new Date(0L));
        assertEquals(7, rows.one().getInt("quality"));
        assertEquals("the columns the UPDATE did not name still come from the chunk",
                     1.5, rows.one().getDouble("value"), 0.0);
    }

    @Test
    public void deletesConfinedToTheHotWindowStillWork() throws Throwable
    {
        // The rule is about COLD data. Ordinary deletes of current data must be untouched -- use real
        // wall-clock clusterings so the rows are unambiguously inside hot_window.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, quality int, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");
        long recent = System.currentTimeMillis() - 60_000L;

        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 1.0, 5) ", new Date(recent));
        execute("INSERT INTO %s (tag, ts, value, quality) VALUES ('t', ?, 2.0, 6) ", new Date(recent + 1000));

        execute("DELETE value FROM %s WHERE tag = 't' AND ts = ?", new Date(recent));
        assertFalse(execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(recent)).one().has("value"));

        execute("UPDATE %s SET quality = null WHERE tag = 't' AND ts = ?", new Date(recent + 1000));
        assertFalse(execute("SELECT quality FROM %s WHERE tag = 't' AND ts = ?", new Date(recent + 1000))
                    .one().has("quality"));

        execute("DELETE FROM %s WHERE tag = 't' AND ts = ?", new Date(recent + 1000));
        assertEquals(0, execute("SELECT value FROM %s WHERE tag = 't' AND ts = ?", new Date(recent + 1000)).size());
    }

    @Test
    public void aBatchCannotSmuggleAColdDeleteThrough() throws Throwable
    {
        chunkOneRow();
        assertRejectedAsColdWrite("BEGIN BATCH " +
                                  "DELETE value FROM %s USING TIMESTAMP 300 WHERE tag = 't' AND ts = ? " +
                                  "APPLY BATCH", new Date(0L));
    }

    // ---- values Cassandra accepts that a fixed-width column codec cannot represent ----

    @Test
    public void anEmptyFixedWidthValueDoesNotWedgeTheTag() throws Throwable
    {
        // `blobAsInt(0x)` is legal CQL and Int32Serializer.validate accepts 0 bytes as well as 4, so
        // a present-but-empty int cell can reach the encoder. Before the width check it hit
        // ByteBuffer.getInt() on a 0-byte array, the per-tag handler logged the BufferUnderflow and
        // skipped -- identically every cycle, so that partition never tiered again.
        createTable("CREATE TABLE %s (tag text, ts timestamp, quality int, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts, quality, value) VALUES ('t', ?, 192, 1.0) USING TIMESTAMP 100",
                new Date(0L));
        // Non-constant in this window (192 vs empty), which is what makes the column take the
        // fixed-width section path rather than the O(1) constant path.
        execute("INSERT INTO %s (tag, ts, quality, value) VALUES ('t', ?, blobAsInt(0x), 2.0) " +
                "USING TIMESTAMP 101", new Date(600_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, stats.windowsEncoded);
        assertEquals(2, stats.rowsEncoded);
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(execute(chunkSelectQuery(), "t", new Date(0L))
                                                          .one().getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(192, (int) Int32Type.instance.compose(cursor.getBytes("quality")));
        assertTrue(cursor.advance());
        assertEquals("the empty value must round-trip as the empty value, byte for byte",
                     0, cursor.getBytes("quality").remaining());
        assertFalse(cursor.advance());
    }

    // ---- the writetime invariant, across columns ----

    @Test
    public void theDeleteTimestampIsTheMaximumWritetimeAcrossEveryColumn() throws Throwable
    {
        // One row whose three columns were written at three different timestamps. Taking any single
        // column's writetime (say the first column's, 100) would tombstone at 100 and leave the
        // cells written at 200/300 alive -- a half-deleted row that is re-encoded forever.
        createTable("CREATE TABLE %s (tag text, ts timestamp, a double, b int, c text, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, ts, a) VALUES ('t', ?, 1.0) USING TIMESTAMP 100", new Date(0L));
        execute("UPDATE %s USING TIMESTAMP 300 SET b = 5 WHERE tag = 't' AND ts = ?", new Date(0L));
        execute("UPDATE %s USING TIMESTAMP 200 SET c = 'z' WHERE tag = 't' AND ts = ?", new Date(0L));
        execute("INSERT INTO %s (tag, ts, a) VALUES ('t', ?, 9.0) USING TIMESTAMP 150", new Date(4 * HOUR));

        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t", new Date(0L)).one();
        assertEquals("max_row_writetime must be the maximum over ALL columns",
                     300L, chunkRow.getLong("max_row_writetime"));

        // Nothing of the row is left: had the delete used 100 (or 200), b (and c) would still be here.
        assertEquals(0, raw("SELECT ts, a, b, c FROM %s WHERE tag = 't' AND ts < ?", new Date(HOUR)).size());

        // A row written after the cycle read the window is newer than the tombstone and survives it.
        execute("INSERT INTO %s (tag, ts, a) VALUES ('t', ?, 4.0) USING TIMESTAMP 301", new Date(600_000L));
        UntypedResultSet late = raw("SELECT a FROM %s WHERE tag = 't' AND ts = ?", new Date(600_000L));
        assertEquals(1, late.size());
        assertEquals(4.0, late.one().getDouble("a"), 0.0);
    }

    // ---- composite partition keys, many columns ----

    @Test
    public void compositePartitionKeyRoundTripsEveryColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, " +
                    "value double, quality int, note text, flag boolean, PRIMARY KEY ((asset_id, date, hour), ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (asset_id, date, hour, ts, value, quality, note, flag) " +
                "VALUES ('a1', '2026-08-01', 0, ?, 1.0, 192, 'x', true) USING TIMESTAMP 101", new Date(600_000L));
        execute("INSERT INTO %s (asset_id, date, hour, ts, value, quality, flag) " +
                "VALUES ('a1', '2026-08-01', 0, ?, 2.0, 192, false) USING TIMESTAMP 102", new Date(1_200_000L));
        // A different partition differing only in the last key column -- must not be conflated.
        execute("INSERT INTO %s (asset_id, date, hour, ts, value, quality, note, flag) " +
                "VALUES ('a1', '2026-08-01', 1, ?, 3.0, 7, 'y', true) USING TIMESTAMP 103", new Date(600_000L));
        execute("INSERT INTO %s (asset_id, date, hour, ts, value) " +
                "VALUES ('a1', '2026-08-01', 0, ?, 9.0) USING TIMESTAMP 110", new Date(4 * HOUR));
        execute("INSERT INTO %s (asset_id, date, hour, ts, value) " +
                "VALUES ('a1', '2026-08-01', 1, ?, 9.0) USING TIMESTAMP 111", new Date(4 * HOUR));

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(2, stats.windowsEncoded);
        assertEquals(3, stats.rowsEncoded);

        String chunkRef = KEYSPACE + '.' + ChunkTables.chunkTableName(currentTable());
        UntypedResultSet.Row firstChunk =
            execute("SELECT samples, payload FROM " + chunkRef +
                    " WHERE asset_id = ? AND date = ? AND hour = ? AND window_start = ?",
                    "a1", "2026-08-01", 0, new Date(0L)).one();
        assertEquals(2, firstChunk.getInt("samples"));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(firstChunk.getBytes("payload"), null);
        assertTrue(cursor.advance());
        assertEquals(600_000L, cursor.timestamp());
        assertEquals(1.0, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertEquals("x", UTF8Type.instance.compose(cursor.getBytes("note")));
        assertTrue(Boolean.TRUE.equals(BooleanType.instance.compose(cursor.getBytes("flag"))));
        assertTrue(cursor.advance());
        assertEquals(1_200_000L, cursor.timestamp());
        assertEquals(2.0, DoubleType.instance.compose(cursor.getBytes("value")), 0.0);
        assertTrue("note was never written on this row", cursor.isNull("note"));
        assertFalse(cursor.advance());

        // The other partition kept its own values.
        UntypedResultSet.Row secondChunk =
            execute("SELECT samples, payload FROM " + chunkRef +
                    " WHERE asset_id = ? AND date = ? AND hour = ? AND window_start = ?",
                    "a1", "2026-08-01", 1, new Date(0L)).one();
        assertEquals(1, secondChunk.getInt("samples"));
        ColumnarCursor other = ColumnarChunkCodec.cursor(secondChunk.getBytes("payload"), null);
        assertTrue(other.advance());
        assertEquals(7, (int) Int32Type.instance.compose(other.getBytes("quality")));
        assertEquals("y", UTF8Type.instance.compose(other.getBytes("note")));

        // ...and both partitions read back completely through the merge.
        UntypedResultSet merged = execute("SELECT ts, value, quality, note, flag FROM %s " +
                                          "WHERE asset_id = ? AND date = ? AND hour = ?", "a1", "2026-08-01", 0);
        assertEquals(3, merged.size());
        List<UntypedResultSet.Row> rows = List.of(merged.stream().toArray(UntypedResultSet.Row[]::new));
        assertEquals(1.0, rows.get(0).getDouble("value"), 0.0);
        assertEquals("x", rows.get(0).getString("note"));
        assertFalse(rows.get(1).has("note"));
        assertEquals(9.0, rows.get(2).getDouble("value"), 0.0);
    }

    // ---- helpers ----

    private static ByteBuffer attribute(String unit)
    {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("unit", unit);
        return ATTRIBUTE_TYPE.decompose(map);
    }

    /** Reads the PHYSICAL base rows, bypassing SP3's transparent hot+chunk merge. */
    private UntypedResultSet raw(String query, Object... values) throws Throwable
    {
        TransparentReads.enterInternalBypass();
        try
        {
            return execute(query, values);
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }
    }

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }

    private String chunkTableRef()
    {
        return KEYSPACE + '.' + ChunkTables.chunkTableName(currentTable());
    }

    private String chunkSelectQuery()
    {
        return "SELECT * FROM " + chunkTableRef() + " WHERE " + partitionKeyName() + " = ? AND window_start = ?";
    }

    private String chunkPayloadQuery()
    {
        return "SELECT payload, WRITETIME(payload) AS chunk_wt FROM " + chunkTableRef() +
               " WHERE " + partitionKeyName() + " = ? AND window_start = ?";
    }

    private String partitionKeyName()
    {
        return getCurrentColumnFamilyStore().metadata().partitionKeyColumns().get(0).name.toCQLString();
    }
}
