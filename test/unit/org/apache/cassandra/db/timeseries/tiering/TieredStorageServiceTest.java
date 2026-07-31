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
import java.util.Date;
import java.util.Random;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.timeseries.ChunkCodecs;
import org.apache.cassandra.db.timeseries.Chimp128Codec;
import org.apache.cassandra.db.timeseries.GorillaCodec;
import org.apache.cassandra.db.timeseries.SampleCursor;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService.TierRunStats;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link TieredStorageService#runOnce}, exercising the whole re-encode cycle
 * against a real (single-node) schema and real CQL queries -- see
 * docs/superpowers/plans/2026-07-31-chunk-store-sp2.md, Task 2, for the scenarios this covers.
 */
public class TieredStorageServiceTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    @Test
    public void encodeClosedWindowsAndDeleteRows() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        String[] tags = { "a", "b", "c" };
        long now = 5 * HOUR;
        long wt = 1;
        for (String tag : tags)
        {
            for (int w = 0; w < 3; w++)
            {
                long windowStart = w * HOUR;
                for (int r = 0; r < 4; r++)
                    insertRow(tag, windowStart + r * 600_000L, w * 100.0 + r, wt++);
            }
            insertRow(tag, 4 * HOUR, 999.0, wt++); // hot row -- must survive untouched
        }

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);

        assertEquals(9, stats.windowsEncoded); // 3 tags * 3 closed windows
        assertEquals(36, stats.rowsEncoded);   // 3 tags * 3 windows * 4 rows
        assertEquals(0, stats.lateMerges);
        assertEquals(0, stats.chunksExpired);
        assertTrue(stats.bytesWritten > 0);

        for (String tag : tags)
        {
            for (int w = 0; w < 3; w++)
            {
                long windowStart = w * HOUR;
                UntypedResultSet chunkRows = execute(chunkSelectQuery(), tag, new Date(windowStart));
                assertEquals(1, chunkRows.size());
                UntypedResultSet.Row chunkRow = chunkRows.one();
                assertEquals(4, chunkRow.getInt("samples"));
                byte codec = chunkRow.getByte("codec");
                assertTrue(codec == GorillaCodec.VERSION || codec == Chimp128Codec.VERSION);

                assertEquals(0, execute("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                        tag, new Date(windowStart), new Date(windowStart + HOUR)).size());
            }

            assertEquals(1, execute("SELECT * FROM %s WHERE tag = ? AND ts = ?", tag, new Date(4 * HOUR)).size());
        }
    }

    @Test
    public void roundtripThroughChunks() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        long[] tsValues = { 0L, 600_000L, 1_200_000L, 1_800_000L, 2_400_000L };
        double[] values = { 1.5, -2.25, 3.0, 0.0, 42.125 };
        long wt = 1;
        for (int i = 0; i < tsValues.length; i++)
            insertRow("solo", tsValues[i], values[i], wt++);
        insertRow("solo", 4 * HOUR, 999.0, wt++);

        new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "solo", new Date(0L)).one();
        SampleCursor cursor = ChunkCodecs.cursor(chunkRow.getBytes("payload"));
        for (int i = 0; i < tsValues.length; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(tsValues[i], cursor.timestamp());
            assertEquals(values[i], cursor.value(), 0.0);
        }
        assertFalse(cursor.advance());
    }

    @Test
    public void deleteTimestampPreservesLateRows() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        for (int i = 0; i < 4; i++)
            insertRow("t", i * 600_000L, i * 1.0, 100 + i); // writetimes 100..103
        insertRow("t", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated after the window is cleared

        TieredStorageService service = new TieredStorageService();
        TierRunStats first = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, first.windowsEncoded);
        assertEquals(4, first.rowsEncoded);
        assertEquals(0, first.lateMerges);

        assertEquals(0, execute("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "t", new Date(0L), new Date(HOUR)).size());

        // Late row: newer writetime (104) than the tombstone the first run issued (USING TIMESTAMP 103).
        long lateTs = 2_400_000L;
        insertRow("t", lateTs, 42.0, 104);

        UntypedResultSet survived = execute("SELECT * FROM %s WHERE tag = ? AND ts = ?", "t", new Date(lateTs));
        assertEquals(1, survived.size());
        assertEquals(42.0, survived.one().getDouble("value"), 0.0);

        TierRunStats second = service.runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, second.windowsEncoded);
        assertEquals(1, second.rowsEncoded);
        assertEquals(1, second.lateMerges);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "t", new Date(0L)).one();
        assertEquals(5, chunkRow.getInt("samples"));

        SampleCursor cursor = ChunkCodecs.cursor(chunkRow.getBytes("payload"));
        boolean foundLate = false;
        while (cursor.advance())
        {
            if (cursor.timestamp() == lateTs)
            {
                foundLate = true;
                assertEquals(42.0, cursor.value(), 0.0);
            }
        }
        assertTrue("expected the late sample to be present in the merged chunk", foundLate);

        assertEquals(0, execute("SELECT * FROM %s WHERE tag = ? AND ts = ?", "t", new Date(lateTs)).size());
    }

    @Test
    public void idempotentWhenInterrupted() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        long[] tsValues = { 0L, 600_000L, 1_200_000L, 1_800_000L };
        double[] values = { 1.0, 2.0, 3.0, 4.0 };
        for (int i = 0; i < tsValues.length; i++)
            insertRow("i", tsValues[i], values[i], 297 + i); // writetimes 297..300
        insertRow("i", 4 * HOUR, 999.0, 400); // hot row -- keeps the tag enumerated

        // Simulate "wrote the chunk, crashed before the delete": pre-write a chunk matching the
        // still-live rows, at the same timestamp (301 = maxWt+1 = 300+1) a genuine first pass would
        // have used -- NOT the wall-clock timestamp a plain execute() would default to (which, being
        // far larger than anything runOnce ever writes, would make every cell of this pre-written row
        // permanently win over runOnce's re-merge and pass the assertions below vacuously).
        ByteBuffer payload = ChunkCodecs.encodeSmallest(tsValues, values, tsValues.length);
        execute(chunkInsertQuery(), "i", new Date(0L), tsValues.length, 300L, payload, 301L);

        assertEquals(4, execute("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "i", new Date(0L), new Date(HOUR)).size());

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(1, stats.windowsEncoded);
        assertEquals(1, stats.lateMerges);
        assertEquals(4, stats.rowsEncoded);

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "i", new Date(0L)).one();
        assertEquals(4, chunkRow.getInt("samples")); // converged, not duplicated

        // Decode and check the actual (ts, value) content, not just the sample count -- proves the
        // merge that runOnce performed (not a stale pre-written row) produced this chunk.
        SampleCursor cursor = ChunkCodecs.cursor(chunkRow.getBytes("payload"));
        for (int i = 0; i < tsValues.length; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(tsValues[i], cursor.timestamp());
            assertEquals(values[i], cursor.value(), 0.0);
        }
        assertFalse(cursor.advance());

        // The interrupted cycle's delete now completes.
        assertEquals(0, execute("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "i", new Date(0L), new Date(HOUR)).size());
    }

    @Test
    public void coldWindowExpiry() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"1h\",\"chunk_window\":\"1h\",\"cold_window\":\"2h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        long now = 10 * HOUR;
        insertRow("cold", now - 100_000L, 1.0, 1); // hot row -- keeps the tag enumerated

        ByteBuffer expiring = ChunkCodecs.encodeSmallest(new long[]{ 0L }, new double[]{ 1.0 }, 1);
        execute(chunkInsertQuery(), "cold", new Date(0L), 1, 500L, expiring, 1L);

        ByteBuffer surviving = ChunkCodecs.encodeSmallest(new long[]{ 9 * HOUR }, new double[]{ 2.0 }, 1);
        execute(chunkInsertQuery(), "cold", new Date(9 * HOUR), 1, 600L, surviving, 2L);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), now);
        assertEquals(1, stats.chunksExpired);

        assertEquals(0, execute(chunkSelectQuery(), "cold", new Date(0L)).size());
        assertEquals(1, execute(chunkSelectQuery(), "cold", new Date(9 * HOUR)).size());
    }

    @Test
    public void nonCanonicalSchemaSkipsWithError() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, extra int, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        TierRunStats stats;
        try
        {
            stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
            assertTrue("expected an ERROR log for the non-canonical schema",
                       appender.list.stream().anyMatch(e -> e.getLevel() == Level.ERROR));
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        assertEquals(0, stats.windowsEncoded);
        assertEquals(0, stats.rowsEncoded);
        assertEquals(0, stats.lateMerges);
        assertEquals(0, stats.chunksExpired);
        assertEquals(0, stats.bytesWritten);
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable())));
    }

    @Test
    public void autoCodecPicksPerWindow() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        int n = 100;
        long wt = 1;

        for (int i = 0; i < n; i++)
            insertRow("const", i * 30_000L, 5.0, wt++);

        Random random = new Random(17);
        double walk = 50.0;
        for (int i = 0; i < n; i++)
        {
            walk += (random.nextInt(3) - 1) * 0.1;
            insertRow("quant", i * 30_000L, Math.round(walk * 10.0) / 10.0, wt++);
        }

        insertRow("const", 4 * HOUR, 999.0, wt++);
        insertRow("quant", 4 * HOUR, 999.0, wt++);

        new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        byte constCodec = execute(chunkSelectQuery(), "const", new Date(0L)).one().getByte("codec");
        byte quantCodec = execute(chunkSelectQuery(), "quant", new Date(0L)).one().getByte("codec");

        assertEquals(GorillaCodec.VERSION, constCodec);
        assertEquals(Chimp128Codec.VERSION, quantCodec);
    }

    @Test
    public void nullValueCellSkippedWithoutAborting() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        insertRow("n", 0L, 7.5, 50);
        // Bare key insert -- a live row with no value cell at all, so WRITETIME(value) is null too.
        execute("INSERT INTO %s (tag, ts) VALUES (?, ?) USING TIMESTAMP 10", "n", new Date(600_000L));
        insertRow("n", 4 * HOUR, 999.0, 200); // hot row -- keeps the tag enumerated

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        assertEquals(1, stats.windowsEncoded);
        assertEquals(1, stats.rowsEncoded); // the null-cell row is skipped, not counted or NPE'd on

        UntypedResultSet.Row chunkRow = execute(chunkSelectQuery(), "n", new Date(0L)).one();
        assertEquals(1, chunkRow.getInt("samples"));
        SampleCursor cursor = ChunkCodecs.cursor(chunkRow.getBytes("payload"));
        assertTrue(cursor.advance());
        assertEquals(0L, cursor.timestamp());
        assertEquals(7.5, cursor.value(), 0.0);
        assertFalse(cursor.advance());

        // The range delete covers the whole window regardless of which individual rows had a value.
        assertEquals(0, execute("SELECT * FROM %s WHERE tag = ? AND ts >= ? AND ts < ?",
                                "n", new Date(0L), new Date(HOUR)).size());
    }

    @Test
    public void corruptChunkOnOneTagDoesNotAbortOtherTags() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        TableMetadata base = getCurrentColumnFamilyStore().metadata();
        ChunkTables.ensureChunkTable(base);

        // "bad": an existing chunk row whose payload is not a valid codec payload at all, so decoding
        // it for the merge throws mid-cycle.
        insertRow("bad", 0L, 1.0, 10);
        insertRow("bad", 4 * HOUR, 999.0, 20);
        ByteBuffer garbage = ByteBufferUtil.bytes("not a valid chunk payload");
        execute(chunkInsertQuery(), "bad", new Date(0L), 1, 5L, garbage, 6L);

        // "good": an ordinary closed window that must still get encoded despite "bad" throwing.
        insertRow("good", 0L, 2.0, 10);
        insertRow("good", 4 * HOUR, 999.0, 20);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);

        assertEquals(1, stats.windowsEncoded); // only "good"'s window succeeded
        assertEquals(1, execute(chunkSelectQuery(), "good", new Date(0L)).size());
    }

    private void insertRow(String tag, long tsMillis, double value, long writetime) throws Throwable
    {
        execute("INSERT INTO %s (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?",
                tag, new Date(tsMillis), value, writetime);
    }

    private void setPolicy(String json) throws Throwable
    {
        String hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(json));
        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");
    }

    private String chunkTableRef()
    {
        return KEYSPACE + "." + ChunkTables.chunkTableName(currentTable());
    }

    private String chunkSelectQuery()
    {
        return "SELECT * FROM " + chunkTableRef() + " WHERE tag = ? AND window_start = ?";
    }

    /** Bind order: tag, window_start, samples, max_row_writetime, payload, USING TIMESTAMP. */
    private String chunkInsertQuery()
    {
        return "INSERT INTO " + chunkTableRef() +
               " (tag, window_start, codec, samples, max_row_writetime, payload) VALUES (?, ?, 1, ?, ?, ?) " +
               "USING TIMESTAMP ?";
    }
}
