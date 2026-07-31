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
import java.util.List;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageService.TierRunStats;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * SP4 Task 2: the schemas tiering accepts and rejects, exercised against real CQL schemas rather than
 * hand-built {@link TableMetadata} (see {@link TieringPolicyTest} for the metadata-level matrix).
 * <p>
 * The cases that need a live schema are here because they cannot be built by hand: real secondary
 * indexes with their real target strings, materialized views (which are registered on the keyspace,
 * not the table), and the end-to-end proofs -- that a composite partition key round-trips through the
 * mirrored chunk table, and that static columns survive the re-encoder's clustering-range delete.
 */
public class TieringSchemaSupportTest extends CQLTester
{
    private static final long HOUR = 3_600_000L;

    // ---- rejections that need a live schema ----

    @Test
    public void materializedViewOverTheTableIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));

        // createViewAsync, not createView: the schema change itself is what this asserts on, and
        // waiting for the (irrelevant, empty-table) view build costs minutes in this harness.
        String view = createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                      "WHERE tag IS NOT NULL AND ts IS NOT NULL AND value IS NOT NULL " +
                                      "PRIMARY KEY (value, tag, ts)");

        // The re-encoder's range delete propagates into the view, but transparent reads only rebuild
        // rows for the base table -- so the view would silently lose all history older than hot_window.
        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains(view));
        assertTrue(error, error.contains("materialized view"));
    }

    @Test
    public void secondaryIndexOnARegularColumnIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'sai'");

        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains(index));
        assertTrue(error, error.contains("value"));
    }

    @Test
    public void secondaryIndexOnTheClusteringColumnIsRejected() throws Throwable
    {
        // Proves the premise as well as the rule: CQL really does allow indexing the timestamp
        // clustering column, and those entries are per row, so the range delete takes them with it.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        String index = createIndex("CREATE CUSTOM INDEX ON %s(ts) USING 'sai'");

        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains(index));
        assertTrue(error, error.contains("clustering column 'ts'"));
    }

    @Test
    public void secondaryIndexOnAStaticColumnIsAccepted() throws Throwable
    {
        // The production table carries SAI on static asset_id/opc_id: static cells are never chunked
        // and never deleted (their index entries sit at Clustering.STATIC_CLUSTERING, outside every
        // clustering range the re-encoder deletes), so the index stays complete.
        createTable("CREATE TABLE %s (tag text, ts timestamp, asset_id text static, opc_id text static, " +
                    "value double, PRIMARY KEY (tag, ts))");
        createIndex("CREATE CUSTOM INDEX ON %s(asset_id) USING 'sai'");
        createIndex("CREATE CUSTOM INDEX ON %s(opc_id) USING 'sai'");

        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));
    }

    @Test
    public void counterTableIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, hits counter, PRIMARY KEY (tag, ts))");

        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains("hits"));
        assertTrue(error, error.contains("counter"));
    }

    @Test
    public void nonFrozenCollectionIsRejectedButAFrozenOneIsNot() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, labels map<text,text>, PRIMARY KEY (tag, ts))");
        String error = TieringPolicy.unsupportedSchemaError(metadata());
        assertNotNull(error);
        assertTrue(error, error.contains("labels"));

        createTable("CREATE TABLE %s (tag text, ts timestamp, attribute frozen<map<text,text>>, " +
                    "PRIMARY KEY (tag, ts))");
        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));
    }

    @Test
    public void productionShapedTableIsAccepted() throws Throwable
    {
        // pp.tm_tag_point, verbatim: composite-free key, DESC clustering, 7 static columns, 7 regular
        // columns of five different types including a frozen map, and a table-level TTL.
        createTable("CREATE TABLE %s (" +
                    "tag_id text, timestamp timestamp, " +
                    "area_id text static, asset_id text static, line_id text static, opc_id text static, " +
                    "site_id text static, tag_name text static, type text static, " +
                    "attribute frozen<map<text,text>>, error_code int, latency int, quality int, " +
                    "value text, value_boolean boolean, value_numeric double, " +
                    "PRIMARY KEY (tag_id, timestamp)) " +
                    "WITH CLUSTERING ORDER BY (timestamp DESC) AND default_time_to_live = 5356800");

        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));
    }

    // ---- composite partition keys, end to end ----

    @Test
    public void compositePartitionKeyIsMirroredByTheChunkTable() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, value double, " +
                    "PRIMARY KEY ((asset_id, date, hour), ts))");
        assertNull(TieringPolicy.unsupportedSchemaError(metadata()));

        TableMetadata chunk = ChunkTables.chunkTableMetadata(metadata());
        List<ColumnMetadata> chunkKey = chunk.partitionKeyColumns();
        assertEquals(3, chunkKey.size());
        List<ColumnMetadata> baseKey = metadata().partitionKeyColumns();
        for (int i = 0; i < baseKey.size(); i++)
        {
            assertEquals(baseKey.get(i).name, chunkKey.get(i).name);
            assertEquals(baseKey.get(i).type, chunkKey.get(i).type);
        }
        assertEquals(1, chunk.clusteringColumns().size());
        assertEquals("window_start", chunk.clusteringColumns().get(0).name.toString());
    }

    @Test
    public void compositePartitionKeyRoundTripsThroughTheReEncoder() throws Throwable
    {
        createTable("CREATE TABLE %s (asset_id text, date text, hour int, ts timestamp, value double, " +
                    "PRIMARY KEY ((asset_id, date, hour), ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        // Two distinct partitions differing only in the 2nd/3rd key column: they must not be conflated
        // by the DISTINCT-key enumeration, and each must get its own chunk partition.
        insert("a1", "2026-08-01", 0, 10 * 60_000L, 1.0, 101);
        insert("a1", "2026-08-01", 0, 20 * 60_000L, 2.0, 102);
        insert("a1", "2026-08-01", 1, 10 * 60_000L, 3.0, 103);
        // A hot row per partition (window [4h,5h) with now = 5h), which must survive untouched.
        insert("a1", "2026-08-01", 0, 4 * HOUR, 9.0, 110);
        insert("a1", "2026-08-01", 1, 4 * HOUR, 9.0, 111);

        TierRunStats stats = new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        assertEquals(2, stats.windowsEncoded);
        assertEquals(3, stats.rowsEncoded);

        String chunkRef = KEYSPACE + '.' + ChunkTables.chunkTableName(currentTable());
        UntypedResultSet firstChunk = execute("SELECT samples FROM " + chunkRef +
                                              " WHERE asset_id = ? AND date = ? AND hour = ? AND window_start = ?",
                                              "a1", "2026-08-01", 0, new Date(0L));
        assertEquals(1, firstChunk.size());
        assertEquals(2, firstChunk.one().getInt("samples"));

        UntypedResultSet secondChunk = execute("SELECT samples FROM " + chunkRef +
                                               " WHERE asset_id = ? AND date = ? AND hour = ? AND window_start = ?",
                                               "a1", "2026-08-01", 1, new Date(0L));
        assertEquals(1, secondChunk.size());
        assertEquals(1, secondChunk.one().getInt("samples"));

        // The encoded base rows are physically gone...
        assertEquals(0, raw("SELECT value FROM %s WHERE asset_id = ? AND date = ? AND hour = ? AND ts < ?",
                            "a1", "2026-08-01", 0, new Date(HOUR)).size());

        // ...but a plain SELECT still sees them: the read path split the composite key correctly.
        UntypedResultSet merged = execute("SELECT value FROM %s WHERE asset_id = ? AND date = ? AND hour = ?",
                                          "a1", "2026-08-01", 0);
        assertEquals(3, merged.size());
        double[] expected = { 1.0, 2.0, 9.0 };
        int i = 0;
        for (UntypedResultSet.Row row : merged)
            assertEquals(expected[i++], row.getDouble("value"), 0.0);
    }

    // ---- static columns ----

    @Test
    public void staticColumnsSurviveTheReEncodersRangeDelete() throws Throwable
    {
        // The whole reason static columns need no rules: the re-encoder deletes a clustering RANGE,
        // and static cells live outside every clustering range, so they are untouched by construction.
        createTable("CREATE TABLE %s (tag text, ts timestamp, site_id text static, unit text static, " +
                    "value double, PRIMARY KEY (tag, ts))");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        execute("INSERT INTO %s (tag, site_id, unit) VALUES ('t1', 'seoul', 'celsius') USING TIMESTAMP 100");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0) USING TIMESTAMP 101", new Date(10 * 60_000L));
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 2.0) USING TIMESTAMP 102", new Date(20 * 60_000L));

        assertEquals(1, new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR).windowsEncoded);

        // Every base row of the encoded window is gone...
        assertEquals(0, raw("SELECT ts FROM %s WHERE tag = 't1' AND ts < ?", new Date(HOUR)).size());

        // ...and the static cells are still there, at their original writetime.
        UntypedResultSet statics = raw("SELECT site_id, unit, WRITETIME(site_id) AS wt FROM %s WHERE tag = 't1'");
        assertEquals(1, statics.size());
        assertEquals("seoul", statics.one().getString("site_id"));
        assertEquals("celsius", statics.one().getString("unit"));
        assertEquals(100L, statics.one().getLong("wt"));
    }

    // ---- default_time_to_live vs hot_window ----

    @Test
    public void ttlShorterThanHotWindowIsWarnedByTheReEncoder() throws Throwable
    {
        // 1h TTL with a 2h hot_window: every row expires before the re-encoder is allowed to touch it,
        // so tiering is on but nothing is ever compressed. Warn, do not reject -- a per-row USING TTL
        // can differ from the table default.
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH default_time_to_live = 3600");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        try
        {
            new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        assertTrue("expected a WARN naming both default_time_to_live and hot_window",
                   appender.list.stream().anyMatch(e -> e.getLevel() == Level.WARN &&
                                                        e.getFormattedMessage().contains("default_time_to_live") &&
                                                        e.getFormattedMessage().contains("hot_window") &&
                                                        e.getFormattedMessage().contains("3600")));

        // It is a warning, not a rejection: the cycle still ran and still created the chunk table.
        assertNotNull(Schema.instance.getTableMetadata(KEYSPACE, ChunkTables.chunkTableName(currentTable())));
    }

    @Test
    public void ttlLongerThanHotWindowIsNotWarned() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts)) " +
                    "WITH default_time_to_live = 864000");
        setPolicy("{\"hot_window\":\"2h\",\"chunk_window\":\"1h\"}");

        Logger serviceLogger = (Logger) LoggerFactory.getLogger(TieredStorageService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        serviceLogger.addAppender(appender);
        try
        {
            new TieredStorageService().runOnce(KEYSPACE, currentTable(), 5 * HOUR);
        }
        finally
        {
            serviceLogger.detachAppender(appender);
        }

        assertTrue("a TTL longer than hot_window must not warn",
                   appender.list.stream().noneMatch(e -> e.getFormattedMessage().contains("default_time_to_live")));
    }

    private TableMetadata metadata()
    {
        return getCurrentColumnFamilyStore().metadata();
    }

    private void insert(String assetId, String date, int hour, long tsMillis, double value, long writetime)
    throws Throwable
    {
        execute("INSERT INTO %s (asset_id, date, hour, ts, value) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                assetId, date, hour, new Date(tsMillis), value, writetime);
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
}
