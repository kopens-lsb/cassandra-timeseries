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
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

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
    public void nonTieredTableUnaffected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts))");
        execute("INSERT INTO %s (tag, ts, value) VALUES ('t1', ?, 1.0)", new Date(1000));
        UntypedResultSet rows = execute("SELECT value FROM %s WHERE tag = 't1'");
        assertEquals(1, rows.size());
    }
}
