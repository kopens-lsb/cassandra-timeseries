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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Which schemas {@link TimeSeriesMemtable} accepts, and which it declines.
 *
 * <p>The cases come from the production keyspace this fork was built for: the tag table's real shape
 * is accepted, and each rejection matches a table shape that exists in that keyspace and would
 * otherwise be mishandled without saying so.
 *
 * <p>Selection is by configuration key, not class name — {@code memtable} is a string property, and
 * the key must exist in {@code cassandra.yaml}. {@code test/conf/cassandra.yaml} defines
 * {@code timeseries} for these tests.
 */
public class TimeSeriesMemtableSchemaSupportTest extends CQLTester
{
    private static final String TSCS =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1d','freeze_after':'2d'}";

    /** The production tag shape: one partition key, a timestamp clustering, statics, mixed columns. */
    @Test
    public void acceptsTheProductionTagShape()
    {
        createTable("CREATE TABLE %s (" +
                    "tag_id text, timestamp timestamp," +
                    "type text static, tag_name text static," +
                    "value text, value_numeric double, value_boolean boolean, quality int," +
                    "PRIMARY KEY (tag_id, timestamp)) " +
                    "WITH CLUSTERING ORDER BY (timestamp DESC) " +
                    "AND compaction = " + TSCS);

        assertNull(TimeSeriesMemtable.unsupportedReason(currentTableMetadata()));
    }

    @Test
    public void rejectsCounterTables()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, hits counter, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS);

        String reason = TimeSeriesMemtable.unsupportedReason(currentTableMetadata());
        assertNotNull("a counter table must be declined", reason);
        assertTrue(reason, reason.contains("counter"));
    }

    /** Non-frozen collections are multi-cell, so the row has no single write timestamp. */
    @Test
    public void rejectsNonFrozenCollections()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, tags map<text,text>, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS);

        String reason = TimeSeriesMemtable.unsupportedReason(currentTableMetadata());
        assertNotNull("a non-frozen collection must be declined", reason);
        assertTrue(reason, reason.contains("multi-cell"));
    }

    /** Frozen collections are one cell — the production tag table's {@code attribute} is this. */
    @Test
    public void acceptsFrozenCollections()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, attribute frozen<map<text,text>>, " +
                    "PRIMARY KEY (k, c)) WITH compaction = " + TSCS);

        assertNull(TimeSeriesMemtable.unsupportedReason(currentTableMetadata()));
    }

    @Test
    public void rejectsTablesNotUsingTimeSeriesCompaction()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, v double, PRIMARY KEY (k, c)) " +
                    "WITH compaction = {'class':'UnifiedCompactionStrategy'}");

        String reason = TimeSeriesMemtable.unsupportedReason(currentTableMetadata());
        assertNotNull("a table not using TSCS must be declined", reason);
        assertTrue(reason, reason.contains("TimeSeriesCompactionStrategy"));
    }

    @Test
    public void rejectsNonTimestampClustering()
    {
        createTable("CREATE TABLE %s (k text, c text, v double, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS);

        String reason = TimeSeriesMemtable.unsupportedReason(currentTableMetadata());
        assertNotNull("a non-timestamp clustering must be declined", reason);
        assertTrue(reason, reason.contains("not a timestamp"));
    }

    @Test
    public void rejectsMultipleClusteringColumns()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, seq int, v double, " +
                    "PRIMARY KEY (k, c, seq)) WITH compaction = " + TSCS);

        String reason = TimeSeriesMemtable.unsupportedReason(currentTableMetadata());
        assertNotNull("two clustering columns must be declined", reason);
        assertTrue(reason, reason.contains("exactly one clustering column"));
    }

    /**
     * An accepted table works end to end with the memtable actually selected. Everything above
     * tests the predicate; this tests that the configuration key resolves and writes survive a
     * flush.
     */
    @Test
    public void anAcceptedTableReadsWritesAndFlushes() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c timestamp, v double, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");

        execute("INSERT INTO %s (k, c, v) VALUES ('a', 1785600000000, 1.5)");
        execute("INSERT INTO %s (k, c, v) VALUES ('a', 1785600060000, 2.5)");
        flush();
        execute("INSERT INTO %s (k, c, v) VALUES ('a', 1785600120000, 3.5)");

        assertRows(execute("SELECT v FROM %s WHERE k = 'a'"), row(1.5), row(2.5), row(3.5));
    }

    /**
     * A declined table must still work. This is the whole reason the gate falls back instead of
     * throwing: setting the memtable on a schema it cannot hold costs the optimisation, not writes.
     */
    @Test
    public void aDeclinedTableStillReadsAndWrites() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c timestamp, hits counter, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");

        execute("UPDATE %s SET hits = hits + 5 WHERE k = 'a' AND c = 1785600000000");
        execute("UPDATE %s SET hits = hits + 3 WHERE k = 'a' AND c = 1785600000000");
        assertRows(execute("SELECT hits FROM %s WHERE k = 'a' AND c = 1785600000000"), row(8L));
    }
}
