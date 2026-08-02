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
 * <p>It declines exactly one thing: a table not using {@code TimeSeriesCompactionStrategy}, because
 * then there is no {@code window_size} to shard by. Nothing else is refused. The window is computed
 * from a cell's <em>write timestamp</em>, which every schema has, so the primary key's shape does not
 * come into it; and splitting one logical row's cells over several sstables and reconciling them on
 * read is what Cassandra does anyway, which is why counters and non-frozen collections need no guard
 * either — the flush-time splitter (`WindowRoutingIterator`) already does exactly that to these same
 * tables today.
 *
 * <p>Each accepted shape is therefore checked twice: the predicate says yes, <em>and</em> writes to
 * that shape survive a read and a flush with the memtable actually selected. A predicate that says
 * yes is worth nothing on its own.
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
    public void acceptsTheProductionTagShape() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "tag_id text, timestamp timestamp," +
                    "type text static, tag_name text static," +
                    "value text, value_numeric double, value_boolean boolean, quality int," +
                    "PRIMARY KEY (tag_id, timestamp)) " +
                    "WITH CLUSTERING ORDER BY (timestamp DESC) " +
                    "AND compaction = " + TSCS + " AND memtable = 'timeseries'");

        assertNull(TimeSeriesMemtable.unsupportedReason(currentTableMetadata()));
        assertMemtableInUse(TimeSeriesMemtable.class);

        execute("INSERT INTO %s (tag_id, timestamp, type, tag_name, value_numeric, quality) " +
                "VALUES ('t1', 1785628800000, 'analog', 'Boiler', 1.5, 192)");
        execute("INSERT INTO %s (tag_id, timestamp, value_numeric, quality) VALUES ('t1', 1785628860000, 2.5, 192)");
        assertRows(execute("SELECT tag_name, value_numeric FROM %s WHERE tag_id = 't1'"),
                   row("Boiler", 2.5), row("Boiler", 1.5));

        flush();
        assertRows(execute("SELECT tag_name, value_numeric FROM %s WHERE tag_id = 't1'"),
                   row("Boiler", 2.5), row("Boiler", 1.5));
    }

    /**
     * A compound partition key with three clustering columns, none of them a timestamp — the shape of
     * a real table in the production keyspace. The window comes from the write timestamp, so the
     * primary key can be anything.
     */
    @Test
    public void acceptsCompoundKeysAndSeveralClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (site_id text, year int, month int, day int, hour int, minute int, " +
                    "v double, PRIMARY KEY ((site_id, year, month), day, hour, minute)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");

        assertNull(TimeSeriesMemtable.unsupportedReason(currentTableMetadata()));
        assertMemtableInUse(TimeSeriesMemtable.class);

        execute("INSERT INTO %s (site_id, year, month, day, hour, minute, v) VALUES ('s1', 2026, 8, 2, 1, 0, 1.0)");
        execute("INSERT INTO %s (site_id, year, month, day, hour, minute, v) VALUES ('s1', 2026, 8, 2, 1, 1, 2.0)");
        execute("INSERT INTO %s (site_id, year, month, day, hour, minute, v) VALUES ('s1', 2026, 8, 3, 0, 0, 3.0)");
        assertRows(execute("SELECT v FROM %s WHERE site_id = 's1' AND year = 2026 AND month = 8"),
                   row(1.0), row(2.0), row(3.0));

        flush();
        assertRows(execute("SELECT v FROM %s WHERE site_id = 's1' AND year = 2026 AND month = 8"),
                   row(1.0), row(2.0), row(3.0));
    }

    /** A non-timestamp clustering column. */
    @Test
    public void acceptsNonTimestampClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, v double, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");

        assertNull(TimeSeriesMemtable.unsupportedReason(currentTableMetadata()));
        assertMemtableInUse(TimeSeriesMemtable.class);

        execute("INSERT INTO %s (k, c, v) VALUES ('k1', 'a', 1.0)");
        execute("INSERT INTO %s (k, c, v) VALUES ('k1', 'b', 2.0)");
        flush();
        execute("INSERT INTO %s (k, c, v) VALUES ('k1', 'c', 3.0)");
        assertRows(execute("SELECT v FROM %s WHERE k = 'k1'"), row(1.0), row(2.0), row(3.0));
    }

    /**
     * A non-frozen collection. Its cells are individually timestamped and may end up in different
     * window shards; the read merge is what puts the collection back together.
     */
    @Test
    public void acceptsNonFrozenCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c timestamp, tags map<text,text>, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");

        assertNull(TimeSeriesMemtable.unsupportedReason(currentTableMetadata()));
        assertMemtableInUse(TimeSeriesMemtable.class);

        long base = 1785628800000L;
        execute("INSERT INTO %s (k, c, tags) VALUES ('k1', ?, ?) USING TIMESTAMP ?",
                base, map("a", "1", "b", "2"), base * 1000L);
        execute("UPDATE %s USING TIMESTAMP ? SET tags = tags + ? WHERE k = 'k1' AND c = ?",
                (base + 86_400_000L) * 1000L, map("c", "3"), base);
        execute("UPDATE %s USING TIMESTAMP ? SET tags = tags - ? WHERE k = 'k1' AND c = ?",
                (base + 172_800_000L) * 1000L, set("a"), base);

        assertRows(execute("SELECT tags FROM %s WHERE k = 'k1'"), row(map("b", "2", "c", "3")));
        flush();
        assertRows(execute("SELECT tags FROM %s WHERE k = 'k1'"), row(map("b", "2", "c", "3")));
    }

    /** Frozen collections are a single cell — the production tag table's {@code attribute} is this. */
    @Test
    public void acceptsFrozenCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c timestamp, attribute frozen<map<text,text>>, " +
                    "PRIMARY KEY (k, c)) WITH compaction = " + TSCS + " AND memtable = 'timeseries'");

        assertNull(TimeSeriesMemtable.unsupportedReason(currentTableMetadata()));
        assertMemtableInUse(TimeSeriesMemtable.class);

        execute("INSERT INTO %s (k, c, attribute) VALUES ('k1', 1785628800000, ?)", map("unit", "degC"));
        assertRows(execute("SELECT attribute FROM %s WHERE k = 'k1'"), row(map("unit", "degC")));
        flush();
        assertRows(execute("SELECT attribute FROM %s WHERE k = 'k1'"), row(map("unit", "degC")));
    }

    /**
     * A counter table. Counter cells reconcile by shard clock, which is exactly what makes them safe
     * to hold in several window shards and merge on read — the same property that lets them live in
     * several sstables.
     */
    @Test
    public void acceptsCounterTables() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, hits counter, PRIMARY KEY (k, c)) " +
                    "WITH compaction = " + TSCS + " AND memtable = 'timeseries'");

        assertNull(TimeSeriesMemtable.unsupportedReason(currentTableMetadata()));
        assertMemtableInUse(TimeSeriesMemtable.class);

        execute("UPDATE %s SET hits = hits + 5 WHERE k = 'a' AND c = 'x'");
        execute("UPDATE %s SET hits = hits + 3 WHERE k = 'a' AND c = 'x'");
        assertRows(execute("SELECT hits FROM %s WHERE k = 'a' AND c = 'x'"), row(8L));

        flush();
        execute("UPDATE %s SET hits = hits - 2 WHERE k = 'a' AND c = 'x'");
        assertRows(execute("SELECT hits FROM %s WHERE k = 'a' AND c = 'x'"), row(6L));
    }

    /** Without TimeSeriesCompactionStrategy there is no {@code window_size}, so there is no window. */
    @Test
    public void rejectsTablesNotUsingTimeSeriesCompaction()
    {
        createTable("CREATE TABLE %s (k text, c timestamp, v double, PRIMARY KEY (k, c)) " +
                    "WITH compaction = {'class':'UnifiedCompactionStrategy'}");

        String reason = TimeSeriesMemtable.unsupportedReason(currentTableMetadata());
        assertNotNull("a table not using TSCS must be declined", reason);
        assertTrue(reason, reason.contains("TimeSeriesCompactionStrategy"));
    }

    /**
     * A declined table must still work. This is the whole reason the gate falls back instead of
     * throwing: setting the memtable on a schema it cannot hold costs the optimisation, not writes.
     */
    @Test
    public void aDeclinedTableStillReadsAndWrites() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c timestamp, v double, PRIMARY KEY (k, c)) " +
                    "WITH compaction = {'class':'UnifiedCompactionStrategy'} AND memtable = 'timeseries'");

        assertMemtableInUse(SkipListMemtable.class);

        execute("INSERT INTO %s (k, c, v) VALUES ('a', 1785628800000, 1.5)");
        execute("INSERT INTO %s (k, c, v) VALUES ('a', 1785628860000, 2.5)");
        flush();
        execute("INSERT INTO %s (k, c, v) VALUES ('a', 1785628920000, 3.5)");
        assertRows(execute("SELECT v FROM %s WHERE k = 'a'"), row(1.5), row(2.5), row(3.5));
    }

    /**
     * The memtable the table is really using. Asserting acceptance through the predicate alone would
     * pass even if {@code Factory.create} silently fell back.
     */
    private void assertMemtableInUse(Class<? extends Memtable> expected)
    {
        Memtable current = getCurrentColumnFamilyStore().getCurrentMemtable();
        assertTrue("expected " + expected.getSimpleName() + ", got " + current.getClass().getName(),
                   expected.isInstance(current));
    }
}
