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

package org.apache.cassandra.distributed.test.timeseries;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.timeseries.tiering.ChunkTables;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Does the chunk table the re-encoder creates on one node reach the others -- and does the node that
 * created it survive its own restart?
 * <p>
 * Both questions have the same answer and the same mechanism behind it. TCM stores a committed schema
 * change in the cluster metadata log <b>as CQL text</b>
 * ({@code SchemaTransformation.SchemaTransformationSerializer} writes {@code transformation.cql()} and
 * reads it back with {@code QueryProcessor.getStatement}), and {@code SchemaTransformation.cql()}
 * defaults to the literal string {@code "null"}. So while
 * {@link ChunkTables#ensureChunkTable} submitted a programmatic {@code SchemaTransformations.addTable}
 * -- which does not override {@code cql()} -- it wrote an entry nothing could read back:
 * <ul>
 *   <li>every <b>peer</b> replaying it threw {@code SyntaxException: no viable alternative at input
 *   'null'} inside {@code FetchPeerLog}, so the chunk table never arrived <em>and</em> schema
 *   propagation stopped there for every later change (in the original 3-node run, nodes 2 and 3 froze
 *   at one schema version and every subsequent {@code CREATE TABLE} failed to reach agreement);</li>
 *   <li>the same entry is written on a <b>single-node</b> deployment, where nothing reads it back
 *   until that node restarts and replays its own log -- at which point it hits the identical parse
 *   failure, before it is up.</li>
 * </ul>
 * {@code CQLSSTableWriter}, the only other caller of {@code SchemaTransformations.addTable}, is the
 * offline SSTable writer: no peers, no log replay, which is why the pattern looked safe.
 * <p>
 * The fix is real {@code CREATE TABLE IF NOT EXISTS} DDL: a {@code CreateTableStatement} parsed from
 * text carries that text in {@code cql()} and therefore round-trips. This class is what holds it in
 * place, over the shapes where generating that DDL is easy to get wrong -- a composite partition key
 * whose column order is load-bearing, and mixed-case/reserved-word identifiers on every side.
 */
public class ChunkTableSchemaPropagationTest extends TestBaseImpl
{
    /** Deliberately mixed-case and, for the columns, reserved: every identifier here needs quoting. */
    private static final String MIXED_KEYSPACE = "MixedTieringKs";
    private static final String MIXED_TABLE = "MixedBase";

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(2)
                              .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                              .start(),
                       2);
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void chunkTableCreatedOnOneNodeReachesTheOthers()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.base (tag text, ts timestamp, value double, " +
                                          "PRIMARY KEY (tag, ts))"));

        CLUSTER.get(1).runOnInstance(() -> ChunkTables.ensureChunkTable(
            Schema.instance.getTableMetadata(KEYSPACE, "base")));

        awaitChunkTable(2, KEYSPACE, "base");
    }

    @Test
    public void compositeKeyChunkTableReachesTheOtherNodes()
    {
        // Three key columns of three different types: the chunk table has to mirror all of them, in
        // this order. A DDL that dropped one, or reordered them, would still be valid CQL -- and every
        // chunk read afterwards would bind the wrong values to the wrong columns.
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.composite (asset_id text, date text, hour int, " +
                                          "ts timestamp, value double, " +
                                          "PRIMARY KEY ((asset_id, date, hour), ts))"));

        CLUSTER.get(1).runOnInstance(() -> ChunkTables.ensureChunkTable(
            Schema.instance.getTableMetadata(KEYSPACE, "composite")));

        awaitChunkTable(2, KEYSPACE, "composite");
        CLUSTER.get(2).runOnInstance(() -> {
            List<ColumnMetadata> key = Schema.instance.getTableMetadata(KEYSPACE, "composite__chunks")
                                                      .partitionKeyColumns();
            assertEquals(3, key.size());
            assertEquals("asset_id", key.get(0).name.toString());
            assertEquals("date", key.get(1).name.toString());
            assertEquals("hour", key.get(2).name.toString());
        });
    }

    @Test
    public void mixedCaseKeyspaceTableAndColumnsReachTheOtherNodes()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS \"" + MIXED_KEYSPACE + "\" WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 2}");
        // "table" is a CQL reserved word and "Asset" is mixed-case: an unquoted rendering of either
        // would not parse, and an unquoted keyspace would silently address a different (lowercased) one.
        CLUSTER.schemaChange("CREATE TABLE \"" + MIXED_KEYSPACE + "\".\"" + MIXED_TABLE + "\" " +
                             "(\"Asset\" text, \"table\" int, ts timestamp, value double, " +
                             "PRIMARY KEY ((\"Asset\", \"table\"), ts))");

        CLUSTER.get(1).runOnInstance(() -> ChunkTables.ensureChunkTable(
            Schema.instance.getTableMetadata(MIXED_KEYSPACE, MIXED_TABLE)));

        awaitChunkTable(2, MIXED_KEYSPACE, MIXED_TABLE);
    }

    /**
     * The blast radius, not just the symptom: one unreadable entry stops peers at that point in the
     * log, so <em>every later</em> schema change stalls too. {@code schemaChange} waits for agreement
     * across the cluster, so a frozen peer fails this outright.
     */
    @Test
    public void schemaChangesAfterAChunkTableStillReachAgreement()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.before_chunks (tag text, ts timestamp, value double, " +
                                          "PRIMARY KEY (tag, ts))"));

        CLUSTER.get(1).runOnInstance(() -> ChunkTables.ensureChunkTable(
            Schema.instance.getTableMetadata(KEYSPACE, "before_chunks")));
        awaitChunkTable(2, KEYSPACE, "before_chunks");

        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.after_chunks (k int PRIMARY KEY)"));
        CLUSTER.get(2).runOnInstance(() -> assertNotNull(
            "schema propagation stalled at the chunk-table entry: node2 never saw the next CREATE TABLE",
            Schema.instance.getTableMetadata(KEYSPACE, "after_chunks")));
    }

    /**
     * The single-node case, which no peer assertion can reach: one node, no peers, and the only reader
     * of the entry it wrote is itself, at its next startup. If the entry does not parse, replay fails
     * and {@code startup()} never returns -- the production shape of this is a node that will not come
     * back up after its first re-encode.
     */
    @Test
    public void aNodeRestartsAfterCreatingItsOwnChunkTable() throws Exception
    {
        // withSubnet(1): the shared 2-node cluster is still up on 127.0.0.x, so this one needs its own
        // addresses (same reason PagingTest does it).
        try (Cluster cluster = init(Cluster.build(1)
                                           .withSubnet(1)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.restarted (tag text, ts timestamp, value double, " +
                                              "PRIMARY KEY (tag, ts))"));
            cluster.get(1).runOnInstance(() -> ChunkTables.ensureChunkTable(
                Schema.instance.getTableMetadata(KEYSPACE, "restarted")));

            cluster.get(1).shutdown().get();
            cluster.get(1).startup();

            cluster.get(1).runOnInstance(() -> assertNotNull(
                "the chunk table did not survive the node replaying its own cluster metadata log",
                Schema.instance.getTableMetadata(KEYSPACE, "restarted__chunks")));

            // ...and the replayed node still accepts schema changes.
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.after_restart (k int PRIMARY KEY)"));
        }
    }

    /**
     * Waits for {@code node} to learn about {@code baseTable}'s chunk table. The re-encoder's creation
     * path is not {@code cluster.schemaChange}, so nothing here waits for schema agreement on our
     * behalf; the peer picks the entry up out of the metadata log asynchronously.
     */
    private static void awaitChunkTable(int node, String keyspace, String baseTable)
    {
        String chunkTable = ChunkTables.chunkTableName(baseTable);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        do
        {
            if (CLUSTER.get(node).callOnInstance(
                () -> Schema.instance.getTableMetadata(keyspace, chunkTable) != null))
                return;
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        }
        while (System.nanoTime() < deadline);

        fail(String.format("node%d never learned about %s.%s, the chunk table node1 created",
                           node, keyspace, chunkTable));
    }
}
