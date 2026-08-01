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

import org.junit.Test;

import org.apache.cassandra.db.timeseries.tiering.ChunkTables;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertNotNull;

/**
 * Minimal reproduction: does the chunk table the re-encoder creates on one node reach the others?
 * <p>
 * <b>This test currently FAILS, and that is the point.</b> It is the reduction of the SP4 Task 5
 * finding that {@link ChunkTables#ensureChunkTable} cannot create a chunk table on a real cluster.
 * {@code ensureChunkTable} commits a programmatic {@code SchemaTransformations.addTable(...)}, and
 * {@code SchemaTransformation.cql()} defaults to the literal string {@code "null"} -- so the entry
 * TCM writes into the cluster metadata log carries {@code "null"} as its CQL. Every peer that
 * replays that entry parses it and throws
 * {@code SyntaxException: line 1:0 no viable alternative at input 'null'} out of
 * {@code SchemaTransformation$SchemaTransformationSerializer.deserialize}, so the entry is
 * undecodable: the chunk table never reaches the other nodes, and -- worse -- schema propagation
 * stops there for <em>every</em> subsequent change, because the peers cannot get past the poisoned
 * entry when fetching the log.
 * <p>
 * {@code CQLSSTableWriter}, the only other caller of {@code SchemaTransformations.addTable}, is the
 * offline SSTable writer: it has no peers and no log replay, which is why the pattern looked safe.
 * <p>
 * The fix belongs in {@code ChunkTables} (emit real {@code CREATE TABLE} DDL so {@code cql()} round
 * trips) and was deliberately left to the owner of that file. Until it lands,
 * {@code TieredStorageDistributedTest} cannot pass either.
 */
public class ChunkTableSchemaPropagationTest extends TestBaseImpl
{
    @Test
    public void chunkTableCreatedOnOneNodeReachesTheOthers() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                            .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                            .start(),
                                    2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.base (tag text, ts timestamp, value double, " +
                                              "PRIMARY KEY (tag, ts))"));

            cluster.get(1).runOnInstance(() -> ChunkTables.ensureChunkTable(
                Schema.instance.getTableMetadata(KEYSPACE, "base")));

            cluster.get(2).runOnInstance(() -> assertNotNull(
                "node2 never learned about the chunk table node1 created",
                Schema.instance.getTableMetadata(KEYSPACE, "base__chunks")));
        }
    }
}
