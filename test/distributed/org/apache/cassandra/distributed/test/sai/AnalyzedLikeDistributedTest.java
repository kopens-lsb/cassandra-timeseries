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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;

/**
 * 3-node cluster coverage for {@code LIKE} on a SAI index with a substring-capable {@code index_analyzer}
 * (RF=3): coordinator/replica agreement of the analyzed semantics across consistency levels, paging across
 * a partition + clustering-range LIKE query, correctness with an unrepaired split row, and reconciliation
 * of an overwrite. The critical property: a replica-side index match must never be dropped or wrongly kept
 * by coordinator-side filtering — the analyzed recheck and the coordinator RowFilter must agree.
 */
public class AnalyzedLikeDistributedTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                          .with(GOSSIP).with(NETWORK))
                              .start(),
                       3);   // RF = 3
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void likeAgreesAcrossConsistencyLevels()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.logs (device text, ts timestamp, msg text, " +
                                          "PRIMARY KEY (device, ts)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.logs(msg) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'ngram' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.logs (device, ts, msg) VALUES " +
                                                    "('pump-01', '2024-01-01 09:00:00+0000', 'connection timeout on port 9042')"),
                                       ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.logs (device, ts, msg) VALUES " +
                                                    "('pump-01', '2024-01-01 09:05:00+0000', '펌프 정지 알림')"),
                                       ConsistencyLevel.ALL);
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.logs (device, ts, msg) VALUES " +
                                                    "('pump-01', '2024-01-01 09:10:00+0000', 'connection refused')"),
                                       ConsistencyLevel.ALL);

        for (ConsistencyLevel cl : new ConsistencyLevel[] { ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL })
        {
            for (int coordinator = 1; coordinator <= 3; coordinator++)
            {
                // mid-word fragment: replica index recall + coordinator-side precision must agree
                assertRows(CLUSTER.coordinator(coordinator)
                                  .execute(withKeyspace("SELECT msg FROM %s.logs WHERE device='pump-01' AND msg LIKE '%%imeou%%'"), cl),
                           row("connection timeout on port 9042"));
                // korean fragment crossing a space
                assertRows(CLUSTER.coordinator(coordinator)
                                  .execute(withKeyspace("SELECT msg FROM %s.logs WHERE device='pump-01' AND msg LIKE '%%프 정%%'"), cl),
                           row("펌프 정지 알림"));
                // conjunction of repeated restrictions
                assertRows(CLUSTER.coordinator(coordinator)
                                  .execute(withKeyspace("SELECT msg FROM %s.logs WHERE device='pump-01' " +
                                                        "AND msg LIKE '%%connection%%' AND msg LIKE '%%refused%%'"), cl),
                           row("connection refused"));
            }
        }
    }

    @Test
    public void likeWithClusteringRangeAndPaging()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.paged_logs (device text, ts timestamp, msg text, " +
                                          "PRIMARY KEY (device, ts)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.paged_logs(msg) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'ngram' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // rows in an out-of-range hour (09:xx) and the queried hour (11:xx), matches interleaved with noise
        for (int i = 0; i < 80; i++)
        {
            String msg = (i % 2 == 0) ? "early timeout " + i : "early heartbeat " + i;
            CLUSTER.coordinator(1).execute(withKeyspace(String.format(
                "INSERT INTO %%s.paged_logs (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:%02d:%02d+0000', '%s')",
                i / 60, i % 60, msg)), ConsistencyLevel.ALL);
        }
        for (int i = 0; i < 80; i++)
        {
            String msg = (i % 2 == 0) ? "late timeout " + i : "late heartbeat " + i;
            CLUSTER.coordinator(1).execute(withKeyspace(String.format(
                "INSERT INTO %%s.paged_logs (device, ts, msg) VALUES ('pump-01', '2024-01-01 11:%02d:%02d+0000', '%s')",
                i / 60, i % 60, msg)), ConsistencyLevel.ALL);
        }

        // page through a range-restricted LIKE with a small fetch size: paging must not lose or duplicate rows
        Iterator<Object[]> rows = CLUSTER.coordinator(2)
                                         .executeWithPaging(withKeyspace("SELECT ts, msg FROM %s.paged_logs WHERE device='pump-01' " +
                                                                         "AND ts >= '2024-01-01 11:00:00+0000' AND ts < '2024-01-01 12:00:00+0000' " +
                                                                         "AND msg LIKE '%%timeout%%'"),
                                                            ConsistencyLevel.QUORUM, 7);
        int matches = 0;
        while (rows.hasNext())
        {
            Object[] row = rows.next();
            assertEquals(true, ((String) row[1]).contains("late timeout"));
            matches++;
        }
        assertEquals(40, matches);
    }

    @Test
    public void unrepairedSplitRowIsNotAFalsePositive()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.split_logs (device text, ts timestamp, msg text, note text, " +
                                          "PRIMARY KEY (device, ts)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.split_logs(msg) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'ngram' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // the row is split across replicas: one has a matching msg, the others a non-matching newer msg.
        // reconciliation keeps the newer value, so the LIKE must not return the row.
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.split_logs (device, ts, msg) VALUES " +
                                                    "('d', '2024-01-01 00:00:00+0000', 'old timeout message') USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.split_logs (device, ts, msg) VALUES " +
                                                    "('d', '2024-01-01 00:00:00+0000', 'replaced clean message') USING TIMESTAMP 2"));
        CLUSTER.get(3).executeInternal(withKeyspace("INSERT INTO %s.split_logs (device, ts, msg) VALUES " +
                                                    "('d', '2024-01-01 00:00:00+0000', 'replaced clean message') USING TIMESTAMP 2"));

        assertEquals(0, CLUSTER.coordinator(1)
                               .execute(withKeyspace("SELECT msg FROM %s.split_logs WHERE device='d' AND msg LIKE '%%timeout%%'"),
                                        ConsistencyLevel.ALL).length);

        // and the reconciled value is still findable
        assertRows(CLUSTER.coordinator(1)
                          .execute(withKeyspace("SELECT msg FROM %s.split_logs WHERE device='d' AND msg LIKE '%%clean%%'"),
                                   ConsistencyLevel.ALL),
                   row("replaced clean message"));
    }

    @Test
    public void nodeDownStillServesAtOne() throws Throwable
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.degraded_logs (device text, ts timestamp, msg text, " +
                                          "PRIMARY KEY (device, ts)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.degraded_logs(msg) USING 'sai' " +
                                          "WITH OPTIONS = { 'index_analyzer' : 'ngram' }"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.degraded_logs (device, ts, msg) VALUES " +
                                                    "('d', '2024-01-01 00:00:00+0000', 'timeout while degraded')"),
                                       ConsistencyLevel.ALL);

        CLUSTER.get(3).shutdown().get();
        try
        {
            assertRows(CLUSTER.coordinator(1)
                              .execute(withKeyspace("SELECT msg FROM %s.degraded_logs WHERE device='d' AND msg LIKE '%%timeout%%'"),
                                       ConsistencyLevel.ONE),
                       row("timeout while degraded"));
        }
        finally
        {
            CLUSTER.get(3).startup();
        }
    }
}
