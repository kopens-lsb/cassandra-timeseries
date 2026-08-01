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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Multi-node validation of {@code TimeSeriesCompactionStrategy} (TSCS). Until this class existed the
 * strategy had no distributed coverage of any kind, and its most distribution-sensitive path — the
 * window-splitting {@code SSTableMultiWriter}, which runs on the <em>receiving</em> node whenever
 * sstables arrive by bootstrap, rebuild or repair — had never run on more than one node.
 *
 * <p>What is here, and why each one needs a cluster:
 * <ul>
 *   <li>{@link #streamedSSTablesAreSplitOnWindowBoundaries} — the streaming split path. A node
 *   holding a legacy sstable that spans several windows streams it to a TSCS node; the invariant
 *   "every sstable belongs to exactly one window" has to be re-established by the receiver, because
 *   nothing on the sending side did it. Single-node testing structurally cannot reach this: it needs
 *   two nodes, a real stream and {@code stream_entire_sstables} off (zero-copy streaming ships the
 *   file verbatim and never touches {@code createSSTableMultiWriter}).</li>
 *   <li>{@link #freezeConvergesOnEveryNodeAndThenStops} — compaction is node-local, so RF=3 means
 *   three independent convergences. A freeze that converged on one node and livelocked on another
 *   would look perfectly healthy from any single node, and from any CQL read.</li>
 *   <li>{@link #expiredWindowIsDroppedOnEveryReplica} — a retention drop that reached only a quorum
 *   of replicas still answers every QUORUM read correctly while silently keeping the data on disk
 *   forever. Only per-node sstable inspection catches that.</li>
 *   <li>{@link #lateBackfillLandsInItsOwnWindow} — the flush half of the same splitting writer, on
 *   replicated data, asserted on all three replicas.</li>
 * </ul>
 *
 * <p>All timestamps are set explicitly with {@code USING TIMESTAMP} (microseconds — TSCS's default
 * {@code timestamp_resolution}), so which window a row lands in is fixed by the test rather than by
 * when the test happens to run. Windows are one minute wide and every window under test other than
 * the current one is minutes old, hence closed.
 *
 * <p>Both clusters run {@code withDataDirCount(1)}. jvm-dtest gives every instance three data
 * directories by default, and both flush and compaction shard their output across disk boundaries —
 * so a perfectly healthy node ends a freeze with up to three sstables per window and the counts here
 * would be meaningless. On one directory the invariant is exactly what the design states: one
 * contained sstable per closed window. (The weaker property — every sstable contained in exactly one
 * window — is asserted by {@link #containedWindowsOf} either way, and is what holds on a real
 * multi-disk node.)
 */
public class TimeSeriesCompactionDistributedTest extends TestBaseImpl
{
    /** {@code window_size} for every table here; also the unit of {@link #windowOf}. */
    private static final long WINDOW_MS = TimeUnit.MINUTES.toMillis(1);

    /** How long convergence polls wait before failing. Generous: a real freeze takes milliseconds. */
    private static final long CONVERGE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(2);

    /** TSCS with one-minute windows that close one minute later. */
    private static final String TSCS = "{'class':'TimeSeriesCompactionStrategy'," +
                                       "'timestamp_resolution':'MICROSECONDS'," +
                                       "'window_size':'1m','freeze_after':'1m'}";

    /**
     * The same strategy with background compaction off, for the tests that assert what the WRITE path
     * produced. {@code enabled} only gates task hand-out — {@code createSSTableMultiWriter} is still
     * served, which is exactly the separation these tests need. It is a table-level option rather than
     * {@code nodetool disableautocompaction} on purpose: nodetool would disable the whole keyspace,
     * and the other tests in this class share it.
     */
    private static final String TSCS_NO_COMPACTION = "{'class':'TimeSeriesCompactionStrategy'," +
                                                     "'timestamp_resolution':'MICROSECONDS'," +
                                                     "'window_size':'1m','freeze_after':'1m','enabled':'false'}";

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3)
                              .withDataDirCount(1)
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

    /** The window a millisecond timestamp belongs to, for {@code window_size = 1m}. */
    private static long windowOf(long millis)
    {
        return millis - Math.floorMod(millis, WINDOW_MS);
    }

    /** A window start {@code minutesAgo} minutes before now, aligned so the test cannot straddle one. */
    private static long windowStartMinutesAgo(int minutesAgo)
    {
        return windowOf(System.currentTimeMillis()) - minutesAgo * WINDOW_MS;
    }

    /**
     * The {@code [windowOf(min), windowOf(max)]} write-timestamp span of every live sstable of
     * {@code ks.table} on {@code node}.
     *
     * <p>The pairs come back over the instance boundary as one String because everything a
     * {@code callOnInstance} lambda returns is deserialised in the caller's classloader: a
     * {@code List<long[]>} would work, but a String keeps the contract obvious and the failure
     * messages readable.
     */
    private static List<long[]> sstableWindows(IInvokableInstance node, String ks, String table)
    {
        String encoded = node.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(table);
            StringBuilder sb = new StringBuilder();
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                if (sb.length() > 0)
                    sb.append('|');
                sb.append(sstable.getMinTimestamp()).append(':').append(sstable.getMaxTimestamp());
            }
            return sb.toString();
        });

        List<long[]> spans = new ArrayList<>();
        if (encoded.isEmpty())
            return spans;
        for (String pair : encoded.split("\\|"))
        {
            String[] parts = pair.split(":");
            // TSCS reads sstable timestamps at timestamp_resolution = MICROSECONDS.
            spans.add(new long[]{ windowOf(Long.parseLong(parts[0]) / 1000L),
                                  windowOf(Long.parseLong(parts[1]) / 1000L) });
        }
        return spans;
    }

    /** The distinct windows {@code node}'s sstables live in; fails if any single sstable spans two. */
    private static Set<Long> containedWindowsOf(IInvokableInstance node, String ks, String table)
    {
        Set<Long> windows = new TreeSet<>();
        for (long[] span : sstableWindows(node, ks, table))
        {
            if (span[0] != span[1])
                fail("node" + node.config().num() + ' ' + ks + '.' + table +
                     ": sstable spans windows [" + span[0] + ".." + span[1] + "], " +
                     "but every sstable must be contained in exactly one window");
            windows.add(span[0]);
        }
        return windows;
    }

    private static int sstableCount(IInvokableInstance node, String ks, String table)
    {
        return sstableWindows(node, ks, table).size();
    }

    /** Nudges the background compaction path; the freeze/drop tasks are handed out from there. */
    private static void submitBackgroundCompaction(String ks, String table)
    {
        CLUSTER.forEach(node -> node.runOnInstance(
            () -> CompactionManager.instance.submitBackground(Keyspace.open(ks).getColumnFamilyStore(table))));
    }

    /**
     * Streaming re-establishes the one-sstable-per-window invariant on the RECEIVING node.
     *
     * <p>The sender holds a single sstable spanning four windows — the shape a table that was
     * switched to TSCS after the fact has on disk, and the shape a pre-TSCS node streams. Nothing on
     * the sending side splits it, and nothing on the receiving side compacts it (background
     * compaction is off on both), so the only thing that can leave the receiver with contained
     * sstables is {@code TimeWindowSplittingMultiWriter} running on the streaming write path.
     *
     * <p>{@code stream_entire_sstables} is off on purpose. Zero-copy streaming hands the file to the
     * receiver byte for byte and never calls {@code createSSTableMultiWriter} at all, so with it on
     * this test would be asserting nothing about the fork — and the receiver would inherit the
     * sender's spanning sstable, which is correct only because TSCS senders do not have one.
     */
    @Test
    public void streamedSSTablesAreSplitOnWindowBoundaries() throws IOException
    {
        int windows = 4;
        int rowsPerWindow = 4;
        long firstWindow = windowStartMinutesAgo(20);

        // withSubnet(1): the shared 3-node cluster is still up on 127.0.0.x, and two clusters on the
        // same subnet would fight over the same addresses and ports.
        try (Cluster cluster = init(Cluster.build(2)
                                           .withSubnet(1)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                       .set("stream_entire_sstables", false)
                                                                       .with(GOSSIP).with(NETWORK))
                                           .start()))
        {
            // Created on a NON-TSCS strategy: TSCS's own flush writer would split this on the way in
            // and there would be no spanning sstable left for the receiver to have to fix.
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.streamed (tag text, ts timestamp, value double, " +
                                              "PRIMARY KEY (tag, ts)) WITH compaction = " +
                                              "{'class':'SizeTieredCompactionStrategy','enabled':'false'}"));

            Set<Long> expected = new TreeSet<>();
            for (int w = 0; w < windows; w++)
            {
                long windowStart = firstWindow + w * WINDOW_MS;
                expected.add(windowStart);
                for (int i = 0; i < rowsPerWindow; i++)
                    // Written locally on node1 only (executeInternal bypasses replication), so node2
                    // holds nothing at all until it streams.
                    cluster.get(1).executeInternal(
                        withKeyspace("INSERT INTO %s.streamed (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?"),
                        "tag-" + i, new Date(windowStart + i * 1000L), (double) w,
                        (windowStart + i * 1000L) * 1000L);
            }
            cluster.get(1).flush(KEYSPACE);

            List<long[]> senderSpans = sstableWindows(cluster.get(1), KEYSPACE, "streamed");
            assertEquals("the sender must hold exactly one sstable", 1, senderSpans.size());
            assertEquals("...spanning every window, or there is nothing for the receiver to split",
                         firstWindow, senderSpans.get(0)[0]);
            assertEquals(firstWindow + (windows - 1) * WINDOW_MS, senderSpans.get(0)[1]);

            // Switch to TSCS with background compaction off: the write path is what is under test, and
            // an autocompaction on the receiver would split the sstables for free.
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.streamed WITH compaction = " + TSCS_NO_COMPACTION));

            cluster.get(2).nodetoolResult("rebuild", "--keyspace", KEYSPACE).asserts().success();

            assertEquals("the receiver must split the streamed sstable, one output per window",
                         expected, containedWindowsOf(cluster.get(2), KEYSPACE, "streamed"));
            assertEquals("...and produce exactly one sstable per window",
                         windows, sstableCount(cluster.get(2), KEYSPACE, "streamed"));

            // The sender is untouched: the split happened on the receive path, not before the wire.
            assertEquals("the sender's spanning sstable must not have been rewritten",
                         1, sstableCount(cluster.get(1), KEYSPACE, "streamed"));

            assertEquals("every streamed row must be readable on the receiver",
                         windows * rowsPerWindow,
                         cluster.get(2).executeInternal(withKeyspace("SELECT tag, ts FROM %s.streamed")).length);
        }
    }

    /**
     * Freezing is node-local work on replicated data: every replica has to reach one contained
     * sstable per closed window on its own, and then stop.
     *
     * <p>The "and then stop" half is the livelock guard. A freeze/split alternation that rewrites a
     * window forever answers every read correctly and shows nothing on any dashboard except CPU; the
     * only observable is that the window's sstables keep being replaced. Holding the sstable identity
     * fixed across a quiet interval is what names that.
     */
    @Test
    public void freezeConvergesOnEveryNodeAndThenStops()
    {
        String table = "freeze_converge";
        int windows = 3;
        int flushes = 3;
        long firstWindow = windowStartMinutesAgo(20);

        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (tag text, ts timestamp, value double, " +
                                          "PRIMARY KEY (tag, ts)) WITH read_repair = 'NONE' AND compaction = " + TSCS));

        Set<Long> expected = new TreeSet<>();
        for (int w = 0; w < windows; w++)
            expected.add(firstWindow + w * WINDOW_MS);

        // Several flushes per window, so each closed window really does start out with several
        // sstables and the freeze has work to do on every node.
        for (int flush = 0; flush < flushes; flush++)
        {
            for (int w = 0; w < windows; w++)
            {
                long windowStart = firstWindow + w * WINDOW_MS;
                CLUSTER.coordinator(1 + (w % 3)).execute(
                    withKeyspace("INSERT INTO %s." + table + " (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?"),
                    ConsistencyLevel.ALL, "tag-" + flush, new Date(windowStart + flush * 1000L),
                    (double) w, (windowStart + flush * 1000L) * 1000L);
            }
            CLUSTER.forEach(node -> node.flush(KEYSPACE));
        }

        submitBackgroundCompaction(KEYSPACE, table);

        // Every node, independently: one contained sstable per closed window.
        awaitOnEveryNode(table, () -> {
            for (int node = 1; node <= 3; node++)
            {
                if (sstableCount(CLUSTER.get(node), KEYSPACE, table) != windows)
                    return false;
                if (!containedWindowsOf(CLUSTER.get(node), KEYSPACE, table).equals(expected))
                    return false;
            }
            return true;
        }, "every node must reach one contained sstable per closed window");

        // ... and then nothing further happens. A livelocking window replaces its sstables every
        // round, so the identity of the files is the observable, not their count.
        List<String> before = sstableIdentities(table);
        Uninterruptibles.sleepUninterruptibly(15, TimeUnit.SECONDS);
        assertEquals("a converged window must not be rewritten again (freeze/split livelock)",
                     before, sstableIdentities(table));

        assertEquals("every row must survive the freeze",
                     (long) windows * flushes,
                     (long) (Long) CLUSTER.coordinator(1).execute(withKeyspace("SELECT count(*) FROM %s." + table),
                                                                  ConsistencyLevel.QUORUM)[0][0]);
    }

    /**
     * A whole-window retention drop has to happen on every replica. Reads cannot tell the difference
     * — a window dropped on two of three replicas answers every QUORUM read exactly as if it had been
     * dropped everywhere — so this asserts on each node's sstables, which is the thing that actually
     * reclaims the disk.
     */
    @Test
    public void expiredWindowIsDroppedOnEveryReplica()
    {
        String table = "retention_drop";
        long expiredWindow = windowStartMinutesAgo(30);
        long keptWindow = windowStartMinutesAgo(1);

        // retention must be >= window_size + freeze_after (TSCS validates it); 5m over 1m windows
        // makes the 30-minutes-old window expired and the 1-minute-old one merely closed.
        //
        // Background compaction starts OFF: the drop is fast enough that "the expired window was on
        // disk to begin with" is otherwise a race against it, and a pre-condition that can lose its
        // own race is worse than none - it would turn a code that never wrote the window at all into
        // a green run.
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (tag text, ts timestamp, value double, " +
                                          "PRIMARY KEY (tag, ts)) WITH read_repair = 'NONE' AND compaction = " +
                                          "{'class':'TimeSeriesCompactionStrategy','timestamp_resolution':'MICROSECONDS'," +
                                          "'window_size':'1m','freeze_after':'1m','retention':'5m','enabled':'false'}"));

        for (int i = 0; i < 4; i++)
        {
            CLUSTER.coordinator(1).execute(
                withKeyspace("INSERT INTO %s." + table + " (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?"),
                ConsistencyLevel.ALL, "old", new Date(expiredWindow + i * 1000L), (double) i,
                (expiredWindow + i * 1000L) * 1000L);
            CLUSTER.coordinator(2).execute(
                withKeyspace("INSERT INTO %s." + table + " (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?"),
                ConsistencyLevel.ALL, "new", new Date(keptWindow + i * 1000L), (double) i,
                (keptWindow + i * 1000L) * 1000L);
        }
        CLUSTER.forEach(node -> node.flush(KEYSPACE));

        for (int node = 1; node <= 3; node++)
            assertTrue("node" + node + ": the expired window must be on disk before the drop",
                       containedWindowsOf(CLUSTER.get(node), KEYSPACE, table).contains(expiredWindow));

        // Now let compaction run. enableAutoCompaction is a node-local runtime switch, so it beats
        // an ALTER here: it does not touch the schema the other tests in this class share.
        String ks = KEYSPACE;
        CLUSTER.forEach(node -> node.runOnInstance(
            () -> Keyspace.open(ks).getColumnFamilyStore(table).enableAutoCompaction()));
        submitBackgroundCompaction(KEYSPACE, table);

        awaitOnEveryNode(table, () -> {
            for (int node = 1; node <= 3; node++)
            {
                Set<Long> windows = containedWindowsOf(CLUSTER.get(node), KEYSPACE, table);
                if (windows.contains(expiredWindow) || !windows.contains(keptWindow))
                    return false;
            }
            return true;
        }, "the expired window must be dropped on every replica, and only the expired one");

        assertEquals("the expired rows must be gone", 0L,
                     (long) (Long) CLUSTER.coordinator(3).execute(
                         withKeyspace("SELECT count(*) FROM %s." + table + " WHERE tag = 'old'"),
                         ConsistencyLevel.QUORUM)[0][0]);
        assertEquals("the retained window must be untouched", 4L,
                     (long) (Long) CLUSTER.coordinator(3).execute(
                         withKeyspace("SELECT count(*) FROM %s." + table + " WHERE tag = 'new'"),
                         ConsistencyLevel.QUORUM)[0][0]);
    }

    /**
     * Backfill isolation, on the flush path, on every replica: rows written now with old timestamps
     * share a memtable with current rows, and the flush must put them in their own window rather than
     * dragging the current window's sstable back across a boundary. A single sstable holding both is
     * what breaks whole-window drops and per-window freezing later.
     */
    @Test
    public void lateBackfillLandsInItsOwnWindow()
    {
        String table = "late_backfill";
        long lateWindow = windowStartMinutesAgo(45);
        long currentWindow = windowOf(System.currentTimeMillis());

        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (tag text, ts timestamp, value double, " +
                                          "PRIMARY KEY (tag, ts)) WITH read_repair = 'NONE' AND compaction = " +
                                          TSCS_NO_COMPACTION));

        // Both go into the SAME memtable, so it is the flush writer - not two separate flushes - that
        // has to keep them apart.
        for (int i = 0; i < 3; i++)
        {
            CLUSTER.coordinator(1).execute(
                withKeyspace("INSERT INTO %s." + table + " (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?"),
                ConsistencyLevel.ALL, "live", new Date(currentWindow + i * 1000L), (double) i,
                (currentWindow + i * 1000L) * 1000L);
            CLUSTER.coordinator(1).execute(
                withKeyspace("INSERT INTO %s." + table + " (tag, ts, value) VALUES (?, ?, ?) USING TIMESTAMP ?"),
                ConsistencyLevel.ALL, "backfill", new Date(lateWindow + i * 1000L), (double) i,
                (lateWindow + i * 1000L) * 1000L);
        }
        CLUSTER.forEach(node -> node.flush(KEYSPACE));

        Set<Long> expected = new TreeSet<>();
        expected.add(lateWindow);
        expected.add(currentWindow);
        for (int node = 1; node <= 3; node++)
        {
            assertEquals("node" + node + ": the backfill must not pollute the current window",
                         expected, containedWindowsOf(CLUSTER.get(node), KEYSPACE, table));
            assertEquals("node" + node + ": one sstable per window",
                         2, sstableCount(CLUSTER.get(node), KEYSPACE, table));
        }

        assertEquals("both windows must still read back", 6L,
                     (long) (Long) CLUSTER.coordinator(2).execute(withKeyspace("SELECT count(*) FROM %s." + table),
                                                                  ConsistencyLevel.QUORUM)[0][0]);
    }

    /** A stable, comparable identity for every node's sstables: node -> sorted sstable file names. */
    private static List<String> sstableIdentities(String table)
    {
        String ks = KEYSPACE;
        List<String> identities = new ArrayList<>();
        for (int node = 1; node <= 3; node++)
            identities.add(node + "=" + CLUSTER.get(node).callOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(table);
                Set<String> names = new TreeSet<>();
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    names.add(sstable.getFilename());
                return names.toString();
            }));
        return identities;
    }

    /**
     * Polls {@code condition} until it holds or {@link #CONVERGE_TIMEOUT_MS} passes, re-nudging the
     * background compaction path each round — {@code submitBackground} is a no-op when the executor
     * already has work, so this only matters when an earlier round finished with tasks still to do.
     */
    private static void awaitOnEveryNode(String table, ConvergenceCheck condition, String what)
    {
        long deadline = System.currentTimeMillis() + CONVERGE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline)
        {
            if (condition.holds())
                return;
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            submitBackgroundCompaction(KEYSPACE, table);
        }
        StringBuilder detail = new StringBuilder(what).append(" (timed out after ")
                                                      .append(CONVERGE_TIMEOUT_MS).append("ms)");
        for (int node = 1; node <= 3; node++)
        {
            detail.append("\n  node").append(node).append(':');
            for (long[] span : sstableWindows(CLUSTER.get(node), KEYSPACE, table))
                detail.append(" [").append(span[0]).append("..").append(span[1]).append(']');
        }
        fail(detail.toString());
    }

    /** A poll body that may itself assert; kept as an interface so lambdas can fail fast. */
    private interface ConvergenceCheck
    {
        boolean holds();
    }
}
