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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * What the flush path must guarantee when one partition's rows live in several window shards.
 *
 * <p><b>Why these assertions look at the sstable and not at query results.</b> A mutation that
 * dropped the cross-shard grouping in the flush set — so the same partition key was handed to the
 * sstable writer once per shard — was not caught by any read-based test: reads merge across
 * sstables and across shards, which reconstitutes the right answer and hides a malformed file. An
 * sstable whose keys repeat still answers queries today and breaks later, during compaction,
 * streaming or repair, far from the change that caused it. So these tests open the files.
 */
public class TimeSeriesMemtableFlushTest extends CQLTester
{
    private static final String TS_MEMTABLE = " AND memtable = 'timeseries'";
    private static final String TSCS_1H =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}";

    /**
     * One partition, rows written across four windows. Every sstable the flush produces must list
     * each partition key exactly once, and the keys must be strictly increasing — the invariant the
     * whole sstable format rests on.
     */
    @Test
    public void flushNeverRepeatsAPartitionKeyWithinAnSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS_1H + TS_MEMTABLE);

        long hour = 3600_000L;
        long base = 1785600000000L;
        for (int w = 0; w < 4; w++)
            for (int i = 0; i < 25; i++)
                execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        "SAME", base + w * hour + i * 1000L, w * 100.0 + i,
                        (base + w * hour + i * 1000L) * 1000L);
        flush();

        assertKeysUniqueAndOrderedInEverySSTable();
    }

    /** The same, with several partitions interleaved across the windows. */
    @Test
    public void flushNeverRepeatsAnyKeyWithManyPartitions() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS_1H + TS_MEMTABLE);

        long hour = 3600_000L;
        long base = 1785600000000L;
        for (int w = 0; w < 3; w++)
            for (int p = 0; p < 12; p++)
                for (int i = 0; i < 8; i++)
                    execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                            "S" + p, base + w * hour + i * 1000L, p * 1000.0 + w * 10 + i,
                            (base + w * hour + i * 1000L) * 1000L);
        flush();

        assertKeysUniqueAndOrderedInEverySSTable();
    }

    /**
     * Nothing may be lost or duplicated across the flush. Read the partition before and after and
     * compare the full row list — this is the check that a real ingest would eventually notice, and
     * the one whose failure means data loss rather than a malformed file.
     */
    @Test
    public void flushPreservesEveryRowOfAWindowSpanningPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS_1H + TS_MEMTABLE);

        long hour = 3600_000L;
        long base = 1785600000000L;
        List<String> expected = new ArrayList<>();
        for (int w = 0; w < 5; w++)
            for (int i = 0; i < 20; i++)
            {
                long ts = base + w * hour + i * 1000L;
                double v = w * 100.0 + i;
                execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        "SAME", ts, v, ts * 1000L);
                expected.add(ts + "=" + v);
            }

        String before = readAll("SAME");
        flush();
        String after = readAll("SAME");

        assertEquals("flush changed what the partition reads back as", before, after);
        assertEquals("row count changed across flush", expected.size(), after.split("\n").length);
    }

    /**
     * The rows of one partition that fall in one window must all end up in the same sstable as each
     * other. If they scatter, the window classification the strategy relies on is wrong, and the
     * frozen window would span more than its own range.
     */
    @Test
    public void eachSSTableHoldsExactlyOneWindow() throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS_1H + TS_MEMTABLE);

        long hour = 3600_000L;
        long base = (1785600000000L / hour) * hour;
        for (int w = 0; w < 3; w++)
            for (int i = 0; i < 30; i++)
                execute("INSERT INTO %s (series, ts, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                        "S" + (i % 4), base + w * hour + i * 1000L, i * 1.0,
                        (base + w * hour + i * 1000L) * 1000L);
        flush();

        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            long minWindow = (sstable.getMinTimestamp() / 1000L / hour) * hour;
            long maxWindow = (sstable.getMaxTimestamp() / 1000L / hour) * hour;
            assertEquals(sstable.getFilename() + " spans more than one window", minWindow, maxWindow);
        }
    }

    private String readAll(String series) throws Throwable
    {
        StringBuilder sb = new StringBuilder();
        UntypedResultSet rs = execute("SELECT ts, v FROM %s WHERE series = ?", series);
        for (UntypedResultSet.Row row : rs)
            sb.append(row.getTimestamp("ts").getTime()).append('=').append(row.getDouble("v")).append('\n');
        return sb.toString();
    }

    /**
     * Opens every live sstable and walks its partitions in file order. A repeated key, or one that
     * goes backwards, means the flush handed the writer the same partition more than once.
     */
    private void assertKeysUniqueAndOrderedInEverySSTable()
    {
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        assertTrue("flush produced no sstables", !sstables.isEmpty());

        for (SSTableReader sstable : sstables)
        {
            Set<DecoratedKey> seen = new HashSet<>();
            DecoratedKey previous = null;
            int rows = 0;
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        DecoratedKey key = partition.partitionKey();
                        if (!seen.add(key))
                            fail(sstable.getFilename() + " lists partition key " + key +
                                 " more than once — the flush handed the writer one partition per " +
                                 "window shard instead of merging them");
                        if (previous != null && key.compareTo(previous) <= 0)
                            fail(sstable.getFilename() + " has keys out of order: " + previous +
                                 " then " + key);
                        previous = key;
                        while (partition.hasNext())
                        {
                            Unfiltered u = partition.next();
                            if (u instanceof Row)
                                rows++;
                        }
                    }
                }
            }
            assertTrue(sstable.getFilename() + " holds no rows", rows > 0);
        }
    }
}
