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

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.ObjectSizes;

import static org.junit.Assert.assertTrue;

/**
 * Measures — not estimates — the bytes of heap one ingested row costs under {@code memtable =
 * 'skiplist'} versus {@code memtable = 'timeseries'}, for the tag-table shape the columnar storage
 * exists for. Two independent gauges:
 * <ul>
 *   <li><b>deep size</b>: jamm's {@link ObjectSizes#measureDeep} over the memtable's partition
 *       structures (shards plus key index here; the partitions map for the skip list). This walks
 *       the real object graph, so it cannot be fooled by an accounting bug, and shared slab regions
 *       are counted once, as the identity-tracked walk sees them.</li>
 *   <li><b>allocator accounting</b>: {@code Memtable.getMemoryUsage().ownsOnHeap}, what the flush
 *       scheduler acts on — reported so a divergence between the two gauges (an accounting bug)
 *       is visible in the log.</li>
 * </ul>
 * The assertion is deliberately below the design's 3–5x prediction so it fails only when the
 * columnar path has actually stopped working (for example every row quietly landing in the object
 * overflow tier), while the log line reports the honest measured ratio, whatever it is.
 */
public class TimeSeriesMemtableHeapTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMemtableHeapTest.class);

    private static final long HOUR_MS = 3_600_000L;
    private static final long BASE_MS = 1785628800000L;
    private static final String TSCS =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}";

    private static final int SERIES = 20;
    private static final int WINDOWS = 4;
    private static final int ROWS_PER_SERIES_PER_WINDOW = 625;
    private static final int ROWS = SERIES * WINDOWS * ROWS_PER_SERIES_PER_WINDOW;

    @Test
    public void columnarStorageCutsHeapPerRow() throws Throwable
    {
        Measurement skiplist = measure("skiplist");
        Measurement timeseries = measure("timeseries");

        double deepRatio = (double) skiplist.deepBytes / timeseries.deepBytes;
        double ownsRatio = (double) skiplist.ownsOnHeap / timeseries.ownsOnHeap;
        String report = String.format("heap per row over %d rows (%d series x %d windows x %d rows):%n" +
                                      "  deep size:  skiplist %d B/row, timeseries %d B/row -> %.2fx%n" +
                                      "  allocator:  skiplist %d B/row, timeseries %d B/row -> %.2fx",
                                      ROWS, SERIES, WINDOWS, ROWS_PER_SERIES_PER_WINDOW,
                                      skiplist.deepBytes / ROWS, timeseries.deepBytes / ROWS, deepRatio,
                                      skiplist.ownsOnHeap / ROWS, timeseries.ownsOnHeap / ROWS, ownsRatio);
        logger.info(report);
        System.out.println(report);
        // The test runner captures neither stdout nor this logger into a file that survives the run,
        // and the whole point of this test is the number, so persist it where CI can pick it up.
        Files.createDirectories(Paths.get("build/test/output"));
        Files.write(Paths.get("build/test/output/timeseries-heap-report.txt"), report.getBytes());

        assertTrue("columnar storage should measurably cut deep heap per row, but " + report,
                   deepRatio >= 2.0);
        assertTrue("columnar storage should cut the allocator-accounted heap too, but " + report,
                   ownsRatio > 1.0);
    }

    private Measurement measure(String memtableConfig) throws Throwable
    {
        createTable("CREATE TABLE %s (series text, ts timestamp, v double, q int, PRIMARY KEY (series, ts)) " +
                    "WITH compaction = " + TSCS + " AND memtable = '" + memtableConfig + "'");

        for (int w = 0; w < WINDOWS; w++)
        {
            for (int i = 0; i < ROWS_PER_SERIES_PER_WINDOW; i++)
            {
                for (int s = 0; s < SERIES; s++)
                {
                    long clusteringMs = BASE_MS + w * HOUR_MS + i * 1000L;
                    execute("INSERT INTO %s (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                            "S" + s, clusteringMs, w + i * 0.001, w * 1000 + i, clusteringMs * 1000L);
                }
            }
        }

        Memtable memtable = getCurrentColumnFamilyStore().getCurrentMemtable();
        Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);

        long deepBytes;
        if (memtable instanceof TimeSeriesMemtable)
        {
            // Shards plus the shared key index; the interned keys are visited once, as in reality.
            deepBytes = ObjectSizes.measureDeep(new Object[]{ ((TimeSeriesMemtable) memtable).shards(),
                                                              fieldOf(memtable, TimeSeriesMemtable.class, "keys") });
        }
        else
        {
            deepBytes = ObjectSizes.measureDeep(fieldOf(memtable, SkipListMemtable.class, "partitions"));
        }
        return new Measurement(deepBytes, usage.ownsOnHeap);
    }

    private static Object fieldOf(Memtable memtable, Class<?> owner, String name) throws Exception
    {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(memtable);
    }

    private static final class Measurement
    {
        final long deepBytes;
        final long ownsOnHeap;

        Measurement(long deepBytes, long ownsOnHeap)
        {
            this.deepBytes = deepBytes;
            this.ownsOnHeap = ownsOnHeap;
        }
    }
}
