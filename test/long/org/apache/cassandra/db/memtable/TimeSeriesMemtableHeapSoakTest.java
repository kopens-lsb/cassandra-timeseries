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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.ObjectSizes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Soak-style heap gate for the {@code memtable = 'timeseries'} read path: sustained interleaved
 * writes and reads against one pinned memtable, with the memtable's <em>retained</em> footprint
 * sampled throughout and the verdict passed on the <b>trend</b>, not on any single value.
 *
 * <p><b>Why this test exists.</b> On 2026-08-02 a read-path change retained one materialized
 * {@code ImmutableBTreePartition} per shard-partition <em>across writes</em>, with no bound on the
 * total. Every short unit test was green — 45 of them — because a leak whose signal is a slow
 * monotonic trend is structurally invisible to a 128-iteration test: the first few retentions are
 * indistinguishable from a cache. Production died after six hours of accumulation (16 GB heap,
 * 451 wedged flushes). The gate for that failure class is a soak: keep writing and reading long
 * enough that "retained footprint keeps growing" separates from "warm-up", and fail on the growth.
 *
 * <p><b>Workload shape.</b> A production-like tag table (a couple of numeric columns, a static
 * column, DESC clustering) on TimeSeriesCompactionStrategy, {@value #PARTITIONS} partitions spread
 * over {@value #WINDOWS} write-time window shards. The soak is built so that the <em>data</em>
 * footprint is stationary after warm-up while the read/write interleave continues at full rate:
 * <ul>
 *   <li><b>Writes</b> continuously overwrite existing (partition, clustering) slots with newer
 *       write timestamps — the columnar store merges them in place, so a correct memtable's
 *       footprint does not move. Every partition and window shard is written in every round.</li>
 *   <li><b>Reads</b> run a point + slice + full-partition-scan mix. The set of partitions the
 *       reads touch expands by {@value #COVERAGE_STRIDE} partitions per round and only covers the
 *       full {@value #PARTITIONS} in the final round — the production slow burn, where queries
 *       reach an ever-growing set of series over the hours. Whole-table range scans are left out
 *       of the mix deliberately: one would touch every partition in round one and let a
 *       read-retention leak plateau before the verdict window opens, hiding exactly the trend this
 *       gate exists to see.</li>
 *   <li><b>Order within a round</b>: reads first, then the overwrite pass, then the sample. At
 *       sample time every shard-partition has been written after it was last read, so a read path
 *       honouring the "reads retain nothing" constraint has nothing left to show — the sample is
 *       data plus whatever the read path failed to let go.</li>
 * </ul>
 *
 * <p><b>Gauges.</b> Each round samples both {@code Memtable.getMemoryUsage().ownsOnHeap} (the
 * allocator's accounting — what flush scheduling acts on) and jamm's
 * {@link ObjectSizes#measureDeep} over the memtable's shards and key index (the truth — an
 * identity-tracked walk of the real object graph). The 2026-08-02 leak was invisible to the
 * allocator: the retained partitions were plain-heap objects the flush threshold never saw, which
 * is why the heap could fill while every memtable reported itself small. Deep size is therefore
 * the gauge that goes red; owns-on-heap is asserted too so an accounted leak cannot hide either,
 * and the per-sample deep/owns ratio is reported to make that blindness visible in the log.
 *
 * <p><b>Verdict.</b> Ordinary least-squares slope over the last third of the samples, normalised
 * by the window's mean, must stay under {@value #MAX_RELATIVE_SLOPE_PER_MINUTE} per minute of
 * workload activity for both gauges. The threshold is justified by measurement, not taste: on a
 * correct build the post-write samples are deterministic to the reported 0.01 MB — the arrays do
 * not move and reads have released everything — and every observed green run regressed to
 * +0.000%/min on both gauges (the workload is fixed by operation count, so machine speed only
 * stretches the x-axis). The real leak, replayed against this exact test (commit 9c5d95d670's
 * TimeSeriesColumnarPartition), rises monotonically from the first sample to the last and
 * regresses to +9.0%/min in the verdict window — deep size 24.6 → 76.3 MB while the allocator
 * gauge never moves off 23.46 MB — see the retro-validation numbers in the commit message.
 * 2%/min therefore sits far above anything a green run has ever measured and 4.5x below the
 * replayed real-leak signal; a genuine slow leak that stayed under 2%/min for the whole window
 * would still double the memtable's footprint within the hour, which the six-hour production
 * heap observation (spec §3's rollout gate) exists to catch.
 *
 * <p><b>Determinism.</b> The schedule is fixed by operation count, not wall clock (a slower
 * machine takes longer but samples the identical object graph); the read mix uses a fixed seed;
 * warm-up (array growth, capacity doublings, tail folds) is excluded by the verdict window; and
 * flushes cannot intrude — the keyspace is {@code durable_writes = false} so the commit log never
 * forces a flush, and the test asserts at every sample that the memtable instance is still the
 * one it started with.
 */
public class TimeSeriesMemtableHeapSoakTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMemtableHeapSoakTest.class);

    private static final long HOUR_MS = 3_600_000L;
    /** Fixed epoch base (2026-08-01), aligned with TimeSeriesMemtableHeapTest — never wall clock. */
    private static final long BASE_MS = 1785628800000L;
    private static final String TSCS =
        "{'class':'TimeSeriesCompactionStrategy','window_size':'1h','freeze_after':'2h'}";

    /**
     * ≥500 per the gate's requirements; chosen so the coverage stride divides evenly. The working
     * set (512 × 256 rows ≈ 23 MB accounted) is deliberately sized well under the default test
     * memtable pool's flush trigger: the soak measures ONE pinned memtable, and a bigger data set
     * (measured at ~90 MB accounted) gets flushed out from under the test by pool pressure — the
     * identity guard below catches that loudly. Soak length comes from overwrite sweeps over the
     * same data ({@link #SWEEPS_PER_ROUND}), never from more data.
     */
    private static final int PARTITIONS = 512;
    private static final int WINDOWS = 4;
    private static final int ROWS_PER_WINDOW = 64;
    private static final int ROWS_PER_PARTITION = WINDOWS * ROWS_PER_WINDOW;

    /**
     * One sample per round; 64 samples is well over the 20 the gate requires, the verdict runs on
     * the last ~21, and one round is roughly nine seconds of activity on the machine this was
     * calibrated on — ~34M operations and ≥8 minutes of sustained interleave in total.
     */
    private static final int ROUNDS = 64;
    private static final int COVERAGE_STRIDE = PARTITIONS / ROUNDS;

    /** Full overwrite passes over every (partition, window, row) per round: 4 × 131,072 writes. */
    private static final int SWEEPS_PER_ROUND = 4;

    private static final int HOT_POINT_READS = 256;
    private static final int HOT_BOUNDED_SLICES = 128;
    private static final int HOT_LATEST_SLICES = 128;
    private static final int SLICE_LIMIT = 16;

    /**
     * Verdict threshold: relative growth of the retained footprint per minute of workload
     * activity, over the last third of the samples. See the class comment for the justification
     * against measured green-run noise (±0.1%/min) and the replayed real leak (≥+8%/min).
     */
    private static final double MAX_RELATIVE_SLOPE_PER_MINUTE = 0.02;

    private static final String REPORT_PATH = "build/test/output/timeseries-heap-soak-report.txt";

    private long operations;
    private long activityNanos;

    @Test
    public void sustainedInterleaveKeepsRetainedFootprintFlat() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                         "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } " +
                                         "AND durable_writes = false");
        String table = createTable(keyspace,
                                   "CREATE TABLE %s (series text, ts timestamp, v double, q int, unit text static, " +
                                   "PRIMARY KEY (series, ts)) " +
                                   "WITH CLUSTERING ORDER BY (ts DESC) " +
                                   "AND compaction = " + TSCS + " AND memtable = 'timeseries'");
        String qualified = keyspace + '.' + table;
        forcePreparedValues();

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        Util.flush(cfs);
        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        assertTrue("table must run on the timeseries memtable, got " + memtable.getClass(),
                   memtable instanceof TimeSeriesMemtable);

        String insert = "INSERT INTO " + qualified + " (series, ts, v, q) VALUES (?, ?, ?, ?) USING TIMESTAMP ?";
        String insertStatic = "INSERT INTO " + qualified + " (series, unit) VALUES (?, ?) USING TIMESTAMP ?";
        String pointRead = "SELECT v, q FROM " + qualified + " WHERE series = ? AND ts = ?";
        String boundedSlice = "SELECT ts, v FROM " + qualified + " WHERE series = ? AND ts <= ? LIMIT " + SLICE_LIMIT;
        String latestSlice = "SELECT ts, v FROM " + qualified + " WHERE series = ? LIMIT " + SLICE_LIMIT;
        String fullScan = "SELECT series, ts, v, q, unit FROM " + qualified + " WHERE series = ?";

        // ---- warm-up: fill the stationary working set (generation 0) --------------------------
        long t0 = System.nanoTime();
        for (int p = 0; p < PARTITIONS; p++)
        {
            execute(insertStatic, series(p), "degC", writeTimestamp(0, 0, 0));
            operations++;
            for (int w = 0; w < WINDOWS; w++)
            {
                for (int i = 0; i < ROWS_PER_WINDOW; i++)
                    writeRow(insert, p, w, i, 0);
            }
        }
        activityNanos += System.nanoTime() - t0;

        // Wiring sanity: a broken WHERE clause would soak nothing and still pass the trend check.
        assertEquals(ROWS_PER_PARTITION, execute(fullScan, series(0)).size());
        assertEquals(1, execute(pointRead, series(1), clusteringMs(2, 3)).size());

        Random random = new Random(20260802L);
        List<Sample> samples = new ArrayList<>(ROUNDS);

        for (int round = 0; round < ROUNDS; round++)
        {
            long roundStart = System.nanoTime();

            // ---- read phase: expanding coverage plus a hot mix over the covered prefix --------
            int covered = (round + 1) * COVERAGE_STRIDE;
            for (int p = round * COVERAGE_STRIDE; p < covered; p++)
            {
                execute(fullScan, series(p));
                operations++;
            }
            for (int r = 0; r < HOT_POINT_READS; r++)
            {
                execute(pointRead, series(random.nextInt(covered)),
                        clusteringMs(random.nextInt(WINDOWS), random.nextInt(ROWS_PER_WINDOW)));
                operations++;
            }
            for (int r = 0; r < HOT_BOUNDED_SLICES; r++)
            {
                execute(boundedSlice, series(random.nextInt(covered)),
                        clusteringMs(random.nextInt(WINDOWS), random.nextInt(ROWS_PER_WINDOW)));
                operations++;
            }
            for (int r = 0; r < HOT_LATEST_SLICES; r++)
            {
                execute(latestSlice, series(random.nextInt(covered)));
                operations++;
            }

            // ---- write phase: overwrite every row of every partition × window, newer timestamps
            for (int sweep = 0; sweep < SWEEPS_PER_ROUND; sweep++)
            {
                int generation = round * SWEEPS_PER_ROUND + sweep + 1;
                for (int p = 0; p < PARTITIONS; p++)
                {
                    for (int w = 0; w < WINDOWS; w++)
                    {
                        for (int i = 0; i < ROWS_PER_WINDOW; i++)
                            writeRow(insert, p, w, i, generation);
                    }
                }
            }
            activityNanos += System.nanoTime() - roundStart;

            // ---- sample (excluded from the activity clock) ------------------------------------
            assertSame("memtable flushed during the soak; the measurement is void — pin it harder",
                       memtable, cfs.getTracker().getView().getCurrentMemtable());
            Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
            long deep = measureDeep(memtable);
            samples.add(new Sample(activityNanos / 60e9, operations, usage.ownsOnHeap, deep));
        }

        // ---- verdict ---------------------------------------------------------------------------
        int from = 2 * samples.size() / 3;
        double deepSlope = normalisedSlopePerMinute(samples, from, s -> (double) s.deepBytes);
        double ownsSlope = normalisedSlopePerMinute(samples, from, s -> (double) s.ownsOnHeap);

        String report = report(samples, from, deepSlope, ownsSlope);
        logger.info(report);
        System.out.println(report);
        // The runner keeps neither stdout nor the logger in a file that survives the run, and a
        // trend failure is only diagnosable from the samples, so persist them where CI can look.
        Files.createDirectories(Paths.get("build/test/output"));
        Files.write(Paths.get(REPORT_PATH), report.getBytes());

        assertTrue("sample count too small for a trend verdict: " + samples.size(), samples.size() >= 20);
        assertTrue("soak too short to be a soak: " + operations + " operations", operations >= 25_000_000L);
        assertTrue("memtable retained footprint (jamm deep size) is RISING under sustained " +
                   "interleaved load — the read path is retaining something across writes.\n" + report,
                   deepSlope < MAX_RELATIVE_SLOPE_PER_MINUTE);
        assertTrue("allocator-accounted memtable heap is RISING under sustained interleaved load.\n" + report,
                   ownsSlope < MAX_RELATIVE_SLOPE_PER_MINUTE);
    }

    // -------------------------------------------------------------------------------- workload

    private static String series(int p)
    {
        return "S" + p;
    }

    /** Clustering timestamp of row (w, i): one second apart, ranges disjoint per window. */
    private static long clusteringMs(int w, int i)
    {
        return BASE_MS + w * HOUR_MS + i * 1000L;
    }

    /**
     * Write timestamp (µs) of generation {@code g} of row (w, i). Stays inside window {@code w}
     * for every generation this soak reaches, so an overwrite merges into the row's existing
     * shard-partition slot instead of appending to another shard — that is what keeps the data
     * footprint stationary while the write rate stays real.
     */
    private static long writeTimestamp(int w, int i, int g)
    {
        return (BASE_MS + w * HOUR_MS) * 1000L + i * 1000L + g;
    }

    private void writeRow(String insert, int p, int w, int i, int generation) throws Throwable
    {
        execute(insert, series(p), clusteringMs(w, i),
                generation + i * 0.001, generation * 100_000 + w * 1000 + i,
                writeTimestamp(w, i, generation));
        operations++;
    }

    // ------------------------------------------------------------------------------ measurement

    /** Same idiom as TimeSeriesMemtableHeapTest: the shards plus the shared key index, keys once. */
    private static long measureDeep(Memtable memtable) throws Exception
    {
        return ObjectSizes.measureDeep(new Object[]{ ((TimeSeriesMemtable) memtable).shards(),
                                                     fieldOf(memtable, TimeSeriesMemtable.class, "keys") });
    }

    private static Object fieldOf(Memtable memtable, Class<?> owner, String name) throws Exception
    {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(memtable);
    }

    private interface Gauge
    {
        double of(Sample sample);
    }

    /** OLS slope of the gauge over samples [from, end) against activity minutes, over the window mean. */
    private static double normalisedSlopePerMinute(List<Sample> samples, int from, Gauge gauge)
    {
        int n = samples.size() - from;
        double meanX = 0;
        double meanY = 0;
        for (int i = from; i < samples.size(); i++)
        {
            meanX += samples.get(i).activityMinutes;
            meanY += gauge.of(samples.get(i));
        }
        meanX /= n;
        meanY /= n;

        double covXY = 0;
        double varX = 0;
        for (int i = from; i < samples.size(); i++)
        {
            double dx = samples.get(i).activityMinutes - meanX;
            covXY += dx * (gauge.of(samples.get(i)) - meanY);
            varX += dx * dx;
        }
        return covXY / varX / meanY;
    }

    private static String report(List<Sample> samples, int from, double deepSlope, double ownsSlope)
    {
        StringBuilder sb = new StringBuilder(4096);
        sb.append(String.format(Locale.ROOT,
                                "timeseries memtable heap soak: %d partitions x %d rows, %d rounds%n" +
                                "verdict window: samples #%02d..#%02d, threshold %.1f%%/min%n" +
                                "  deep-size slope: %+.3f%%/min   owns-on-heap slope: %+.3f%%/min%n" +
                                "  #    activity        ops     owns MB     deep MB  deep/owns%n",
                                PARTITIONS, ROWS_PER_PARTITION, samples.size(),
                                from, samples.size() - 1, MAX_RELATIVE_SLOPE_PER_MINUTE * 100,
                                deepSlope * 100, ownsSlope * 100));
        for (int i = 0; i < samples.size(); i++)
        {
            Sample s = samples.get(i);
            sb.append(String.format(Locale.ROOT, "  %02d  %7.2f min  %9d  %10.2f  %10.2f  %9.2f%s%n",
                                    i, s.activityMinutes, s.operations,
                                    s.ownsOnHeap / 1048576.0, s.deepBytes / 1048576.0,
                                    (double) s.deepBytes / s.ownsOnHeap,
                                    i >= from ? "  *" : ""));
        }
        return sb.toString();
    }

    private static final class Sample
    {
        final double activityMinutes;
        final long operations;
        final long ownsOnHeap;
        final long deepBytes;

        Sample(double activityMinutes, long operations, long ownsOnHeap, long deepBytes)
        {
            this.activityMinutes = activityMinutes;
            this.operations = operations;
            this.ownsOnHeap = ownsOnHeap;
            this.deepBytes = deepBytes;
        }
    }
}
