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

package org.apache.cassandra.cql3.functions;

import java.util.Date;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

/**
 * Tests for the time-series native functions defined in {@link TimeSeriesFcts}
 * ({@code time_bucket}, {@code first}, {@code last}), exercised through real CQL.
 */
public class TimeSeriesFctsTest extends CQLTester
{
    private static Date date(String iso)
    {
        return new Date(java.time.Instant.parse(iso).toEpochMilli());
    }

    @Test
    public void testTimeBucketScalar() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");

        // 09:17 and 09:43 both fall into the 09:00 hour bucket; 10:05 into the 10:00 bucket.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:17:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:43:00+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:05:00+0000', 3)");

        assertRows(execute("SELECT v, time_bucket(1h, ts) FROM %s WHERE k = 1"),
                   row(1, date("2024-01-01T09:00:00Z")),
                   row(2, date("2024-01-01T09:00:00Z")),
                   row(3, date("2024-01-01T10:00:00Z")));

        // time_bucket is the same computation as the equivalent floor() call.
        Object[][] expectedBuckets = { row(date("2024-01-01T09:00:00Z")),
                                       row(date("2024-01-01T09:00:00Z")),
                                       row(date("2024-01-01T10:00:00Z")) };
        assertRows(execute("SELECT time_bucket(1h, ts) FROM %s WHERE k = 1"), expectedBuckets);
        assertRows(execute("SELECT floor(ts, 1h) FROM %s WHERE k = 1"), expectedBuckets);
    }

    @Test
    public void testTimeBucketWithOrigin() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:17:00+0000', 1)");

        // Bucketing into 1-hour windows offset by 30 minutes: 09:17 lands in the [08:30, 09:30) bucket.
        assertRows(execute("SELECT time_bucket(1h, ts, '2024-01-01 00:30:00+0000') FROM %s WHERE k = 1"),
                   row(date("2024-01-01T08:30:00Z")));
    }

    @Test
    public void testTimeBucketGroupBy() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:17:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:43:00+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:05:00+0000', 10)");

        // Downsample to hourly buckets and aggregate within each bucket.
        assertRows(execute("SELECT time_bucket(1h, ts) AS bucket, sum(v), count(v) " +
                           "FROM %s WHERE k = 1 GROUP BY k, time_bucket(1h, ts)"),
                   row(date("2024-01-01T09:00:00Z"), 4, 2L),
                   row(date("2024-01-01T10:00:00Z"), 10, 1L));
    }

    @Test
    public void testFirstAndLast() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v text, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 'a')");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:00:00+0000', 'b')");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:00:00+0000', 'c')");

        assertRows(execute("SELECT first(v, ts), last(v, ts) FROM %s WHERE k = 1"),
                   row("a", "c"));
    }

    @Test
    public void testFirstAndLastIgnoreInsertionOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");

        // Insert out of time order to prove the result depends on the timestamp, not arrival order.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:00:00+0000', 30)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:00:00+0000', 20)");

        assertRows(execute("SELECT first(v, ts), last(v, ts) FROM %s WHERE k = 1"),
                   row(10, 30));
    }

    @Test
    public void testFirstAndLastPerBucket() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:10:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:50:00+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:10:00+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:50:00+0000', 4)");

        assertRows(execute("SELECT first(v, ts), last(v, ts) " +
                           "FROM %s WHERE k = 1 GROUP BY k, time_bucket(1h, ts)"),
                   row(1, 2),
                   row(3, 4));
    }

    @Test
    public void testFirstAndLastWithNoRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT first(v, ts), last(v, ts) FROM %s WHERE k = 1"),
                   row(null, null));
    }

    @Test
    public void testDeltaRateDerivativeOnLinearSeries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Perfectly linear: +10 every 10 seconds over a 30-second span.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 20)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:30+0000', 30)");

        // delta = 30 - 0; rate = 30 / 30s = 1.0/s; regression slope of a line = its slope = 1.0/s.
        assertRows(execute("SELECT delta(v, ts), rate(v, ts), derivative(v, ts) FROM %s WHERE k = 1"),
                   row(30.0, 1.0, 1.0));
    }

    @Test
    public void testDerivativeDiffersFromRateOnNonLinearSeries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Flat then a jump: endpoint rate and least-squares slope diverge.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:30+0000', 40)");

        // delta = 40; rate = 40 / 30s; least-squares slope = 1.2/s (computed from sums).
        assertRows(execute("SELECT delta(v, ts), rate(v, ts), derivative(v, ts) FROM %s WHERE k = 1"),
                   row(40.0, 40.0 / 30.0, 2400.0 / 2000.0));
    }

    @Test
    public void testRatePerBucketIgnoresInsertionOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Out-of-order inserts within one hourly bucket; rate must use timestamp endpoints, not arrival order.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 20)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");

        assertRows(execute("SELECT delta(v, ts), rate(v, ts) " +
                           "FROM %s WHERE k = 1 GROUP BY k, time_bucket(1h, ts)"),
                   row(20.0, 1.0));
    }

    @Test
    public void testSeriesFunctionsWithNoRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT delta(v, ts), rate(v, ts), derivative(v, ts) FROM %s WHERE k = 1"),
                   row(null, null, null));
    }

    @Test
    public void testRateAndDerivativeSinglePointAreNull() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 5)");
        // A single sample has zero time span, so rate and derivative are undefined; delta is 0.
        assertRows(execute("SELECT delta(v, ts), rate(v, ts), derivative(v, ts) FROM %s WHERE k = 1"),
                   row(0.0, null, null));
    }

    @Test
    public void testPercentile() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Values 0,10,20,...,100 across 11 rows.
        for (int i = 0; i <= 10; i++)
            execute("INSERT INTO %s (k, ts, v) VALUES (1, ?, ?)",
                    new java.util.Date(1_700_000_000_000L + i * 1000L), (double) (i * 10));

        // Continuous percentile with linear interpolation over [0..100].
        assertRows(execute("SELECT percentile(v, 0) FROM %s WHERE k = 1"), row(0.0));
        assertRows(execute("SELECT percentile(v, 1) FROM %s WHERE k = 1"), row(100.0));
        assertRows(execute("SELECT percentile(v, 0.5) FROM %s WHERE k = 1"), row(50.0));
        assertRows(execute("SELECT percentile(v, 0.95) FROM %s WHERE k = 1"), row(95.0));
    }

    @Test
    public void testPercentileInterpolatesBetweenValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Four values 1,2,3,4: the 0.5 quantile sits between 2 and 3 -> 2.5 (linear interpolation).
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:02+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:03+0000', 4)");

        assertRows(execute("SELECT percentile(v, 0.5) FROM %s WHERE k = 1"), row(2.5));
    }

    @Test
    public void testPercentileRejectsOutOfRangeQuantile() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 1)");
        assertInvalidThrowMessage("between 0 and 1",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT percentile(v, 2) FROM %s WHERE k = 1");
    }

    @Test
    public void testPercentileWithNoRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT percentile(v, 0.5) FROM %s WHERE k = 1"), row((Object) null));
    }

    @Test
    public void testTimeWeightedAverageEqualsMeanForUniformSpacing() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Linear values at uniform 10s spacing: time-weighted average equals the plain mean (10).
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:10+0000', 10)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:20+0000', 20)");

        assertRows(execute("SELECT time_weighted_average(v, ts) FROM %s WHERE k = 1"), row(10.0));
    }

    @Test
    public void testTimeWeightedAverageWeightsByDuration() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // 0 held for 1s, then ramps to 10 over 10s. Trapezoidal area = 0*1 + (0+10)/2*10 = 50 over 11s.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 0)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:11+0000', 10)");

        // Time-weighted (50/11) differs from the plain mean of the values (10/3).
        assertRows(execute("SELECT time_weighted_average(v, ts) FROM %s WHERE k = 1"),
                   row(50.0 / 11.0));
    }

    @Test
    public void testTimeWeightedAverageSinglePointAndEmpty() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT time_weighted_average(v, ts) FROM %s WHERE k = 1"), row((Object) null));

        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 7)");
        assertRows(execute("SELECT time_weighted_average(v, ts) FROM %s WHERE k = 1"), row(7.0));
    }

    @Test
    public void testVarianceAndStddev() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Values 2,4,6: mean 4, sample variance = (4+0+4)/2 = 4, stddev = 2.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 4)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:02+0000', 6)");

        assertRows(execute("SELECT variance(v), stddev(v) FROM %s WHERE k = 1"), row(4.0, 2.0));
    }

    @Test
    public void testVarianceNeedsTwoValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 5)");
        // Sample variance is undefined for a single value.
        assertRows(execute("SELECT variance(v), stddev(v) FROM %s WHERE k = 1"), row(null, null));
    }

    @Test
    public void testHistogram() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        // Buckets over [0, 20) with nbuckets=4 (width 5). Result is [underflow, b1, b2, b3, b4, overflow].
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', -1)");  // underflow
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 0)");   // bucket 1 [0,5)
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:02+0000', 1)");   // bucket 1
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:03+0000', 2)");   // bucket 1
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:04+0000', 7)");   // bucket 2 [5,10)
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:05+0000', 100)"); // overflow

        assertRows(execute("SELECT histogram(v, 0, 20, 4) FROM %s WHERE k = 1"),
                   row(java.util.Arrays.asList(1L, 3L, 1L, 0L, 0L, 1L)));
    }

    @Test
    public void testHistogramRejectsBadBounds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v double, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 1)");
        assertInvalidThrowMessage("nbuckets > 0",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT histogram(v, 0, 20, 0) FROM %s WHERE k = 1");
        assertInvalidThrowMessage("max > min",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT histogram(v, 20, 0, 4) FROM %s WHERE k = 1");
    }

    @Test
    public void testApproxCountDistinct() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        // Six rows but only three distinct values; HyperLogLog++ is exact at this tiny cardinality.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:01+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:02+0000', 2)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:03+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:04+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:00:05+0000', 2)");

        assertRows(execute("SELECT approx_count_distinct(v) FROM %s WHERE k = 1"), row(3L));
    }

    @Test
    public void testApproxCountDistinctEmptyIsZero() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        assertRows(execute("SELECT approx_count_distinct(v) FROM %s WHERE k = 1"), row(0L));
    }

    @Test
    public void testTimeBucketGapfillGroupsLikeTimeBucket() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:05:00+0000', 1)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:35:00+0000', 3)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 10:15:00+0000', 10)");

        // Scalar: returns the bucket start aligned to the start argument.
        assertRows(execute("SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', " +
                           "'2024-01-01 12:00:00+0000') FROM %s WHERE k = 1"),
                   row(date("2024-01-01T09:00:00Z")),
                   row(date("2024-01-01T09:00:00Z")),
                   row(date("2024-01-01T10:00:00Z")));

        // As a GROUP BY selector it buckets exactly like time_bucket (gap-fill of empty buckets is layered on later).
        assertRows(execute("SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', " +
                           "'2024-01-01 12:00:00+0000') AS bucket, sum(v) " +
                           "FROM %s WHERE k = 1 GROUP BY k, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', " +
                           "'2024-01-01 12:00:00+0000')"),
                   row(date("2024-01-01T09:00:00Z"), 4),
                   row(date("2024-01-01T10:00:00Z"), 10));
    }

    @Test
    public void testTimeBucketGapfillDensifiesEmptyBuckets() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        // Data in the 09:00 and 11:00 buckets; the 10:00 bucket is empty.
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:30:00+0000', 5)");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 11:15:00+0000', 7)");

        String gf = "time_bucket_gapfill(1h, ts, '2024-01-01 09:00:00+0000', '2024-01-01 12:00:00+0000')";
        // The empty 10:00 bucket is materialized with a null aggregate (do not alias the bucket column in v1).
        assertRows(execute("SELECT " + gf + ", sum(v) FROM %s WHERE k = 1 GROUP BY k, " + gf),
                   row(date("2024-01-01T09:00:00Z"), 5),
                   row(date("2024-01-01T10:00:00Z"), null),
                   row(date("2024-01-01T11:00:00Z"), 7));
    }

    @Test
    public void testTimeBucketGapfillRejectsBadRange() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, ts timestamp, v int, PRIMARY KEY (k, ts))");
        execute("INSERT INTO %s (k, ts, v) VALUES (1, '2024-01-01 09:05:00+0000', 1)");
        assertInvalidThrowMessage("finish must be greater than start",
                                  org.apache.cassandra.exceptions.InvalidRequestException.class,
                                  "SELECT time_bucket_gapfill(1h, ts, '2024-01-01 12:00:00+0000', " +
                                  "'2024-01-01 00:00:00+0000') FROM %s WHERE k = 1");
    }
}
