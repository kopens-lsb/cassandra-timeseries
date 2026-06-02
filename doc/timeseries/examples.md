<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Time-Series Query Examples

Worked CQL examples for the native time-series functions: `time_bucket`, `first`, `last`,
`delta`, `rate`, `derivative`, `percentile`. For semantics and design notes see
[timeseries-functions-design.md](timeseries-functions-design.md).

All examples are runnable in `cqlsh`. Function reference (argument order matters):

| Function | Signature | Returns |
|---|---|---|
| `time_bucket` | `time_bucket(duration, timestamp [, origin])` | `timestamp` (bucket start) |
| `first` / `last` | `first(value, timestamp)` / `last(value, timestamp)` | type of `value` |
| `delta` | `delta(value, timestamp)` | `double` |
| `rate` | `rate(value, timestamp)` | `double` (per second) |
| `derivative` | `derivative(value, timestamp)` | `double` (per second, least-squares slope) |
| `counter_delta` / `counter_rate` | `counter_delta(value, ts)` / `counter_rate(value, ts)` | `double` (reset-aware counter increase / per second) |
| `percentile` | `percentile(value, q)` with `q` in `[0,1]` | `double` |
| `time_weighted_average` | `time_weighted_average(value, timestamp)` | `double` |
| `integral` | `integral(value, timestamp)` | `double` (area under curve, value-seconds) |
| `variance` / `stddev` | `variance(value)` / `stddev(value)` | `double` (sample) |
| `corr` / `covar_pop` / `covar_samp` | `corr(y, x)` / `covar_pop(y, x)` / `covar_samp(y, x)` | `double` (two-variable stats) |
| `regr_slope` / `regr_intercept` / `regr_r2` | `regr_slope(y, x)` / `regr_intercept(y, x)` / `regr_r2(y, x)` | `double` (linear regression of y on x) |
| `histogram` | `histogram(value, min, max, nbuckets)` | `list<bigint>` (nbuckets+2) |
| `approx_count_distinct` | `approx_count_distinct(value)` | `bigint` |
| `time_bucket_gapfill` | `time_bucket_gapfill(width, ts, start, finish)` | `timestamp` (gap-filling GROUP BY selector) |
| `locf` | `locf(aggregate)` | same as argument (carry-forward fill for gap-filled buckets) |
| `interpolate` | `interpolate(aggregate)` | `double` (linear-interpolation fill for gap-filled buckets) |

---

## 1. Schema and sample data

A canonical time-series table: one partition per series, clustered by time. `TimeWindowCompactionStrategy`
groups SSTables by time window so old windows can be dropped whole when they expire.

```sql
CREATE KEYSPACE IF NOT EXISTS ts
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ts;

CREATE TABLE metrics (
    series text,            -- e.g. 'cpu.host1'
    ts     timestamp,
    value  double,
    PRIMARY KEY (series, ts)
) WITH CLUSTERING ORDER BY (ts ASC)
   AND compaction = {'class': 'TimeWindowCompactionStrategy',
                     'compaction_window_unit': 'HOURS',
                     'compaction_window_size': 1}
   AND default_time_to_live = 2592000;   -- 30 days

INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:05:00+0000', 10);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:35:00+0000', 30);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:15:00+0000', 50);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:45:00+0000', 70);
```

---

## 2. Bucketing & downsampling — `time_bucket`

### 2.1 Assign each row to its bucket (scalar use)

```sql
SELECT ts, time_bucket(1h, ts) AS bucket, value
FROM   metrics
WHERE  series = 'cpu';
```

### 2.2 Downsample to fixed intervals with `GROUP BY`

```sql
-- Hourly average / min / max / count
SELECT time_bucket(1h, ts) AS bucket,
       avg(value), min(value), max(value), count(value)
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket(1h, ts);

-- Other intervals: 5 minutes, 1 day
SELECT time_bucket(5m, ts) AS bucket, avg(value) FROM metrics
  WHERE series = 'cpu' GROUP BY series, time_bucket(5m, ts);

SELECT time_bucket(1d, ts) AS bucket, avg(value) FROM metrics
  WHERE series = 'cpu' GROUP BY series, time_bucket(1d, ts);
```

### 2.3 Shifted buckets with an origin

```sql
-- Hourly buckets offset by 30 minutes: windows are [08:30, 09:30), [09:30, 10:30), ...
SELECT time_bucket(1h, ts, '2024-01-01 00:30:00+0000') AS bucket, avg(value)
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket(1h, ts, '2024-01-01 00:30:00+0000');
```

### 2.4 Gap-filling empty buckets — `time_bucket_gapfill`

Plain `time_bucket` only emits buckets that have data. `time_bucket_gapfill` additionally materializes a row for
*every* bucket in `[start, finish)`, so dashboards get a continuous time axis (empty buckets carry null aggregates).

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(value)
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

By default empty buckets carry null aggregates. Wrap a selected aggregate in `locf(...)` to instead carry the
previous non-empty bucket's value forward (last-observation-carried-forward):

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       locf(avg(value))   -- empty buckets repeat the previous hour's average instead of null
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

`locf` is a no-op on real rows (returns its argument); it only affects synthesized empty buckets. Buckets before the
first real value remain null (nothing to carry yet).

Use `interpolate(...)` instead to linearly interpolate empty buckets between the surrounding non-empty values
(the result is a `double`):

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       interpolate(avg(value))   -- empty buckets ramp linearly between neighbours
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

Empty buckets before the first or after the last real value stay null (nothing to interpolate from).

Multiple series are gap-filled independently — include the partition key in both `SELECT` and `GROUP BY`:

```sql
SELECT series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'), avg(value)
FROM   metrics WHERE series IN ('cpu', 'mem')
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

Notes: use a fixed-width bucket (no month component), do not alias the bucket column, and avoid paging across the
bucket range. A query is rejected if its range/width would materialize more than 1,000,000 buckets.

---

## 3. First / last value — `first`, `last`

### 3.1 Series open/close

```sql
SELECT first(value, ts) AS day_open,
       last(value, ts)  AS day_close
FROM   metrics
WHERE  series = 'cpu';
```

### 3.2 OHLC (open/high/low/close) candles per hour

```sql
SELECT time_bucket(1h, ts) AS bucket,
       first(value, ts) AS open,
       max(value)       AS high,
       min(value)       AS low,
       last(value, ts)  AS close
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket(1h, ts);
```

`first`/`last` are ordered by the timestamp argument, not by insertion order, so out-of-order
writes still produce the correct open/close.

---

## 4. Change over time — `delta`, `rate`, `derivative`

### 4.1 Gauge change and slope per bucket

```sql
SELECT time_bucket(1h, ts) AS bucket,
       delta(value, ts)      AS change,
       rate(value, ts)       AS per_second,
       derivative(value, ts) AS slope_per_second
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket(1h, ts);
```

- `delta` = last sample − first sample in the bucket.
- `rate` = `delta` ÷ elapsed seconds (endpoint rate).
- `derivative` = least-squares regression slope; differs from `rate` when the series is non-linear
  (it uses every point, not just the endpoints).

### 4.2 Counter throughput (e.g. requests served)

```sql
CREATE TABLE counters (
    series text, ts timestamp, total counter,
    PRIMARY KEY (series, ts)
);                                     -- counters cannot use TWCS/TTL; shown for the query shape

-- Requests per second over each minute. counter_rate compensates for counter resets (use it, not rate(),
-- for monotonic counters); rate() would treat a reset as a large negative step.
SELECT time_bucket(1m, ts) AS minute, counter_rate(total, ts) AS req_per_sec
FROM   counters
WHERE  series = 'api.requests'
GROUP  BY series, time_bucket(1m, ts);
```

### 4.3 Whole-range rate using a bigint epoch column

`delta`/`rate`/`derivative` accept a `bigint` timestamp (epoch millis) as well as `timestamp`:

```sql
SELECT rate(value, ts_millis) AS per_second
FROM   metrics_epoch
WHERE  series = 'cpu';
```

---

## 5. Percentiles & SLOs — `percentile`

```sql
-- p50 / p95 / p99 latency per minute
SELECT time_bucket(1m, ts) AS minute,
       percentile(latency_ms, 0.50) AS p50,
       percentile(latency_ms, 0.95) AS p95,
       percentile(latency_ms, 0.99) AS p99
FROM   latencies
WHERE  service = 'checkout'
GROUP  BY service, time_bucket(1m, ts);

-- Median across the whole series
SELECT percentile(value, 0.5) AS median FROM metrics WHERE series = 'cpu';
```

`percentile` is an exact continuous percentile (linear interpolation between adjacent values); `q`
must be between 0 and 1. It keeps the group's values in memory, so it suits bounded downsampled
buckets rather than unbounded scans.

---

## 5b. Distribution, spread & cardinality

```sql
-- Time-weighted average: weights each value by how long it was in effect
-- (use this, not avg(), when samples are irregularly spaced).
SELECT time_bucket(1h, ts) AS bucket, time_weighted_average(value, ts) AS twa
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- Spread of values per bucket
SELECT time_bucket(1h, ts) AS bucket, variance(value) AS var, stddev(value) AS sd
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- Histogram of latencies into 10 equal-width buckets over [0, 1000) ms.
-- Result is a list: [ <0ms, bucket1, .. bucket10, >=1000ms ].
SELECT histogram(latency_ms, 0, 1000, 10) AS dist
FROM   latencies WHERE service = 'checkout';

-- Approximate number of distinct client IPs per minute (HyperLogLog; bounded memory)
SELECT time_bucket(1m, ts) AS minute, approx_count_distinct(client_ip) AS unique_ips
FROM   requests WHERE service = 'api' GROUP BY service, time_bucket(1m, ts);
```

---

## 6. Dashboard query — everything together

A single hourly summary combining bucketing, OHLC, change, and percentiles:

```sql
SELECT time_bucket(1h, ts)     AS bucket,
       count(value)            AS samples,
       first(value, ts)        AS open,
       last(value, ts)         AS close,
       min(value)              AS low,
       max(value)              AS high,
       avg(value)              AS mean,
       delta(value, ts)        AS change,
       rate(value, ts)         AS per_second,
       percentile(value, 0.95) AS p95
FROM   metrics
WHERE  series = 'cpu'
  AND  ts >= '2024-01-01 00:00:00+0000'
  AND  ts <  '2024-01-02 00:00:00+0000'
GROUP  BY series, time_bucket(1h, ts);
```

---

## 7. Tips

- Always restrict the partition (`WHERE series = ...`) and a time range; time-series scans are
  cheapest within a single partition ordered by `ts`.
- Keep partitions bounded: for high-rate series, include a coarse time bucket in the partition key,
  e.g. `PRIMARY KEY ((series, day), ts)`, to avoid unbounded partitions.
- Use `default_time_to_live` aligned with the TWCS window so expired windows are dropped efficiently.
- `time_bucket(interval, ts)` must appear as the trailing `GROUP BY` element (after the partition key
  columns) for the grouping to be pushed into the read path.
