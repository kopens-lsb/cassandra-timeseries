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

Every example below runs against `tm_tag_point`, the real industrial tag table this fork is built for:
one partition per tag, clustered by time, **newest first**. Compaction uses Cassandra 6.0's
`UnifiedCompactionStrategy` (UCS): tiered scaling parameters suit append-only ingest, and fully expired
SSTables are dropped whole on the expiry check interval. (For the time-series-specific alternative, see
`TimeSeriesCompactionStrategy` in the [operations tuning guide](operations-tuning.md).)

```sql
CREATE KEYSPACE IF NOT EXISTS pp
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE pp;

CREATE TABLE tm_tag_point (
    tag_id     text,                              -- partition key: one partition per tag
    timestamp  timestamp,
    area_id    text static, asset_id text static, line_id text static,
    opc_id     text static, site_id  text static, tag_name text static,
    type       text static,                       -- 'boolean' | 'long' | 'double' | ...
    attribute  frozen<map<text,text>>,
    error_code int,
    latency    int,                               -- acquisition latency, always present
    quality    int,
    value      text,                              -- string copy of the reading
    value_boolean boolean,                        -- populated when type = 'boolean'
    value_numeric double,                         -- populated when type is numeric
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
   AND compaction = {'class': 'UnifiedCompactionStrategy',
                     'scaling_parameters': 'T4',                        -- 4-way tiered (write-optimised)
                     'target_sstable_size': '1GiB',
                     'expired_sstable_check_frequency_seconds': 600}
   AND default_time_to_live = 5356800;   -- 62 days

-- Statics are written once per tag, not per sample.
INSERT INTO tm_tag_point (tag_id, area_id, asset_id, line_id, opc_id, site_id, tag_name, type)
     VALUES ('TAG-001', 'A1', 'AS1', 'L1', 'OPC1', 'S1', 'boiler.temp', 'double');

INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 09:05:00+0000', {}, 0,  17, 192, '20.1', 20.1);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 09:35:00+0000', {}, 0, 431, 192, '20.8', 20.8);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 10:15:00+0000', {}, 0,   3, 192, '21.4', 21.4);
INSERT INTO tm_tag_point (tag_id, timestamp, attribute, error_code, latency, quality, value, value_numeric)
     VALUES ('TAG-001', '2024-01-01 10:45:00+0000', {}, 0, 902, 192, '22.0', 22.0);
```

### 1.1 Which column can you aggregate?

**`value` is `text`, so it cannot be aggregated numerically.** `avg(value)`, `percentile(value, 0.95)`,
`delta(value, timestamp)` and every other numeric aggregate reject it outright — they require a numeric
type (`tinyint`/`smallint`/`int`/`bigint`/`varint`/`float`/`double`/`decimal`/`counter`). Worse than the
rejection is what is *accepted*: `min(value)`/`max(value)`/`count(value)` all work on `text` and compare
**lexicographically**, so `max(value)` over `'9.1'` and `'20.76'` returns `'9.1'`. That is the single
most common trap in this schema.

The columns you can actually do arithmetic on are:

| Column | Type | Availability |
| --- | --- | --- |
| `latency` | `int` | always present — the safe default for examples and smoke tests |
| `value_numeric` | `double` | only for tags whose static `type` is numeric; `null` otherwise |
| `error_code`, `quality` | `int` | present but constant in practice (0 and 192) |

`first`/`last`/`approx_count_distinct` are the exceptions — they accept **any** type, so
`first(value, timestamp)` on the `text` column is valid and returns `text` (see §3).

Which value column carries the reading is fixed per tag by the static `type` column, so a query that
targets one tag never has to cope with a mixture.

---

## 2. Bucketing & downsampling — `time_bucket`

### 2.1 Assign each row to its bucket (scalar use)

```sql
SELECT timestamp, time_bucket(1h, timestamp) AS bucket, latency, value
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001';
```

The duration is a bare CQL duration literal (`1h`), **not** a quoted string — `time_bucket('1h', ts)`
fails to resolve. The optional origin argument, by contrast, is a `timestamp` and is quoted (§2.3).

### 2.2 Downsample to fixed intervals with `GROUP BY`

```sql
-- Hourly average / min / max / count of acquisition latency
SELECT time_bucket(1h, timestamp) AS bucket,
       avg(latency), min(latency), max(latency), count(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);

-- The reading itself, for a numerically-typed tag
SELECT time_bucket(1h, timestamp) AS bucket, avg(value_numeric)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);

-- Other intervals: 5 minutes, 1 day
SELECT time_bucket(5m, timestamp) AS bucket, avg(latency) FROM tm_tag_point
  WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(5m, timestamp);

SELECT time_bucket(1d, timestamp) AS bucket, avg(latency) FROM tm_tag_point
  WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(1d, timestamp);
```

Grouping works unchanged on a `DESC`-clustered table — buckets simply come back newest-first. `avg` of an
`int` column returns an `int` (it truncates), which is standard Cassandra behaviour; cast or use
`value_numeric` if you need the fraction.

### 2.3 Shifted buckets with an origin

```sql
-- Hourly buckets offset by 30 minutes: windows are [08:30, 09:30), [09:30, 10:30), ...
SELECT time_bucket(1h, timestamp, '2024-01-01 00:30:00+0000') AS bucket, avg(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp, '2024-01-01 00:30:00+0000');
```

### 2.4 Gap-filling empty buckets — `time_bucket_gapfill`

Plain `time_bucket` only emits buckets that have data. `time_bucket_gapfill` additionally materializes a row for
*every* bucket in `[start, finish)`, so dashboards get a continuous time axis (empty buckets carry null aggregates).

> **`ORDER BY timestamp ASC` is required on this table.** Gap-fill densifies assuming buckets arrive in
> ascending order, and nothing enforces it. On a `DESC`-clustered table the rows arrive newest-first and
> the fill is applied backwards — with **no error**. Adding `ORDER BY timestamp ASC` makes the read itself
> ascending (a `DESC` declaration plus an `ASC` request cancel out into a reversed slice filter), which is
> what gap-fill needs. This combination is pinned by a test (`TimeSeriesFctsTest`, gap-fill on a
> `DESC`-clustered table); see [gapfill-design.md §4](gapfill-design.md).

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(latency)
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

The `WHERE timestamp >= <start>` is not decoration: if any scanned row is **older** than the gap-fill
`start`, the query fails with *"The floor function starting time is greater than the provided time"*.

By default empty buckets carry null aggregates. Wrap a selected aggregate in `locf(...)` to instead carry the
previous non-empty bucket's value forward (last-observation-carried-forward):

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       locf(avg(latency))   -- empty buckets repeat the previous hour's average instead of null
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

`locf` is a no-op on real rows (returns its argument); it only affects synthesized empty buckets. Buckets before the
first real value remain null (nothing to carry yet).

Use `interpolate(...)` instead to linearly interpolate empty buckets between the surrounding non-empty values
(the result is a `double`):

```sql
SELECT time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       interpolate(avg(value_numeric))   -- empty buckets ramp linearly between neighbours
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

Empty buckets before the first or after the last real value stay null (nothing to interpolate from).

Multiple tags are gap-filled independently — include the partition key in both `SELECT` and `GROUP BY`:

```sql
SELECT tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(latency)
FROM   tm_tag_point WHERE tag_id IN ('TAG-001', 'TAG-002')
  AND  timestamp >= '2024-01-01 00:00:00+0000' AND timestamp < '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

The multi-tag form inherits the ascending requirement above, and `IN` + `ORDER BY` additionally applies a
post-query sort **across** partitions, so the output is ordered globally by bucket rather than tag-by-tag.
On this schema the one-tag-at-a-time form is the easier one to reason about.

Notes: use a fixed-width bucket (no month component); **do not alias** the bucket column or the
`locf(...)`/`interpolate(...)` selectors — they are located by matching the function name in the result
metadata, so an alias makes gap-fill silently do nothing; and avoid paging across the bucket range. A query
is rejected if its range/width would materialize more than 1,000,000 buckets.

---

## 3. First / last value — `first`, `last`

### 3.1 First/last reading of a tag

`first`/`last` accept **any** value type, so they work directly on the `text` `value` column and return
`text` — one of the few things you can do with it server-side:

```sql
SELECT first(value, timestamp) AS first_reading,   -- text
       last(value, timestamp)  AS last_reading
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001';
```

### 3.2 OHLC (open/high/low/close) candles per hour

Use the **numeric** column here. `max`/`min` on `text` would compare lexicographically:

```sql
SELECT time_bucket(1h, timestamp) AS bucket,
       first(value_numeric, timestamp) AS open,
       max(value_numeric)              AS high,
       min(value_numeric)              AS low,
       last(value_numeric, timestamp)  AS close
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

`first`/`last` are ordered by the timestamp argument, not by insertion order, so out-of-order
writes still produce the correct open/close — and on a `DESC`-clustered table they are still correct,
because the ordering comes from the argument rather than the row order.

---

## 4. Change over time — `delta`, `rate`, `derivative`

### 4.1 Gauge change and slope per bucket

```sql
SELECT time_bucket(1h, timestamp) AS bucket,
       delta(value_numeric, timestamp)      AS change,
       rate(value_numeric, timestamp)       AS per_second,
       derivative(value_numeric, timestamp) AS slope_per_second
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

- `delta` = last sample − first sample in the bucket.
- `rate` = `delta` ÷ elapsed seconds (endpoint rate).
- `derivative` = least-squares regression slope; differs from `rate` when the series is non-linear
  (it uses every point, not just the endpoints).

The second argument must be a `timestamp` or a `bigint` (epoch millis) column — an `int` or `timeuuid`
time column is rejected. Swap `value_numeric` for `latency` if the tag is not numerically typed.

### 4.2 Counter throughput with reset compensation

`counter_delta`/`counter_rate` need only a **numeric** column, not the CQL `counter` type: any monotonic
`int`/`bigint` gauge works, which is the form the tests cover. Use them instead of `rate()` whenever the
source can reset — `rate()` reads a reset as one large negative step.

```sql
CREATE TABLE tag_counters (
    tag_id text, timestamp timestamp, total bigint,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Events per second over each minute, tolerating counter resets.
SELECT time_bucket(1m, timestamp) AS minute, counter_rate(total, timestamp) AS per_sec
FROM   tag_counters
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1m, timestamp);
```

A separate table is used here because `tm_tag_point` has no monotonic counter column. Note that a table
using the CQL `counter` **type** cannot be tiered at all — the re-encoder deletes and re-inserts rows, and
a deleted counter can never be written again — so a `bigint` gauge is also the tiering-compatible choice.

---

## 5. Percentiles & SLOs — `percentile`

```sql
-- p50 / p95 / p99 acquisition latency per minute
SELECT time_bucket(1m, timestamp) AS minute,
       percentile(latency, 0.50) AS p50,
       percentile(latency, 0.95) AS p95,
       percentile(latency, 0.99) AS p99
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1m, timestamp);

-- Median reading across the whole tag
SELECT percentile(value_numeric, 0.5) AS median FROM tm_tag_point WHERE tag_id = 'TAG-001';
```

`percentile` takes an `int` column happily — `latency` is the natural subject here, and being always
present it is also the safest column to smoke-test a cluster with.

`percentile` is an exact continuous percentile (linear interpolation between adjacent values); `q`
must be between 0 and 1. It keeps the group's values in memory, so it suits bounded downsampled
buckets rather than unbounded scans.

---

## 5b. Distribution, spread & cardinality

```sql
-- Time-weighted average: weights each value by how long it was in effect
-- (use this, not avg(), when samples are irregularly spaced -- which industrial tags usually are).
SELECT time_bucket(1h, timestamp) AS bucket, time_weighted_average(value_numeric, timestamp) AS twa
FROM   tm_tag_point WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(1h, timestamp);

-- Area under the curve (value-seconds). E.g. power (W) -> energy (J).
SELECT time_bucket(1h, timestamp) AS bucket, integral(value_numeric, timestamp) AS area
FROM   tm_tag_point WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(1h, timestamp);

-- Spread of values per bucket
SELECT time_bucket(1h, timestamp) AS bucket, variance(latency) AS var, stddev(latency) AS sd
FROM   tm_tag_point WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(1h, timestamp);

-- Histogram of acquisition latency into 10 equal-width buckets over [0, 1000) ms.
-- Result is a list of nbuckets+2 entries: [ <0, bucket1, .. bucket10, >=1000 ].
SELECT histogram(latency, 0, 1000, 10) AS dist
FROM   tm_tag_point WHERE tag_id = 'TAG-001';

-- Approximate number of distinct readings per minute (HyperLogLog; bounded memory).
-- approx_count_distinct accepts any type, so it works on the text `value` column.
SELECT time_bucket(1m, timestamp) AS minute, approx_count_distinct(value) AS distinct_readings
FROM   tm_tag_point WHERE tag_id = 'TAG-001' GROUP BY tag_id, time_bucket(1m, timestamp);
```

---

## 6. Dashboard query — everything together

A single hourly summary combining bucketing, OHLC, change, and percentiles:

```sql
SELECT time_bucket(1h, timestamp)          AS bucket,
       count(latency)                      AS samples,
       first(value_numeric, timestamp)     AS open,
       last(value_numeric, timestamp)      AS close,
       min(value_numeric)                  AS low,
       max(value_numeric)                  AS high,
       avg(value_numeric)                  AS mean,
       delta(value_numeric, timestamp)     AS change,
       rate(value_numeric, timestamp)      AS per_second,
       percentile(latency, 0.95)           AS latency_p95
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2024-01-01 00:00:00+0000'
  AND  timestamp <  '2024-01-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

---

## 6b. Two-variable statistics & regression

`(y, x)` argument order — `y` is the dependent variable. Both must be numeric, so on this table the pair
is `value_numeric` and `latency`, which answers a real diagnostic question: does the reading move with
acquisition latency (i.e. is a slow OPC link distorting the measurement)?

```sql
SELECT time_bucket(1h, timestamp)                AS bucket,
       corr(value_numeric, latency)              AS r,
       covar_samp(value_numeric, latency)        AS cov,
       regr_slope(value_numeric, latency)        AS slope,
       regr_intercept(value_numeric, latency)    AS intercept,
       regr_r2(value_numeric, latency)           AS r_squared
FROM   tm_tag_point
WHERE  tag_id = 'TAG-001'
GROUP  BY tag_id, time_bucket(1h, timestamp);
```

---

## 7. Tips

- Always restrict the partition (`WHERE tag_id = ...`) and a time range; time-series scans are
  cheapest within a single partition ordered by `timestamp`.
- `CLUSTERING ORDER BY (timestamp DESC)` is the right default for this workload — reads are almost always
  "the most recent N of this tag". Only gap-fill needs the ascending form (§2.4).
- Aggregate `latency` or `value_numeric`, never `value` — it is `text`, so numeric aggregates reject it and
  `min`/`max` silently compare it lexicographically (§1.1).
- Keep partitions bounded: for high-rate tags, include a coarse time bucket in the partition key,
  e.g. `PRIMARY KEY ((tag_id, day), timestamp)`, to avoid unbounded partitions.
- Use UCS with tiered scaling parameters (e.g. `'T4'`) plus `default_time_to_live`; fully expired SSTables
  are then reclaimed whole on the `expired_sstable_check_frequency_seconds` interval.
- `time_bucket(interval, timestamp)` must appear as the trailing `GROUP BY` element (after the partition key
  columns) for the grouping to be pushed into the read path.
- Static columns (`site_id`, `tag_name`, `type`, …) are per-tag, not per-sample: they never appear in a
  `GROUP BY` over buckets, and they survive tiering untouched.
