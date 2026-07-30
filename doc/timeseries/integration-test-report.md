# time-series CQL integration test

image `cassandra-timeseries:6.0.0-it` · runtime `docker` · 2026-07-30 19:47 UTC

**41 passed, 0 failed** — assertions run against a live node booted from the image.

> Times are the full cqlsh round trip for one query; a trivial query on this host costs
> 810 ms of that (process startup + connect), so subtract roughly that much to read the CQL
> execution time. Server-side timings on a 100M-row data set are in the scale-test report.

## schema & fixture data

## time_bucket / downsampling

### ✅ time_bucket(1h) hourly avg = 20 / 60 — `770 ms`

```sql
SELECT time_bucket(1h, ts), avg(value) FROM it.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);
```

```
 system.time_bucket(1h, ts)      | system.avg(value)
---------------------------------+-------------------
 2024-01-01 09:00:00.000000+0000 |                20
 2024-01-01 10:00:00.000000+0000 |                60
(2 rows)
```

### ✅ time_bucket(1h) second bucket = 60 — `747 ms`

```sql
SELECT time_bucket(1h, ts), avg(value) FROM it.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);
```

```
 system.time_bucket(1h, ts)      | system.avg(value)
---------------------------------+-------------------
 2024-01-01 09:00:00.000000+0000 |                20
 2024-01-01 10:00:00.000000+0000 |                60
(2 rows)
```

### ✅ time_bucket scalar assigns bucket start — `757 ms`

```sql
SELECT ts, time_bucket(1h, ts) AS b FROM it.metrics WHERE series='cpu' LIMIT 1;
```

```
 ts                              | b
---------------------------------+---------------------------------
 2024-01-01 09:05:00.000000+0000 | 2024-01-01 09:00:00.000000+0000
(1 rows)
```

## first / last / delta / rate / derivative

### ✅ first(value, ts) = 10 — `767 ms`

```sql
SELECT first(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.first(value, ts)
-------------------------
                      10
(1 rows)
```

### ✅ last(value, ts) = 70 — `761 ms`

```sql
SELECT last(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.last(value, ts)
------------------------
                     70
(1 rows)
```

### ✅ delta(value, ts) = 60 — `752 ms`

```sql
SELECT delta(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.delta(value, ts)
-------------------------
                      60
(1 rows)
```

### ✅ rate(value, ts) = 60/6000s = 0.01 — `747 ms`

```sql
SELECT rate(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.rate(value, ts)
------------------------
                   0.01
(1 rows)
```

### ✅ derivative(value, ts) ~ 0.00977 (least squares, != rate) — `773 ms`

```sql
SELECT derivative(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.derivative(value, ts)
------------------------------
                      0.00977
(1 rows)
```

## counters (reset aware)

### ✅ counter_delta detects the reset (= 250) — `746 ms`

```sql
SELECT counter_delta(total, ts) FROM it.counters WHERE series='api';
```

```
 system.counter_delta(total, ts)
---------------------------------
                             250
(1 rows)
```

### ✅ counter_rate = 250/120s ~ 2.083 — `729 ms`

```sql
SELECT counter_rate(total, ts) FROM it.counters WHERE series='api';
```

```
 system.counter_rate(total, ts)
--------------------------------
                        2.08333
(1 rows)
```

## percentile / spread / distribution

### ✅ percentile(value, 0.5) = 40 — `732 ms`

```sql
SELECT percentile(value, 0.5) FROM it.metrics WHERE series='cpu';
```

```
 system.percentile(value, 0.5)
-------------------------------
                            40
(1 rows)
```

### ✅ percentile(value, 0.95) = 67 — `756 ms`

```sql
SELECT percentile(value, 0.95) FROM it.metrics WHERE series='cpu';
```

```
 system.percentile(value, 0.95)
--------------------------------
                             67
(1 rows)
```

### ✅ variance(value) = 666.66 (sample) — `738 ms`

```sql
SELECT variance(value) FROM it.metrics WHERE series='cpu';
```

```
 system.variance(value)
------------------------
              666.66667
(1 rows)
```

### ✅ stddev(value) = 25.81 — `740 ms`

```sql
SELECT stddev(value) FROM it.metrics WHERE series='cpu';
```

```
 system.stddev(value)
----------------------
             25.81989
(1 rows)
```

### ✅ approx_count_distinct = 4 — `731 ms`

```sql
SELECT approx_count_distinct(value) FROM it.metrics WHERE series='cpu';
```

```
 system.approx_count_distinct(value)
-------------------------------------
                                   4
(1 rows)
```

### ✅ histogram returns nbuckets+2 entries — `738 ms`

```sql
SELECT histogram(value, 0, 100, 10) FROM it.metrics WHERE series='cpu';
```

```
 system.histogram(value, 0, 100, 10)
--------------------------------------
 [0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0]
(1 rows)
```

## integral / time-weighted average

### ✅ integral(value, ts) = 240000 value-seconds — `744 ms`

```sql
SELECT integral(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.integral(value, ts)
----------------------------
                    2.4e+05
(1 rows)
```

### ✅ time_weighted_average = 240000/6000 = 40 — `741 ms`

```sql
SELECT time_weighted_average(value, ts) FROM it.metrics WHERE series='cpu';
```

```
 system.time_weighted_average(value, ts)
-----------------------------------------
                                      40
(1 rows)
```

## two-variable statistics / regression (y = 2x + 1)

### ✅ regr_slope(y, x) = 2 — `727 ms`

```sql
SELECT regr_slope(y, x) FROM it.xy WHERE k='r';
```

```
 system.regr_slope(y, x)
-------------------------
                       2
(1 rows)
```

### ✅ regr_intercept(y, x) = 1 — `720 ms`

```sql
SELECT regr_intercept(y, x) FROM it.xy WHERE k='r';
```

```
 system.regr_intercept(y, x)
-----------------------------
                           1
(1 rows)
```

### ✅ regr_r2(y, x) = 1 — `723 ms`

```sql
SELECT regr_r2(y, x) FROM it.xy WHERE k='r';
```

```
 system.regr_r2(y, x)
----------------------
                    1
(1 rows)
```

### ✅ corr(y, x) = 1 — `726 ms`

```sql
SELECT corr(y, x) FROM it.xy WHERE k='r';
```

```
 system.corr(y, x)
-------------------
                 1
(1 rows)
```

### ✅ covar_pop(y, x) = 2.5 — `759 ms`

```sql
SELECT covar_pop(y, x) FROM it.xy WHERE k='r';
```

```
 system.covar_pop(y, x)
------------------------
                    2.5
(1 rows)
```

### ✅ covar_samp(y, x) = 3.33 — `731 ms`

```sql
SELECT covar_samp(y, x) FROM it.xy WHERE k='r';
```

```
 system.covar_samp(y, x)
-------------------------
                 3.33333
(1 rows)
```

## gap-fill: time_bucket_gapfill + locf + interpolate

### ✅ gapfill materialises every bucket (6 rows) — `766 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), avg(value) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.avg(value)
--------------------------------------------------------------------------------------------+-------------------
                                                            2024-01-01 08:00:00.000000+0000 |              null
                                                            2024-01-01 09:00:00.000000+0000 |                60
                                                            2024-01-01 10:00:00.000000+0000 |              null
                                                            2024-01-01 11:00:00.000000+0000 |              null
                                                            2024-01-01 12:00:00.000000+0000 |               120
                                                            2024-01-01 13:00:00.000000+0000 |              null
(6 rows)
```

### ✅ gapfill empty bucket is null by default — `744 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), avg(value) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.avg(value)
--------------------------------------------------------------------------------------------+-------------------
                                                            2024-01-01 08:00:00.000000+0000 |              null
                                                            2024-01-01 09:00:00.000000+0000 |                60
                                                            2024-01-01 10:00:00.000000+0000 |              null
                                                            2024-01-01 11:00:00.000000+0000 |              null
                                                            2024-01-01 12:00:00.000000+0000 |               120
                                                            2024-01-01 13:00:00.000000+0000 |              null
(6 rows)
```

### ✅ locf carries the previous bucket forward (10:00 -> 60) — `724 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), locf(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.locf(system.avg(value))
--------------------------------------------------------------------------------------------+--------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                           null
                                                            2024-01-01 09:00:00.000000+0000 |                             60
                                                            2024-01-01 10:00:00.000000+0000 |                             60
                                                            2024-01-01 11:00:00.000000+0000 |                             60
                                                            2024-01-01 12:00:00.000000+0000 |                            120
                                                            2024-01-01 13:00:00.000000+0000 |                            120
(6 rows)
```

### ✅ locf leaves buckets before the first value null (08:00) — `747 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), locf(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.locf(system.avg(value))
--------------------------------------------------------------------------------------------+--------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                           null
                                                            2024-01-01 09:00:00.000000+0000 |                             60
                                                            2024-01-01 10:00:00.000000+0000 |                             60
                                                            2024-01-01 11:00:00.000000+0000 |                             60
                                                            2024-01-01 12:00:00.000000+0000 |                            120
                                                            2024-01-01 13:00:00.000000+0000 |                            120
(6 rows)
```

### ✅ interpolate ramps 60 -> 120 linearly (10:00 -> 80) — `729 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.interpolate(system.avg(value))
--------------------------------------------------------------------------------------------+---------------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                                  null
                                                            2024-01-01 09:00:00.000000+0000 |                                    60
                                                            2024-01-01 10:00:00.000000+0000 |                                    80
                                                            2024-01-01 11:00:00.000000+0000 |                                   100
                                                            2024-01-01 12:00:00.000000+0000 |                                   120
                                                            2024-01-01 13:00:00.000000+0000 |                                  null
(6 rows)
```

### ✅ interpolate ramps 60 -> 120 linearly (11:00 -> 100) — `728 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.interpolate(system.avg(value))
--------------------------------------------------------------------------------------------+---------------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                                  null
                                                            2024-01-01 09:00:00.000000+0000 |                                    60
                                                            2024-01-01 10:00:00.000000+0000 |                                    80
                                                            2024-01-01 11:00:00.000000+0000 |                                   100
                                                            2024-01-01 12:00:00.000000+0000 |                                   120
                                                            2024-01-01 13:00:00.000000+0000 |                                  null
(6 rows)
```

### ✅ interpolate leaves the trailing bucket null (13:00) — `744 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000'), interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000');
```

```
 system.time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000') | system.interpolate(system.avg(value))
--------------------------------------------------------------------------------------------+---------------------------------------
                                                            2024-01-01 08:00:00.000000+0000 |                                  null
                                                            2024-01-01 09:00:00.000000+0000 |                                    60
                                                            2024-01-01 10:00:00.000000+0000 |                                    80
                                                            2024-01-01 11:00:00.000000+0000 |                                   100
                                                            2024-01-01 12:00:00.000000+0000 |                                   120
                                                            2024-01-01 13:00:00.000000+0000 |                                  null
(6 rows)
```

## dashboard query (all together)

### ✅ combined OHLC + change + p95 query runs — `742 ms`

```sql
SELECT time_bucket(1h, ts) AS bucket, count(value) AS samples, first(value, ts) AS open, last(value, ts) AS close, min(value) AS low, max(value) AS high, avg(value) AS mean, delta(value, ts) AS change, rate(value, ts) AS per_second, percentile(value, 0.95) AS p95 FROM it.metrics WHERE series='cpu' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000' GROUP BY series, time_bucket(1h, ts);
```

```
 bucket                          | samples | open | close | low | high | mean | change | per_second | p95
---------------------------------+---------+------+-------+-----+------+------+--------+------------+-----
 2024-01-01 09:00:00.000000+0000 |       2 |   10 |    30 |  10 |   30 |   20 |     20 |   0.011111 |  29
 2024-01-01 10:00:00.000000+0000 |       2 |   50 |    70 |  50 |   70 |   60 |     20 |   0.011111 |  69
(2 rows)
```

## full-text search - SAI LIKE with index_analyzer

### ✅ LIKE '%timeout%' within partition + time range — `785 ms`

```sql
SELECT ts, msg FROM it.logs WHERE device='pump-01' AND ts >= '2024-01-01 09:00:00+0000' AND ts < '2024-01-01 10:00:00+0000' AND msg LIKE '%timeout%';
```

```
 ts                              | msg
---------------------------------+---------------------------------
 2024-01-01 09:00:00.000000+0000 | connection timeout on port 9042
(1 rows)
```

### ✅ LIKE mid-word fragment '%imeou%' (true substring) — `741 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%imeou%';
```

```
 msg
---------------------------------
 connection timeout on port 9042
(1 rows)
```

### ✅ LIKE korean fragment '%정지%' — `723 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%정지%';
```

```
 msg
----------------
 펌프 정지 알림
(1 rows)
```

### ✅ LIKE korean fragment crossing a space '%프 정%' — `712 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%프 정%';
```

```
 msg
----------------
 펌프 정지 알림
(1 rows)
```

### ✅ LIKE excludes non-matching rows (1 row only) — `744 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%refused%';
```

```
 msg
--------------------
 connection refused
(1 rows)
```

### ✅ LIKE prefix 'connection%' — `745 ms`

```sql
SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg LIKE 'connection%';
```

```
 count
-------
     2
(1 rows)
```

### ✅ LIKE suffix '%9042' — `717 ms`

```sql
SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%9042';
```

```
 msg
---------------------------------
 connection timeout on port 9042
(1 rows)
```

### ✅ '=' keeps exact whole-value semantics on the analyzed column — `727 ms`

```sql
SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg = 'connection refused';
```

```
 count
-------
     1
(1 rows)
```

### ✅ LIKE combines with time_bucket aggregation — `768 ms`

```sql
SELECT time_bucket(1h, ts), count(*) FROM it.logs WHERE device='pump-01' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000' AND msg LIKE '%connection%' GROUP BY device, time_bucket(1h, ts);
```

```
 system.time_bucket(1h, ts)      | count
---------------------------------+-------
 2024-01-01 09:00:00.000000+0000 |     2
(1 rows)
```
