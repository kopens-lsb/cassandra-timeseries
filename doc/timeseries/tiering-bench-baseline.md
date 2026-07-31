# time-series CQL scale test

image `cassandra-timeseries:6.0.0` · single node in a container · 2026-07-31 11:55 UTC

- **99,999,000** rows loaded
- **3,000** partitions (33,333 rows each)
- **0 s** load time (0 rows/s)

## Query times

| section | query | first result row | CQL time |
|---|---|---|---|
| single partition (33333 rows) | count(*) | `100000` | **329 ms** |
| single partition (33333 rows) | time_bucket 1h + avg/min/max | `2024-01-01 00:00:00 \| 55.17 \| 10.0006 \| 95.9995` | **524 ms** |
| single partition (33333 rows) | time_bucket 5m + avg | `2024-01-01 00:00:00 \| 64.5966` | **437 ms** |
| single partition (33333 rows) | first/last/delta/rate per hour | `2024-01-01 00:00:00 \| 50 \| 82.698 \| 32.698 \| 0.0090853` | **471 ms** |
| single partition (33333 rows) | derivative per hour | `2024-01-01 00:00:00 \| -0.0128558` | **413 ms** |
| single partition (33333 rows) | percentile p50/p95/p99 (whole partition) | `53.3253 \| 92.7757 \| 95.0416` | **415 ms** |
| single partition (33333 rows) | variance/stddev (whole partition) | `805.694 \| 28.3847` | **300 ms** |
| single partition (33333 rows) | histogram 20 buckets | `[0, 0, 0, 8627, 10363, 6234, 5173, 4609, 4284, 4093, 3977, 3` | **311 ms** |
| single partition (33333 rows) | approx_count_distinct | `99741` | **315 ms** |
| single partition (33333 rows) | integral + time_weighted_average | `5.31023e+06 \| 53.1029` | **336 ms** |
| gap-fill | gapfill 1h + locf over the full span | `2024-01-01 00:00:00 \| 55.17` | **410 ms** |
| gap-fill | gapfill 5m + interpolate over 6h | `2024-01-01 00:00:00 \| 64.5966` | **100 ms** |
| multi-partition | 10 series, hourly avg (333330 rows) | `sensor-000000 \| 2024-01-01 00:00:00 \| 55.17` | **4,086 ms** |
| multi-partition | 100 series, hourly avg (3333300 rows) | `sensor-000000 \| 2024-01-01 00:00:00 \| 55.17` | **39,866 ms** |
| multi-partition | 100 series, p95 per series | `sensor-000000 \| 92.7757` | **31,641 ms** |
| dashboard query | OHLC + change + p95 per hour | `2024-01-01 00:00:00 \| 3600 \| 50 \| 82.698 \| 10.0006 \| 95` | **512 ms** |
| full table scan (100000000 rows) | count(*) over the whole table | `100000000` | **237,457 ms** |

## Details

### single partition (33333 rows)

**count(*)** — `329 ms`

```sql
SELECT count(*) FROM scale.metrics WHERE series='sensor-000000';
```

```
count
-----
100000
(1 rows)
```

**time_bucket 1h + avg/min/max** — `524 ms`

```sql
SELECT time_bucket(1h, ts), avg(value), min(value), max(value) FROM scale.metrics WHERE series='sensor-000000' GROUP BY series, time_bucket(1h, ts);
```

```
system_time_bucket_1h__ts | system_avg_value | system_min_value | system_max_value
--------------------------+------------------+------------------+-----------------
2024-01-01 00:00:00 | 55.17 | 10.0006 | 95.9995
2024-01-01 01:00:00 | 57.8219 | 10.0006 | 96
2024-01-01 02:00:00 | 56.6966 | 10.0002 | 95.9997
2024-01-01 03:00:00 | 52.6745 | 10 | 95.9992
2024-01-01 04:00:00 | 48.9073 | 10.0002 | 95.9996
2024-01-01 05:00:00 | 48.3472 | 10.0006 | 95.9999
2024-01-01 06:00:00 | 51.4319 | 10.0001 | 96
2024-01-01 07:00:00 | 55.742 | 10 | 95.9997
... (28 rows total)
```

**time_bucket 5m + avg** — `437 ms`

```sql
SELECT time_bucket(5m, ts), avg(value) FROM scale.metrics WHERE series='sensor-000000' GROUP BY series, time_bucket(5m, ts);
```

```
system_time_bucket_5m__ts | system_avg_value
--------------------------+-----------------
2024-01-01 00:00:00 | 64.5966
2024-01-01 00:05:00 | 83.834
2024-01-01 00:10:00 | 92.2979
2024-01-01 00:15:00 | 87.0327
2024-01-01 00:20:00 | 69.8789
2024-01-01 00:25:00 | 46.83
2024-01-01 00:30:00 | 25.9388
2024-01-01 00:35:00 | 14.481
... (334 rows total)
```

**first/last/delta/rate per hour** — `471 ms`

```sql
SELECT time_bucket(1h, ts), first(value, ts), last(value, ts), delta(value, ts), rate(value, ts) FROM scale.metrics WHERE series='sensor-000000' GROUP BY series, time_bucket(1h, ts);
```

```
system_time_bucket_1h__ts | system_first_value__ts | system_last_value__ts | system_delta_value__ts | system_rate_value__ts
--------------------------+------------------------+-----------------------+------------------------+----------------------
2024-01-01 00:00:00 | 50 | 82.698 | 32.698 | 0.0090853
2024-01-01 01:00:00 | 83.7467 | 91.647 | 7.9003 | 0.00219514
2024-01-01 02:00:00 | 92.6263 | 70.3239 | -22.3024 | -0.00619682
2024-01-01 03:00:00 | 71.25 | 29.9977 | -41.2524 | -0.0114622
2024-01-01 04:00:00 | 30.9284 | 12.3392 | -18.5893 | -0.00516512
2024-01-01 05:00:00 | 13.3288 | 25.7469 | 12.418 | 0.00345041
2024-01-01 06:00:00 | 26.8036 | 61.2852 | 34.4816 | 0.00958089
2024-01-01 07:00:00 | 55.3645 | 85.6836 | 30.3191 | 0.00842432
... (28 rows total)
```

**derivative per hour** — `413 ms`

```sql
SELECT time_bucket(1h, ts), derivative(value, ts) FROM scale.metrics WHERE series='sensor-000000' GROUP BY series, time_bucket(1h, ts);
```

```
system_time_bucket_1h__ts | system_derivative_value__ts
--------------------------+----------------------------
2024-01-01 00:00:00 | -0.0128558
2024-01-01 01:00:00 | -0.00280025
2024-01-01 02:00:00 | 0.00945131
2024-01-01 03:00:00 | 0.0142998
2024-01-01 04:00:00 | 0.00794522
2024-01-01 05:00:00 | -0.00463271
2024-01-01 06:00:00 | -0.0135792
2024-01-01 07:00:00 | -0.0118909
... (28 rows total)
```

**percentile p50/p95/p99 (whole partition)** — `415 ms`

```sql
SELECT percentile(value, 0.5), percentile(value, 0.95), percentile(value, 0.99) FROM scale.metrics WHERE series='sensor-000000';
```

```
system_percentile_value__0_5 | system_percentile_value__0_95 | system_percentile_value__0_99
-----------------------------+-------------------------------+------------------------------
53.3253 | 92.7757 | 95.0416
(1 rows)
```

**variance/stddev (whole partition)** — `300 ms`

```sql
SELECT variance(value), stddev(value) FROM scale.metrics WHERE series='sensor-000000';
```

```
system_variance_value | system_stddev_value
----------------------+--------------------
805.694 | 28.3847
(1 rows)
```

**histogram 20 buckets** — `311 ms`

```sql
SELECT histogram(value, 0, 100, 20) FROM scale.metrics WHERE series='sensor-000000';
```

```
system_histogram_value__0__100__20
----------------------------------
[0, 0, 0, 8627, 10363, 6234, 5173, 4609, 4284, 4093, 3977, 3975, 4043, 4119, 4309, 4609, 5090, 6057, 9277, 10137, 1024, 0]
(1 rows)
```

**approx_count_distinct** — `315 ms`

```sql
SELECT approx_count_distinct(value) FROM scale.metrics WHERE series='sensor-000000';
```

```
system_approx_count_distinct_value
----------------------------------
99741
(1 rows)
```

**integral + time_weighted_average** — `336 ms`

```sql
SELECT integral(value, ts), time_weighted_average(value, ts) FROM scale.metrics WHERE series='sensor-000000';
```

```
system_integral_value__ts | system_time_weighted_average_value__ts
--------------------------+---------------------------------------
5.31023e+06 | 53.1029
(1 rows)
```

### gap-fill

**gapfill 1h + locf over the full span** — `410 ms`

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-01 09:15:33+0000'), locf(avg(value)) FROM scale.metrics WHERE series='sensor-000000' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-01 09:15:33+0000');
```

```
system_time_bucket_gapfill_1h__ts___2024_01_01_00_00_00_0000____2024_01_01_09_15_33_0000 | system_locf_system_avg_value
-----------------------------------------------------------------------------------------+-----------------------------
2024-01-01 00:00:00 | 55.17
2024-01-01 01:00:00 | 57.8219
2024-01-01 02:00:00 | 56.6966
2024-01-01 03:00:00 | 52.6745
2024-01-01 04:00:00 | 48.9073
2024-01-01 05:00:00 | 48.3472
2024-01-01 06:00:00 | 51.4319
2024-01-01 07:00:00 | 55.742
... (28 rows total)
```

**gapfill 5m + interpolate over 6h** — `100 ms`

```sql
SELECT time_bucket_gapfill(5m, ts, '2024-01-01 00:00:00+0000', '2024-01-01 06:00:00+0000'), interpolate(avg(value)) FROM scale.metrics WHERE series='sensor-000000' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-01 06:00:00+0000' GROUP BY series, time_bucket_gapfill(5m, ts, '2024-01-01 00:00:00+0000', '2024-01-01 06:00:00+0000');
```

```
system_time_bucket_gapfill_5m__ts___2024_01_01_00_00_00_0000____2024_01_01_06_00_00_0000 | system_interpolate_system_avg_value
-----------------------------------------------------------------------------------------+------------------------------------
2024-01-01 00:00:00 | 64.5966
2024-01-01 00:05:00 | 83.834
2024-01-01 00:10:00 | 92.2979
2024-01-01 00:15:00 | 87.0327
2024-01-01 00:20:00 | 69.8789
2024-01-01 00:25:00 | 46.83
2024-01-01 00:30:00 | 25.9388
2024-01-01 00:35:00 | 14.481
... (72 rows total)
```

### multi-partition

**10 series, hourly avg (333330 rows)** — `4,086 ms`

```sql
SELECT series, time_bucket(1h, ts), avg(value) FROM scale.metrics WHERE series IN ('sensor-000000', 'sensor-000001', 'sensor-000002', 'sensor-000003', 'sensor-000004', 'sensor-000005', 'sensor-000006', 'sensor-000007', 'sensor-000008', 'sensor-000009') GROUP BY series, time_bucket(1h, ts);
```

```
series | system_time_bucket_1h__ts | system_avg_value
-------+---------------------------+-----------------
sensor-000000 | 2024-01-01 00:00:00 | 55.17
sensor-000000 | 2024-01-01 01:00:00 | 57.8219
sensor-000000 | 2024-01-01 02:00:00 | 56.6966
sensor-000000 | 2024-01-01 03:00:00 | 52.6745
sensor-000000 | 2024-01-01 04:00:00 | 48.9073
sensor-000000 | 2024-01-01 05:00:00 | 48.3472
sensor-000000 | 2024-01-01 06:00:00 | 51.4319
sensor-000000 | 2024-01-01 07:00:00 | 55.742
... (280 rows total)
```

**100 series, hourly avg (3333300 rows)** — `39,866 ms`

```sql
SELECT series, time_bucket(1h, ts), avg(value) FROM scale.metrics WHERE series IN ('sensor-000000', 'sensor-000001', 'sensor-000002', 'sensor-000003', 'sensor-000004', 'sensor-000005', 'sensor-000006', 'sensor-000007', 'sensor-000008', 'sensor-000009', 'sensor-000010', 'sensor-000011', 'sensor-000012', 'sensor-000013', 'sensor-000014', 'sensor-000015', 'sensor-000016', 'sensor-000017', 'sensor-000018', 'sensor-000019', 'sensor-000020', 'sensor-000021', 'sensor-000022', 'sensor-000023', 'sensor-000024', 'sensor-000025', 'sensor-000026', 'sensor-000027', 'sensor-000028', 'sensor-000029', 'sensor-000030', 'sensor-000031', 'sensor-000032', 'sensor-000033', 'sensor-000034', 'sensor-000035', 'sensor-000036', 'sensor-000037', 'sensor-000038', 'sensor-000039', 'sensor-000040', 'sensor-000041', 'sensor-000042', 'sensor-000043', 'sensor-000044', 'sensor-000045', 'sensor-000046', 'sensor-000047', 'sensor-000048', 'sensor-000049', 'sensor-000050', 'sensor-000051', 'sensor-000052', 'sensor-000053', 'sensor-000054', 'sensor-000055', 'sensor-000056', 'sensor-000057', 'sensor-000058', 'sensor-000059', 'sensor-000060', 'sensor-000061', 'sensor-000062', 'sensor-000063', 'sensor-000064', 'sensor-000065', 'sensor-000066', 'sensor-000067', 'sensor-000068', 'sensor-000069', 'sensor-000070', 'sensor-000071', 'sensor-000072', 'sensor-000073', 'sensor-000074', 'sensor-000075', 'sensor-000076', 'sensor-000077', 'sensor-000078', 'sensor-000079', 'sensor-000080', 'sensor-000081', 'sensor-000082', 'sensor-000083', 'sensor-000084', 'sensor-000085', 'sensor-000086', 'sensor-000087', 'sensor-000088', 'sensor-000089', 'sensor-000090', 'sensor-000091', 'sensor-000092', 'sensor-000093', 'sensor-000094', 'sensor-000095', 'sensor-000096', 'sensor-000097', 'sensor-000098', 'sensor-000099') GROUP BY series, time_bucket(1h, ts);
```

```
series | system_time_bucket_1h__ts | system_avg_value
-------+---------------------------+-----------------
sensor-000000 | 2024-01-01 00:00:00 | 55.17
sensor-000000 | 2024-01-01 01:00:00 | 57.8219
sensor-000000 | 2024-01-01 02:00:00 | 56.6966
sensor-000000 | 2024-01-01 03:00:00 | 52.6745
sensor-000000 | 2024-01-01 04:00:00 | 48.9073
sensor-000000 | 2024-01-01 05:00:00 | 48.3472
sensor-000000 | 2024-01-01 06:00:00 | 51.4319
sensor-000000 | 2024-01-01 07:00:00 | 55.742
... (2800 rows total)
```

**100 series, p95 per series** — `31,641 ms`

```sql
SELECT series, percentile(value, 0.95) FROM scale.metrics WHERE series IN ('sensor-000000', 'sensor-000001', 'sensor-000002', 'sensor-000003', 'sensor-000004', 'sensor-000005', 'sensor-000006', 'sensor-000007', 'sensor-000008', 'sensor-000009', 'sensor-000010', 'sensor-000011', 'sensor-000012', 'sensor-000013', 'sensor-000014', 'sensor-000015', 'sensor-000016', 'sensor-000017', 'sensor-000018', 'sensor-000019', 'sensor-000020', 'sensor-000021', 'sensor-000022', 'sensor-000023', 'sensor-000024', 'sensor-000025', 'sensor-000026', 'sensor-000027', 'sensor-000028', 'sensor-000029', 'sensor-000030', 'sensor-000031', 'sensor-000032', 'sensor-000033', 'sensor-000034', 'sensor-000035', 'sensor-000036', 'sensor-000037', 'sensor-000038', 'sensor-000039', 'sensor-000040', 'sensor-000041', 'sensor-000042', 'sensor-000043', 'sensor-000044', 'sensor-000045', 'sensor-000046', 'sensor-000047', 'sensor-000048', 'sensor-000049', 'sensor-000050', 'sensor-000051', 'sensor-000052', 'sensor-000053', 'sensor-000054', 'sensor-000055', 'sensor-000056', 'sensor-000057', 'sensor-000058', 'sensor-000059', 'sensor-000060', 'sensor-000061', 'sensor-000062', 'sensor-000063', 'sensor-000064', 'sensor-000065', 'sensor-000066', 'sensor-000067', 'sensor-000068', 'sensor-000069', 'sensor-000070', 'sensor-000071', 'sensor-000072', 'sensor-000073', 'sensor-000074', 'sensor-000075', 'sensor-000076', 'sensor-000077', 'sensor-000078', 'sensor-000079', 'sensor-000080', 'sensor-000081', 'sensor-000082', 'sensor-000083', 'sensor-000084', 'sensor-000085', 'sensor-000086', 'sensor-000087', 'sensor-000088', 'sensor-000089', 'sensor-000090', 'sensor-000091', 'sensor-000092', 'sensor-000093', 'sensor-000094', 'sensor-000095', 'sensor-000096', 'sensor-000097', 'sensor-000098', 'sensor-000099') GROUP BY series;
```

```
series | system_percentile_value__0_95
-------+------------------------------
sensor-000000 | 92.7757
sensor-000001 | 92.7757
sensor-000002 | 92.7757
sensor-000003 | 92.7757
sensor-000004 | 92.7757
sensor-000005 | 92.7757
sensor-000006 | 92.7757
sensor-000007 | 92.7757
... (100 rows total)
```

### dashboard query

**OHLC + change + p95 per hour** — `512 ms`

```sql
SELECT time_bucket(1h, ts) AS bucket, count(value) AS samples, first(value, ts) AS open, last(value, ts) AS close, min(value) AS low, max(value) AS high, avg(value) AS mean, delta(value, ts) AS change, rate(value, ts) AS per_second, percentile(value, 0.95) AS p95 FROM scale.metrics WHERE series='sensor-000000' GROUP BY series, time_bucket(1h, ts);
```

```
bucket | samples | open | close | low | high | mean | change | per_second | p95
-------+---------+------+-------+-----+------+------+--------+------------+----
2024-01-01 00:00:00 | 3600 | 50 | 82.698 | 10.0006 | 95.9995 | 55.17 | 32.698 | 0.0090853 | 92.3628
2024-01-01 01:00:00 | 3600 | 83.7467 | 91.647 | 10.0006 | 96 | 57.8219 | 7.9003 | 0.00219514 | 93.8698
2024-01-01 02:00:00 | 3600 | 92.6263 | 70.3239 | 10.0002 | 95.9997 | 56.6966 | -22.3024 | -0.00619682 | 92.6719
2024-01-01 03:00:00 | 3600 | 71.25 | 29.9977 | 10 | 95.9992 | 52.6745 | -41.2524 | -0.0114622 | 92.3455
2024-01-01 04:00:00 | 3600 | 30.9284 | 12.3392 | 10.0002 | 95.9996 | 48.9073 | -18.5893 | -0.00516512 | 92.3633
2024-01-01 05:00:00 | 3600 | 13.3288 | 25.7469 | 10.0006 | 95.9999 | 48.3472 | 12.418 | 0.00345041 | 92.3527
2024-01-01 06:00:00 | 3600 | 26.8036 | 61.2852 | 10.0001 | 96 | 51.4319 | 34.4816 | 0.00958089 | 92.3642
2024-01-01 07:00:00 | 3600 | 55.3645 | 85.6836 | 10 | 95.9997 | 55.742 | 30.3191 | 0.00842432 | 92.3547
... (28 rows total)
```

### full table scan (100000000 rows)

**count(*) over the whole table** — `237,457 ms`

```sql
SELECT count(*) FROM scale.metrics;
```

```
count
-----
100000000
(1 rows)
```
