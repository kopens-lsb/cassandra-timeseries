# cassandra-timeseries

**Apache Cassandra 6.0.0 + native time-series functions — a distributed time-series database.**

Fork of [apache/cassandra](https://github.com/apache/cassandra) (branch `cassandra-6.0`) that adds server-side time-series CQL functions. Spark access is provided by the companion fork [cassandra-spark-connector](https://dev.kopens.io/common/cassandra-spark-connector) (Spark 4.1.2).

## Time-series features

Aggregates and functions available in CQL out of the box:

- **Bucketing / gap-fill**: `time_bucket`, `GROUP BY time_bucket_gapfill(width, ts, start, finish)` with `locf()` and `interpolate()` fill policies (missing buckets are materialized)
- **First/last & deltas**: `first`, `last`, `delta`, `rate`, `derivative`
- **Counters (reset-aware)**: `counter_delta`, `counter_rate`
- **Statistics**: `variance`, `stddev`, `percentile`, `histogram`, `approx_count_distinct`, `time_weighted_average`, `integral(value, ts)`
- **Two-variable statistics / regression**: `corr`, `covar_pop`, `covar_samp`, `regr_slope`, `regr_intercept`, `regr_r2`

Design docs and CQL examples: [doc/timeseries/](doc/timeseries/) (`timeseries-functions-design.md`, `gapfill-design.md`, `continuous-aggregates-design.md`, `examples.md`).

```sql
-- 1-minute buckets over an hour, gaps filled by interpolation
SELECT time_bucket_gapfill(1m, ts, '2026-07-12 00:00', '2026-07-12 01:00') AS bucket,
       interpolate(avg(value))
FROM metrics
WHERE sensor = 42 AND ts >= '2026-07-12 00:00' AND ts < '2026-07-12 01:00'
GROUP BY sensor, time_bucket_gapfill(1m, ts, '2026-07-12 00:00', '2026-07-12 01:00');
```

## Build

Requirements: **Java 21**, Ant ≥ 1.10 (plus ant-junit for tests). `modules/accord` is a git submodule (`git submodule update --init`).

```bash
.build/sh/ai-build     # clean + jar + checkstyle -> build/apache-cassandra-6.0.0.jar
```

The build always produces `apache-cassandra-6.0.0.jar` (`base.version` is pinned to 6.0.0).

## CI & releases

- Every push builds the jar and runs the time-series test suites (`.gitlab-ci.yml`).
- The jar of the latest master build: *CI/CD → Pipelines → build-jar artifact*.
- Tag pushes (e.g. `v6.0.0`) publish a [Release](../../-/releases) with a jar download link.

## Branches & upstream policy

- `master` (= branch `6.0.0`): the release line. It must be kept **merged with the latest upstream `cassandra-6.0`** branch of apache/cassandra (remote `upstream`).
- Recurring merge conflict spots: `CHANGES.txt`, `debian/changelog`, the `modules/accord` submodule pointer, and `cql3/statements/SelectStatement.java` (gap-fill wiring).

## Development

See [CLAUDE.md](CLAUDE.md) and [AGENTS.md](AGENTS.md) for build/test/style rules (targeted tests only — the full suite takes hours), and [TESTING.md](TESTING.md) for test layout. Time-series test entry points: `org.apache.cassandra.cql3.functions.TimeSeriesFctsTest`, `org.apache.cassandra.db.aggregation.TimeBucketGapFillerTest`.
