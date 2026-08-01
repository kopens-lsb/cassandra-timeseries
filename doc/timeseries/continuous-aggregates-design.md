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

# Design: Continuous Aggregates (Time-Bucket Rollups)

Status: **DRAFT / Proposal** · Branch: `cassandra-timeseries-functions`
Builds on the time-series functions ([timeseries-functions-design.md](timeseries-functions-design.md)) — especially
`time_bucket` and the GROUP BY pushdown.

## 1. Goal

Keep a **downsampled rollup table** continuously up to date from a raw time-series table, so dashboards read
pre-aggregated buckets instead of re-scanning raw data every query. This is TimescaleDB's flagship "continuous
aggregate". Example intent:

```
raw:    pp.tm_tag_point(tag_id, timestamp, latency, value_numeric, ...)  -- millions of rows/day
rollup: pp.tm_tag_point_1h(tag_id, bucket, avg, min, max, count)        -- one row per tag per hour, auto-maintained
```

A query then hits `tm_tag_point_1h` directly (cheap), and recent/not-yet-rolled-up data can optionally be unioned from raw.

Two properties of the real shape that a rollup design must respect:

- **Only some columns can be rolled up numerically.** `value` is `text` (a string copy of the reading), so
  it has no `avg`/`min`/`max`. The roll-uppable columns on this table are `latency` (`int`) and, for tags
  whose static `type` is numeric, `value_numeric` (`double`). A rollup definition therefore names columns
  explicitly rather than "aggregate everything".
- **Static columns do not belong in the rollup.** `site_id`/`tag_name`/`type` are per-tag, not per-bucket;
  the rollup table should either omit them or carry them as statics of its own `tag_id` partition, not
  re-derive them per bucket.

## 2. Why Cassandra can't reuse Materialized Views

Cassandra MVs ([db/view/](../../src/java/org/apache/cassandra/db/view/)) maintain a 1:1 row mapping with a different
primary key; they explicitly **do not support aggregation or GROUP BY**. A continuous aggregate is fundamentally a
*many-rows-to-one-bucket* fold, so MV maintenance machinery cannot express it. We therefore need a separate
**incremental rollup** mechanism rather than extending MVs.

## 3. Approach: scheduled incremental rollup with a watermark

Rather than maintain the rollup synchronously on every write (expensive, and the partial-bucket problem), refresh it
**asynchronously over closed buckets**:

- A bucket `[b, b+width)` is **closed** once `now - max_lag >= b + width` (a configurable lag absorbs late/out-of-order
  writes and clock skew).
- A **watermark** per continuous aggregate records the highest bucket boundary already materialized.
- A periodic **refresh** aggregates raw rows for buckets in `(watermark, newest_closed_bucket]` and writes them to the
  target table, then advances the watermark. Re-running is idempotent (it overwrites the same bucket rows).

This is the same shape as TimescaleDB's refresh policy and avoids touching the write hot path.

### 3.1 Which aggregates can be chained, and which cannot

Rollups are most useful chained — the daily rollup should be built from the hourly one, not by re-scanning raw.
That only works for aggregates that are **decomposable**, and getting this wrong produces answers that look
plausible and are quietly incorrect.

`avg(avg)` is the trap. Averaging hourly averages weights every hour equally, so an hour with 5 samples counts
as much as an hour with 3,600. The daily average is then wrong whenever sample counts differ — which, on an
event-driven historian with gaps and backfills, is most of the time.

| aggregate | chainable? | how |
| --- | --- | --- |
| `min` / `max` | yes, exactly | `min(min)`, `max(max)` |
| `count` | yes | `sum(count)` |
| `sum` | yes | `sum(sum)` |
| `avg` | **only with a companion `count`** | `sum(avg × count) / sum(count)` |
| `variance` / `stddev` | only with `count`, `sum`, `sum_sq` | recompute from the three; variance alone is not enough |
| `first` / `last` | only with the companion timestamp | pick the value whose kept timestamp is min/max |
| `delta` / `rate` / `derivative` | no | defined over endpoints of a range; recompute from `first`/`last` instead |
| `percentile` | **no** | p95 of a day is not derivable from 24 hourly p95s |
| `histogram` | yes, if bucket bounds are identical | element-wise `sum` of the bucket arrays |
| `approx_count_distinct` | only if the **HLL sketch** is stored, not the estimate | merge sketches |
| `time_weighted_average` / `integral` | only with the covered duration | weight by each bucket's time span |

Consequences for the design:

- **A rollup target schema must carry the companion columns**, not just the headline aggregate. Storing `avg`
  alone makes that rollup permanently unchainable — the information needed to combine it is gone.
- **A CA definition should reject a chained source whose aggregate is not decomposable** rather than silently
  computing `avg(avg)`. If someone wants a daily p95 they must roll it up from raw, not from the hourly rollup.
- `percentile` and `approx_count_distinct` therefore either roll up from raw at every resolution, or require
  storing a mergeable sketch — a larger piece of work, and a reasonable v1 exclusion.

### 3.2 Backfill is a primary workload here, not an edge case

The watermark design silently loses late data. A refresh processes
`(watermark, newest_closed_bucket]`, so a write landing in a bucket *below* the watermark leaves that
bucket's rollup stale forever, with no error. `max_lag` only rescues writes that arrive within it.

That is not an acceptable v1 limitation for this fork specifically. Its stated workload is edge devices
that lose connectivity and then push **days** of history at once — no sane `max_lag` covers that, and the
rollup would be quietly wrong for exactly the periods an operator is most likely to go back and look at.

Options, roughly in increasing cost:

| mechanism | cost | notes |
| --- | --- | --- |
| **Manual range refresh** (`--from/--to`) | trivial | Necessary regardless. Requires someone to know a backfill happened. |
| **Trailing re-refresh** — always re-do the last N buckets each cycle | cheap, bounded | Good default. Catches backfill up to N; idempotent, so re-running costs only IO. |
| **Invalidation log** — record buckets touched by late writes | expensive | TimescaleDB's approach; needs a hook on the write path. |
| **Reuse the TSCS late-isolation signal** | moderate | This fork already isolates late arrivals into their own window (T3's window-splitting writer). A closed window receiving new sstables *is* an invalidation signal, and it is per-window — which maps onto bucket ranges directly. TimescaleDB has no equivalent; this fork does. |

Recommended: trailing re-refresh as the default, manual range refresh always available, and the TSCS signal
as the precise mechanism once the executor is proven.

**Invalidation must cascade through chained rollups.** If an hourly bucket is recomputed, every daily bucket
built from it is stale too. A design that chains resolutions (§3.1) must propagate invalidation down the chain,
or the daily rollup silently disagrees with the hourly one it came from.

## 4. Components

1. **Definition / catalog.** A new system table, e.g.
   `system_timeseries.continuous_aggregates(name, keyspace, source_table, target_table, bucket_width, group_keys,
   aggregates, refresh_interval, max_lag, watermark, enabled)`. Created/edited via new CQL DDL (see §5) or, for a
   smaller v1, via inserts into this table plus a `nodetool`/CQL trigger.
2. **Rollup executor.** Given a CA definition and a `[from, to)` bucket range, run
   `INSERT INTO target SELECT <group_keys>, time_bucket(width, ts) AS bucket, <aggregates> FROM source
    WHERE ts >= from AND ts < to GROUP BY <group_keys>, time_bucket(width, ts)`.
   Cassandra has **no server-side `INSERT … SELECT`**, so this is the core new execution primitive — it reuses the
   existing aggregation/`time_bucket` pushdown to read, and writes mutations to the target. See
   [SelectStatement.java](../../src/java/org/apache/cassandra/cql3/statements/SelectStatement.java) (aggregation path)
   and the modification statements for the write path.
3. **Scheduler.** A periodic task (mirror the existing scheduled subsystems — e.g. the patterns around
   `ScheduledExecutors`/`CompactionManager` background tasks) that, per `refresh_interval`, computes the closed-bucket
   range from the watermark and invokes the rollup executor, then persists the new watermark.
4. **Read path (optional, later).** A "real-time" view that unions the materialized rollup with an on-the-fly
   aggregation of the still-open tail from raw, so the latest partial bucket is also visible.

## 5. CQL surface (options)

- **(A) Extend DDL** — `CREATE CONTINUOUS AGGREGATE x AS SELECT …, time_bucket(1h, ts), avg(v) FROM raw GROUP BY …
  WITH refresh_interval = '5m' AND max_lag = '10m';` Cleanest UX, biggest grammar/schema change (`src/antlr/Cql.g`,
  schema objects, replication of the catalog).
- **(B) Catalog table + functions/nodetool (v1)** — register a CA by inserting into `system_timeseries.continuous_
  aggregates`; a `nodetool refreshcontinuousaggregate <name>` (and an auto-refresh scheduler) drives it. Much smaller
  surface; no grammar change. Recommended for v1 to de-risk the executor first.

## 6. Distributed coordination (the hard part)

- **Who refreshes?** Exactly one node must own each refresh tick to avoid duplicate/competing rollups. Options: run on
  the coordinator that holds the CA catalog's partition; or elect via the existing cluster-metadata/`tcm` layer; or a
  per-token-range split so each node rolls up the ranges it replicates. v1 can restrict to a single designated node
  (documented limitation) and harden later.
- **Idempotency & correctness.** Writing bucket rows is naturally idempotent (last-writer-wins on the same bucket key),
  but the watermark advance must be crash-safe (persist only after the rollup write is acknowledged at the chosen
  consistency level). Late writes older than the watermark are missed unless `max_lag` covered them — document this and
  optionally support a manual backfill over a range.
- **Paging/limits.** The rollup read must page through large ranges; reuse the aggregation pager.

## 7. Milestones

1. **CA1 — manual rollup primitive.** Implement the rollup executor as an explicit operation (CQL statement or
   `nodetool`) that materializes a given `[from, to)` range into the target. No scheduling, single node. This is the
   load-bearing, fully-testable core (server-side aggregate INSERT…SELECT). Unit + CQLTester + Docker real-CQL tests.
2. **CA2 — catalog + watermark.** Persist CA definitions and watermarks in `system_timeseries`; manual refresh advances
   the watermark over closed buckets only.
3. **CA3 — scheduled auto-refresh.** Background scheduler drives refresh per interval on a single designated node.
4. **CA4 — distributed ownership + real-time union read.** Per-range ownership via cluster metadata; optional union of
   the open tail.

## 8. Open decisions (sign-off before CA1)

- **Scope of CA1**: is a `nodetool refresh`-style primitive acceptable for v1 (recommended), or is `CREATE CONTINUOUS
  AGGREGATE` DDL required up front? The former de-risks the executor without a grammar change.
- **Target table ownership**: user pre-creates the target table with a matching schema (v1), or the system creates it?
- **Consistency/lag defaults** for `refresh_interval` and `max_lag`.
- **This is CEP-scale.** Recommend implementing CA1 (the aggregate `INSERT … SELECT` primitive) first as an isolated,
  testable unit before committing to the catalog/scheduler/distribution layers.
