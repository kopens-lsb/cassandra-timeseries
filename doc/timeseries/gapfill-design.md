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

# Design: Gap-Fill & Interpolation for Time-Series Queries (B1)

Status: **DRAFT / Proposal** · Branch: `cassandra-timeseries-functions`
Builds on the native functions in [timeseries-functions-design.md](timeseries-functions-design.md).

## 1. Problem

When a series is downsampled with `time_bucket` + `GROUP BY`, **buckets with no rows produce no
output**. Dashboards and SLO math need a row for *every* bucket in a range, with missing values
filled. Today the user must post-process client-side.

```sql
-- 10:00 bucket is simply absent if no samples landed in it
SELECT time_bucket(1h, ts) AS bucket, avg(value)
FROM metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);
```

We want a continuous bucket series over `[start, finish)` with a fill policy:
- **none/null** — emit the bucket with `null` aggregates (just densify the time axis).
- **locf** — last observation carried forward (repeat the previous non-empty bucket's value).
- **linear** — linearly interpolate between the surrounding non-empty buckets.
- **constant** — fill with a supplied constant (e.g. `0`).

## 2. Why this is NOT just another function

`first/last/percentile` are aggregates: they fold existing rows into one output row per group.
Gap-fill must **generate groups (rows) that have no underlying data** — an aggregate function cannot
do that because it is only ever invoked for groups the storage layer produced. So gap-fill is a
**query-engine feature**, touching grouping and result assembly, not the `functions/` package alone.

Key code touch-points (read these before implementing):
- [db/aggregation/AggregationSpecification.java](../../src/java/org/apache/cassandra/db/aggregation/AggregationSpecification.java)
  and [db/aggregation/GroupMaker.java](../../src/java/org/apache/cassandra/db/aggregation/GroupMaker.java)
  — decide group boundaries from the floor/`time_bucket` selector.
- [cql3/statements/SelectStatement.java](../../src/java/org/apache/cassandra/cql3/statements/SelectStatement.java)
  (`getAggregationSpec`, result-set construction) — where synthesized empty buckets must be spliced in.
- [cql3/selection/](../../src/java/org/apache/cassandra/cql3/selection/) — selectors that would carry
  the fill policy and apply locf/linear across the emitted bucket sequence.

## 3. Proposed CQL surface

Two candidate syntaxes; recommendation is **(A)** — it follows TimescaleDB, keeps the range explicit,
and localizes the engine change to the grouping selector.

**(A) `time_bucket_gapfill` + `locf()`/`interpolate()` (recommended)**

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000') AS bucket,
       locf(avg(value))        AS v_locf,
       interpolate(avg(value)) AS v_linear
FROM   metrics
WHERE  series = 'cpu'
  AND  ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000'
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

- `time_bucket_gapfill(width, ts, start, finish)` behaves like `time_bucket` for grouping but also
  declares the **dense range**; the engine emits every bucket in `[start, finish)` even when empty.
- `locf(expr)` / `interpolate(expr)` are *finalizing* selectors applied over the ordered bucket
  sequence of one partition; without them, empty buckets carry `null`.

**(B) `GROUP BY ... WITH FILL`** clause (ClickHouse-style) — more general but a bigger grammar change
to `Cql.g` (which AGENTS.md says to treat with care). Deferred unless (A) proves too limiting.

## 4. Semantics & correctness

- **Range source.** `start`/`finish` come from the `*_gapfill` args (explicit), not inferred from the
  `WHERE` clause, to keep behavior deterministic. Validate that `finish > start` and the range is
  bounded (reject unbounded gap-fill to avoid generating unbounded rows — a guardrail).
- **Per-partition only.** locf/interpolate carry state across buckets *within a single partition key*
  (one series). They must reset at partition boundaries. This mirrors how `GroupMaker` already tracks
  group transitions.
- **Ordering.** Requires buckets in ascending time order within the partition (clustering order). Must
  reject or handle `ORDER BY ts DESC` for interpolation, or buffer+sort.
- **Distributed path.** Aggregation runs at the coordinator over merged rows; the synthesized buckets
  and locf/linear fill are applied there, after merge — so no replica-side protocol change. Confirm the
  paged path: gap-fill interacts with paging (a bucket spanning a page boundary must not be filled
  twice). **This is the highest-risk area** and needs explicit paging tests.
- **Null vs filled.** Distinguish "bucket had data whose aggregate is null" from "bucket was empty and
  filled" — document and test.

## 5. Guardrails

- Reject gap-fill when the emitted bucket count would exceed a configurable cap (e.g.
  `time_series_gapfill_max_buckets`, default 100k) — prevents a query from materializing millions of
  synthetic rows. Add under [db/guardrails/](../../src/java/org/apache/cassandra/db/guardrails/).
- Require a bounded `start`/`finish`; require the gap-fill selector to be the trailing `GROUP BY` term.

## 6. Milestones

1. **G1 — densify only (null fill).** `time_bucket_gapfill(width, ts, start, finish)` emits every
   bucket in range with `null` for empty ones. No locf/interpolate yet. Proves the engine can
   synthesize groups; smallest correct slice. Tests: single partition, empty ranges, boundary buckets.
2. **G2 — `locf()`.** Carry the previous non-empty bucket's value forward; reset per partition.
3. **G3 — `interpolate()` (linear).** Interpolate between surrounding non-empty buckets; endpoints with
   only one side default to locf or null (decide + document).
4. **G4 — paging + guardrails.** Correct behavior across page boundaries; bucket-count cap.

Each milestone: implementation + CQLTester unit tests (incl. paging in G4) + at least one jvm-dtest for
the distributed/paged path + Docker real-CQL smoke check + `CHANGES.txt` + examples in
[examples.md](examples.md).

## 7. Open decisions (sign-off before coding G1)

- **Syntax (A) vs (B).** Recommend (A); no `Cql.g` change if `time_bucket_gapfill`/`locf`/`interpolate`
  are ordinary function calls recognized specially by `getAggregationSpec`/selection. Confirm they can
  be modeled without grammar edits.
- **Range from args vs WHERE.** Recommend args (explicit, deterministic).
- **Interaction with existing `floor`/`time_bucket` pushdown** — ensure gap-fill grouping reuses the
  monotonic-selector path rather than forking it.
- **DESC ordering** — support via buffering, or reject for interpolation in v1?
