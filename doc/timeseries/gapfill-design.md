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

Status: **COMPLETE (G1–G4)** — null/locf/interpolate fill, multi-partition, bucket-count guardrail · Branch: `cassandra-timeseries-functions`
Builds on the native functions in [timeseries-functions-design.md](timeseries-functions-design.md).

G1 is done and verified end-to-end (CQLTester + Docker real CQL): `GROUP BY time_bucket_gapfill(width, ts,
start, finish)` materializes a row for every bucket in `[start, finish)`, empty ones with null aggregates.
v1 scope: single-partition, fixed-width buckets, unaliased bucket column, non-paged. Next: G2 `locf()` and
G3 `interpolate()` fill policies, then G4 paging + guardrails.

## 1. Problem

When a series is downsampled with `time_bucket` + `GROUP BY`, **buckets with no rows produce no
output**. Dashboards and SLO math need a row for *every* bucket in a range, with missing values
filled. Today the user must post-process client-side.

```sql
-- 10:00 bucket is simply absent if no samples landed in it
SELECT time_bucket(1h, timestamp) AS bucket, avg(latency)
FROM pp.tm_tag_point WHERE tag_id='TAG-001' GROUP BY tag_id, time_bucket(1h, timestamp);
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
SELECT time_bucket_gapfill(1h, timestamp, '2026-07-01 00:00:00+0000', '2026-07-02 00:00:00+0000'),
       locf(avg(latency)),
       interpolate(avg(latency))
FROM   pp.tm_tag_point
WHERE  tag_id = 'TAG-001'
  AND  timestamp >= '2026-07-01 00:00:00+0000' AND timestamp < '2026-07-02 00:00:00+0000'
GROUP  BY tag_id, time_bucket_gapfill(1h, timestamp, '2026-07-01 00:00:00+0000', '2026-07-02 00:00:00+0000')
ORDER  BY timestamp ASC;
```

Three things this example is careful about, all load-bearing:

- **The aggregate must be numeric.** On this table that is `latency` (`int`, always present) or, for tags
  whose static `type` is numeric, `value_numeric` (`double`) — **not** `value`, which is `text`.
- **`start` must not be later than any scanned row**, or the query throws *"The floor function starting
  time is greater than the provided time"*. Hence the matching `WHERE timestamp >= <start>`.
- **`ORDER BY timestamp ASC` is mandatory on a `DESC`-clustered table** — see §4 below. Note also that the
  bucket column and the `locf`/`interpolate` selectors must **not** be aliased: the spec is located by
  matching the function name in the result metadata, so an alias makes gap-fill silently do nothing.

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
- **Ordering — the sharp edge on the production schema.** `TimeBucketGapFiller.densify` assumes rows
  arrive grouped by partition and, within a partition, by **ascending** bucket. Nothing enforces it:
  `densify` runs unconditionally in `SelectStatement.process`, before `orderResults`, with no check on
  the clustering order or the `ORDER BY` clause. Fed descending buckets it emits every synthetic bucket
  first and then passes the real rows through — **wrong output, no error.**

  This matters because the production idiom is `CLUSTERING ORDER BY (timestamp DESC)`. On such a table a
  gap-fill query **must** carry `ORDER BY timestamp ASC`: a DESC declaration plus an ASC request cancel
  out into a reversed slice filter (`SelectStatement.isReversed` computes
  `reversed != def.isReversedType()`), so the storage engine hands `densify` ascending rows, which is
  exactly what it wants. Without the clause the rows arrive newest-first and the fill is applied
  backwards.

  Status: no test covers gap-fill on a DESC-clustered table in either direction — every gap-fill test in
  the tree uses an ASC table. Rejecting the unsafe combination outright (rather than relying on the
  operator to add `ORDER BY`) is still open, and is the single highest-value follow-up here.
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

## 6b. G1 implementation status & remaining wiring (Step 2b)

**Done (pushed):**
- `time_bucket_gapfill(duration, ts, start, finish)` — a 4-arg monotonic scalar that buckets like
  `time_bucket` (origin = start) and works as a `GROUP BY` selector (`TimeSeriesFcts`).
- `TimeBucketGapFiller.densify(rows, bucketIndex, partitionKeyIndices, columnCount, start, finish, step)`
  — pure logic that inserts synthetic null rows for empty buckets per partition. Unit-tested.

**Remaining — wire densify into the query path (non-paging only, per decision):**
1. **Capture params at prepare.** In `SelectStatement.RawStatement.getAggregationSpecFactory` (the loop over
   `parameters.groups`), when the `WithFunction.function` name is `time_bucket_gapfill`, the constant args are
   `Selectable.WithTerm` (args 0=duration, 2=start, 3=finish). Build a `Selector.Factory` for each via
   `arg.newSelectorFactory(...)` (or evaluate now), and record the bucket column's index in the result (match the
   gapfill column name against `getResultMetadata`). Bundle into a `GapFillSpec { Selector.Factory width,start,finish;
   int bucketIndex; List<Integer> partitionKeyIndices; }`.
2. **Thread `GapFillSpec` to `SelectStatement`.** Add a field + constructor param. Three call sites:
   `SelectStatement.java:300`, `:1384` (pass the real spec here), and `ModificationStatement.java:1389` (pass null).
3. **Evaluate + densify at execute.** In `process(...)` (after `ResultSet cqlRows = result.build()`, before
   `orderResults`), if `gapFillSpec != null`: build the three arg selectors with `options`, read their `getOutput`
   → decode width (Duration → fixed `stepMillis`; reject month components in v1), start/finish (timestamp millis);
   then `cqlRows.rows = TimeBucketGapFiller.densify(...)`. Partition-key indices = the result columns for the
   grouping PK prefix.
4. **Guards (v1):** reject/disable when the query is paged (densify is page-local); require literal (non-bind)
   width/start/finish or bind them via `options`; enforce a bucket-count cap (guardrail) so a huge range can't
   materialize unbounded synthetic rows.
5. **Tests:** CQLTester integration tests (empty interior buckets filled with null aggregates; per-partition reset;
   range edges) + a Docker real-CQL check.

## 7. Open decisions (sign-off before coding G1)

- **Syntax (A) vs (B).** Recommend (A); no `Cql.g` change if `time_bucket_gapfill`/`locf`/`interpolate`
  are ordinary function calls recognized specially by `getAggregationSpec`/selection. Confirm they can
  be modeled without grammar edits.
- **Range from args vs WHERE.** Recommend args (explicit, deterministic).
- **Interaction with existing `floor`/`time_bucket` pushdown** — ensure gap-fill grouping reuses the
  monotonic-selector path rather than forking it.
- **DESC ordering** — support via buffering, or reject for interpolation in v1?
