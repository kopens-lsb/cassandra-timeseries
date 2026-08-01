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

# Design: Time-Series Aggregation & Functions for Apache Cassandra

Status: **IN PROGRESS** · Branch: `cassandra-timeseries-functions`

Implemented & tested so far (18 unit tests + Docker real-CQL smoke test, all passing):
`time_bucket`, `first`, `last` (M1); `delta`, `rate`, `derivative` (M2); `percentile` (M3, exact
PERCENTILE_CONT — no t-digest dependency added). `moving_avg` is deferred: a sliding window does not fit
Cassandra's one-result-per-group aggregate model and would need CQL window-function support.

## 1. Goal & Scope

Make Cassandra a first-class time-series store by adding native time-series
aggregation/scalar functions on top of the existing LSM + compaction + CQL
aggregation machinery, **without** introducing a separate engine or storage
format. We extend what already exists.

In-scope for the first milestone (per product decision):

1. `time_bucket` — time bucketing / downsampling key
2. `first` / `last` by time — value at earliest/latest timestamp in a group
3. `rate` / `delta` / `derivative` — counter/gauge change over time
4. `moving_avg` / `percentile` (approximate, t-digest)

Out of scope (future): continuous aggregates / automatic rollups, gap-filling
(`locf`/`interpolate`), retention automation beyond TTL.

## 2. What Cassandra already provides (do not reinvent)

Understanding the existing surface is essential — several "time-series"
features already exist and we must extend rather than duplicate.

| Capability | Where | Notes |
|---|---|---|
| Time-window compaction | `db/compaction/TimeWindowCompactionStrategy.java`, `UnifiedCompactionStrategy.java` | Groups SSTables by time window; whole-window TTL drop. Foundation for read efficiency. |
| Time bucketing in `GROUP BY` | `cql3/functions/TimeFcts.java` (`floor(ts, duration[, start])`) + `db/aggregation/AggregationSpecification.java` (`AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR`) | **`time_bucket` largely already exists** as `floor(...)` and is pushed down into the read path when used as the trailing `GROUP BY` selector on a clustering column. |
| Aggregates `sum/avg/min/max/count` | `cql3/functions/AggregateFcts.java` | Per-type `NativeAggregateFunction`; the model to copy for new aggregates. |
| Native function registration | `cql3/functions/NativeFunctions.java` | All function families register via `addFunctionsTo(...)`. New family = new `*Fcts.java` + one line here. |
| User-defined aggregates | `cql3/functions/UDAggregate.java` | Fallback path users have today; our native functions should match/beat these. |
| Grouped aggregation execution | `db/aggregation/GroupMaker.java`, `cql3/statements/SelectStatement.java`, `cql3/selection/` | How GROUP BY rows are folded into aggregate state. |

**Key implication for `time_bucket`:** rather than a brand-new function, we
(a) add `time_bucket(duration, ts)` as an *ergonomic alias* over `floor`, and
(b) ensure it participates in the same `AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR`
pushdown so downsampling stays a replica-side operation, not a coordinator-side
fold.

## 3. Function specifications

CQL aggregate functions in Cassandra are *2-arg-incompatible* with windowing —
they fold one column value per row into running state and emit one result per
group. This shapes every design choice below: anything needing the *timestamp
alongside the value* must take both as arguments.

### 3.1 `time_bucket(duration, timestamp [, origin])` — scalar

- Returns the bucket-start `timestamp`/`date` for the given value.
- Semantics identical to `floor(timestamp, duration[, origin])` — implemented
  as an alias so it reuses the existing read-path GROUP BY pushdown.
- Example (the duration is a bare CQL duration literal, not a quoted string):
  ```sql
  SELECT time_bucket(1h, timestamp) AS hour, avg(latency)
  FROM pp.tm_tag_point WHERE tag_id = ? AND timestamp >= ? AND timestamp < ?
  GROUP BY tag_id, time_bucket(1h, timestamp);
  ```
- Implementation: thin registration in a new `TimeSeriesFcts` delegating to
  `TimeFcts.FloorTimestampFunction` / `FloorDateFunction`. Must register the
  same `Factory` so `AggregationSpecification` recognizes it for pushdown.

### 3.2 `first(value, timestamp)` / `last(value, timestamp)` — aggregate

- `first` = value whose paired `timestamp` is minimal in the group; `last` =
  maximal. Ties broken deterministically (lower/higher value bytes).
- Two arguments → requires a `NativeAggregateFunction` whose `Aggregate.state`
  holds `(bestTs, value)`. Model on `AggregateFcts.makeMaxFunction` but the
  comparison key is the *second* argument.
- Generic over `value` type; `timestamp` arg constrained to `timestamp`/`date`/
  `bigint`/`timeuuid` (resolve via a `FunctionFactory`, like `CastFcts`).
- Serialization for distributed aggregation: state = (8-byte ts, serialized
  value). Must implement `Aggregate` so partial state merges across replicas.

### 3.3 `rate` / `delta` / `derivative` — aggregate over (value, timestamp)

These are *ordered, pairwise* computations — they need the first and last
samples (and for monotonic counters, reset handling). Because Cassandra
aggregates see rows in clustering order within a group, we accumulate
`(firstTs, firstVal, lastTs, lastVal, resetAccumulator)`:

- `delta(value, ts)` = `lastVal - firstVal` (+ reset compensation for counters).
- `rate(value, ts)` = `delta / (lastTs - firstTs)` in per-second units.
- `derivative(value, ts)` = like `rate` but gauge semantics (no reset handling).
- Counter-reset detection: when `current < previous`, add `previous` to the
  reset accumulator (Prometheus-style). Documented as best-effort.
- Numeric inputs only; output `double`.

### 3.4 `moving_avg(value, n)` / `percentile(value, q)` — aggregate

- `percentile(value, q)`: approximate quantile via **t-digest**. State is a
  serializable digest; merge = digest union (naturally distributed-friendly).
  Needs a dependency decision (see §6) — t-digest lib vs. in-tree
  implementation. `q` in `[0,1]`.
- `moving_avg(value, n)`: trailing N-sample average. **Caveat:** a true sliding
  window over arbitrary N conflicts with single-pass aggregate state. First cut
  = whole-group mean with a windowed variant deferred, OR implement as a
  bounded ring-buffer in `Aggregate.state` (memory = O(n) per group). Flag this
  as the riskiest of the four; may ship as scalar-over-GROUP-BY instead.

## 4. Where the code goes

```
src/java/org/apache/cassandra/cql3/functions/
  TimeSeriesFcts.java         (NEW) — registers all functions above
  NativeFunctions.java        (EDIT) — add TimeSeriesFcts.addFunctionsTo(this)
src/java/org/apache/cassandra/db/aggregation/
  AggregationSpecification.java (MAYBE) — recognize time_bucket alias for pushdown
```

Pattern to follow, file-for-file:
- Scalar (`time_bucket`): copy `TimeFcts.FloorTimestampFunction` shape.
- 2-arg aggregate (`first/last/rate/...`): copy `AggregateFcts` aggregate
  classes; the novelty is multi-arg state + custom serialization.
- Dynamic type resolution: copy `FunctionFactory` usage from `CastFcts`/`MathFcts`.

## 5. Distributed correctness (the part that bites)

Aggregates run **partially on each replica/coordinator and merge**. Every new
aggregate MUST:

1. Define associative, commutative-enough merge of partial state. `first/last`,
   `percentile` (t-digest), `min/max`-style are fine. `rate/delta` need
   first/last-sample state, not just deltas, to merge correctly.
2. Provide stable serialization of intermediate state (versioned, like existing
   `Aggregate` implementations) for cross-node transport.
3. Behave correctly under `GROUP BY` *and* whole-partition aggregation.
4. Respect ordering: do not assume global time order across SSTables/replicas —
   carry timestamps in state, never rely on arrival order.

This is the primary source of subtle bugs and must be covered by jvm-dtests
(`test/distributed/`), not just unit tests.

## 6. Open decisions (need sign-off before coding)

- **t-digest dependency**: adding a `lib/` jar requires OSS community approval
  (per AGENTS.md). Options: (a) request approval, (b) vendor a minimal
  in-tree digest, (c) drop `percentile` from milestone 1. *Recommendation: (b)
  or defer.*
- **`time_bucket` naming**: alias vs. just documenting `floor`. Alias improves
  discoverability/migration from TimescaleDB; costs a name in the global
  function namespace.
- **`moving_avg` semantics**: true sliding window (O(n) state) vs. simple group
  mean. *Recommendation: defer true windowing.*
- **CQL grammar**: none of these require grammar changes (all are ordinary
  function calls) — confirms low blast radius. No `Cql.g` edits expected.

## 7. Milestones

1. **M1 — `time_bucket` alias + pushdown wiring + docs.** Lowest risk; validates
   the registration + `AggregationSpecification` path end to end.
2. **M2 — `first`/`last`.** Introduces 2-arg aggregate state + serialization.
3. **M3 — `rate`/`delta`/`derivative`.** Reuses M2 state machinery.
4. **M4 — `percentile` (t-digest) and/or `moving_avg`.** Gated on §6 decisions.

Each milestone: native function + unit tests + at least one jvm-dtest proving
distributed merge correctness + `CHANGES.txt` entry + cqlsh/doc updates.

## 8. Testing plan

- Unit (`test/unit/.../cql3/functions/`): per-function correctness, type
  resolution, null/empty-group handling, overflow.
- jvm-dtest (`test/distributed/`): multi-node GROUP BY with each new aggregate,
  asserting equality with a single-node reference result (catches bad merges).
- Microbench (`test/microbench/`): `time_bucket` GROUP BY pushdown vs.
  client-side downsampling, to quantify the performance win.
