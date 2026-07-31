#!/bin/bash
#
# Docker integration test for the time-series CQL functions.
#
# Boots the cassandra-timeseries image, loads a deterministic data set and asserts the
# result of every time-series function family against hand-computed values. This is the
# release gate: the unit tests exercise the functions in-process, this exercises them
# through a real node (schema, read path, aggregation, native protocol, cqlsh output).
#
#   ./docker/integration-test.sh [image]           # default: cassandra-timeseries:6.0.0
#   CONTAINER_RUNTIME=podman ./docker/integration-test.sh
#
# Build the image first:
#   docker build -t cassandra-timeseries:6.0.0 -f docker/Dockerfile .
#
set -uo pipefail

IMAGE="${1:-cassandra-timeseries:6.0.0}"
RUNTIME="${CONTAINER_RUNTIME:-}"
CONTAINER="cassandra-ts-it-$$"
READY_TIMEOUT="${READY_TIMEOUT:-300}"

if [ -z "$RUNTIME" ]; then
    if command -v docker > /dev/null 2>&1; then RUNTIME=docker
    elif command -v podman > /dev/null 2>&1; then RUNTIME=podman
    else echo "FATAL: neither docker nor podman found"; exit 2
    fi
fi

cleanup() { $RUNTIME rm -f "$CONTAINER" > /dev/null 2>&1; }
trap cleanup EXIT

echo "== cassandra-timeseries time-series CQL integration test =="
echo "   runtime: $RUNTIME   image: $IMAGE"

$RUNTIME run -d --name "$CONTAINER" "$IMAGE" > /dev/null || { echo "FATAL: container did not start"; exit 2; }

# The node is ready once it answers CQL on the native protocol.
printf '   waiting for CQL'
ready=0
for _ in $(seq 1 $((READY_TIMEOUT / 5))); do
    if $RUNTIME exec "$CONTAINER" cqlsh -e "SELECT release_version FROM system.local" > /dev/null 2>&1; then
        ready=1; break
    fi
    printf '.'
    sleep 5
done
echo
if [ "$ready" != 1 ]; then
    echo "FATAL: node did not become ready within ${READY_TIMEOUT}s"
    $RUNTIME logs "$CONTAINER" 2>&1 | tail -40
    exit 2
fi

cql() { $RUNTIME exec "$CONTAINER" cqlsh --no-color -e "$1" 2>&1; }

# Each check runs its own cqlsh, so a fixed startup cost sits in every measurement. Measure it
# once on a trivial query and report it, so the per-check numbers can be read honestly.
baseline_ms() {
    local t0 t1
    t0=$(date +%s%3N); cql "SELECT key FROM system.local;" > /dev/null 2>&1; t1=$(date +%s%3N)
    echo $((t1 - t0))
}
BASELINE=$(baseline_ms)

# HTML report (CI artifact): every assertion with the CQL it ran and the rows it got back.
REPORT="${IT_REPORT:-build/timeseries-it-report.html}"
REPORT_MD="${IT_REPORT_MD:-${REPORT%.html}.md}"
ROWS=$(mktemp); MDROWS=$(mktemp)
trap 'cleanup; rm -f "$ROWS" "$MDROWS"' EXIT
esc() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; }

section() {
    printf '%s\n' "-- $1"
    printf '<tr class="section"><td colspan="5">%s</td></tr>\n' "$(printf '%s' "$1" | esc)" >> "$ROWS"
    printf '\n## %s\n' "$1" >> "$MDROWS"
}

PASS=0; FAIL=0
# check <description> <cql> <extended regex the output must match>
# Prints the assertion, the CQL it ran and the rows it got back, so a CI log (or a reviewer)
# can see exactly what was executed and what the node answered.
check() {
    local desc="$1" query="$2" expect="$3" out status t0 t1 ms
    t0=$(date +%s%3N)
    out=$(cql "$query")
    t1=$(date +%s%3N); ms=$((t1 - t0))
    if printf '%s' "$out" | grep -qE -- "$expect"; then
        PASS=$((PASS + 1)); status="ok  "
    else
        FAIL=$((FAIL + 1)); status="FAIL"
    fi
    local flat; flat=$(printf '%s' "$query" | tr '\n' ' ' | tr -s ' ')
    printf '\n   [%s] %-56s %6s ms\n' "$status" "$desc" "$ms"
    printf '        cql> %s\n' "$flat"
    printf '%s\n' "$out" | grep -vE '^[[:space:]]*$' | sed 's/^/        /'
    [ "$status" = FAIL ] && printf '        !! expected to match: /%s/\n' "$expect"

    {
        printf '<tr class="%s"><td class="st">%s</td><td>%s</td><td><pre>%s</pre></td><td><pre>%s</pre></td><td class="ms">%s ms</td></tr>\n' \
            "$([ "$status" = FAIL ] && echo fail || echo pass)" \
            "$([ "$status" = FAIL ] && echo '✗ FAIL' || echo '✓ pass')" \
            "$(printf '%s' "$desc" | esc)" \
            "$(printf '%s' "$flat" | esc)" \
            "$(printf '%s' "$out" | grep -vE '^[[:space:]]*$' | esc)" \
            "$ms"
    } >> "$ROWS"
    {
        printf '\n### %s %s — `%s ms`\n\n```sql\n%s\n```\n\n```\n%s\n```\n' \
            "$([ "$status" = FAIL ] && echo '❌' || echo '✅')" "$desc" "$ms" "$flat" \
            "$(printf '%s\n' "$out" | grep -vE '^[[:space:]]*$')"
    } >> "$MDROWS"
    return 0
}

ntool() { $RUNTIME exec "$CONTAINER" nodetool "$@" 2>&1; }

# check_nodetool <description> <extended regex, or '' for exit-code-only> <nodetool args...>
# Same contract and reporting as check, but runs nodetool in the container instead of cqlsh;
# the command must exit 0 AND (if a regex is given) its output must match it.
check_nodetool() {
    local desc="$1" expect="$2"; shift 2
    local out rc status t0 t1 ms
    t0=$(date +%s%3N)
    out=$(ntool "$@"); rc=$?
    t1=$(date +%s%3N); ms=$((t1 - t0))
    if [ "$rc" -eq 0 ] && { [ -z "$expect" ] || printf '%s' "$out" | grep -qE -- "$expect"; }; then
        PASS=$((PASS + 1)); status="ok  "
    else
        FAIL=$((FAIL + 1)); status="FAIL"
    fi
    local flat="nodetool $*"
    printf '\n   [%s] %-56s %6s ms\n' "$status" "$desc" "$ms"
    printf '        cmd> %s\n' "$flat"
    printf '%s\n' "$out" | grep -vE '^[[:space:]]*$' | sed 's/^/        /'
    [ "$status" = FAIL ] && printf '        !! exit=%s, expected to match: /%s/\n' "$rc" "$expect"

    {
        printf '<tr class="%s"><td class="st">%s</td><td>%s</td><td><pre>%s</pre></td><td><pre>%s</pre></td><td class="ms">%s ms</td></tr>\n' \
            "$([ "$status" = FAIL ] && echo fail || echo pass)" \
            "$([ "$status" = FAIL ] && echo '✗ FAIL' || echo '✓ pass')" \
            "$(printf '%s' "$desc" | esc)" \
            "$(printf '%s' "$flat" | esc)" \
            "$(printf '%s' "$out" | grep -vE '^[[:space:]]*$' | esc)" \
            "$ms"
    } >> "$ROWS"
    {
        printf '\n### %s %s — `%s ms`\n\n```sql\n%s\n```\n\n```\n%s\n```\n' \
            "$([ "$status" = FAIL ] && echo '❌' || echo '✅')" "$desc" "$ms" "$flat" \
            "$(printf '%s\n' "$out" | grep -vE '^[[:space:]]*$')"
    } >> "$MDROWS"
    return 0
}

write_report() {
    mkdir -p "$(dirname "$REPORT")"
    {
        cat <<HTML
<!doctype html>
<meta charset="utf-8">
<title>cassandra-timeseries · time-series CQL integration test</title>
<style>
  :root { color-scheme: light dark; --bg:#fff; --fg:#1a1a1a; --muted:#666; --line:#e3e3e3;
          --pass:#0a7d33; --fail:#c62828; --code:#f6f7f9; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#14161a; --fg:#e6e6e6; --muted:#9aa0a6; --line:#2c3036;
            --pass:#5ec97f; --fail:#ff6b6b; --code:#1c1f24; }
  }
  body { background:var(--bg); color:var(--fg); margin:0 auto; padding:2rem 1.25rem; max-width:1100px;
         font:14px/1.5 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif; }
  h1 { font-size:1.35rem; margin:0 0 .25rem; }
  .meta { color:var(--muted); font-size:.85rem; margin-bottom:1.25rem; }
  .meta code { background:var(--code); padding:.1rem .35rem; border-radius:4px; }
  .summary { display:flex; gap:1.5rem; align-items:baseline; margin:1rem 0 1.5rem;
             padding:.9rem 1.1rem; background:var(--code); border-radius:8px; }
  .summary b { font-size:1.6rem; }
  .ok-n { color:var(--pass); } .fail-n { color:var(--fail); }
  table { border-collapse:collapse; width:100%; }
  td { border-top:1px solid var(--line); padding:.55rem .6rem; vertical-align:top; }
  tr.section td { background:var(--code); font-weight:600; letter-spacing:.02em; }
  td.st { white-space:nowrap; font-weight:600; }
  tr.pass td.st { color:var(--pass); } tr.fail td.st { color:var(--fail); }
  tr.fail { background:color-mix(in srgb, var(--fail) 8%, transparent); }
  pre { margin:0; background:var(--code); padding:.5rem .6rem; border-radius:6px; overflow-x:auto;
        font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; white-space:pre; }
  td.ms { white-space:nowrap; text-align:right; font-variant-numeric:tabular-nums; color:var(--muted); }
  th { color:var(--muted); font-weight:600; font-size:.78rem; text-transform:uppercase; letter-spacing:.04em;
       text-align:left; padding:.4rem .6rem; }
  td:nth-child(3) { width:32%; } td:nth-child(4) { width:34%; }
</style>
<h1>time-series CQL integration test</h1>
<div class="meta">
  image <code>$IMAGE</code> · runtime <code>$RUNTIME</code> · $(date -u '+%Y-%m-%d %H:%M UTC')
</div>
<div class="summary">
  <span><b class="ok-n">$PASS</b> passed</span>
  <span><b class="fail-n">$FAIL</b> failed</span>
  <span style="color:var(--muted)">assertions run against a live node booted from the image</span>
</div>
<p class="meta">Times are the full cqlsh round trip for one query; a trivial query on this host
costs <b>${BASELINE} ms</b> of that (process startup + connect), so subtract roughly that much
to read the CQL execution time. Server-side timings on a 100M-row data set are in the
scale-test report.</p>
<table>
<tr><th></th><th>assertion</th><th>cql</th><th>result</th><th>elapsed</th></tr>
HTML
        cat "$ROWS"
        printf '</table>\n'
    } > "$REPORT"

    # Markdown twin of the same report — this is the copy that gets committed and linked
    # from the README, since GitLab renders Markdown but not HTML blobs.
    {
        printf '# time-series CQL integration test\n\n'
        printf 'image `%s` · runtime `%s` · %s\n\n' "$IMAGE" "$RUNTIME" "$(date -u '+%Y-%m-%d %H:%M UTC')"
        printf '**%s passed, %s failed** — assertions run against a live node booted from the image.\n\n' \
            "$PASS" "$FAIL"
        printf '> Times are the full cqlsh round trip for one query; a trivial query on this host costs\n'
        printf '> %s ms of that (process startup + connect), so subtract roughly that much to read the CQL\n' "$BASELINE"
        printf '> execution time. Server-side timings on a 100M-row data set are in the scale-test report.\n'
        cat "$MDROWS"
    } > "$REPORT_MD"
    echo "   report: $REPORT"
    echo "   report: $REPORT_MD"
}

section "schema & fixture data"
cql "
CREATE KEYSPACE IF NOT EXISTS it WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS it.metrics (
    series text, ts timestamp, value double,
    PRIMARY KEY (series, ts)
) WITH CLUSTERING ORDER BY (ts ASC)
   AND compaction = {'class':'UnifiedCompactionStrategy',
                     'scaling_parameters':'T4',
                     'target_sstable_size':'1GiB',
                     'expired_sstable_check_frequency_seconds':600};

CREATE TABLE IF NOT EXISTS it.counters (
    series text, ts timestamp, total counter,
    PRIMARY KEY (series, ts)
);

CREATE TABLE IF NOT EXISTS it.xy (
    k text, ts timestamp, x double, y double,
    PRIMARY KEY (k, ts)
);
" > /dev/null

# 'cpu': 10 @09:05, 30 @09:35, 50 @10:15, 70 @10:45  (span 6000s, delta 60)
# 'gap': 60 @09:30, 120 @12:30                       (10:00 and 11:00 buckets empty)
cql "
INSERT INTO it.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:05:00+0000', 10);
INSERT INTO it.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:35:00+0000', 30);
INSERT INTO it.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:15:00+0000', 50);
INSERT INTO it.metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:45:00+0000', 70);
INSERT INTO it.metrics (series, ts, value) VALUES ('gap', '2024-01-01 09:30:00+0000', 60);
INSERT INTO it.metrics (series, ts, value) VALUES ('gap', '2024-01-01 12:30:00+0000', 120);

UPDATE it.counters SET total = total + 100 WHERE series='api' AND ts='2024-01-01 09:00:00+0000';
UPDATE it.counters SET total = total + 300 WHERE series='api' AND ts='2024-01-01 09:01:00+0000';
UPDATE it.counters SET total = total +  50 WHERE series='api' AND ts='2024-01-01 09:02:00+0000';

INSERT INTO it.xy (k, ts, x, y) VALUES ('r', '2024-01-01 09:00:00+0000', 1, 3);
INSERT INTO it.xy (k, ts, x, y) VALUES ('r', '2024-01-01 09:01:00+0000', 2, 5);
INSERT INTO it.xy (k, ts, x, y) VALUES ('r', '2024-01-01 09:02:00+0000', 3, 7);
INSERT INTO it.xy (k, ts, x, y) VALUES ('r', '2024-01-01 09:03:00+0000', 4, 9);
" > /dev/null

section "time_bucket / downsampling"
# hourly avg: 09:00 -> (10+30)/2 = 20, 10:00 -> (50+70)/2 = 60
check "time_bucket(1h) hourly avg = 20 / 60" \
    "SELECT time_bucket(1h, ts), avg(value) FROM it.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);" \
    '09:00:00.*\| *20'
check "time_bucket(1h) second bucket = 60" \
    "SELECT time_bucket(1h, ts), avg(value) FROM it.metrics WHERE series='cpu' GROUP BY series, time_bucket(1h, ts);" \
    '10:00:00.*\| *60'
check "time_bucket scalar assigns bucket start" \
    "SELECT ts, time_bucket(1h, ts) AS b FROM it.metrics WHERE series='cpu' LIMIT 1;" \
    '09:05:00.*09:00:00'

section "first / last / delta / rate / derivative"
check "first(value, ts) = 10" \
    "SELECT first(value, ts) FROM it.metrics WHERE series='cpu';" '^ *10'
check "last(value, ts) = 70" \
    "SELECT last(value, ts) FROM it.metrics WHERE series='cpu';" '^ *70'
check "delta(value, ts) = 60" \
    "SELECT delta(value, ts) FROM it.metrics WHERE series='cpu';" '^ *60'
check "rate(value, ts) = 60/6000s = 0.01" \
    "SELECT rate(value, ts) FROM it.metrics WHERE series='cpu';" '^ *0\.01'
# least-squares slope over (0s,10) (1800s,30) (4200s,50) (6000s,70) = 204000/20880000
check "derivative(value, ts) ~ 0.00977 (least squares, != rate)" \
    "SELECT derivative(value, ts) FROM it.metrics WHERE series='cpu';" '^ *0\.0097'

section "counters (reset aware)"
# counter samples 100 -> 300 -> 50: +200, then the drop is a reset so the new value counts
# in full => 250 over 120s. Plain rate() would see a -250 step instead.
check "counter_delta detects the reset (= 250)" \
    "SELECT counter_delta(total, ts) FROM it.counters WHERE series='api';" '^ *250'
check "counter_rate = 250/120s ~ 2.083" \
    "SELECT counter_rate(total, ts) FROM it.counters WHERE series='api';" '^ *2\.08'

section "percentile / spread / distribution"
check "percentile(value, 0.5) = 40" \
    "SELECT percentile(value, 0.5) FROM it.metrics WHERE series='cpu';" '^ *40'
check "percentile(value, 0.95) = 67" \
    "SELECT percentile(value, 0.95) FROM it.metrics WHERE series='cpu';" '^ *67'
check "variance(value) = 666.66 (sample)" \
    "SELECT variance(value) FROM it.metrics WHERE series='cpu';" '^ *666\.6'
check "stddev(value) = 25.81" \
    "SELECT stddev(value) FROM it.metrics WHERE series='cpu';" '^ *25\.8'
check "approx_count_distinct = 4" \
    "SELECT approx_count_distinct(value) FROM it.metrics WHERE series='cpu';" '^ *4'
check "histogram returns nbuckets+2 entries" \
    "SELECT histogram(value, 0, 100, 10) FROM it.metrics WHERE series='cpu';" '\[0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0\]'

section "integral / time-weighted average"
# trapezoids: (10+30)/2*1800 + (30+50)/2*2400 + (50+70)/2*1800 = 240000 value-seconds
# cqlsh prints large doubles in scientific notation, so accept both spellings of 240000
check "integral(value, ts) = 240000 value-seconds" \
    "SELECT integral(value, ts) FROM it.metrics WHERE series='cpu';" '^ *(240000|2\.4e\+05)'
check "time_weighted_average = 240000/6000 = 40" \
    "SELECT time_weighted_average(value, ts) FROM it.metrics WHERE series='cpu';" '^ *40'

section "two-variable statistics / regression (y = 2x + 1)"
check "regr_slope(y, x) = 2" "SELECT regr_slope(y, x) FROM it.xy WHERE k='r';" '^ *2'
check "regr_intercept(y, x) = 1" "SELECT regr_intercept(y, x) FROM it.xy WHERE k='r';" '^ *1'
check "regr_r2(y, x) = 1" "SELECT regr_r2(y, x) FROM it.xy WHERE k='r';" '^ *1'
check "corr(y, x) = 1" "SELECT corr(y, x) FROM it.xy WHERE k='r';" '^ *1'
check "covar_pop(y, x) = 2.5" "SELECT covar_pop(y, x) FROM it.xy WHERE k='r';" '^ *2\.5'
check "covar_samp(y, x) = 3.33" "SELECT covar_samp(y, x) FROM it.xy WHERE k='r';" '^ *3\.3'

section "gap-fill: time_bucket_gapfill + locf + interpolate"
GAPFILL="time_bucket_gapfill(1h, ts, '2024-01-01 08:00:00+0000', '2024-01-01 14:00:00+0000')"
# 6 buckets 08:00..13:00; real data only in 09:00 (60) and 12:00 (120)
check "gapfill materialises every bucket (6 rows)" \
    "SELECT $GAPFILL, avg(value) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '\(6 rows\)'
check "gapfill empty bucket is null by default" \
    "SELECT $GAPFILL, avg(value) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '10:00:00.*\| *null'
check "locf carries the previous bucket forward (10:00 -> 60)" \
    "SELECT $GAPFILL, locf(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '10:00:00.*\| *60'
check "locf leaves buckets before the first value null (08:00)" \
    "SELECT $GAPFILL, locf(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '08:00:00.*\| *null'
check "interpolate ramps 60 -> 120 linearly (10:00 -> 80)" \
    "SELECT $GAPFILL, interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '10:00:00.*\| *80'
check "interpolate ramps 60 -> 120 linearly (11:00 -> 100)" \
    "SELECT $GAPFILL, interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '11:00:00.*\| *100'
check "interpolate leaves the trailing bucket null (13:00)" \
    "SELECT $GAPFILL, interpolate(avg(value)) FROM it.metrics WHERE series='gap' GROUP BY series, $GAPFILL;" \
    '13:00:00.*\| *null'

section "dashboard query (all together)"
check "combined OHLC + change + p95 query runs" \
    "SELECT time_bucket(1h, ts) AS bucket, count(value) AS samples, first(value, ts) AS open,
            last(value, ts) AS close, min(value) AS low, max(value) AS high, avg(value) AS mean,
            delta(value, ts) AS change, rate(value, ts) AS per_second, percentile(value, 0.95) AS p95
     FROM it.metrics WHERE series='cpu' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000'
     GROUP BY series, time_bucket(1h, ts);" \
    '\(2 rows\)'

section "full-text search - SAI LIKE with index_analyzer"
cql "
CREATE TABLE IF NOT EXISTS it.logs (
    device text, ts timestamp, msg text,
    PRIMARY KEY (device, ts)
) WITH CLUSTERING ORDER BY (ts ASC);
CREATE INDEX IF NOT EXISTS logs_msg_idx ON it.logs(msg) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'ngram' };

INSERT INTO it.logs (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:00:00+0000', 'connection timeout on port 9042');
INSERT INTO it.logs (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:05:00+0000', '펌프 정지 알림');
INSERT INTO it.logs (device, ts, msg) VALUES ('pump-01', '2024-01-01 09:10:00+0000', 'connection refused');
INSERT INTO it.logs (device, ts, msg) VALUES ('pump-02', '2024-01-01 09:00:00+0000', 'timeout waiting for pump-02');
" > /dev/null
sleep 3   # wait for the index to become queryable

check "LIKE '%timeout%' within partition + time range" \
    "SELECT ts, msg FROM it.logs WHERE device='pump-01'
       AND ts >= '2024-01-01 09:00:00+0000' AND ts < '2024-01-01 10:00:00+0000'
       AND msg LIKE '%timeout%';" \
    'connection timeout on port 9042'
check "LIKE mid-word fragment '%imeou%' (true substring)" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%imeou%';" \
    'connection timeout on port 9042'
check "LIKE korean fragment '%정지%'" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%정지%';" \
    '펌프 정지 알림'
check "LIKE korean fragment crossing a space '%프 정%'" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%프 정%';" \
    '펌프 정지 알림'
check "LIKE excludes non-matching rows (1 row only)" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%refused%';" \
    '\(1 rows\)'
check "LIKE prefix 'connection%'" \
    "SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg LIKE 'connection%';" \
    '^ *2'
check "LIKE suffix '%9042'" \
    "SELECT msg FROM it.logs WHERE device='pump-01' AND msg LIKE '%9042';" \
    'connection timeout on port 9042'
check "'=' keeps exact whole-value semantics on the analyzed column" \
    "SELECT count(*) FROM it.logs WHERE device='pump-01' AND msg = 'connection refused';" \
    '^ *1'
check "LIKE combines with time_bucket aggregation" \
    "SELECT time_bucket(1h, ts), count(*) FROM it.logs
      WHERE device='pump-01' AND ts >= '2024-01-01 00:00:00+0000' AND ts < '2024-01-02 00:00:00+0000'
        AND msg LIKE '%connection%'
      GROUP BY device, time_bucket(1h, ts);" \
    '^ *2024-01-01 09:00:00.* 2'

section "tiered storage: policy, retier, chunk re-encode, late merge"
# Canonical time-series table for the re-encoder: single text partition key, timestamp
# clustering, double value (the exact shape TieringPolicy.canonicalSchemaError demands).
cql "
CREATE TABLE IF NOT EXISTS it.sensor (
    tag_id text, timestamp timestamp, value double,
    PRIMARY KEY (tag_id, timestamp)
);
" > /dev/null

# Policy: hot_window 1h / chunk_window 1h, so the rows inserted 4 hours in the past below are
# already closed. The extension value is the policy JSON as a hex blob literal — this constant is
# hex of exactly (regenerate with: printf '%s' '<json>' | od -An -v -tx1 | tr -d ' \n'):
#   {"hot_window":"1h","chunk_window":"1h","interval":"5m"}
TIERING_HEX=7b22686f745f77696e646f77223a223168222c226368756e6b5f77696e646f77223a223168222c22696e74657276616c223a22356d227d

# Hour-aligned chunk window 4 hours in the past (epoch-millis CQL timestamp literals throughout,
# so the assertions do not depend on any date-formatting tool).
NOW_MS=$(date +%s%3N)
WIN_MS=$(( (NOW_MS / 3600000 - 4) * 3600000 ))
WIN_END_MS=$(( WIN_MS + 3600000 ))

check "ALTER TABLE installs the tiering policy (hex JSON)" \
    "ALTER TABLE it.sensor WITH extensions = {'timeseries_tiering': 0x$TIERING_HEX};
     SELECT extensions FROM system_schema.tables WHERE keyspace_name='it' AND table_name='sensor';" \
    'timeseries_tiering'

cql "
INSERT INTO it.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS +  300000 )), 10);
INSERT INTO it.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS +  900000 )), 20);
INSERT INTO it.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS + 1500000 )), 30);
" > /dev/null

check_nodetool "nodetool retier runs one re-encode cycle" '' retier it sensor
check "chunk row created for the window (samples = 3)" \
    "SELECT samples FROM it.sensor__chunks WHERE tag_id='pump-01' AND window_start=$WIN_MS;" \
    '^ *3'
check "re-encoded rows stay transparently readable (SP3: merged from the chunk)" \
    "SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *3'
check "transparent point lookup returns the encoded value" \
    "SELECT value FROM it.sensor WHERE tag_id='pump-01' AND timestamp = $(( WIN_MS + 900000 ));" \
    '^ *20'
check "window remains queryable via the chunk table" \
    "SELECT tag_id, window_start, codec, samples FROM it.sensor__chunks WHERE tag_id='pump-01';" \
    '\(1 rows\)'

# Late row into the already-encoded window: its server-side writetime is newer than the range
# tombstone the first cycle issued (USING TIMESTAMP maxWt), so it survives to be merged.
cql "INSERT INTO it.sensor (tag_id, timestamp, value) VALUES ('pump-01', $(( WIN_MS + 2100000 )), 40);" > /dev/null
check "late row joins the merged view (3 chunk samples + 1 live late row)" \
    "SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *4'

check_nodetool "nodetool retier merges the late row" '' retier it sensor
check "chunk re-encoded merged (samples 3 -> 4)" \
    "SELECT samples FROM it.sensor__chunks WHERE tag_id='pump-01' AND window_start=$WIN_MS;" \
    '^ *4'
check "merged view still complete after the late row is re-encoded (4 chunk samples)" \
    "SELECT count(*) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *4'
check "aggregate spans the chunk transparently (avg of 10,20,30,40)" \
    "SELECT avg(value) FROM it.sensor WHERE tag_id='pump-01' AND timestamp >= $WIN_MS AND timestamp < $WIN_END_MS;" \
    '^ *25'

check_nodetool "nodetool tieringstatus lists it.sensor (interval 5m)" 'it +sensor +300000' tieringstatus
check "system_views.timeseries_tiering exposes policy and run stats" \
    "SELECT keyspace_name, table_name, windows_encoded, rows_encoded, late_merges
       FROM system_views.timeseries_tiering;" \
    'it \| +sensor \| +1 \| +1 \| +1'

echo
echo "== $PASS passed, $FAIL failed =="
write_report
[ "$FAIL" -eq 0 ] || exit 1
