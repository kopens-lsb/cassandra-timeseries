#!/usr/bin/env python3
"""Loader + timed query workload for the cassandra-timeseries scale test.

Runs *inside* the container (see docker/scale-test.sh) using the python driver that ships
with cqlsh, so the measured query times are the real client-observed CQL execution times
without any cqlsh/docker-exec startup in the way.

  python3 scale-workload.py load  --rows 100000000 --series 3000 --loaders 12
  python3 scale-workload.py query --rows 100000000 --series 3000 > report.html
"""
import argparse
import datetime
import glob
import html
import math
import multiprocessing
import os
import sys
import time

# cqlsh ships the driver as a zip; mirror what bin/cqlsh.py does to import it.
for _zip in glob.glob('/opt/cassandra/lib/cassandra-driver-internal-only-*.zip'):
    _ver = os.path.basename(_zip)[len('cassandra-driver-internal-only-'):-len('.zip')]
    sys.path.insert(0, os.path.join(_zip, 'cassandra-driver-' + _ver))
for _extra in ('geomet-0.1.0.zip', 'futures-2.1.6-py2.py3-none-any.zip'):
    _p = os.path.join('/opt/cassandra/lib', _extra)
    if os.path.exists(_p):
        sys.path.insert(0, _p)

from cassandra.cluster import Cluster                      # noqa: E402
from cassandra.query import BatchStatement, BatchType      # noqa: E402

KEYSPACE = 'scale'
TABLE = 'metrics'
EPOCH = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
BATCH_ROWS = 100          # rows per unlogged batch (stays under batch_size_warn_threshold)
IN_FLIGHT = 96            # async batches in flight per loader process


def connect(timeout=900):
    cluster = Cluster(['127.0.0.1'], connect_timeout=60)
    session = cluster.connect()
    session.default_timeout = timeout
    return cluster, session


def series_name(i):
    return 'sensor-%06d' % i


def sample_value(i):
    """Deterministic, non-trivial series: a slow sine plus a small sawtooth."""
    return 50.0 + 40.0 * math.sin(i / 500.0) + (i % 7)


def load_slice(args):
    worker, total_series, rows_per_series = args
    loaders = LOADERS
    cluster, session = connect()
    insert = session.prepare(
        "INSERT INTO %s.%s (series, ts, value) VALUES (?, ?, ?)" % (KEYSPACE, TABLE))
    written = 0
    started = time.time()
    pending = []
    for s in range(worker, total_series, loaders):
        name = series_name(s)
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        n = 0
        for i in range(rows_per_series):
            batch.add(insert, (name, EPOCH + datetime.timedelta(seconds=i), sample_value(i)))
            n += 1
            if n == BATCH_ROWS:
                pending.append(session.execute_async(batch))
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                n = 0
                if len(pending) >= IN_FLIGHT:
                    for f in pending:
                        f.result()
                    written += IN_FLIGHT * BATCH_ROWS
                    pending = []
        if n:
            pending.append(session.execute_async(batch))
        for f in pending:
            f.result()
        written += n + len(pending) * 0   # remainder counted below
        pending = []
        if worker == 0:
            done = (s // loaders) + 1
            per = (time.time() - started) or 1
            sys.stderr.write('   loader0: %d/%d partitions, %.0f rows/s (this worker)\n'
                             % (done, math.ceil(total_series / loaders),
                                done * rows_per_series / per))
            sys.stderr.flush()
    cluster.shutdown()
    return worker


def cmd_load(a):
    global LOADERS
    LOADERS = a.loaders
    rows_per_series = a.rows // a.series
    print('   %d partitions x %d rows = %d rows' % (a.series, rows_per_series,
                                                    a.series * rows_per_series))
    started = time.time()
    with multiprocessing.Pool(a.loaders, initializer=_init_loaders, initargs=(a.loaders,)) as pool:
        pool.map(load_slice, [(w, a.series, rows_per_series) for w in range(a.loaders)])
    elapsed = time.time() - started
    total = a.series * rows_per_series
    print('   loaded %d rows in %.0fs (%.0f rows/s)' % (total, elapsed, total / elapsed))


def _init_loaders(n):
    global LOADERS
    LOADERS = n


# --------------------------------------------------------------------------- query phase

def fmt_rows(rows, limit=8):
    if not rows:
        return '(0 rows)'
    out = []
    cols = rows[0]._fields
    out.append(' | '.join(cols))
    out.append('-+-'.join('-' * len(c) for c in cols))
    for r in rows[:limit]:
        vals = []
        for v in r:
            if isinstance(v, float):
                vals.append('%.6g' % v)
            elif isinstance(v, datetime.datetime):
                vals.append(v.strftime('%Y-%m-%d %H:%M:%S%z'))
            else:
                vals.append(str(v))
        out.append(' | '.join(vals))
    if len(rows) > limit:
        out.append('... (%d rows total)' % len(rows))
    else:
        out.append('(%d rows)' % len(rows))
    return '\n'.join(out)


def timed(session, cql):
    started = time.perf_counter()
    try:
        rows = list(session.execute(cql))
        ms = (time.perf_counter() - started) * 1000.0
        return ms, fmt_rows(rows), None
    except Exception as exc:                                  # noqa: BLE001 - reported in the HTML
        ms = (time.perf_counter() - started) * 1000.0
        return ms, '', '%s: %s' % (type(exc).__name__, exc)


def cmd_query(a):
    rows_per_series = a.rows // a.series
    one = series_name(0)
    ten = ', '.join("'%s'" % series_name(i) for i in range(10))
    hundred = ', '.join("'%s'" % series_name(i) for i in range(100))
    span_end = EPOCH + datetime.timedelta(seconds=rows_per_series)
    t0 = EPOCH.strftime('%Y-%m-%d %H:%M:%S+0000')
    t1 = span_end.strftime('%Y-%m-%d %H:%M:%S+0000')
    t6h = (EPOCH + datetime.timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S+0000')
    tbl = '%s.%s' % (KEYSPACE, TABLE)

    plan = [
        ('single partition (%d rows)' % rows_per_series, [
            ("count(*)", "SELECT count(*) FROM %s WHERE series='%s';" % (tbl, one)),
            ("time_bucket 1h + avg/min/max",
             "SELECT time_bucket(1h, ts), avg(value), min(value), max(value) FROM %s "
             "WHERE series='%s' GROUP BY series, time_bucket(1h, ts);" % (tbl, one)),
            ("time_bucket 5m + avg",
             "SELECT time_bucket(5m, ts), avg(value) FROM %s WHERE series='%s' "
             "GROUP BY series, time_bucket(5m, ts);" % (tbl, one)),
            ("first/last/delta/rate per hour",
             "SELECT time_bucket(1h, ts), first(value, ts), last(value, ts), delta(value, ts), "
             "rate(value, ts) FROM %s WHERE series='%s' GROUP BY series, time_bucket(1h, ts);"
             % (tbl, one)),
            ("derivative per hour",
             "SELECT time_bucket(1h, ts), derivative(value, ts) FROM %s WHERE series='%s' "
             "GROUP BY series, time_bucket(1h, ts);" % (tbl, one)),
            ("percentile p50/p95/p99 (whole partition)",
             "SELECT percentile(value, 0.5), percentile(value, 0.95), percentile(value, 0.99) "
             "FROM %s WHERE series='%s';" % (tbl, one)),
            ("variance/stddev (whole partition)",
             "SELECT variance(value), stddev(value) FROM %s WHERE series='%s';" % (tbl, one)),
            ("histogram 20 buckets",
             "SELECT histogram(value, 0, 100, 20) FROM %s WHERE series='%s';" % (tbl, one)),
            ("approx_count_distinct",
             "SELECT approx_count_distinct(value) FROM %s WHERE series='%s';" % (tbl, one)),
            ("integral + time_weighted_average",
             "SELECT integral(value, ts), time_weighted_average(value, ts) FROM %s "
             "WHERE series='%s';" % (tbl, one)),
        ]),
        ('gap-fill', [
            ("gapfill 1h + locf over the full span",
             "SELECT time_bucket_gapfill(1h, ts, '%s', '%s'), locf(avg(value)) FROM %s "
             "WHERE series='%s' GROUP BY series, time_bucket_gapfill(1h, ts, '%s', '%s');"
             % (t0, t1, tbl, one, t0, t1)),
            ("gapfill 5m + interpolate over 6h",
             "SELECT time_bucket_gapfill(5m, ts, '%s', '%s'), interpolate(avg(value)) FROM %s "
             "WHERE series='%s' AND ts >= '%s' AND ts < '%s' "
             "GROUP BY series, time_bucket_gapfill(5m, ts, '%s', '%s');"
             % (t0, t6h, tbl, one, t0, t6h, t0, t6h)),
        ]),
        ('multi-partition', [
            ("10 series, hourly avg (%d rows)" % (10 * rows_per_series),
             "SELECT series, time_bucket(1h, ts), avg(value) FROM %s WHERE series IN (%s) "
             "GROUP BY series, time_bucket(1h, ts);" % (tbl, ten)),
            ("100 series, hourly avg (%d rows)" % (100 * rows_per_series),
             "SELECT series, time_bucket(1h, ts), avg(value) FROM %s WHERE series IN (%s) "
             "GROUP BY series, time_bucket(1h, ts);" % (tbl, hundred)),
            ("100 series, p95 per series",
             "SELECT series, percentile(value, 0.95) FROM %s WHERE series IN (%s) "
             "GROUP BY series;" % (tbl, hundred)),
        ]),
        ('dashboard query', [
            ("OHLC + change + p95 per hour",
             "SELECT time_bucket(1h, ts) AS bucket, count(value) AS samples, "
             "first(value, ts) AS open, last(value, ts) AS close, min(value) AS low, "
             "max(value) AS high, avg(value) AS mean, delta(value, ts) AS change, "
             "rate(value, ts) AS per_second, percentile(value, 0.95) AS p95 "
             "FROM %s WHERE series='%s' GROUP BY series, time_bucket(1h, ts);" % (tbl, one)),
        ]),
        ('full table scan (%d rows)' % a.rows, [
            ("count(*) over the whole table",
             "SELECT count(*) FROM %s;" % tbl),
        ]),
    ]

    cluster, session = connect()
    results = []
    for section, items in plan:
        results.append(('section', section, '', 0.0, None))
        for desc, cql in items:
            sys.stderr.write('   timing: %s\n' % desc)
            sys.stderr.flush()
            ms, out, err = timed(session, cql)
            sys.stderr.write('      %.0f ms\n' % ms)
            sys.stderr.flush()
            results.append(('row', desc, cql, ms, (out, err)))
    cluster.shutdown()
    emit_html(a, results, rows_per_series)
    if a.md_out:
        with open(a.md_out, 'w') as fh:
            emit_md(a, results, rows_per_series, fh)
        sys.stderr.write('   markdown report written to %s\n' % a.md_out)


def emit_html(a, results, rows_per_series):
    total = a.series * rows_per_series
    thr = total / a.load_secs if a.load_secs else 0
    w = sys.stdout.write
    w("""<!doctype html>
<meta charset="utf-8">
<title>cassandra-timeseries · time-series CQL scale test</title>
<style>
  :root { color-scheme: light dark; --bg:#fff; --fg:#1a1a1a; --muted:#666; --line:#e3e3e3;
          --accent:#0a6cbd; --warn:#c62828; --code:#f6f7f9; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#14161a; --fg:#e6e6e6; --muted:#9aa0a6; --line:#2c3036;
            --accent:#6cb6ff; --warn:#ff6b6b; --code:#1c1f24; }
  }
  body { background:var(--bg); color:var(--fg); margin:0 auto; padding:2rem 1.25rem; max-width:1180px;
         font:14px/1.5 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif; }
  h1 { font-size:1.35rem; margin:0 0 .25rem; }
  .meta { color:var(--muted); font-size:.85rem; margin-bottom:1rem; }
  .meta code { background:var(--code); padding:.1rem .35rem; border-radius:4px; }
  .cards { display:flex; flex-wrap:wrap; gap:1rem; margin:1rem 0 1.5rem; }
  .card { background:var(--code); border-radius:8px; padding:.8rem 1.1rem; min-width:150px; }
  .card b { display:block; font-size:1.45rem; line-height:1.2; }
  .card span { color:var(--muted); font-size:.8rem; }
  table { border-collapse:collapse; width:100%; }
  td, th { border-top:1px solid var(--line); padding:.55rem .6rem; vertical-align:top; text-align:left; }
  th { color:var(--muted); font-weight:600; font-size:.8rem; text-transform:uppercase; letter-spacing:.04em; }
  tr.section td { background:var(--code); font-weight:600; }
  td.ms { white-space:nowrap; font-variant-numeric:tabular-nums; font-weight:600; color:var(--accent); text-align:right; }
  td.err { color:var(--warn); }
  pre { margin:0; background:var(--code); padding:.5rem .6rem; border-radius:6px; overflow-x:auto;
        font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; white-space:pre; }
  td:nth-child(2) { width:30%; } td:nth-child(3) { width:34%; }
</style>
""")
    w('<h1>time-series CQL scale test</h1>\n')
    w('<div class="meta">image <code>%s</code> · single node in a container · %s</div>\n'
      % (html.escape(a.image), time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())))
    w('<div class="cards">')
    w('<div class="card"><b>%s</b><span>rows loaded</span></div>' % f'{total:,}')
    w('<div class="card"><b>%s</b><span>partitions (%s rows each)</span></div>'
      % (f'{a.series:,}', f'{rows_per_series:,}'))
    w('<div class="card"><b>%s s</b><span>load time (%s rows/s)</span></div>'
      % (f'{a.load_secs:,}', f'{int(thr):,}'))
    w('</div>\n')
    w('<table><tr><th>assertion</th><th>query</th><th>result</th><th>CQL time</th></tr>\n')
    for kind, desc, cql, ms, payload in results:
        if kind == 'section':
            w('<tr class="section"><td colspan="4">%s</td></tr>\n' % html.escape(desc))
            continue
        out, err = payload
        body = ('<td class="err"><pre>%s</pre></td>' % html.escape(err)) if err \
            else ('<td><pre>%s</pre></td>' % html.escape(out))
        w('<tr><td>%s</td><td><pre>%s</pre></td>%s<td class="ms">%s ms</td></tr>\n'
          % (html.escape(desc), html.escape(cql), body, f'{ms:,.0f}'))
    w('</table>\n')


def emit_md(a, results, rows_per_series, out):
    total = a.series * rows_per_series
    thr = total / a.load_secs if a.load_secs else 0
    w = out.write
    w('# time-series CQL scale test\n\n')
    w('image `%s` · single node in a container · %s\n\n'
      % (a.image, time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())))
    w('- **%s** rows loaded\n' % f'{total:,}')
    w('- **%s** partitions (%s rows each)\n' % (f'{a.series:,}', f'{rows_per_series:,}'))
    w('- **%s s** load time (%s rows/s)\n' % (f'{a.load_secs:,}', f'{int(thr):,}'))
    w('\n## Query times\n\n| section | query | first result row | CQL time |\n|---|---|---|---|\n')
    section = ''
    for kind, desc, cql, ms, payload in results:
        if kind == 'section':
            section = desc
            continue
        out_text, err = payload
        lines = (err or out_text).splitlines()
        value = lines[2] if len(lines) > 2 else (lines[0] if lines else '')
        w('| %s | %s | `%s` | **%s ms** |\n'
          % (section, desc, value.replace('|', '\\|')[:60], f'{ms:,.0f}'))
    w('\n## Details\n')
    section = ''
    for kind, desc, cql, ms, payload in results:
        if kind == 'section':
            section = desc
            w('\n### %s\n' % section)
            continue
        out_text, err = payload
        w('\n**%s** — `%s ms`\n' % (desc, f'{ms:,.0f}'))
        w('\n```sql\n%s\n```\n' % cql)
        w('\n```\n%s\n```\n' % (err or out_text))


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)
    for name in ('load', 'query'):
        s = sub.add_parser(name)
        s.add_argument('--rows', type=int, default=100_000_000)
        s.add_argument('--series', type=int, default=3000)
        s.add_argument('--loaders', type=int, default=12)
        s.add_argument('--load-secs', type=int, default=0)
        s.add_argument('--image', default='cassandra-timeseries:6.0.0')
        s.add_argument('--md-out', default='', help='also write a Markdown report to this path')
    a = p.parse_args()
    (cmd_load if a.cmd == 'load' else cmd_query)(a)


if __name__ == '__main__':
    main()
