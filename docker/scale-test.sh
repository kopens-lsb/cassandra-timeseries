#!/bin/bash
#
# Scale test: load ~100M rows into a container-hosted node and measure how long the
# time-series CQL functions take on a real data set. Produces an HTML report with the
# measured server-side execution time of every query.
#
#   ./docker/scale-test.sh [image]
#     SCALE_ROWS=100000000      total rows to load
#     SCALE_SERIES=3000         partitions (rows per partition = SCALE_ROWS / SCALE_SERIES)
#     SCALE_LOADERS=12          loader processes inside the container
#     SCALE_DATA=/home/common/cassandra-ts-scale-data   host dir for the node's data files
#     SCALE_HEAP=16G
#     SCALE_REPORT=build/timeseries-scale-report.html
#
# The data directory is bind-mounted from the host because 100M rows do not fit in the
# container layer on a small /var partition.
#
set -uo pipefail

IMAGE="${1:-cassandra-timeseries:6.0.0}"
ROWS="${SCALE_ROWS:-100000000}"
SERIES="${SCALE_SERIES:-3000}"
LOADERS="${SCALE_LOADERS:-12}"
DATA="${SCALE_DATA:-/home/common/cassandra-ts-scale-data}"
HEAP="${SCALE_HEAP:-16G}"
REPORT="${SCALE_REPORT:-build/timeseries-scale-report.html}"
CONTAINER="cassandra-ts-scale"
RUNTIME="${CONTAINER_RUNTIME:-docker}"

echo "== cassandra-timeseries scale test =="
echo "   image=$IMAGE rows=$ROWS series=$SERIES loaders=$LOADERS heap=$HEAP"
echo "   data=$DATA report=$REPORT"

SKIP_LOAD="${SCALE_SKIP_LOAD:-0}"      # reuse an already-loaded data directory
$RUNTIME rm -f "$CONTAINER" > /dev/null 2>&1
[ "$SKIP_LOAD" = 1 ] || rm -rf "$DATA"
mkdir -p "$DATA"; chmod 777 "$DATA"
mkdir -p "$(dirname "$REPORT")"

# Aggregating over millions of rows takes longer than the stock request timeouts allow, so raise
# them before the node starts. Everything else is the shipped configuration.
$RUNTIME run -d --name "$CONTAINER" \
    -e MAX_HEAP_SIZE="$HEAP" -e HEAP_NEWSIZE=4G \
    -v "$DATA:/opt/cassandra/data" \
    --entrypoint bash "$IMAGE" -c '
        CFG="$CASSANDRA_CONF/cassandra.yaml"
        sed -ri "s/^read_request_timeout:.*/read_request_timeout: 600000ms/" "$CFG"
        sed -ri "s/^range_request_timeout:.*/range_request_timeout: 600000ms/" "$CFG"
        sed -ri "s/^write_request_timeout:.*/write_request_timeout: 60000ms/" "$CFG"
        sed -ri "s/^request_timeout:.*/request_timeout: 600000ms/" "$CFG"
        # native_transport_timeout caps the whole request server-side (default 12s) and is not
        # present in the shipped yaml, so it has to be appended rather than substituted.
        grep -q "^native_transport_timeout:" "$CFG" \
            && sed -ri "s/^native_transport_timeout:.*/native_transport_timeout: 600s/" "$CFG" \
            || printf "\nnative_transport_timeout: 600s\n" >> "$CFG"
        exec docker-entrypoint.sh
    ' > /dev/null || { echo "FATAL: container did not start"; exit 2; }

printf '   waiting for CQL'
ready=0
for _ in $(seq 1 120); do
    if $RUNTIME exec "$CONTAINER" cqlsh -e "SELECT release_version FROM system.local" > /dev/null 2>&1; then
        ready=1; break
    fi
    printf '.'; sleep 5
done
echo
[ "$ready" = 1 ] || { echo "FATAL: node not ready"; $RUNTIME logs "$CONTAINER" 2>&1 | tail -30; exit 2; }

$RUNTIME exec "$CONTAINER" cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS scale WITH replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS scale.metrics (
    series text, ts timestamp, value double,
    PRIMARY KEY (series, ts)
) WITH CLUSTERING ORDER BY (ts ASC)
   AND compaction = {'class':'UnifiedCompactionStrategy',
                     'scaling_parameters':'T4',
                     'target_sstable_size':'1GiB',
                     'expired_sstable_check_frequency_seconds':600};
" || { echo "FATAL: schema creation failed"; exit 2; }

# ---------------------------------------------------------------- load
# Loader and query client both run inside the container against 127.0.0.1 with the python
# driver that ships with cqlsh, so the measured times contain no docker-exec/cqlsh startup.
$RUNTIME cp docker/scale-workload.py "$CONTAINER:/tmp/scale-workload.py"

if [ "$SKIP_LOAD" = 1 ]; then
    LOAD_SECS="${SCALE_LOAD_SECS:-0}"
    echo "-- reusing the data already in $DATA (skipping load)"
else
    echo "-- loading $ROWS rows ($SERIES partitions, $LOADERS processes)"
    LOAD_START=$(date +%s)
    $RUNTIME exec "$CONTAINER" python3 /tmp/scale-workload.py load \
        --rows "$ROWS" --series "$SERIES" --loaders "$LOADERS" || { echo "FATAL: load failed"; exit 2; }
    LOAD_SECS=$(( $(date +%s) - LOAD_START ))
    echo "-- load finished in ${LOAD_SECS}s"
fi

$RUNTIME exec "$CONTAINER" nodetool flush scale metrics
$RUNTIME exec "$CONTAINER" nodetool tablestats scale.metrics 2>/dev/null | grep -E "Space used \(live\)|Number of partitions" | head -2

# ---------------------------------------------------------------- query + report
echo "-- timing time-series queries"
$RUNTIME exec "$CONTAINER" python3 /tmp/scale-workload.py query \
    --rows "$ROWS" --series "$SERIES" --load-secs "$LOAD_SECS" --image "$IMAGE" \
    --md-out /tmp/scale-report.md > "$REPORT"
rc=$?

if [ $rc -ne 0 ]; then
    echo "FATAL: query phase failed"; exit 1
fi
# The Markdown twin is the copy that gets committed (GitLab renders .md, not .html blobs).
$RUNTIME cp "$CONTAINER:/tmp/scale-report.md" "${REPORT%.html}.md"
echo "   report: $REPORT"
echo "   report: ${REPORT%.html}.md"
echo "-- container '$CONTAINER' left running; remove with: $RUNTIME rm -f $CONTAINER"
