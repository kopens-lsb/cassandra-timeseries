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
GC="${SCALE_GC:-zgc}"                  # zgc (generational, as shipped) | g1
PASSES="${SCALE_PASSES:-1}"            # query passes; the last one is the reported (warm) run
WBENCH_ROWS="${SCALE_WBENCH_ROWS:-0}"  # if >0, also time a write of this many rows
CONTAINER="cassandra-ts-scale"
RUNTIME="${CONTAINER_RUNTIME:-docker}"
# A reused data directory pins the node's address in cluster metadata, so the container needs a
# stable IP: on the default bridge docker hands out whatever is free and the node then tries to
# gossip with its former self and never reaches NORMAL. Hence a dedicated network + static IP.
NET="${SCALE_NET:-ts-scale-net}"
NET_SUBNET="${SCALE_NET_SUBNET:-172.30.0.0/16}"
NODE_IP="${SCALE_NODE_IP:-172.30.0.10}"

echo "== cassandra-timeseries scale test =="
echo "   image=$IMAGE rows=$ROWS series=$SERIES loaders=$LOADERS heap=$HEAP gc=$GC passes=$PASSES"
echo "   data=$DATA report=$REPORT"

SKIP_LOAD="${SCALE_SKIP_LOAD:-0}"      # reuse an already-loaded data directory
$RUNTIME rm -f "$CONTAINER" > /dev/null 2>&1
[ "$SKIP_LOAD" = 1 ] || rm -rf "$DATA"
mkdir -p "$DATA"; chmod 777 "$DATA"
mkdir -p "$(dirname "$REPORT")"

# Aggregating over millions of rows takes longer than the stock request timeouts allow, so raise
# them before the node starts. Everything else is the shipped configuration.
$RUNTIME network inspect "$NET" > /dev/null 2>&1 \
    || $RUNTIME network create --subnet "$NET_SUBNET" "$NET" > /dev/null

$RUNTIME run -d --name "$CONTAINER" \
    --network "$NET" --ip "$NODE_IP" \
    -e MAX_HEAP_SIZE="$HEAP" -e HEAP_NEWSIZE=4G -e GC_CHOICE="$GC" \
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
        OPTS="$CASSANDRA_CONF/jvm21-server.options"
        if [ "$GC_CHOICE" = g1 ]; then
            # Turn the shipped generational-ZGC block off and the G1 block on, and restore
            # compressed oops (the -UseCompressedOops line is a ZGC-only workaround).
            sed -ri "s/^-XX:\+UseZGC/#-XX:+UseZGC/; s/^-XX:\+ZGenerational/#-XX:+ZGenerational/" "$OPTS"
            sed -ri "s/^-XX:-UseCompressedOops/#-XX:-UseCompressedOops/" "$OPTS"
            sed -ri "s/^#(-XX:\+UseG1GC|-XX:\+ParallelRefProcEnabled|-XX:MaxTenuringThreshold=2|-XX:G1HeapRegionSize=16m|-XX:\+UnlockExperimentalVMOptions|-XX:G1NewSizePercent=50|-XX:G1RSetUpdatingPauseTimePercent=5|-XX:MaxGCPauseMillis=300)$/\1/" "$OPTS"
        fi
        # safepoint logging is what makes the two collectors comparable: it records the actual
        # stop-the-world time, which for ZGC is not visible in the plain gc lines.
        printf "\n-Xlog:gc*,safepoint:file=%s/logs/gc.log:time,uptime:filecount=0\n" "$CASSANDRA_HOME" >> "$OPTS"
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
echo "-- timing time-series queries ($PASSES pass(es), gc=$GC)"
pass=1
while [ "$pass" -le "$PASSES" ]; do
    [ "$pass" -lt "$PASSES" ] && echo "   warm-up pass $pass/$PASSES"
    $RUNTIME exec "$CONTAINER" python3 /tmp/scale-workload.py query \
        --rows "$ROWS" --series "$SERIES" --load-secs "$LOAD_SECS" --image "$IMAGE" \
        --md-out /tmp/scale-report.md --json-out /tmp/scale-results.json > "$REPORT"
    rc=$?
    [ $rc -eq 0 ] || break
    pass=$((pass + 1))
done

if [ $rc -ne 0 ]; then
    echo "FATAL: query phase failed"; exit 1
fi
# The Markdown twin is the copy that gets committed (GitLab renders .md, not .html blobs).
$RUNTIME cp "$CONTAINER:/tmp/scale-report.md" "${REPORT%.html}.md"
$RUNTIME cp "$CONTAINER:/tmp/scale-results.json" "${REPORT%.html}.json"

if [ "$WBENCH_ROWS" -gt 0 ]; then
    echo "-- write benchmark: $WBENCH_ROWS rows into scale.wbench (gc=$GC)"
    $RUNTIME exec "$CONTAINER" cqlsh -e "
        DROP TABLE IF EXISTS scale.wbench;
        CREATE TABLE scale.wbench (series text, ts timestamp, value double, PRIMARY KEY (series, ts))
          WITH compaction = {'class':'UnifiedCompactionStrategy','scaling_parameters':'T4'};"
    $RUNTIME exec "$CONTAINER" python3 /tmp/scale-workload.py load \
        --rows "$WBENCH_ROWS" --series 200 --loaders "$LOADERS" --table wbench \
        --json-out /tmp/wbench.json
    $RUNTIME cp "$CONTAINER:/tmp/wbench.json" "${REPORT%.html}-write.json"
fi

# GC pause summary straight from the JVM log
$RUNTIME exec "$CONTAINER" sh -c 'cat $CASSANDRA_HOME/logs/gc.log' > "${REPORT%.html}-gc.log" 2>/dev/null
echo "   gc log: ${REPORT%.html}-gc.log"
echo "   report: $REPORT"
echo "   report: ${REPORT%.html}.md"
echo "-- container '$CONTAINER' left running; remove with: $RUNTIME rm -f $CONTAINER"
