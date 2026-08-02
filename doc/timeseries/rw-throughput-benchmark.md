# 초당 읽기/쓰기 처리량 벤치마크

이미지 `cassandra-timeseries:6.0.0-rwbench` (master `f45485ce39`, 스트리밍 memtable 읽기 + direct-memory 픽스 포함) · 단일 노드 컨테이너 · 2026-08-03 05:27–05:46 KST

기존 [스케일 테스트](scale-test-report.md)가 **분석 쿼리 1건의 실행 시간**을 재는 것이라면, 이
벤치는 **초당 몇 건을 처리하는가(ops/s, rows/s)** 를 잰다. 운영 형태(`tm_tag_point`) 워크로드와
표준 `cassandra-stress` 워크로드를 둘 다 측정해, 앞의 것으로 "실제 이 스키마로 얼마나 나오는가"를,
뒤의 것으로 "서버 자체의 한계가 어디인가"를 본다.

## 한눈에

| 항목 | 결과 | 지연시간 |
|---|---|---|
| **쓰기** — 운영 형태 `tm_tag_point`, 100행 unlogged batch × 12 프로세스 | **145,406 rows/s** (2,000만 행 / 138 s) | — |
| **쓰기** — `cassandra-stress` 단건 INSERT, 128 스레드 | **82,508 ops/s** | p50 1.2 ms · p99 5.7 ms |
| **읽기** — `cassandra-stress` 단건(파티션) 조회, 128 스레드 | **64,610 ops/s** | p50 1.6 ms · p99 7.5 ms |
| **읽기** — 위와 같으나 256 스레드 (**서버 포화점**) | **66,492 ops/s** (+3 %뿐 → 한계) | p50 3.0 ms · p99 13.7 ms |
| **읽기** — `tm_tag_point` 태그 최신값 (`LIMIT 1`) | 16,426 ops/s † | p50 0.91 ms · p99 2.04 ms |
| **읽기** — `tm_tag_point` 타임스탬프 단건 | 14,777 ops/s † | p50 1.19 ms · p99 2.70 ms |
| **읽기** — `tm_tag_point` 100행 시간창 스캔 | 2,850 ops/s = **285,009 rows/s** | p50 3.16 ms · p99 5.08 ms |

† 운영 형태 읽기는 cqlsh 번들 **순수 파이썬 드라이버**로 측정한 값이라 클라이언트가 먼저
포화한다(워커당 ~1.4 k ops/s). 서버가 실제로 감당하는 단건 조회 한계는 stress가 보여준
**~66 k ops/s** 쪽이다. 즉 † 값들은 하한치다. 지연시간은 depth-1 순차 프로브(2,000회)의 값.

## 환경

| 항목 | 값 |
|---|---|
| 호스트 | 2 × Xeon X5670 (합계 24 스레드, 2.93 GHz Westmere) · 62 GB RAM · RAID(PERC H700, 비회전) |
| 컨테이너 | `--cpus=24 --memory=48g` — **요청 사양은 32 코어/64 GB였으나 호스트 물리 한계(24 코어/62 GB)로 조정**. 메모리 상한 48 GB는 같은 호스트의 다른 서비스(23 GB 사용 중) 보호선 |
| JVM | OpenJDK 21.0.11 · 힙 16 GB · generational ZGC (출하 설정) |
| 노드 설정 | [docker/bench-node-config.sh](../../docker/bench-node-config.sh) — 스케일/계층화 벤치와 동일 (타임아웃 상향 외 출하 설정) |
| 토폴로지 | 단일 노드, RF=1, CL=LOCAL_ONE — 복제·쿼럼 비용이 없는 수치 |
| 클라이언트 | 같은 컨테이너 안에서 실행 — 네트워크 왕복이 없는 대신 서버와 CPU를 나눠 씀 |

2010년대 초 CPU라는 점을 감안할 것: 현세대 서버에서는 절대치가 이보다 상당히 높게 나온다.

## 쓰기 상세

**운영 형태** — [docker/scale-workload.py](../../docker/scale-workload.py) `load`가 2,000만 행
(500 태그 × 40,000행, 측정된 운영 값 분포 그대로)을 12개 로더 프로세스, 프로세스당 96개
in-flight, 100행 unlogged batch로 적재:

- **145,406 rows/s** 지속 (138 s), 로더당 ~12.1 k rows/s로 처음부터 끝까지 평탄
- 적재 후 flush 기준 디스크 237.7 MB / SSTable 6개 → **행당 ~11.9 B** (LZ4 압축 후, 계층화 전)
- 오류 0

**cassandra-stress** — `write n=20000000 -rate threads=128` (표준 `standard1` 스키마, 행당
34 B blob 5컬럼, 단건 INSERT): **82,508 ops/s**, p50 1.2 ms / p95 3.5 ms / p99 5.7 ms /
p99.9 10.2 ms, 4 분 2 초, 오류 0. 배치 없이 요청 1건 = 1행일 때의 수치다.

## 읽기 상세

읽기 전 `nodetool flush` 완료. 20 GB(stress)+0.24 GB(tm_tag_point) 데이터는 페이지 캐시에
충분히 들어가는 크기라 디스크가 아니라 CPU가 병목이다.

**cassandra-stress** — `read duration=60s -pop dist='uniform(1..20000000)'`:

| 스레드 | ops/s | p50 | p95 | p99 | p99.9 |
|---|---|---|---|---|---|
| 128 | 64,610 | 1.6 ms | 4.5 ms | 7.5 ms | 14.0 ms |
| 256 | 66,492 | 3.0 ms | 8.8 ms | 13.7 ms | 23.1 ms |

스레드를 2배로 올려도 처리량이 3 %만 늘고 지연시간은 2배가 됐다 — **이 장비의 단건 조회
한계는 약 66 k ops/s**이고 128 스레드 시점에 이미 사실상 포화다.

**운영 형태** — [docker/rwbench-read.py](../../docker/rwbench-read.py), 12 워커 × 64 in-flight
× 60 s, 패턴별 2패스:

| 패턴 | 쿼리 | 1차 패스 | 2차 패스 | 순차 지연 (p50/p99) |
|---|---|---|---|---|
| `latest` | `WHERE tag_id=? LIMIT 1` | 16,426 ops/s | 12,130 ops/s | 0.91 / 2.04 ms |
| `point` | `WHERE tag_id=? AND timestamp=?` | 14,777 ops/s | 9,618 ops/s | 1.19 / 2.70 ms |
| `range100` | 100 초 시간창 (100행) | 2,850 ops/s = 285,009 rows/s | 2,776 ops/s = 277,586 rows/s | 3.16 / 5.08 ms |

윈도우 스캔이 **행 기준으로는 단건 조회의 17배**(285 k rows/s)를 뽑아낸다 — 요청 1건의 고정
비용이 지배적이라, 시계열 조회는 넓게 끊어 읽는 쪽이 압도적으로 유리하다는 뜻이다.

## 해석과 주의점

- **2차 패스가 1차보다 느린 이유는 컴팩션이다.** 적재 직후라 벤치 내내 UCS 컴팩션이 돌았고
  (누적 9회, 768 MB 처리) 읽기와 CPU를 놓고 경쟁했다. 페이지 캐시는 두 패스 모두 뜨거웠으므로
  cold/warm 차이가 아니다. 컴팩션이 끝난 정상 상태에서는 1차 패스 쪽 수치에 가깝다.
- **운영 형태 읽기 ops/s는 클라이언트 하한치다.** 순수 파이썬 드라이버는 워커당 ~1.4 k ops/s에서
  먼저 포화한다. 같은 노드에서 Java 클라이언트(stress)는 66 k ops/s까지 뽑았다.
- **RF=1 · CL=LOCAL_ONE · 클라이언트 동거**라는 점에서 절대치는 낙관적이고(복제 비용 없음),
  동시에 클라이언트가 서버 CPU를 깎아 먹는다는 점에서는 비관적이다. 3노드 RF=3 쿼럼이라면
  쓰기는 대략 1/2–1/3 수준을 예상하는 것이 안전하다.
- 쓰기 145 k rows/s는 **100행 배치** 덕이다. 단건 쓰기의 한계는 stress의 82.5 k ops/s 쪽이 맞다.
  운영 유입(현 24 k rows/s)의 6배 여유.

## 재현

```bash
docker build -t cassandra-timeseries:6.0.0-rwbench -f docker/Dockerfile .

# 노드: bench-node-config.sh를 적용해 기동 (scale-test.sh와 동일한 방식, 24c/48g 제한)
docker run -d --name cassandra-ts-rwbench --network ts-scale-net --ip 172.30.0.11 \
  --cpus=24 --memory=48g -e MAX_HEAP_SIZE=16G -e HEAP_NEWSIZE=4G -e GC_CHOICE=zgc \
  -v /path/to/data:/opt/cassandra/data \
  --entrypoint bash cassandra-timeseries:6.0.0-rwbench \
  -c "$(cat docker/bench-node-config.sh); exec docker-entrypoint.sh"

# 쓰기(운영 형태): 적재가 곧 측정 — rows/s를 출력
docker cp docker/scale-workload.py cassandra-ts-rwbench:/tmp/
docker exec cassandra-ts-rwbench sh -c \
  "python3 /tmp/scale-workload.py ddl --table tm_tag_point > /tmp/s.cql && cqlsh -f /tmp/s.cql"
docker exec cassandra-ts-rwbench python3 /tmp/scale-workload.py load \
  --rows 20000000 --series 500 --loaders 12 --table tm_tag_point

# 읽기(운영 형태): flush 후 패턴별 60초
docker exec cassandra-ts-rwbench nodetool flush scale tm_tag_point
docker cp docker/rwbench-read.py cassandra-ts-rwbench:/tmp/
docker exec cassandra-ts-rwbench python3 /tmp/rwbench-read.py \
  --series 500 --rows-per-series 40000 --workers 12 --inflight 64 \
  --duration 60 --pattern latest   # point | range100 도 동일

# 한계치(표준 stress): tools/bin은 이미지에서 실행권한이 없어 chmod가 필요
docker exec cassandra-ts-rwbench chmod +x /opt/cassandra/tools/bin/cassandra-stress
docker exec cassandra-ts-rwbench sh -c \
  "/opt/cassandra/tools/bin/cassandra-stress write n=20000000 -rate threads=128"
docker exec cassandra-ts-rwbench sh -c \
  "/opt/cassandra/tools/bin/cassandra-stress read duration=60s \
     -pop dist='uniform(1..20000000)' -rate threads=128"
```
