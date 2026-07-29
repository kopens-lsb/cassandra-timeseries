# cassandra-timeseries

**Apache Cassandra 6.0.0 + 네이티브 시계열 함수 — 분산 시계열 데이터베이스.**

[apache/cassandra](https://github.com/apache/cassandra)(`cassandra-6.0` 브랜치)의 포크로, 서버 사이드에서 동작하는 시계열 CQL 함수를 추가했습니다. Spark 연동은 짝이 되는 포크 [cassandra-spark-connector](https://dev.kopens.io/common/cassandra-spark-connector)(Spark 4.1.2)로 제공됩니다.

## 📖 문서

| 문서 | 내용 |
| --- | --- |
| **[사용 예제 (examples.md)](doc/timeseries/examples.md)** | 아래 "시계열 CQL 사용법"의 원본 예제 모음 (영문) |
| [시계열 함수 설계 (timeseries-functions-design.md)](doc/timeseries/timeseries-functions-design.md) | 각 함수의 시그니처·의미론(semantics), 분산 환경에서의 정확성, 코드 위치 |
| [Gap-Fill 설계 (gapfill-design.md)](doc/timeseries/gapfill-design.md) | `time_bucket_gapfill`의 CQL 문법, 보간 규칙, 가드레일 |
| [Continuous Aggregates 설계 (continuous-aggregates-design.md)](doc/timeseries/continuous-aggregates-design.md) | 시간 버킷 롤업(연속 집계) 설계안 — 진행 중 |
| [통합 테스트 보고서](doc/timeseries/integration-test-report.md) | 실제 컨테이너에서 실행한 32개 검증의 CQL·결과·소요 시간 |
| [스케일 테스트 보고서 (1억 건)](doc/timeseries/scale-test-report.md) | 1억 행 적재 후 측정한 쿼리별 CQL 실행 시간 |

전체 문서 디렉터리: [doc/timeseries/](doc/timeseries/)

---

# 시계열 CQL 사용법

별도 설치나 UDF 등록 없이 `cqlsh`에서 바로 쓸 수 있는 네이티브 함수입니다. 아래 예제는 모두 실행 가능한 CQL입니다.

## 함수 레퍼런스

**인자 순서가 중요합니다.** 대부분의 시계열 집계는 `(값, 타임스탬프)` 순서입니다.

| 함수 | 시그니처 | 반환 | 설명 |
| --- | --- | --- | --- |
| `time_bucket` | `time_bucket(duration, ts [, origin])` | `timestamp` | 버킷 시작 시각 (다운샘플링용 스칼라) |
| `time_bucket_gapfill` | `time_bucket_gapfill(width, ts, start, finish)` | `timestamp` | 빈 버킷까지 생성하는 `GROUP BY` 셀렉터 |
| `locf` | `locf(집계)` | 인자와 동일 | 빈 버킷을 직전 값으로 채움 (LOCF) |
| `interpolate` | `interpolate(집계)` | `double` | 빈 버킷을 앞뒤 값의 선형 보간으로 채움 |
| `first` / `last` | `first(value, ts)` / `last(value, ts)` | `value`의 타입 | 시각 기준 최초/최종 값 |
| `delta` | `delta(value, ts)` | `double` | 마지막 − 첫 샘플 |
| `rate` | `rate(value, ts)` | `double` | `delta` ÷ 경과 초 (양 끝점 기준) |
| `derivative` | `derivative(value, ts)` | `double` | 최소제곱 회귀 기울기 (초당) |
| `counter_delta` / `counter_rate` | `counter_delta(value, ts)` / `counter_rate(value, ts)` | `double` | 리셋을 보정한 카운터 증가량 / 초당 증가율 |
| `percentile` | `percentile(value, q)` — `q`는 `[0,1]` | `double` | 정확한 연속 백분위 (선형 보간) |
| `time_weighted_average` | `time_weighted_average(value, ts)` | `double` | 시간 가중 평균 |
| `integral` | `integral(value, ts)` | `double` | 곡선 아래 면적 (value·초) |
| `variance` / `stddev` | `variance(value)` / `stddev(value)` | `double` | 표본 분산 / 표준편차 |
| `corr` / `covar_pop` / `covar_samp` | `corr(y, x)` 등 | `double` | 상관계수 / 모공분산 / 표본공분산 |
| `regr_slope` / `regr_intercept` / `regr_r2` | `regr_slope(y, x)` 등 | `double` | y의 x에 대한 선형 회귀 |
| `histogram` | `histogram(value, min, max, nbuckets)` | `list<bigint>` | 등간격 히스토그램 (길이 `nbuckets+2`) |
| `approx_count_distinct` | `approx_count_distinct(value)` | `bigint` | HyperLogLog 근사 고유값 개수 |

## 1. 스키마와 샘플 데이터

시계열의 정석 스키마입니다. 시리즈당 파티션 하나, 시간으로 클러스터링. 컴팩션은 Cassandra 6.0의 통합 전략인 `UnifiedCompactionStrategy`(UCS)를 씁니다 — 쓰기 위주(append-only) 시계열에는 tiered 쪽 스케일링 파라미터가 맞고, TTL로 만료된 SSTable은 주기적으로 통째 삭제됩니다.

```sql
CREATE KEYSPACE IF NOT EXISTS ts
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ts;

CREATE TABLE metrics (
    series text,            -- 예: 'cpu.host1'
    ts     timestamp,
    value  double,
    PRIMARY KEY (series, ts)
) WITH CLUSTERING ORDER BY (ts ASC)
   AND compaction = {'class': 'UnifiedCompactionStrategy',
                     'scaling_parameters': 'T4',                        -- tiered(쓰기 최적) 4-way
                     'target_sstable_size': '1GiB',
                     'expired_sstable_check_frequency_seconds': 600}
   AND default_time_to_live = 2592000;   -- 30일

INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:05:00+0000', 10);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:35:00+0000', 30);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:15:00+0000', 50);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:45:00+0000', 70);
```

### 1.1 UCS 컴팩션 옵션 요약

Cassandra 6.0의 UCS는 STCS/LCS/TWCS를 하나의 전략으로 통합했습니다. 시계열에서 중요한 옵션은 다음과 같습니다.

| 옵션 | 설명 |
| --- | --- |
| `scaling_parameters` | `T4` = 4-way tiered(쓰기 최적, 시계열 수집에 적합), `L10` = 10-way leveled(읽기 최적), `N` = 중간. 레벨별로 `'T4, T4, L10'`처럼 콤마로 나열 가능 |
| `target_sstable_size` | 샤딩된 SSTable의 목표 크기(예: `1GiB`). 컴팩션 단위를 잘게 유지해 만료 데이터 회수를 빠르게 함 |
| `base_shard_count` | 최하위 레벨의 샤드 수. 병렬 컴팩션 정도를 결정 |
| `expired_sstable_check_frequency_seconds` | 전부 만료된 SSTable을 찾아 통째로 버리는 주기(기본 600초) |

TTL로 오래된 데이터를 버리는 시계열이라면 `default_time_to_live`를 지정하고 위 검사 주기를 짧게 유지하면, 만료된 SSTable이 컴팩션 없이 바로 회수됩니다.

## 2. 버킷팅 · 다운샘플링 — `time_bucket`

### 2.1 각 행에 버킷 붙이기 (스칼라로 사용)

```sql
SELECT ts, time_bucket(1h, ts) AS bucket, value
FROM   metrics
WHERE  series = 'cpu';
```

### 2.2 `GROUP BY`로 고정 간격 다운샘플링

```sql
-- 시간별 평균 / 최소 / 최대 / 개수
SELECT time_bucket(1h, ts) AS bucket,
       avg(value), min(value), max(value), count(value)
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket(1h, ts);

-- 다른 간격: 5분, 1일
SELECT time_bucket(5m, ts) AS bucket, avg(value) FROM metrics
  WHERE series = 'cpu' GROUP BY series, time_bucket(5m, ts);

SELECT time_bucket(1d, ts) AS bucket, avg(value) FROM metrics
  WHERE series = 'cpu' GROUP BY series, time_bucket(1d, ts);
```

`time_bucket`은 `GROUP BY`의 **마지막 요소**(파티션 키 컬럼 뒤)로 와야 그룹핑이 읽기 경로로 내려갑니다.

### 2.3 기준점(origin)을 옮긴 버킷

```sql
-- 30분 밀린 1시간 버킷: [08:30, 09:30), [09:30, 10:30), ...
SELECT time_bucket(1h, ts, '2024-01-01 00:30:00+0000') AS bucket, avg(value)
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket(1h, ts, '2024-01-01 00:30:00+0000');
```

## 3. 빈 구간 채우기 — `time_bucket_gapfill`

일반 `time_bucket`은 **데이터가 있는 버킷만** 반환합니다. `time_bucket_gapfill`은 `[start, finish)` 범위의 **모든** 버킷에 대해 행을 만들어 주므로, 대시보드가 끊김 없는 시간축을 얻습니다. 데이터가 없는 버킷의 집계값은 기본적으로 null입니다.

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       avg(value)
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

### 3.1 `locf()` — 직전 값 이어가기

집계를 `locf(...)`로 감싸면 빈 버킷이 null 대신 **직전 비어있지 않은 버킷의 값**을 그대로 이어받습니다(last-observation-carried-forward).

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       locf(avg(value))   -- 빈 버킷은 직전 시간의 평균을 반복
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

`locf`는 실제 데이터가 있는 행에는 아무 영향이 없습니다(인자를 그대로 반환). 첫 실제 값보다 앞선 버킷은 이어받을 값이 없으므로 null로 남습니다.

### 3.2 `interpolate()` — 선형 보간

빈 버킷을 앞뒤 값 사이에서 선형 보간하려면 `interpolate(...)`를 씁니다(결과 타입은 `double`).

```sql
SELECT time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'),
       interpolate(avg(value))   -- 빈 버킷은 양옆 값 사이를 직선으로 채움
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

첫 실제 값 이전 / 마지막 실제 값 이후의 빈 버킷은 보간할 대상이 없으므로 null로 남습니다.

### 3.3 여러 시리즈

시리즈별로 독립적으로 채워집니다. 파티션 키를 `SELECT`와 `GROUP BY` 양쪽에 포함하세요.

```sql
SELECT series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000'), avg(value)
FROM   metrics WHERE series IN ('cpu', 'mem')
GROUP  BY series, time_bucket_gapfill(1h, ts, '2024-01-01 00:00:00+0000', '2024-01-02 00:00:00+0000');
```

### 3.4 제약 사항

- 폭(width)은 **고정 길이**여야 합니다(월 단위 성분 불가).
- 버킷 컬럼에 별칭(alias)을 붙이지 마세요.
- 버킷 범위를 가로지르는 페이징은 피하세요.
- 범위 ÷ 폭이 **1,000,000 버킷**을 넘으면 쿼리가 거부됩니다.

## 4. 최초/최종 값 — `first`, `last`

```sql
-- 시리즈 시가/종가
SELECT first(value, ts) AS day_open,
       last(value, ts)  AS day_close
FROM   metrics
WHERE  series = 'cpu';
```

시간별 OHLC(시가/고가/저가/종가) 캔들:

```sql
SELECT time_bucket(1h, ts) AS bucket,
       first(value, ts) AS open,
       max(value)       AS high,
       min(value)       AS low,
       last(value, ts)  AS close
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket(1h, ts);
```

`first`/`last`는 **삽입 순서가 아니라 타임스탬프 인자 기준**으로 정렬하므로, 순서가 뒤바뀐 쓰기가 있어도 시가/종가가 정확합니다.

## 5. 변화량 — `delta`, `rate`, `derivative`

```sql
SELECT time_bucket(1h, ts) AS bucket,
       delta(value, ts)      AS change,
       rate(value, ts)       AS per_second,
       derivative(value, ts) AS slope_per_second
FROM   metrics
WHERE  series = 'cpu'
GROUP  BY series, time_bucket(1h, ts);
```

- `delta` = 버킷 내 마지막 샘플 − 첫 샘플
- `rate` = `delta` ÷ 경과 초 (양 끝점 기준 변화율)
- `derivative` = 최소제곱 회귀 기울기. 모든 점을 사용하므로 시계열이 비선형일 때 `rate`와 값이 달라집니다.

### 5.1 카운터 처리량 — `counter_rate`

단조 증가 카운터에는 `rate()`가 아니라 `counter_rate()`를 쓰세요. `rate()`는 카운터 리셋을 큰 음수 계단으로 오해합니다.

```sql
CREATE TABLE counters (
    series text, ts timestamp, total counter,
    PRIMARY KEY (series, ts)
);                                     -- counter 테이블은 TTL 불가. 쿼리 형태만 참고.

-- 분당 초당 요청 수
SELECT time_bucket(1m, ts) AS minute, counter_rate(total, ts) AS req_per_sec
FROM   counters
WHERE  series = 'api.requests'
GROUP  BY series, time_bucket(1m, ts);
```

### 5.2 `bigint` epoch 컬럼

`delta`/`rate`/`derivative`는 `timestamp`뿐 아니라 `bigint`(epoch 밀리초) 타임스탬프도 받습니다.

```sql
SELECT rate(value, ts_millis) AS per_second
FROM   metrics_epoch
WHERE  series = 'cpu';
```

## 6. 백분위 · SLO — `percentile`

```sql
-- 분당 p50 / p95 / p99 지연시간
SELECT time_bucket(1m, ts) AS minute,
       percentile(latency_ms, 0.50) AS p50,
       percentile(latency_ms, 0.95) AS p95,
       percentile(latency_ms, 0.99) AS p99
FROM   latencies
WHERE  service = 'checkout'
GROUP  BY service, time_bucket(1m, ts);

-- 전체 구간 중앙값
SELECT percentile(value, 0.5) AS median FROM metrics WHERE series = 'cpu';
```

`percentile`은 인접 값 사이를 선형 보간하는 **정확한** 연속 백분위입니다(`q`는 0~1). 그룹의 값을 메모리에 유지하므로, 무제한 스캔보다는 크기가 제한된 다운샘플 버킷에 적합합니다.

## 7. 분포 · 산포 · 카디널리티

```sql
-- 시간 가중 평균: 각 값이 유효했던 시간만큼 가중.
-- 샘플 간격이 불규칙할 때는 avg() 대신 이것을 쓰세요.
SELECT time_bucket(1h, ts) AS bucket, time_weighted_average(value, ts) AS twa
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- 곡선 아래 면적 (value·초). 예: 전력(W) → 에너지(J)
SELECT time_bucket(1h, ts) AS bucket, integral(value, ts) AS area
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- 버킷별 산포
SELECT time_bucket(1h, ts) AS bucket, variance(value) AS var, stddev(value) AS sd
FROM   metrics WHERE series = 'cpu' GROUP BY series, time_bucket(1h, ts);

-- [0, 1000) ms를 10개 등간격 버킷으로 나눈 히스토그램.
-- 결과 리스트: [ <0ms, bucket1, .. bucket10, >=1000ms ]
SELECT histogram(latency_ms, 0, 1000, 10) AS dist
FROM   latencies WHERE service = 'checkout';

-- 분당 고유 클라이언트 IP 근사 개수 (HyperLogLog, 메모리 사용량 고정)
SELECT time_bucket(1m, ts) AS minute, approx_count_distinct(client_ip) AS unique_ips
FROM   requests WHERE service = 'api' GROUP BY service, time_bucket(1m, ts);
```

## 8. 이변량 통계 · 회귀

두 컬럼 사이의 관계를 서버에서 바로 계산합니다. 인자 순서는 `(y, x)` — y가 종속변수입니다.

```sql
-- 온도와 전력 사용량의 상관관계 (시간별)
SELECT time_bucket(1h, ts)          AS bucket,
       corr(power, temperature)     AS r,
       covar_samp(power, temperature) AS cov,
       regr_slope(power, temperature) AS slope,      -- 1도당 전력 증가량
       regr_intercept(power, temperature) AS intercept,
       regr_r2(power, temperature)  AS r_squared
FROM   sensors
WHERE  site = 'plant1'
GROUP  BY site, time_bucket(1h, ts);
```

## 9. 대시보드 종합 쿼리

버킷팅, OHLC, 변화량, 백분위를 한 번에:

```sql
SELECT time_bucket(1h, ts)     AS bucket,
       count(value)            AS samples,
       first(value, ts)        AS open,
       last(value, ts)         AS close,
       min(value)              AS low,
       max(value)              AS high,
       avg(value)              AS mean,
       delta(value, ts)        AS change,
       rate(value, ts)         AS per_second,
       percentile(value, 0.95) AS p95
FROM   metrics
WHERE  series = 'cpu'
  AND  ts >= '2024-01-01 00:00:00+0000'
  AND  ts <  '2024-01-02 00:00:00+0000'
GROUP  BY series, time_bucket(1h, ts);
```

## 10. 운영 팁

- **항상 파티션을 지정하세요**(`WHERE series = ...`) 그리고 시간 범위도 함께. 시계열 스캔은 `ts`로 정렬된 단일 파티션 안에서 가장 저렴합니다.
- **파티션 크기를 제한하세요.** 고빈도 시리즈라면 파티션 키에 굵은 시간 버킷을 넣어 무한정 커지는 파티션을 막습니다. 예: `PRIMARY KEY ((series, day), ts)`.
- **UCS 컴팩션**을 쓰고(`scaling_parameters: 'T4'` 같은 tiered 설정), `default_time_to_live`로 보존 기간을 지정하세요. 만료된 SSTable은 `expired_sstable_check_frequency_seconds` 주기로 통째 회수됩니다.
- `time_bucket(interval, ts)`는 `GROUP BY`의 마지막 요소(파티션 키 컬럼들 뒤)여야 그룹핑이 읽기 경로로 푸시다운됩니다.

---

## 빌드

요구 사항: **Java 21**, Ant 1.10 이상(테스트 실행 시 ant-junit 포함). `modules/accord`는 git 서브모듈이므로 `git submodule update --init`이 필요합니다.

```bash
.build/sh/ai-build     # clean + jar + checkstyle -> build/apache-cassandra-6.0.0.jar
```

빌드 산출물은 항상 `apache-cassandra-6.0.0.jar`입니다(`base.version`이 6.0.0으로 고정되어 있습니다).

## 통합 테스트 (릴리스 게이트)

유닛 테스트는 함수를 프로세스 안에서 검증하지만, [docker/integration-test.sh](docker/integration-test.sh)는 **실제 이미지를 띄워** 스키마 생성부터 읽기 경로·집계·네이티브 프로토콜까지 통과하는 시계열 CQL 결과를 손으로 계산한 값과 대조합니다(32개 검증).

```bash
docker build -t cassandra-timeseries:6.0.0 -f docker/Dockerfile .
./docker/integration-test.sh cassandra-timeseries:6.0.0     # CONTAINER_RUNTIME=podman 도 지원
```

실행하면 항목·CQL·결과가 그대로 출력되고, `build/timeseries-it-report.html`(+ 같은 내용의 `.md`)에 보고서가 생성됩니다. **실행 결과 예시: [통합 테스트 보고서](doc/timeseries/integration-test-report.md)** — 32개 검증의 CQL·응답·소요 시간이 그대로 들어 있습니다.

CI에서는 태그를 밀면 `docker-image → docker-integration-test → docker-image-publish + release` 순서로 자동 실행되며, **이 테스트가 통과해야만** 이미지 배포와 릴리스가 진행됩니다. 기본 브랜치에서는 이미지 빌드 비용 때문에 수동(manual) 실행입니다.

### 스케일 테스트 (1억 건)

[docker/scale-test.sh](docker/scale-test.sh)는 컨테이너 노드에 대량 데이터를 적재하고 각 시계열 쿼리의 **CQL 실행 시간**을 측정합니다. 적재와 쿼리 모두 컨테이너 안에서 cqlsh 번들 파이썬 드라이버로 수행하므로(→ [docker/scale-workload.py](docker/scale-workload.py)) 측정값에 cqlsh 기동 시간이 섞이지 않습니다.

```bash
SCALE_ROWS=100000000 SCALE_SERIES=1000 SCALE_LOADERS=16 SCALE_HEAP=16G \
  ./docker/scale-test.sh cassandra-timeseries:6.0.0
# 적재된 데이터를 재사용해 쿼리만 다시 재기: SCALE_SKIP_LOAD=1
```

결과는 `build/timeseries-scale-report.html`(+ 같은 내용의 `.md`)에 생성됩니다. **실행 결과 예시: [스케일 테스트 보고서 (1억 건)](doc/timeseries/scale-test-report.md)** — 쿼리별 CQL 실행 시간이 요약표로 정리돼 있습니다.

주의: 수백만 행 이상을 집계하려면 서버 타임아웃을 올려야 합니다. `read/range_request_timeout`뿐 아니라 **`native_transport_timeout`(기본 12초)** 이 요청 전체를 자르므로 이 값도 함께 올려야 하며, 이 키는 기본 `cassandra.yaml`에 없어서 추가해야 합니다. 스크립트가 이 설정을 대신 해 줍니다.

## CI 및 릴리스

- 푸시할 때마다 jar를 빌드하고 시계열 테스트 스위트를 실행합니다(`.gitlab-ci.yml`).
- 최신 master 빌드의 jar: *CI/CD → Pipelines → build-jar 아티팩트*.
- 태그 푸시(예: `v6.0.0`) 시 jar 다운로드 링크가 포함된 [Release](../../-/releases)가 발행됩니다.

## 브랜치 및 업스트림 정책

- `master`(= `6.0.0` 브랜치): 릴리스 라인. apache/cassandra의 최신 업스트림 `cassandra-6.0` 브랜치(리모트 `upstream`)와 **항상 머지된 상태로 유지**해야 합니다.
- 자주 충돌하는 지점: `CHANGES.txt`, `debian/changelog`, `modules/accord` 서브모듈 포인터, `cql3/statements/SelectStatement.java`(gap-fill 연결부).

## 개발

빌드/테스트/코드 스타일 규칙은 [CLAUDE.md](CLAUDE.md)와 [AGENTS.md](AGENTS.md)를 참고하세요(전체 테스트 스위트는 몇 시간이 걸리므로 대상 테스트만 실행합니다). 테스트 레이아웃은 [TESTING.md](TESTING.md)에 있습니다. 시계열 테스트 진입점: `org.apache.cassandra.cql3.functions.TimeSeriesFctsTest`, `org.apache.cassandra.db.aggregation.TimeBucketGapFillerTest`.
