# cassandra-timeseries

**Apache Cassandra for Industrial Timeseries Workload**
— 산업 현장의 센서·태그 데이터를 위한 분산 시계열 데이터베이스.

공장·플랜트의 시계열 데이터는 몇 가지 고유한 성질을 가집니다: 태그(시리즈)마다 초 단위로 끝없이 쌓이고, 몇 년치를 규정상 보관해야 하며, 엣지 장비가 통신 두절 뒤 며칠치를 한꺼번에 밀어 넣고(지각 백필), 조회는 거의 항상 "이 태그의 이 기간"입니다. 범용 Cassandra는 이 워크로드를 감당하지만, 압축·보존·집계는 전부 애플리케이션 몫으로 남습니다.

이 포크는 그 부분을 **데이터베이스 안으로 가져옵니다** — 시계열 연산을 서버에서 끝내고(21종 CQL 함수 + gap-fill), 오래된 데이터를 자동으로 압축·보존하며(계층형 저장 + 시계열 전용 컴팩션), 그러면서도 **CQL은 그대로**입니다. 압축된 과거 데이터도 평범한 `SELECT`로 읽힙니다(투명 읽기). 애플리케이션은 데이터가 압축돼 있는지 알 필요가 없습니다.

[apache/cassandra](https://github.com/apache/cassandra)(`cassandra-6.0` 브랜치)의 포크이며, 온디스크 포맷·CQL 문법은 업스트림 그대로라 **기존 6.0 데이터를 그대로 읽습니다**(새 기능은 전부 옵트인). Spark 연동은 짝이 되는 포크 [cassandra-spark-connector](https://dev.kopens.io/common/cassandra-spark-connector)(Spark 4.1.2)로 제공됩니다.

## 🎯 핵심 — 무엇이 좋아지나 (업스트림 Cassandra 6.0.0 대비)

**1. 서버에서 끝나는 시계열 연산.** 버킷팅·집계·보간·회귀를 CQL 한 줄로 처리합니다. 애플리케이션이 원시 데이터를 끌어와 계산하던 왕복이 사라집니다.

```sql
-- 시간별 평균 + 빈 구간 자동 채움 — 업스트림에서는 앱이 100k행을 받아 직접 계산해야 하는 작업
SELECT time_bucket_gapfill(1h, ts, '2024-01-01', '2024-01-02'), locf(avg(value))
FROM ts.metrics WHERE series='cpu' GROUP BY series, time_bucket_gapfill(1h, ts, '2024-01-01', '2024-01-02');
```

**2. 오래된 데이터는 자동 압축, 조회는 그대로.** 계층형 저장이 지난 데이터를 Chimp128 청크로 압축해 옮기고, `SELECT`는 압축 여부를 몰라도 됩니다(투명 읽기가 자동 병합).

**3. 압축이 조회까지 빠르게 만듭니다.** 1억 건 실측(단일 노드, 동일 하드웨어·동일 CQL·동일 결과값) — [벤치마크 전문](doc/timeseries/tiering-benchmark.md):

| 항목 | 업스트림 방식(행 저장) | 이 포크(계층화) | 효과 |
| --- | --- | --- | --- |
| 저장 용량 | 1.47 GB | **0.70 GB** | **2.1× 절감** (풀정밀 double = 압축 최악 조건; 양자화된 실제 산업 데이터는 [코덱 실측](doc/timeseries/codec-bakeoff.md) 기준 10배 이상). 코덱 통합(Chimp128 단일화) 이전 실측이라 재측정 필요 |
| 단일 시리즈 집계 (10만 행) | 329~524 ms | **39~100 ms** | **5~8× 빠름** |
| 100 시리즈 시간별 평균 (1,000만 행) | 39.9 s | **5.6 s** | **7.1× 빠름** |
| 대시보드 종합 쿼리(OHLC+p95) | 512 ms | **150 ms** | **3.4× 빠름** |
| 재인코딩 처리량 | — | 265k rows/s | 1억 건을 6분 만에 압축 |

> 조회가 빨라지는 이유: 파티션당 10만 행을 SSTable에서 훑는 대신 **압축 청크 28개(~0.7MB)만 읽어 디코드**하기 때문입니다. IO·역직렬화가 지배적인 시계열 집계에서는 압축이 곧 속도입니다.

**4. 시계열에 맞는 컴팩션·보존.** 시간 창 단위로 SSTable을 정렬·동결(창당 1개)하고, 보존 만료 창은 컴팩션 없이 통째 삭제합니다. 엣지 장비의 지각 백필도 자기 시간대로 자동 격리됩니다.

**5. 로그·이벤트 본문 검색.** SAI `LIKE` + `index_analyzer`로 한글 포함 부분문자열 검색이 `ALLOW FILTERING` 없이 동작합니다.

## ✨ 구현 기능 (업스트림 대비 이 포크의 델타)

| 기능 | 내용 | 상세 |
| --- | --- | --- |
| **시계열 CQL 함수 21종** | `time_bucket`, `first`/`last`, `delta`/`rate`/`derivative`, 리셋 보정 `counter_delta`/`counter_rate`, `percentile`, `time_weighted_average`, `integral`, `variance`/`stddev`, `histogram`, `approx_count_distinct`, 이변량 `corr`/`covar_*`/`regr_*` | [사용법 §2~9](#시계열-cql-사용법) |
| **Gap-fill** | `GROUP BY time_bucket_gapfill(width, ts, start, finish)` — 빈 버킷 실체화 + `locf()`/`interpolate()` 채움 정책 | [사용법 §3](#3-빈-구간-채우기--time_bucket_gapfill) |
| **풀텍스트 검색** | SAI `LIKE` + `index_analyzer`(ngram/standard/cjk/keyword + JSON) — 단어 중간 조각·공백 걸침·한글까지 진짜 부분문자열 매치, ALLOW FILTERING 불필요 | [fulltext-search.md](doc/timeseries/fulltext-search.md) |
| **시계열 컴팩션 (TSCS)** | `TimeSeriesCompactionStrategy` — 창 정렬 + 창 내부 UCS 위임 + retention 창 통삭제 + 닫힌 창 동결(창당 1 SSTable, `WindowFrozenListener` 이벤트 훅, far-future 가드 `max_future_window`, 닫힌 창 TTL 회수에 retention 불필요) + 지각 격리(flush/스트리밍 창 경계 스플릿 — 백필이 과거 창에 국소 편입, 레거시 걸침 SSTable 자동 분할) | [설계 스펙](docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md) |
| **Chimp128 청크 코덱** *(계층형 저장 1단계)* | `(timestamp, double)` 무손실 압축 — 델타-오브-델타 타임스탬프 + Chimp128 값 스트림. 양자화 워크/주기 신호(실제 산업 센서값)에서 1.4~2.5 B/샘플. 상수 계열은 코덱보다 먼저 컬럼 지향 청크의 CONSTANT 플래그가 O(1)로 처리 | [bake-off 결과](doc/timeseries/codec-bakeoff.md) · [설계 스펙](docs/superpowers/specs/2026-07-31-industrial-tiered-storage-design.md) |
| **계층형 저장 (청크 스토어)** *(계층형 저장 2단계)* | 테이블 확장 `timeseries_tiering` 정책 — 백그라운드 재인코더가 hot_window를 지난 창을 청크로 압축해 `<테이블>__chunks`로 이동(지각 데이터 병합, cold_window 만료, CL 쿼럼 하한). `nodetool retier`/`tieringstatus`, `system_views.timeseries_tiering`. **투명 읽기(SP3)**: 베이스 테이블 SELECT가 핫 로우+청크를 자동 병합 — 시간범위·포인트·집계·gap-fill·LIMIT/DESC가 핫·콜드에 걸쳐 동작 | [tiered-storage.md](doc/timeseries/tiered-storage.md) |
| **테스트 인프라** | 도커 통합 테스트 52건(릴리스 게이트), 1억 건 스케일 하네스, 3노드 jvm-dtest, GC 비교(ZGC vs G1) | [보고서들](doc/timeseries/) |
| **배포/CI** | Testcontainers 호환 도커 이미지, GitLab CI(빌드→테스트→이미지→통합 게이트→릴리스), 태그 릴리스 자동화 | [.gitlab-ci.yml](.gitlab-ci.yml) |

## 📖 문서

| 문서 | 내용 |
| --- | --- |
| **[사용 예제 (examples.md)](doc/timeseries/examples.md)** | 아래 "시계열 CQL 사용법"의 원본 예제 모음 (영문) |
| [시계열 함수 설계 (timeseries-functions-design.md)](doc/timeseries/timeseries-functions-design.md) | 각 함수의 시그니처·의미론(semantics), 분산 환경에서의 정확성, 코드 위치 |
| [Gap-Fill 설계 (gapfill-design.md)](doc/timeseries/gapfill-design.md) | `time_bucket_gapfill`의 CQL 문법, 보간 규칙, 가드레일 |
| [Continuous Aggregates 설계 (continuous-aggregates-design.md)](doc/timeseries/continuous-aggregates-design.md) | 시간 버킷 롤업(연속 집계) 설계안 — 진행 중 |
| **[풀텍스트 검색 (fulltext-search.md)](doc/timeseries/fulltext-search.md)** | SAI `LIKE` + `index_analyzer` — 로그/메시지 본문 부분문자열 검색 (한글 포함) |
| **[계층화 벤치마크 (tiering-benchmark.md)](doc/timeseries/tiering-benchmark.md)** | 1억 건 전/후 실측 — 저장 2.1×↓(풀정밀 최악 케이스), 재인코딩 265k rows/s, 동일 결과로 질의 5~8× 가속. 저장 수치는 코덱 통합 이전 실측 |
| **[운영 튜닝 가이드 (operations-tuning.md)](doc/timeseries/operations-tuning.md)** | 장기 보존(10년) 전환 실전 가이드 — 용량 산수, 적용 순서, 원본·**청크 테이블** 튜닝값과 근거, TTL과 계층화의 관계, 점검 목록 |
| **[계층형 저장 (tiered-storage.md)](doc/timeseries/tiered-storage.md)** | `timeseries_tiering` 정책·청크 재인코더 — 설정, 청크 조회 패턴, 운영(nodetool/가상 테이블), 불변식과 제한사항 |
| [통합 테스트 보고서](doc/timeseries/integration-test-report.md) | 실제 컨테이너에서 실행한 52개 검증의 CQL·결과·소요 시간 |
| [스케일 테스트 보고서 (1억 건)](doc/timeseries/scale-test-report.md) | 1억 행 적재 후 측정한 쿼리별 CQL 실행 시간 |
| [GC 비교: ZGC generational vs G1](doc/timeseries/gc-comparison.md) | 같은 1억 건 데이터로 두 GC의 쿼리 시간·쓰기 처리량 비교 (원자료) |
| **[아티클: 시계열 DB에서 G1GC vs Generational ZGC](doc/timeseries/g1gc-vs-zgc-article.md)** | 위 측정을 정리한 성능 비교 아티클 (환경·방법·해석·권장 설정) |

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

시계열의 정석 스키마입니다. 시리즈당 파티션 하나, 시간으로 클러스터링. 컴팩션은 이 포크의 시계열 전용 전략 `TimeSeriesCompactionStrategy`(TSCS)를 씁니다 — SSTable을 시간 창으로 정렬하고, 닫힌 창은 창당 1 SSTable로 동결하며, 보존기간이 지난 창은 컴팩션 없이 통째 삭제합니다. 현재 창 내부의 컴팩션 선택은 UCS 컨트롤러에 위임되므로 UCS의 쓰기 최적 특성은 그대로 유지됩니다.

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
   AND compaction = {'class': 'TimeSeriesCompactionStrategy',
                     'window_size': '1h',          -- 시간 창 폭
                     'freeze_after': '2h',         -- 창이 닫히고 이 시간 후 창당 1 SSTable로 동결
                     'scaling_parameters': 'T4',   -- 현재 창 내부는 UCS 위임 (쓰기 최적 4-way)
                     'retention': '30d'}           -- 창 상한이 30일을 지나면 통째 삭제
   AND default_time_to_live = 2592000;   -- 30일 (retention과 병행 시 먼저 도래하는 쪽 적용)

INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:05:00+0000', 10);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 09:35:00+0000', 30);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:15:00+0000', 50);
INSERT INTO metrics (series, ts, value) VALUES ('cpu', '2024-01-01 10:45:00+0000', 70);
```

### 1.1 TSCS 컴팩션 옵션 요약

| 옵션 | 설명 |
| --- | --- |
| `window_size` | 시간 창 폭 (`<정수><m\|h\|d>`). 계층형 저장의 `chunk_window`와 맞추길 권장 |
| `freeze_after` | 창이 닫힌 뒤 이 시간이 지나면 동결 대상 — 지각 데이터 유예 기간 |
| `scaling_parameters`, `target_sstable_size` | 현재 창 내부 UCS 위임 파라미터 (UCS 문법 그대로: `T4` = 쓰기 최적 4-way tiered 등) |
| `retention` | 선택 — 창 상한이 `now - retention`을 지나면 SSTable 통째 삭제 (`window_size + freeze_after` 이상) |
| `max_future_window` | 미래 타임스탬프 가드(기본 `1d`) — 오입력이 창을 오염시키지 않게 격리 |

동결(창당 1 SSTable) 덕에 닫힌 창의 TTL 데이터는 `retention` 없이도 회수되고, 파티션+시간범위 조회의 읽기 증폭이 최소화됩니다. 지각(백필) 데이터는 flush 시 창 경계에서 분리되어 자기 창에 국소 편입됩니다. 상세: [§11 시계열 컴팩션 설정](#11-시계열-컴팩션tscs-설정)

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

## 10. 풀텍스트 검색 — SAI `LIKE` + `index_analyzer`

로그·이벤트 메시지 본문을 시계열 조회 패턴 안에서 검색합니다. `ngram` 분석기가 진짜 부분문자열 매치를 제공합니다(단어 중간 조각, 공백 걸침, 한글 전부 지원). 자세한 내용: [fulltext-search.md](doc/timeseries/fulltext-search.md)

```sql
CREATE TABLE logs (
    device text, ts timestamp, msg text,
    PRIMARY KEY (device, ts)
) WITH CLUSTERING ORDER BY (ts ASC);

CREATE INDEX logs_msg_idx ON logs(msg) USING 'sai'
  WITH OPTIONS = { 'index_analyzer': 'ngram' };

-- 장비 1대 · 1시간 구간에서 본문 검색 (ALLOW FILTERING 불필요)
SELECT ts, msg FROM logs
 WHERE device = 'pump-01'
   AND ts >= '2026-07-31 00:00' AND ts < '2026-07-31 01:00'
   AND msg LIKE '%타임아웃%';

-- 단어 중간 조각도 매치: '%imeou%' 가 "timeout" 을 찾음
-- 접두/접미/완전일치: 'connection%', '%9042', LIKE 'connection refused'
-- 다중 조각 AND: msg LIKE '%connection%' AND msg LIKE '%refused%'

-- 시계열 함수와 조합: 5분 버킷별 에러 건수
SELECT time_bucket(5m, ts), count(*) FROM logs
 WHERE device='pump-01' AND ts >= ? AND ts < ? AND msg LIKE '%timeout%'
 GROUP BY device, time_bucket(5m, ts);
```

동작 원리: 값 전체를 2~3글자 n-gram으로 색인(재현율) → 그램 교집합으로 후보 추출 → **원문에 LIKE 패턴 재적용**(정밀도). 색인은 원본 컬럼의 수 배 크기가 되므로 로그성 테이블에 선별 적용하세요. 2글자 미만 조각은 명시적 에러로 거부됩니다. `=`는 완전일치 의미를 유지합니다.

## 11. 시계열 컴팩션(TSCS) 설정

TWCS의 시간 정렬·통삭제와 UCS의 창 내부 컴팩션을 결합한 전용 전략입니다. 테이블 생성(또는 ALTER) 시 지정합니다:

```sql
CREATE TABLE ts.sensor (
  tag_id text, timestamp timestamp, value double,
  PRIMARY KEY (tag_id, timestamp)
) WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1h',           -- 시간 창 폭 (계층화 chunk_window와 맞추길 권장)
  'freeze_after': '2h',          -- 창이 닫히고 이 시간이 지나면 동결(창당 1 SSTable로 수렴)
  'scaling_parameters': 'T4',    -- 현재 창 내부는 UCS에 위임 (UCS 문법 그대로)
  'retention': '30d',            -- 선택: 창 상한이 now-30d를 지나면 컴팩션 없이 통째 삭제
  'max_future_window': '1d'      -- 선택: 미래 타임스탬프 가드 (기본 1d)
};
```

- 닫힌 창은 자동으로 **창당 1 SSTable**로 동결되어 읽기 증폭이 최소화되고, TTL 데이터도 retention 없이 회수됩니다.
- 지각(백필) 데이터는 flush/스트리밍 시 창 경계에서 분리되어 **자기 창에 국소 편입**됩니다 — 현재 창 컴팩션을 오염시키지 않습니다.
- 상세: [설계 스펙](docs/superpowers/specs/2026-07-31-timeseries-compaction-design.md)

## 12. 계층형 저장(tiered storage) 설정

오래된 창을 Chimp128 청크로 압축해 `<테이블>__chunks`로 옮기고, **SELECT는 그대로**(투명 읽기가 핫+콜드 자동 병합) 쓰는 기능입니다. 테이블 `extensions`에 JSON 정책을 hex로 넣습니다:

### 12.1 대상 스키마

**시간축(`timestamp` 클러스터링 컬럼)이 하나인 시계열 테이블이면 형태를 가리지 않습니다.** 파티션 키는 복합이어도 되고, 일반 컬럼은 개수·타입 무관, static 컬럼은 몇 개든 그대로 보존됩니다 (static 셀은 청크화 대상이 아니고, 재인코더의 클러스터링 레인지 딜리트가 건드리지 않습니다).

```sql
CREATE TABLE ts.tag_point (
    tag_id     text,                    -- 파티션 키: 개수 무관 (복합 키 가능)
    timestamp  timestamp,               -- 클러스터링 1개, timestamp (ASC/DESC 모두 가능)
    site_id    text STATIC,             -- static 컬럼: 개수·타입 무관, 그대로 보존
    attribute  frozen<map<text,text>>,  -- 일반 컬럼: 개수·타입 무관
    quality    int,
    value      double,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

지원되지 않는 형태에 정책을 걸면 60초마다 **사유를 밝힌** ERROR 로그를 남기고 건너뜁니다. 거부 대상은 다섯 가지뿐입니다:

| 형태 | 사유 |
| --- | --- |
| `counter` 컬럼 | 재인코더는 행을 삭제 후 재삽입하는데, 삭제된 카운터는 영구히 다시 쓸 수 없습니다 |
| 비frozen 컬렉션 **일반** 컬럼 | 멀티셀 값은 청크로 인코딩할 수 없습니다 — `frozen<...>`으로 감싸면 지원됩니다 |
| 클러스터링이 0개·2개 이상이거나 `timestamp`가 아님 | 인코딩할 시간축이 없습니다 |
| **일반 컬럼**에 걸린 보조 인덱스(SAI 포함) | 재인코딩된 행이 인덱스에서 사라져 인덱스 질의가 콜드 데이터를 조용히 누락합니다 (static·키 컬럼 인덱스는 무방) |
| 이 테이블 위의 머티리얼라이즈드 뷰 | 투명 읽기는 베이스 테이블만 복원하므로 뷰가 오래된 이력을 영구히 잃습니다 |

> `default_time_to_live`가 있고 `hot_window >= TTL`이면 재인코더가 데이터를 보기 전에 TTL이 먼저 지워 **아무것도 압축되지 않습니다**. 거부하지는 않지만 두 값을 밝힌 WARN을 남깁니다.

> **진행 중(SP4)**: 스키마 수용은 위와 같이 일반화됐지만, 재인코더는 아직 `double` 일반 컬럼 1개짜리 청크만 씁니다. 그 외 형태는 수용된 뒤에도 ERROR와 함께 건너뜁니다 — 한 컬럼만 인코딩하고 나머지를 지우는 일은 하지 않습니다.

### 12.2 압축 켜기 — CQL 한 줄

정책 JSON을 테이블 `extensions`에 그대로 넣으면 끝입니다 (hex 변환 불필요):

```sql
ALTER TABLE ts.sensor WITH extensions = {
  'timeseries_tiering': '{"hot_window":"1d","chunk_window":"6h","cold_window":"365d","interval":"1h"}'
};

-- 적용 확인 (정책과 실행 통계가 함께 보입니다)
SELECT * FROM system_views.timeseries_tiering;
```

> `extensions`는 스키마상 blob 맵이지만, 이 포크는 **평문 문자열을 UTF-8 바이트로 저장**합니다.
> `0x`로 시작하는 값만 hex 블롭으로 해석하므로 기존 hex 표기(`0x7b22...`)도 그대로 동작합니다.

적용 후에는 60초 스위퍼가 `interval` 주기로 알아서 압축합니다. **바로 확인하고 싶으면** 수동으로 한 사이클 실행:

```bash
nodetool retier ts sensor      # 1회 재인코딩 (동기 실행)
nodetool tieringstatus         # 테이블별 정책·마지막 실행·누적 통계
```

```sql
-- 압축 결과 확인: 청크가 생기고, SELECT 결과는 그대로 (투명 읽기)
SELECT count(*) FROM ts.sensor__chunks WHERE tag_id='pump-01';
SELECT count(*) FROM ts.sensor        WHERE tag_id='pump-01';   -- 압축 전과 동일한 값
```

### 12.3 정책 필드

| 필드 | 의미 |
| --- | --- |
| `hot_window` | **이 기간 안의 데이터는 건드리지 않습니다**(행 그대로). 실시간 조회·수정이 잦은 구간보다 넉넉히 잡으세요 (예: `7d`) |
| `chunk_window` | 청크 1개가 담는 시간 폭 (최대 `31d`). TSCS `window_size`와 맞추길 권장. 1초 주기 데이터면 `1h`(=3,600샘플)가 무난 |
| `cold_window` | 선택 — 이 기간을 지난 청크는 통째 삭제(보존 정책). 미지정(`-1`)이면 영구 보관 |
| `interval` | 백그라운드 재인코딩 주기 (예: `1h`). 60초 스위퍼가 주기 도래 테이블만 처리 |
| `consistency` | 재인코더 CL — `LOCAL_QUORUM`(기본) / `QUORUM` / `EACH_QUORUM` / `ALL`만 허용 (약한 CL은 데이터 유실 위험이라 차단) |

**코덱 선택은 없습니다**: Chimp128이 유일한 청크 코덱이라 고를 것이 없습니다 (예전 `codec` 옵션은 제거됐고, 남아 있으면 `ALTER TABLE`이 거부합니다). 값이 거의 변하지 않는 상수 계열은 코덱을 타기 전에 컬럼 지향 청크의 CONSTANT 플래그가 O(1)로 처리합니다 — [실측](doc/timeseries/codec-bakeoff.md) 참고.

### 12.4 끄기·바꾸기

```sql
-- 정책 변경: 같은 방식으로 새 JSON을 넣으면 다음 사이클부터 적용
-- 완전히 끄기: 확장에서 키 제거 (이미 만들어진 청크는 그대로 남습니다)
ALTER TABLE ts.sensor WITH extensions = {};
```

끈 뒤에도 **투명 읽기는 정책이 있어야 동작**하므로, 청크가 남은 상태에서 정책만 제거하면 과거 데이터가 조회되지 않습니다. 되돌리려면 정책을 다시 넣으세요(청크는 그대로 재사용됩니다).

운영 참고: 지각 데이터는 이미 청크화된 창에 들어와도 다음 사이클에 자동 병합됩니다(같은 타임스탬프면 나중에 들어온 행이 이김). 상세·제한사항(범위 스캔·페이징 등): [tiered-storage.md](doc/timeseries/tiered-storage.md) · 실측: [벤치마크](doc/timeseries/tiering-benchmark.md)

## 13. 운영 팁

- **항상 파티션을 지정하세요**(`WHERE series = ...`) 그리고 시간 범위도 함께. 시계열 스캔은 `ts`로 정렬된 단일 파티션 안에서 가장 저렴합니다.
- **파티션 크기를 제한하세요.** 고빈도 시리즈라면 파티션 키에 굵은 시간 버킷을 넣어 무한정 커지는 파티션을 막습니다. 예: `PRIMARY KEY ((series, day), ts)`.
- **TSCS 컴팩션**(`TimeSeriesCompactionStrategy`)을 쓰세요 — 창 정렬·동결·통삭제가 시계열에 맞게 자동화되고, 현재 창 내부는 UCS(`scaling_parameters: 'T4'`)에 위임됩니다. 보존은 `retention`(창 통삭제) 또는 `default_time_to_live`로 지정하면 됩니다.
- `time_bucket(interval, ts)`는 `GROUP BY`의 마지막 요소(파티션 키 컬럼들 뒤)여야 그룹핑이 읽기 경로로 푸시다운됩니다.

---

## 빌드

요구 사항: **Java 21**, Ant 1.10 이상(테스트 실행 시 ant-junit 포함). `modules/accord`는 git 서브모듈이므로 `git submodule update --init`이 필요합니다.

```bash
.build/sh/ai-build     # clean + jar + checkstyle -> build/apache-cassandra-6.0.0.jar
```

빌드 산출물은 항상 `apache-cassandra-6.0.0.jar`입니다(`base.version`이 6.0.0으로 고정되어 있습니다).

## 통합 테스트 (릴리스 게이트)

유닛 테스트는 함수를 프로세스 안에서 검증하지만, [docker/integration-test.sh](docker/integration-test.sh)는 **실제 이미지를 띄워** 스키마 생성부터 읽기 경로·집계·네이티브 프로토콜까지 통과하는 시계열 CQL 결과를 손으로 계산한 값과 대조합니다(52개 검증).

```bash
docker build -t cassandra-timeseries:6.0.0 -f docker/Dockerfile .
./docker/integration-test.sh cassandra-timeseries:6.0.0     # CONTAINER_RUNTIME=podman 도 지원
```

실행하면 항목·CQL·결과가 그대로 출력되고, `build/timeseries-it-report.html`(+ 같은 내용의 `.md`)에 보고서가 생성됩니다. **실행 결과 예시: [통합 테스트 보고서](doc/timeseries/integration-test-report.md)** — 52개 검증의 CQL·응답·소요 시간이 그대로 들어 있습니다.

CI에서는 태그를 밀면 `docker-image → docker-integration-test → docker-image-publish + release` 순서로 자동 실행되며, **이 테스트가 통과해야만** 이미지 배포와 릴리스가 진행됩니다. 기본 브랜치에서는 이미지 빌드 비용 때문에 수동(manual) 실행입니다.

### 스케일 테스트 (1억 건)

[docker/scale-test.sh](docker/scale-test.sh)는 컨테이너 노드에 대량 데이터를 적재하고 각 시계열 쿼리의 **CQL 실행 시간**을 측정합니다. 적재와 쿼리 모두 컨테이너 안에서 cqlsh 번들 파이썬 드라이버로 수행하므로(→ [docker/scale-workload.py](docker/scale-workload.py)) 측정값에 cqlsh 기동 시간이 섞이지 않습니다.

```bash
SCALE_ROWS=100000000 SCALE_SERIES=1000 SCALE_LOADERS=16 SCALE_HEAP=16G \
  ./docker/scale-test.sh cassandra-timeseries:6.0.0
# 적재된 데이터를 재사용해 쿼리만 다시 재기: SCALE_SKIP_LOAD=1
```

GC를 바꿔 비교할 수도 있습니다 — `SCALE_GC=g1`(기본은 `zgc`, `conf/jvm21-server.options`에 이미 generational ZGC가 켜져 있음), `SCALE_PASSES=2`(웜업 후 측정), `SCALE_WBENCH_ROWS=10000000`(쓰기 벤치). 두 실행 결과를 `docker/gc-compare.py <prefix-a> <prefix-b>`에 넣으면 비교표가 나옵니다 → **[GC 비교 결과](doc/timeseries/gc-comparison.md)**.

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
