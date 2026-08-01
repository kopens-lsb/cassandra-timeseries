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

# 계층형 저장 (Tiered Storage): 청크 스토어 + 백그라운드 재인코더

시계열 테이블의 오래된(닫힌) 행을 백그라운드에서 **컬럼 지향 청크**로 압축해 섀도 테이블
`<테이블>__chunks`로 옮기고, 원본 행은 삭제하는 서버 내장 계층화 엔진입니다. 최근 데이터(핫 구간)는
행 단위로 그대로 남아 쓰기·조회 모두 기존과 동일하고, 오래된 데이터는 행당 수 바이트 수준으로
압축된 청크로 보관됩니다. 청크 1개는 한 창의 타임스탬프 축 하나에 **일반 컬럼 전부**를 컬럼별 독립
섹션으로 담습니다(§3.1.1). `double` 컬럼에 쓰이는 Chimp128의 압축 특성은
[bake-off 결과](codec-bakeoff.md)를 참고하세요.

> **투명 읽기(SP3) 포함**: 베이스 테이블 `SELECT`가 핫 로우와 청크 디코드 로우를 **자동 병합**해
> 돌려줍니다 — 애플리케이션은 압축의 존재를 모릅니다. 파티션(태그) + 시간 범위/포인트 질의,
> 집계(`avg`/`count`/`time_bucket` GROUP BY), gap-fill, `LIMIT`/`ORDER BY DESC` 모두 핫·콜드에
> 걸쳐 동작합니다. `__chunks` 직접 조회(아래 §3)는 이제 운영·디버그 용도입니다.
> 제한: ① 병합 질의가 한 페이지를 넘으면 명확한 에러 + 해결 힌트(페이지 크기 확대·범위 축소·집계)로
> 실패합니다(침묵 중복/유실 방지, v1 스코프). ② 디코드 로우의 `writetime(value)`은 청크의
> `max_row_writetime` 근사값입니다. ③ 손상 청크는 경고와 함께 건너뛰고 나머지 데이터를 제공합니다.

## 1. 대상 스키마 — 시간으로 클러스터링된 아무 테이블

계층화는 **시간축이 하나인 시계열 테이블이면 형태를 가리지 않습니다.** 지원되지 않는 형태에 정책을
걸면 60초 스위프마다 사유를 밝힌 ERROR 로그를 남기고 건너뜁니다.

**지원:**

| 요소 | 조건 |
| --- | --- |
| 파티션 키 | **개수 무관** — 복합 키(`PRIMARY KEY ((asset_id, date, hour), ts)`) 가능. 청크 테이블이 전체 파티션 키를 그대로(이름·타입·순서) 미러링합니다 |
| 클러스터링 | **정확히 1개**, 타입 `timestamp` (`ASC`/`DESC` 무관) |
| 일반 컬럼 | **개수·타입 무관** — `text`/`int`/`bigint`/`double`/`boolean`/`blob`/`uuid`/`frozen<...>` 등 |
| static 컬럼 | **개수·타입 무관** (비frozen 컬렉션도 가능). static 셀은 청크화 대상이 아니며, 재인코더의 클러스터링 레인지 딜리트가 static을 건드리지 않으므로 그대로 보존됩니다 |
| 보조 인덱스 | **static 컬럼에 걸린 인덱스만** 무방 (예: static `asset_id`의 SAI) — static 인덱스 엔트리는 `Clustering.STATIC_CLUSTERING`에 있어 재인코더가 지우는 클러스터링 레인지 밖입니다 |

**거부 (각각 사유를 명시한 ERROR 로그):**

| 형태 | 사유 |
| --- | --- |
| `counter` 컬럼 | 재인코더는 행을 삭제 후 재삽입하는데, 삭제된 카운터는 영구히 다시 쓸 수 없습니다 — 정합성 정지 조건이지 한계가 아닙니다 |
| 비frozen 컬렉션 **일반** 컬럼 | 멀티셀 값(셀·타임스탬프가 원소별로 존재)은 청크의 행 단위 불투명 바이트로 표현할 수 없습니다. `frozen<...>`으로 감싸면 지원됩니다 |
| 클러스터링이 0개·2개 이상이거나 `timestamp`가 아닌 경우 | 청크가 인코딩할 시간축이 없습니다 |
| **static이 아닌 컬럼**에 걸린 보조 인덱스(SAI 포함) | 인덱스 엔트리는 베이스 **행 단위**입니다. 재인코딩된 행이 삭제되면 엔트리도 사라져 인덱스 질의가 `hot_window`보다 오래된 데이터를 조용히 누락합니다. 일반 컬럼뿐 아니라 **클러스터링 컬럼**(`CREATE INDEX ON t(ts)`는 CQL상 허용됩니다)과 **복합 파티션 키의 구성 컬럼**도 마찬가지입니다 |
| 이 테이블 위의 **머티리얼라이즈드 뷰** | 레인지 딜리트가 뷰까지 전파되지만 투명 읽기는 **베이스 테이블만** 복원하므로, 뷰는 `hot_window` 이전 이력을 아무 에러 없이 영구히 잃습니다 |
| 청크 테이블 예약어와 겹치는 파티션 키 이름 | `window_start`/`codec`/`samples`/`max_row_writetime`/`payload` — 미러링 시 같은 이름이 두 번 선언됩니다 |

```sql
-- 가장 단순한 형태
CREATE TABLE ts.sensor (
    tag_id    text,
    timestamp timestamp,
    value     double,
    PRIMARY KEY (tag_id, timestamp)
);

-- 실 운영형: 복합 파티션 키 + static 다수 + 일반 컬럼 다수(혼합 타입) + DESC
CREATE TABLE ts.tag_point (
    tag_id     text,
    timestamp  timestamp,
    site_id    text STATIC,
    tag_name   text STATIC,
    attribute  frozen<map<text,text>>,
    quality    int,
    latency    int,
    value      text,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

재인코더는 **일반 컬럼 전부**를 청크 1개에 담습니다 — 창의 타임스탬프 축을 한 번만 저장하고, 컬럼마다
독립 섹션에 그 컬럼의 **직렬화 바이트 그대로** 넣습니다. `null` 셀은 `null`로 그대로 왕복하며(기본값으로
바뀌지 않습니다), 어떤 타입이든 담깁니다 (아래 §3.1.1 코덱 표). 일반 컬럼이 **하나도 없는** 테이블은
수용은 되지만 실제로는 아무것도 인코딩되지 않습니다 — 셀 writetime이 존재하지 않아 원본 삭제에 쓸
타임스탬프가 없기 때문이며, 이 경우 그 사실을 밝힌 WARN을 남깁니다(§5.1).

### 1.1 `default_time_to_live`와 `hot_window`

베이스 테이블에 `default_time_to_live`가 있고 `hot_window >= TTL`이면 **재인코더가 데이터를 볼 기회가
없습니다** (TTL이 먼저 지웁니다) — 계층화를 켜 두고도 아무것도 압축되지 않습니다. 이 조합은 거부되지
않고(행 단위 `USING TTL`이 테이블 기본값과 다를 수 있으므로) 두 값을 함께 밝힌 WARN을 남깁니다.
`hot_window`를 TTL보다 짧게 잡거나 `default_time_to_live`를 올리십시오.

> **⚠️ TTL은 청크화되면서 사라집니다.** 재인코더는 셀의 `WRITETIME`만 읽고 `TTL`은 읽지 않으며, 청크
> 포맷에도 TTL 자리가 없습니다. 복원된 행은 TTL 없는 셀로 돌아오므로, **`default_time_to_live`(또는
> 행별 `USING TTL`)로 데이터를 만료시키던 테이블은 청크로 옮겨진 순간 그 데이터가 영구 보존됩니다.**
> 청크화된 데이터의 유일한 보존 장치는 `cold_window`입니다 — TTL에 의존하고 있었다면 그와 같은 기간을
> `cold_window`에 반드시 설정하십시오. (TTL을 청크에 실어 나르는 것은 포맷 변경이 필요해 이월돼
> 있습니다.)

청크로 옮겨진 데이터에는 베이스 TTL이 더 이상 적용되지 않습니다 — `cold_window`가 유일한 보존
장치이며, 이것이 "압축해서 보존 기간을 늘린다"의 메커니즘입니다.

## 2. 정책 설정 — `timeseries_tiering` 테이블 확장

정책은 테이블의 `extensions` 맵에 `timeseries_tiering` 키로 저장된 JSON 문서입니다. JSON을
**그대로 문자열로** 넣으면 됩니다 — CQL 한 줄이면 끝입니다:

```sql
ALTER TABLE ts.sensor WITH extensions = {
  'timeseries_tiering': '{"hot_window":"1h","chunk_window":"1h","interval":"5m"}'
};
```

`extensions`는 스키마상 blob 맵이라 원래는 hex 리터럴만 받지만, 이 포크는 평문 문자열을 UTF-8
바이트로 저장한다(`TableAttributes.parseExtensionValue`). `0x`로 시작하는 값만 hex 블롭으로
해석하므로 기존 hex 표기(`0x7b22...`)도 그대로 유효하다.

정책 해제는 `extensions = {}`로 맵을 비우면 됩니다. 설정된 값은
`SELECT extensions FROM system_schema.tables WHERE keyspace_name=? AND table_name=?`로 확인합니다.

### 2.1 필드

| 키 | 필수 | 기본값 | 의미 |
| --- | --- | --- | --- |
| `hot_window` | **필수** | — | 이 나이보다 젊은 행은 건드리지 않음 (행 단위 핫 구간). `chunk_window` 이상이어야 함 |
| `chunk_window` | | `1h` | 재인코딩 창의 고정 길이. 청크 1개 = 태그 1개 × 창 1개 (epoch 정렬) |
| `cold_window` | | 없음 | 지정 시, 이보다 오래된 청크는 통째로 삭제(보존 기한). `hot_window`보다 커야 함 |
| `consistency` | | `LOCAL_QUORUM` | 재인코더의 모든 읽기/쓰기/삭제에 쓰는 CL. **쿼럼 계열만 허용** — [5.3](#53-cl-쿼럼-하한) 참고 |
| `interval` | | `5m` | 이 테이블의 재인코딩 주기 (전역 스위프는 60초마다 돌며, interval이 지난 테이블만 실행) |

기간 값의 문법은 `<양의 정수><m|h|d>` (분/시/일)입니다. 알 수 없는 키, 규칙 위반(JSON 오류,
`hot_window < chunk_window`, `cold_window <= hot_window`, 쿼럼 미만 CL 등)은
`ALTER TABLE`/실행 시점에 거부됩니다.

### 2.2 코덱

고를 것이 없습니다. 컬럼 타입이 인코딩을 결정하고(§3.1.1), `double` 컬럼의 값 코덱은 **Chimp128
하나뿐**입니다 — 양자화된 워크/주기 신호(소수점이 잘린 실제 산업 센서값)에서 1.4~2.5 B/샘플이며,
값이 전혀 변하지 않는 컬럼은 코덱을 타기 전에 CONSTANT 플래그가 행 수와 무관하게 O(1) 바이트로
처리합니다. 페이로드에는 여전히 버전 바이트가 있어 디코딩은 자동입니다.

> 예전에는 `codec`(`auto`/`gorilla`/`chimp128`) 정책 필드가 있었지만 **제거**됐습니다. 정책 JSON에
> 아직 `codec`이 남아 있으면 `ALTER TABLE`이 그 키를 지목해 거부하므로, 조용히 무시되는 일은
> 없습니다. 선택의 근거와 Gorilla를 뺀 이유는 [코덱 bake-off](codec-bakeoff.md) 참고.

> **통합 이전 청크는 읽을 수 없습니다.** 그런 청크를 만나면 `SELECT`는 조용히 건너뛰지 않고
> **실패합니다**(손상 청크와 달리 전 청크에 해당하므로, 건너뛰면 쿼리가 성공하면서 과거 데이터만
> 빠진 결과를 계속 돌려주게 됩니다). `<테이블>__chunks`를 `DROP`하고 계층화를 다시 돌리세요 —
> 인코딩 시점에 베이스 행이 이미 삭제됐으므로 **그 데이터는 복구되지 않습니다.**

## 3. 청크 직접 조회 — 운영·디버그 용도

> SP3 투명 읽기가 켜진 지금은 베이스 테이블 `SELECT`가 청크를 자동 병합하므로 아래 패턴은
> 애플리케이션에 **불필요**합니다. 청크 자체를 점검할 때(압축률 확인, 손상 진단 등)만 사용하세요.

재인코딩된 창의 행은 베이스 테이블에서 물리적으로 **삭제**되지만, 투명 읽기 이전 관점에서 보면 핫 구간만
반환합니다. 오래된 데이터는 섀도 테이블을 직접 조회합니다.

### 3.1 섀도 테이블 스키마

`<베이스 테이블>__chunks`는 정책 첫 실행 시 자동 생성됩니다 (UCS `T4` 컴팩션):

| 컬럼 | 타입 | 의미 |
| --- | --- | --- |
| *(베이스 파티션 키 전체)* | 동일 | 베이스 테이블의 파티션 키 컬럼 **전부**를 이름·타입·순서 그대로 복제 (예: `tag_id text`, 또는 복합 키면 `(asset_id text, date text, hour int)` 세 컬럼 모두) |
| `window_start` | `timestamp` | 청크가 덮는 창의 시작 (클러스터링 키; 창 길이 = `chunk_window`) |
| `codec` | `tinyint` | 페이로드의 **첫 바이트(포맷 버전)를 그대로 복사한 값**. 현재 쓰이는 값은 `3`(컬럼 지향)뿐입니다 — 인코딩 후 페이로드에서 읽어 넣으므로 항상 실제 저장된 포맷을 가리킵니다. `1`(제거된 Gorilla)·`2`(제거된 단일 컬럼 Chimp128)는 더 이상 쓰이지도 읽히지도 않습니다 |
| `samples` | `int` | 청크에 인코딩된 **행 수** (값 개수가 아닙니다 — 행 1개가 컬럼 N개를 가집니다) |
| `max_row_writetime` | `bigint` | 청크에 포함된 원본 행들의 **모든 컬럼**을 통틀어 최대 writetime (원본 삭제 타임스탬프이자 지각 병합 판정 기준) |
| `payload` | `blob` | 창 1개의 인코딩 결과: 공유 타임스탬프 축 + 베이스 테이블 **일반 컬럼별 독립 섹션** |

#### 3.1.1 컬럼 타입별 인코딩

타입 코드는 **직렬화된 바이트를 어떤 방식으로 압축할지**만 고릅니다 — 바이트 자체는 바꾸지 않으므로
디코딩 결과는 원본 셀과 바이트 단위로 동일합니다.

| 베이스 컬럼 타입 | 청크 인코딩 |
| --- | --- |
| `double` | Chimp128 값 스트림 |
| `boolean` | 1비트 팩 |
| `int`, `date` | 4바이트 고정폭 + zigzag varint 델타 (`date`는 부호 없는 일수지만 4바이트 그대로 왕복) |
| `bigint`, `timestamp`, `time` | 8바이트 고정폭 + zigzag varint 델타 (정규화 없음: 각각 원값·epoch millis·자정 이후 나노초) |
| `text`, `varchar`, `ascii` | 사전(서로 다른 값 256개 이하) / 아니면 길이 접두 raw |
| **그 외 전부** | **불투명 바이트**(직렬화 그대로) + 사전/RLE — `blob`, `uuid`, `timeuuid`, `decimal`, `varint`, `inet`, `smallint`, `tinyint`, `float`, `duration`, frozen 컬렉션·UDT·튜플 |

`smallint`(2바이트)·`tinyint`(1바이트)·`float`(4바이트)를 굳이 불투명으로 두는 이유는, 고정폭 코드가
자기 폭으로 다시 직렬화하기 때문에(그리고 `boolean` 코드는 값을 0/1 비트로 접기 때문에) 폭이 다르면
왕복이 조용히 깨지기 때문입니다.

값이 전부 같은 컬럼은 디렉토리에 **한 번만** 저장되고 데이터 섹션이 0바이트가 되며(CONSTANT), 전부
`null`인 컬럼은 아무것도 저장하지 않습니다(ALL_NULL) — 행 수와 무관하게 O(1)입니다.

### 3.2 조회 CQL

```sql
-- [a, b) 구간의 콜드 데이터: 구간에 걸친 창들을 window_start 범위로 가져온다.
-- 첫 창은 a를 chunk_window 경계로 내림한 값부터 시작해야 a 직전 경계에 걸친 청크를 놓치지 않는다.
SELECT window_start, codec, samples, payload
FROM   ts.sensor__chunks
WHERE  tag_id = 'pump-01'
  AND  window_start >= '2026-07-01 00:00:00+0000'   -- floor(a, chunk_window)
  AND  window_start <  '2026-07-08 00:00:00+0000';  -- b
```

### 3.3 페이로드 디코딩 (JVM 클라이언트)

`payload`는 이 포크의 jar에 있는 컬럼 지향 디코더로 읽습니다. 두 번째 인자는 **프로젝션**입니다 —
`null`이면 전체 컬럼, 집합을 주면 그 컬럼들의 데이터 섹션만 디코딩하고 나머지는 건너뜁니다:

```java
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;

ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, Set.of("value_numeric", "quality"));
while (cursor.advance())
{
    long ts = cursor.timestamp();                       // epoch millis
    ByteBuffer v = cursor.getBytes("value_numeric");    // null = 그 행에서 null인 셀
    if (v != null)
        handle(ts, DoubleType.instance.compose(v));     // 베이스 컬럼 타입으로 compose
}
```

`getBytes`가 돌려주는 것은 **베이스 컬럼 타입의 직렬화 바이트 그대로**이므로, 그 컬럼의
`AbstractType.compose(...)`로 그대로 복원하면 됩니다. `hasColumn`은 그 컬럼이 이 청크(그리고 프로젝션)에
있는지, `isNull`은 현재 행에서 값이 없는지를 알려줍니다 — 청크가 쓰인 뒤 `ALTER TABLE ADD`된 컬럼은
없는 컬럼으로, `DROP`된 컬럼은 남아 있는 채로 보일 수 있습니다.

창 경계에 걸친 요청이라면 디코딩 후 `timestamp`로 한 번 더 필터하세요. 집계 대시보드용이라면
`samples`·`window_start`만으로도 창 단위 카운트/커버리지 확인이 가능합니다.

## 4. 운영

### 4.1 nodetool

```bash
nodetool retier <keyspace> <table>   # 지금 즉시 한 사이클 실행 (동기, interval 무시)
nodetool tieringstatus               # 정책이 있는 모든 테이블의 상태 표
```

- `retier`는 해당 테이블의 실행이 이미 진행 중이면(스위프든 다른 retier든) 오류로 알려줍니다.
- `tieringstatus` 출력: Keyspace, Table, Interval (ms), Last Run At, Windows Encoded,
  Rows Encoded, Late Merges, Chunks Expired.

### 4.2 가상 테이블

같은 데이터를 CQL로: 정책 필드 + 마지막 완료 실행의 통계입니다.

```sql
SELECT * FROM system_views.timeseries_tiering;
-- keyspace_name, table_name, hot_window_ms, chunk_window_ms, cold_window_ms(-1=미설정),
-- interval_ms, last_run_at(-1=실행 전), windows_encoded, rows_encoded,
-- late_merges, chunks_expired
```

### 4.3 MBean

JMX `org.apache.cassandra.db:type=TieredStorage` — `retier(keyspace, table)`,
`statusRows()` (nodetool 두 명령의 백엔드).

### 4.4 알아둘 것

- 통계는 **노드 로컬·인메모리**이며 마지막 완료 실행 기준입니다. 재시작하면 초기화됩니다.
- 다중 노드에서는 각 노드가 **자기 primary 토큰 레인지의 태그만** 재인코딩해 작업을 분할합니다.
- 전역 스위프는 60초 주기 1개이며, 테이블별 `interval`이 실제 실행 빈도를 결정합니다.

## 5. 불변식 — 왜 데이터가 유실되지 않는가

### 5.1 레인지 딜리트의 writetime 규칙 (지각 데이터 생존 원리)

한 창을 재인코딩한 뒤 원본 행을 지우는 레인지 딜리트는 항상
`DELETE ... USING TIMESTAMP <maxWt>` — **그 사이클이 청크에 인코딩한 행들의 최대 writetime** —
으로 발행됩니다. 사이클이 창을 읽은 **이후** 도착한 지각(late) 행은 정의상 더 새로운 writetime을
가지므로 이 톰스톤보다 새롭고, 따라서 살아남습니다. 다음 사이클이 그 행을 발견해 기존 청크와
**병합 재인코딩**하고(`samples` 증가, `late_merges` 카운트), 그때의 새 maxWt로 다시 지웁니다.
즉 "인코딩 안 된 행이 지워지는" 시점은 존재하지 않습니다.

`WRITETIME`은 **컬럼 단위**이므로, 여기서 말하는 최대치는 그 창에서 읽은 **모든 행 × 모든 일반 컬럼**의
셀 writetime을 통틀어 취한 값입니다(교체 대상 청크의 `max_row_writetime`도 함께 고려). 한 컬럼만
보고 지우면 다른 컬럼이 더 나중에 갱신된 행이 반쯤 남습니다.

> **불변식의 내재적 한계**: 이 논증은 "나중에 쓰인 행은 더 큰 writetime을 가진다"에 기대고 있습니다.
> 클라이언트가 `USING TIMESTAMP`로 타임스탬프를 직접 지정하거나, 코디네이터 간 시계가 어긋난 경우,
> 사이클이 창을 읽은 **뒤** 도착한 쓰기가 `maxWt` 이하의 타임스탬프를 가질 수 있고 그러면 톰스톤이
> 그 쓰기를 지웁니다. 계층화 대상 테이블에는 서버 타임스탬프를 쓰고, 과거 타임스탬프를 지정한
> 백필은 `hot_window` 안에서만 하십시오.

**셀 writetime이 하나도 없는 창은 통째로 건드리지 않습니다.** 일반 컬럼이 전부 `null`인 행(키만 넣은
`INSERT`, 셀이 전부 삭제·TTL 만료된 행)만 있는 창에는 안전하게 쓸 톰스톤 타임스탬프가 존재하지
않기 때문입니다 — 인코딩도, 삭제도 하지 않고 다음 사이클에 다시 시도하며, 실행 끝에 그 사실을 요약한
WARN을 남깁니다. 반대로 **일부** 컬럼만 `null`인 행은 정상이며 그대로 인코딩되고, 같은 창에 writetime을
가진 행이 하나라도 있으면 전부-`null` 행도 (존재 자체를 잃지 않도록) 함께 인코딩됩니다.

### 5.1.1 지각 행 병합은 **컬럼 단위**입니다

이미 청크에 들어간 타임스탬프에 대해 베이스 행이 다시 나타나면(`UPDATE t SET quality = ? WHERE ...`),
그 행이 **실제로 셀을 가진 컬럼만** 청크 값을 덮어씁니다. 나머지 컬럼은 청크에 있던 값을 그대로
유지합니다 — CQL의 셀 단위 last-write-wins와 같은 규칙이며, 행 단위로 통째 교체하면 `UPDATE`가
언급하지 않은 컬럼이 전부 지워집니다. (같은 이유로 콜드 구간의 **삭제**는 병합으로 표현할 수 없어
아예 거부됩니다 — §5.1.2.)

### 5.1.2 콜드 데이터는 **불변**입니다 (삭제는 거부됩니다)

계층화를 켠 테이블에서 **핫 윈도를 넘어서는 클러스터링을 톰스톤으로 지우는 쓰기는 거부됩니다.**

| 거부되는 쓰기 | 예 |
| --- | --- |
| 파티션 전체 삭제 | `DELETE FROM t WHERE tag = ?` (클러스터링 경계가 없으므로 반드시 콜드 구간을 덮습니다) |
| 레인지 삭제 | `DELETE FROM t WHERE tag = ? AND ts >= ? AND ts < ?` |
| 행 삭제 | `DELETE FROM t WHERE tag = ? AND ts = ?` |
| 셀 삭제 | `DELETE value FROM t WHERE ...`, `UPDATE t SET value = null WHERE ...`, `INSERT ... VALUES (..., null)` |

**허용되는 것**: 핫 윈도 안에 완전히 들어가는 삭제는 평소와 똑같이 동작합니다. 그리고 콜드 클러스터링에
**실제 값을 쓰는 것**(지각 `UPDATE t SET quality = 7`, 과거 시각으로의 `INSERT`)도 그대로 허용됩니다 —
다음 사이클이 컬럼 단위로 청크에 병합합니다(§5.1.1). 거부되는 것은 콜드 데이터를 **지우는** 쓰기뿐입니다.

**왜 문서화가 아니라 거부인가.** 청크화된 창의 베이스 행은 이미 삭제됐고 청크가 유일한 사본입니다.
그 창에 톰스톤을 쓰면 투명 읽기의 병합에서 청크 행을 가려주긴 하지만, 그것은 `gc_grace_seconds`가
톰스톤을 수거할 때까지뿐입니다. 그 뒤에는 지운 데이터가 **조용히, 영구히 되살아납니다** — 타이머 달린
데이터 부활이라 문서로 넘길 수 없습니다. 콜드 데이터를 지우는 지원되는 방법은 `cold_window` 만료이며,
파티션을 통째로 없애야 한다면 베이스 테이블과 `<테이블>__chunks`를 **함께** 지우십시오.

재인코더 자신의 레인지 딜리트는 예외입니다 — 방금 청크에 복사한 행만 지우며, 계층화 내부 경로임을
알리는 같은 스레드 로컬 플래그로 이 검사를 우회합니다.

### 5.1.3 복원 타임스탬프 = `max_row_writetime + 1`

투명 읽기가 청크에서 복원하는 셀의 writetime은 `max_row_writetime`이 **아니라 그보다 1μs 큰 값**입니다.
선택이 아니라 **강제**입니다: 재인코더는 원본 행을 `USING TIMESTAMP maxWt`로 지우고 `max_row_writetime`은
같은 `maxWt`인데, Cassandra의 삭제 판정은 `timestamp <= markedForDeleteAt`이므로 `maxWt`로 복원하면
**재인코더 자신의 톰스톤이 복원된 행을 전부 가려버립니다**. 톰스톤이 지운 행을 복원하려면 그 톰스톤보다
뒤여야 합니다.

- 사용자 톰스톤은 창을 청크화한 사이클보다 뒤이므로 여전히 복원본을 가립니다 → 삭제는 병합에서 정상
  동작합니다(다만 §5.1.2대로 애초에 거부됩니다).
- 대가: 톰스톤을 넘어 살아남는 지각 행은 writetime `> maxWt`, 즉 `>= maxWt + 1`이므로, **정확히
  `maxWt + 1`에 쓰인 지각 행은 복원본과 동률**이 됩니다. Cassandra는 셀 동률을 값 비교로 깨므로, 값이
  더 작은 지각 수정이 청크의 옛 값에 질 수 있습니다. 클라이언트가 `USING TIMESTAMP`를 직접 지정할 때만
  도달 가능하고(서버 마이크로초 타임스탬프면 사실상 불가능), 명시적 타임스탬프가 평범한 Cassandra에서도
  이미 갖는 위험과 같은 종류입니다.

### 5.2 청크 INSERT 타임스탬프 규칙

청크 upsert는 `USING TIMESTAMP max(maxWt + 1, 기존_청크_writetime + 1)`로 씁니다. 항상
(1) 방금 인코딩한 모든 원본 행보다 뒤, (2) 교체 대상인 기존 청크 행보다 뒤가 보장되어, 크래시 후
재실행이 기존 청크와 **같은 타임스탬프로 충돌**해 행의 셀들이 이전/이후 실행 사이에 찢어지는
(column-level tie-break) 경우를 차단합니다. 덕분에 "청크 쓰고 삭제 전에 죽는" 어떤 시점에서도
재실행은 안전하게 수렴합니다(멱등).

### 5.3 CL 쿼럼 하한

`consistency`는 `QUORUM`/`LOCAL_QUORUM`/`EACH_QUORUM`/`ALL`만 허용됩니다. `ONE` 같은 약한 CL을
허용하면: 재인코더가 직전 사이클의 청크를 **못 본 채**(복제 지연) 새 행만으로 청크를 만들어 쓰고,
원본 레인지 딜리트는 그대로 나가므로 — 이전 청크에만 있던 샘플이 **조용히, 영구히** 유실됩니다.
쿼럼 읽기는 쿼럼 쓰기와 반드시 겹치므로 이 경로가 봉쇄됩니다.

## 6. 제한사항 (현 단계에서 이연된 항목)

1. **cold 만료 경계**: 만료 기준이 `window_start < now - cold_window`이므로, 삭제되는 마지막 창의
   후반부 샘플은 `cold_window`보다 최대 `chunk_window` 하나만큼 덜 오래됐는데도 함께 삭제될 수
   있습니다. `cold_window`를 정할 때 `chunk_window`만큼의 여유를 두세요.
2. **서버측 write timestamp 가정**: 설계는 행 writetime이 서버가 찍는 현재 시각이라는 전제 위에
   있습니다. 클라이언트가 `USING TIMESTAMP`로 **이미 스윕된 maxWt 이하의** writetime을 지정해 행을
   넣으면, 그 행은 기존 레인지 톰스톤보다 오래된 것으로 취급되어 재인코딩 없이 사라질 수 있습니다.
   계층화 대상 테이블에는 클라이언트 지정 타임스탬프를 쓰지 마세요.
3. **베이스 행이 전부 사라진 태그의 청크 만료**: 태그 열거가 베이스 테이블 `DISTINCT` 스캔이므로,
   모든 행이 재인코딩·삭제된 뒤 새 쓰기가 없는 태그는 더 이상 열거되지 않고, 그 태그의 콜드 청크는
   `cold_window`가 지나도 만료되지 않습니다 (후속 서브프로젝트에서 청크 테이블 기준 만료로 해결
   예정).
4. **잘못된 정책 JSON**: 파싱에 실패하는 정책이 걸린 테이블은 고칠 때까지 60초 스위프마다 ERROR
   로그를 남깁니다 (조용히 무시되어 잊히는 것보다 시끄러운 쪽을 선택).
5. **동명 테이블 DROP + CREATE**: 실행 통계가 `keyspace.table` 이름 키의 인메모리 맵이라, 같은
   이름으로 다시 만든 테이블은 첫 실행 전까지 이전 테이블의 통계가 가상 테이블/`tieringstatus`에
   잠깐 보입니다.

## 7. 테스트

- `org.apache.cassandra.db.timeseries.tiering.TieringPolicyTest` — 정책 파싱/검증 규칙
- `org.apache.cassandra.db.timeseries.tiering.TieredStorageServiceTest` — 재인코딩 사이클 전체
  (인코딩·삭제, 핫 구간 보존, 지각 병합, 멱등 수렴, cold 만료, auto 코덱, 스위프 격리, 재진입 가드,
  가상 테이블)
- `org.apache.cassandra.tools.nodetool.mock.TieredStorageMockTest` — nodetool 두 명령의 JMX
  패스스루/오류 표면화
- [docker/integration-test.sh](../../docker/integration-test.sh) — 실제 이미지에서 정책 설정 →
  retier → 청크 검증 → 지각 병합 → 상태 표까지 (릴리스 게이트)
