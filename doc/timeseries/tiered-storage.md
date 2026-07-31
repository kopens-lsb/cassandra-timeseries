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

시계열 테이블의 오래된(닫힌) 행을 백그라운드에서 Chimp128 청크로 압축해 섀도 테이블
`<테이블>__chunks`로 옮기고, 원본 행은 삭제하는 서버 내장 계층화 엔진입니다. 최근 데이터(핫 구간)는
행 단위로 그대로 남아 쓰기·조회 모두 기존과 동일하고, 오래된 데이터는 샘플당 수 바이트 수준으로
압축된 청크로 보관됩니다. 코덱(Chimp128)의 압축 특성은
[bake-off 결과](codec-bakeoff.md)를 참고하세요.

> **투명 읽기(SP3) 포함**: 베이스 테이블 `SELECT`가 핫 로우와 청크 디코드 로우를 **자동 병합**해
> 돌려줍니다 — 애플리케이션은 압축의 존재를 모릅니다. 파티션(태그) + 시간 범위/포인트 질의,
> 집계(`avg`/`count`/`time_bucket` GROUP BY), gap-fill, `LIMIT`/`ORDER BY DESC` 모두 핫·콜드에
> 걸쳐 동작합니다. `__chunks` 직접 조회(아래 §3)는 이제 운영·디버그 용도입니다.
> 제한: ① 병합 질의가 한 페이지를 넘으면 명확한 에러 + 해결 힌트(페이지 크기 확대·범위 축소·집계)로
> 실패합니다(침묵 중복/유실 방지, v1 스코프). ② 디코드 로우의 `writetime(value)`은 청크의
> `max_row_writetime` 근사값입니다. ③ 손상 청크는 경고와 함께 건너뛰고 나머지 데이터를 제공합니다.

## 1. 대상 스키마 — 정준(canonical) 시계열 테이블

재인코더는 다음 형태의 테이블만 처리합니다 (그 외 형태에 정책을 걸면 60초 스위프마다 ERROR 로그를
남기고 건너뜁니다):

- 파티션 키 컬럼 **정확히 1개** (이름·타입 자유 — 예: `text` 태그)
- 클러스터링 컬럼 **정확히 1개**, 타입 `timestamp`
- 일반 컬럼 **정확히 1개**, 타입 `double`

```sql
CREATE TABLE ts.sensor (
    tag_id    text,
    timestamp timestamp,
    value     double,
    PRIMARY KEY (tag_id, timestamp)
);
```

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

코덱은 **Chimp128 하나뿐**이라 고를 것이 없습니다. 양자화된 워크/주기 신호(소수점이 잘린 실제
산업 센서값)에서 1.4~2.5 B/샘플이며, 값이 전혀 변하지 않는 구간은 코덱을 타기 전에 컬럼 지향
청크의 CONSTANT 플래그가 행 수와 무관하게 O(1) 바이트로 처리합니다. 페이로드에는 여전히 버전
바이트가 있어 디코딩은 자동입니다.

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
| *(베이스 파티션 키)* | 동일 | 베이스 테이블의 파티션 키 컬럼을 이름·타입 그대로 복제 (예: `tag_id text`) |
| `window_start` | `timestamp` | 청크가 덮는 창의 시작 (클러스터링 키; 창 길이 = `chunk_window`) |
| `codec` | `tinyint` | 페이로드 코덱 버전 (2 = Chimp128; 1 = 제거된 Gorilla) |
| `samples` | `int` | 청크에 인코딩된 샘플 수 |
| `max_row_writetime` | `bigint` | 청크에 포함된 원본 행들의 최대 writetime (지각 병합 판정 기준) |
| `payload` | `blob` | 인코딩된 `(timestamp, value)` 샘플들 |

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

`payload`는 이 포크의 jar에 있는 버전 디스패처로 디코딩합니다 (첫 바이트가 코덱 버전이므로 호출
쪽에서 코덱을 구분할 필요가 없습니다):

```java
import org.apache.cassandra.db.timeseries.ChunkCodecs;
import org.apache.cassandra.db.timeseries.SampleCursor;

SampleCursor cursor = ChunkCodecs.cursor(payload);   // payload: ByteBuffer
while (cursor.advance())
    handle(cursor.timestamp(), cursor.value());       // epoch millis, double
```

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
