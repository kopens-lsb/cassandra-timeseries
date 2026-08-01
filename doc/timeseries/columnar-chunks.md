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

# 청크 포맷 v3 — 컬럼 지향 청크

[계층형 저장](tiered-storage.md)이 한 창(window)의 행들을 압축해 `<테이블>__chunks.payload`에
넣을 때 쓰는 바이트 포맷입니다. 구현은
[`ColumnarChunkCodec`](../../src/java/org/apache/cassandra/db/timeseries/ColumnarChunkCodec.java),
읽기 커서는
[`ColumnarCursor`](../../src/java/org/apache/cassandra/db/timeseries/ColumnarCursor.java).

이 문서는 **와이어 포맷 규격**입니다. 외부 도구로 페이로드를 직접 디코드하려는 경우, 또는 포맷을
바꾸려는 경우에 읽으세요. 운영자 관점의 설정·튜닝은 [운영 튜닝 가이드](operations-tuning.md)를
보면 됩니다.

## 1. 왜 컬럼 지향인가

v1/v2 청크는 `(timestamp, double)` 한 쌍만 담을 수 있었습니다. 그래서 계층화는 "파티션 키 1개 +
timestamp 클러스터링 1개 + double 컬럼 정확히 1개"라는 **정준 스키마**에만 적용됐고, 실제 산업
현장 테이블에는 **단 하나도 적용되지 않았습니다**. 실 운영 키스페이스의 대표 테이블은 이렇게
생겼습니다:

```sql
CREATE TABLE tm_tag_point (
    tag_id text, timestamp timestamp,
    area_id text static, asset_id text static, /* … static 7개 */
    attribute frozen<map<text,text>>, error_code int, latency int,
    quality int, value text, value_boolean boolean, value_numeric double,
    PRIMARY KEY (tag_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

v3는 **하나의 타임스탬프 축에 일반 컬럼 전부**를 각자 독립된 섹션으로 담습니다. 그 결과:

- **모든 시계열 테이블에 적용**됩니다. 어떤 타입이든 불투명(opaque) 바이트 폴백이 항상 있으므로
  "이 타입은 지원 안 됨"으로 테이블이 통째로 탈락하는 일이 없습니다. (계층화 자체가 거부하는
  스키마는 따로 있습니다 — counter 컬럼, 비frozen 컬렉션, 머티리얼라이즈드 뷰 등.
  [tiered-storage.md §1](tiered-storage.md) 참고.)
- **상수 컬럼이 0바이트**가 됩니다. 위 테이블에서 `quality`는 항상 192, `error_code`는 항상 0,
  `attribute`는 항상 `{}`, `value_numeric`은 전부 null입니다. 행 지향 포맷은 이 값들을 행마다
  다시 씁니다. v3는 창당 한 번 씁니다.
- **컬럼 단위 스킵**이 가능합니다. 디렉토리가 컬럼마다 자기 데이터 섹션의 바이트 길이를 들고
  있으므로, `SELECT latency`는 나머지 6개 컬럼의 섹션을 디코드하지 않고 건너뜁니다.

### 1.1 실측: 행당 바이트

위 `tm_tag_point` 형태를 실제 밀도로 인코딩한 결과입니다.
`TieredStorageColumnsTest.realShapeBytesPerRowIsMeasuredWithValueBooleanPopulated`가 실행할 때마다
이 값을 로그로 남기므로, 재현하려면 그 테스트를 돌리면 됩니다. 측정 조건:

- **형태** — 위 §1의 `tm_tag_point` 그대로. static 7개(청크화 대상이 아님) + 일반 컬럼 8개.
- **밀도** — `chunk_window` 1시간에 **1초 간격 3,600행**. 창 하나 = 청크 하나입니다. 밀도를 밝히지
  않은 행당 바이트는 의미가 없습니다 — 행이 몇 개 안 되는 창은 전부 헤더입니다.
- **분포** — 시드 고정(`Random(20260801L)`). `quality`는 항상 192, `error_code`는 항상 0,
  `attribute`는 항상 빈 맵(→ CONSTANT); `value_numeric`은 한 번도 쓰지 않음(→ ALL_NULL);
  `latency`는 1..999 균등 난수(고엔트로피); `value`는 소수 첫째 자리로 반올림한 랜덤 워크를 담은
  짧은 텍스트(백색 잡음이 아니라 천천히 변하는 센서값); `value_boolean`은 300샘플마다 뒤집히는
  상태 비트.

| `value_boolean` | 페이로드 | 행당 |
| --- | --- | --- |
| 쓰지 않음 (ALL_NULL) | 11,747 B | 3.263 B |
| **모든 행에 씀** (1비트 팩) | **12,198 B** | **3.388 B** |

**인용해야 하는 값은 3.39 B/행입니다.** 위쪽 값은 `value_boolean`을 "아무도 쓰지 않는 컬럼"으로
모델링한 것이라, 선언된 일반 컬럼 8개 중 **2개가 공짜인 테이블**을 잰 셈이 됩니다 — 포맷에
유리하게 기운 숫자입니다. 두 값의 차이 451 B는 전부 설명됩니다: 비트팩 섹션 450 B(행당 정확히
1비트) + 디렉토리 `sectionLen` varint가 1 B에서 2 B로 늘어난 몫 1 B.

비교 대상은 두 개입니다.

| 기준 | 행당 | 측정값과의 관계 |
| --- | --- | --- |
| 운영 중인 행 저장 (디스크 실측, 이미 Zstd 압축됨) | ≈9 B | 측정값이 **2.7× 작음** |
| 설계 단계 추정 | ≈2.9 B | 측정값이 **17% 큼** |

즉 **설계 추정은 빗나갔습니다.** 추정에 맞는 숫자가 나오도록 측정 조건을 고르지 않았습니다 —
여기 적힌 것이 실제로 나온 값입니다. 운영 기준선을 2.7배 이기는 결론은 그대로입니다.

## 2. 레이아웃

모든 정수는 **빅엔디언**. `varint`는 부호 없는 LEB128(7비트씩, 최상위 비트가 continuation).

```
┌──────────────────────────────────────────────────────────── 헤더 25B ─┐
│ version : int8   = 3                                                  │
│ rowCount: int32  창에 담긴 행 수 (1 이상, MAX_ROWS=16,777,216 이하)    │
│ firstTs : int64  첫 타임스탬프 (epoch millis)                          │
│ lastTs  : int64  마지막 타임스탬프                                     │
│ colCount: int16  컬럼 수                                              │
│ dirSize : int16  아래 디렉토리의 총 바이트 수                          │
├───────────────────────────────────────────── 컬럼 디렉토리 (dirSize B) ─┤
│ 컬럼마다, 이름의 자연 String 순서로:                                    │
│   typeCode  : int8   §3의 타입 코드                                    │
│   flags     : int8   0x01 ALL_PRESENT · 0x02 ALL_NULL · 0x04 CONSTANT  │
│   nameLen   : int8   이름의 UTF-8 바이트 길이 (1..255)                 │
│   name      : UTF-8 bytes                                             │
│   sectionLen: varint 이 컬럼의 데이터 섹션 길이 (스킵용)                │
│   [CONSTANT일 때만] constLen: varint, constBytes: bytes                │
├──────────────────────────────────────────────────────── null 비트맵 ─┤
│ ALL_PRESENT도 ALL_NULL도 아닌 컬럼만, 디렉토리와 같은 순서로:           │
│   firstBit: int8  0 = null로 시작, 1 = 값 있음으로 시작                │
│   run 길이들: varint 를 번갈아 (있음↔없음), 합이 rowCount              │
├───────────────────────────────────────────────────── 타임스탬프 축 ─┤
│ first : int64  (헤더의 firstTs와 같은 값을 한 번 더 씁니다)            │
│ DoD 비트스트림: i>=1 에 대해 (Δᵢ − Δᵢ₋₁), TimestampCodec.writeDod      │
├──────────────────────────────────────────────────────── 데이터 섹션 ─┤
│ 컬럼마다 정확히 sectionLen 바이트, 디렉토리와 같은 순서                 │
│ (CONSTANT 또는 ALL_NULL 컬럼은 sectionLen = 0)                        │
└───────────────────────────────────────────────────────────────────────┘
```

타임스탬프는 **엄격히 증가**해야 합니다(같은 값 불가). 인코더가 위반을 거부합니다.

### 2.1 null과 상수의 상호작용

`flags`의 세 비트는 서로 배타적이지 않습니다. 판정 순서는:

| 컬럼 상태 | flags | 비트맵 | 섹션 | 상수 바이트 |
| --- | --- | --- | --- | --- |
| 값이 하나도 없음 | `ALL_NULL` (+`ALL_PRESENT` 아님) | 없음 | 0B | 없음 |
| 전부 값 있고 전부 같은 값 | `ALL_PRESENT｜CONSTANT` | 없음 | 0B | 있음 |
| 일부 null, 있는 값은 전부 같음 | `CONSTANT` | 있음 | 0B | 있음 |
| 일부 null, 값이 여러 가지 | 없음 | 있음 | 있음 | 없음 |
| 전부 값 있고 값이 여러 가지 | `ALL_PRESENT` | 없음 | 있음 | 없음 |

**상수 판정은 "존재하는 값들"만 봅니다.** 위 테이블의 `quality`는 창 안에서 일부 행이 null이어도
나머지가 전부 192면 CONSTANT로 잡히고, 어느 행이 null인지는 비트맵(보통 수 바이트)이 기억합니다.

## 3. 타입별 인코딩

타입 코드는 **컬럼별·청크별**로 기록됩니다. 즉 같은 컬럼이 창마다 다른 코드로 인코딩될 수
있고, 리더는 디렉토리가 말하는 코드만 믿으면 됩니다.

| 코드 | 이름 | CQL 타입 | 섹션 인코딩 |
| --- | --- | --- | --- |
| `0x00` | (영구 예약) | — | 옛 Gorilla double. **재사용 금지**, 읽으면 코드를 지목하며 실패 |
| `0x01` | `DOUBLE_CHIMP` | `double` | Chimp128 값 스트림 ([bake-off](codec-bakeoff.md)) |
| `0x02` | `BOOLEAN` | `boolean` | 1비트 팩 |
| `0x03` | `INT32` | `int`, `date` | 첫 값 + zigzag varint 델타 |
| `0x04` | `INT64` | `bigint`, `timestamp`, `time` | 첫 값 + zigzag varint 델타 |
| `0x05` | `TEXT` | `text`, `varchar`, `ascii` | 사전 또는 raw (아래) |
| `0x06` | `OPAQUE` | 그 밖의 전부 | 사전 또는 raw (아래) |

`0x00`은 Chimp128을 유일한 double 코덱으로 정리하면서 제거됐습니다. 코드를 다른 타입에
재할당하지 않고 **영구히 비워 두는** 이유는, 제거 이전 코드가 쓴 청크를 "다른 타입"으로 조용히
디코드하는 사고를 막기 위해서입니다.

**`date`가 `INT64`가 아니라 `INT32`인 이유**: 카산드라는 `date`를 4바이트로 직렬화합니다. 타입
코드는 값을 정규화하지 않고 **압축 방식만 고릅니다** — 그래서 폭(width)이 맞아야 합니다. 같은
이유로 `smallint`/`tinyint`/`float`는 전용 코드 없이 `OPAQUE`로 갑니다.

**폭이 안 맞으면 자동 강등**: 카산드라는 고정폭 타입에도 길이 0인 값을 허용합니다
(`blobAsInt(0x)`는 합법이고 `Int32Serializer.validate`는 0바이트도 통과시킵니다). 그런 값이 섞인
컬럼은 **그 청크에 한해** `OPAQUE`로 강등돼 바이트 그대로 저장됩니다. 강등하지 않으면 인코더가
underflow로 터지고, 계층화는 그 파티션을 매 주기 실패시키며 영원히 재시도합니다.

### 3.1 TEXT / OPAQUE 섹션

```
mode: int8   0 = 사전(dictionary), 1 = raw
```

- **사전 모드** — 서로 다른 값이 256개 이하일 때. 값들을 정렬해 한 번씩 저장하고, 행마다 인덱스를
  씁니다. 카디널리티가 낮은 태그 상태 문자열·enum류가 여기 걸립니다.
- **raw 모드** — 그 외. 값마다 `varint length + bytes`.

읽을 때 사전 크기는 256으로 **강제 검증**합니다. 손상된 페이로드가 큰 사전 크기를 주장해 그만큼
할당하게 만드는 것을 막기 위한 것으로, 페이로드가 제시하는 길이로 배열을 잡는 모든 지점에 같은
종류의 상한 검사가 들어 있습니다.

## 4. 결정성과 멱등성

재인코더는 같은 입력에 대해 **바이트까지 동일한** 페이로드를 만들어야 합니다. 그러지 않으면 매
주기 청크를 다시 써서 무한 루프가 됩니다. 그래서:

- 컬럼은 **이름의 자연 String 순서**로 정렬합니다. 호출자가 넘긴 `SortedMap`의 comparator를
  물려받지 않도록 인코더가 새 `TreeMap`에 `putAll` 합니다 (`new TreeMap<>(map)` 생성자는 원본의
  comparator를 채택해 버리므로 이 정렬을 조용히 무력화합니다).
- 사전은 **값 정렬**로 만듭니다.
- double 코덱은 하나뿐이므로(Chimp128) 선택의 여지가 없습니다.

## 5. 스키마 진화

컬럼은 **이름 문자열로** 식별됩니다. 오프셋이나 ID가 아니라서:

- 청크를 쓴 뒤 `ALTER TABLE ... DROP col` → 그 컬럼은 읽을 때 **무시**됩니다.
- 청크를 쓴 뒤 `ALTER TABLE ... ADD col` → 옛 청크의 행에서 그 컬럼은 **null**로 읽힙니다.
- 컬럼 순서 변경 → 무관합니다.

행 복원 시에는 `BTreeRow.unsortedBuilder()`를 씁니다. 청크 디렉토리는 Java `String` 비교 순서인
반면 행의 셀은 이름의 **UTF-8 바이트** 비교 순서라, 비ASCII 컬럼명에서 둘이 어긋나기 때문입니다.

## 6. 손상·미지원 버전 처리

두 가지를 **다르게** 다룹니다.

- **손상(corruption)** — 잘린 페이로드, 말이 안 되는 길이, 범위 밖 인덱스. `IllegalArgumentException`
  으로 통일해 던집니다. 읽기 경로는 이걸 잡아 **경고 후 그 청크만 건너뛰고** 나머지 데이터를
  돌려줍니다. 한 청크의 손상이 전체 조회를 죽이지 않게 하려는 것입니다.
- **미지원 포맷 버전** — 버전 바이트가 *실재하는 포맷을 지목*하지만 이 빌드가 읽지 못하는 경우
  (`UnsupportedChunkFormatException`). 이건 **절대 삼키지 않고 전파**합니다. 손상은 산발적이지만
  포맷 불일치는 **체계적**이라, 건너뛰면 모든 읽기에서 과거 데이터가 조용히 잘려 나갑니다.
  버전 바이트가 아무 포맷도 지목하지 않으면 그냥 손상으로 취급합니다(건너뛰기 가능).

v1(Gorilla)과 v2(Chimp128 단일 컬럼) 청크는 **삭제됐습니다**. 계층화가 어디에도 적용된 적이 없어
현장에 v1/v2 페이로드가 존재하지 않으므로 하위 호환을 유지할 이유가 없었습니다. 그 버전 바이트를
만나면 위 규칙에 따라 크게 실패합니다.

## 7. 값 버퍼 계약

디코더가 돌려주는 `ByteBuffer`는 **반드시 배열 기반(`hasArray() == true`)** 이어야 합니다.

읽기 전용 힙 버퍼(`asReadOnlyBuffer()`의 결과)는 `hasArray() == false`이면서 `isDirect() == false`
입니다. 카산드라의 `FastByteOperations`는 이 조합을 만나면 다이렉트 버퍼 경로로 들어가 주소 0을
역참조하고, **JVM이 SIGSEGV로 죽습니다**. 셀 값이 실제 조정(reconciliation)·비교 경로에 들어가는
순간 — 동률 비교, 툼스톤 비교 — 재현됩니다.

`ColumnarChunkCodecTest`가 모든 디코드 경로의 반환 버퍼를 실제 비교 기계
(`FastByteOperations.compareUnsigned`, `ValueAccessor.compare`)에 통과시키되 **`hasArray()`를 먼저
검사**합니다. 회귀가 나면 코어 덤프 대신 원인을 지목하는 실패 메시지가 나오게 하려는 배치입니다.

## 8. 크기 한계

| 항목 | 한계 | 이유 |
| --- | --- | --- |
| 행 수 / 청크 | 16,777,216 (`MAX_ROWS`) | 청크는 통째로 디코드되므로 무한 행 수 = 무한 할당 |
| 실효 행 수 / 창 | 정책의 `max_samples_per_window` (기본 200,000) | 재인코더가 **페이징 중에** 중단할 수 있어야 함 |
| 컬럼 수 | 65,535 | `colCount`가 int16 |
| 디렉토리 | 65,535 B | `dirSize`가 int16 |
| 컬럼명 | UTF-8 255 B | `nameLen`이 int8 |
| 사전 크기 | 256 | 넘으면 raw 모드 |

`max_samples_per_window`가 포맷 한계(16.7M)가 아니라 20만인 것은, 컬럼이 N개면 창 하나가 그
N배의 값 버퍼를 물고 있기 때문입니다. 창 폭(`chunk_window`)은 **시간**을 제한하지 행 수를 제한하지
않으므로, 초당 수집률이 높은 태그에서는 이쪽이 실효 한계가 됩니다.
