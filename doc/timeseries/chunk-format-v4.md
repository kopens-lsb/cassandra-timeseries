# 청크 포맷 v4 — 블록 기반 컬럼나 청크 (설계)

상태: 설계 확정, 구현 전. v3를 **완전히 대체**하며 이중 읽기 경로는 두지 않는다.

**왜 지금인가.** v3를 깰 수 있는 창이 열려 있다 — 계층화가 운영에 켜진 적이 없어 재작성해야 할
청크가 없다(2026-08-03 확인. 단 node 41에는 테스트로 만든 청크 테이블이 있었고 폐기 결정됨).
**이 창은 계층화를 첫 운영 테이블에 켜는 순간 닫힌다.** 그래서 한 번에 제대로 잡는다.

## 0. v3에서 유지 / 폐기

| v3 결정 | v4 |
|---|---|
| 컬럼별 `typeCode`/`flags`/`sectionLen` 디렉토리 | **유지.** 타입 코드는 여전히 *압축만* 선택 |
| `CONSTANT`/`ALL_NULL`이 O(1) 바이트 | **유지, 불변.** 운영 형태에서 8컬럼 중 4개가 0바이트인 최대 이득 |
| 미투영 컬럼 섹션 건너뛰기 | **유지+강화** — 디렉토리에 `sectionOffset`을 명시해 접두합 없이 점프 |
| 바이트 결정성 (`chunkUnchanged`) | **유지, 규칙 강화**(§5) — v4는 인코더 선택지가 늘어 틀릴 여지가 커짐 |
| `hasArray()` 계약 | **그대로 유지** |
| 손상(`IllegalArgumentException`, 건너뜀) vs 미지원 버전(`UnsupportedChunkFormatException`, 전파) | **유지**, 내부 코드까지 확장 |
| Chimp128 double | **폐기.** ALP / ALP-RD 단독 |
| DoD 타임스탬프 비트스트림 | **폐기.** 타임스탬프는 평범한 블록형 INT64 유사 컬럼 |
| 컬럼 전체에 걸친 RLE null 비트맵 | **폐기.** 블록별 4모드(§4) |
| 청크 전체 순차 디코드 | **폐기.** 모든 블록이 독립 디코드·랜덤 접근 |
| 25바이트 헤더, `dirSize` u16, varint 메타데이터 | 40바이트 헤더, `directoryLen` u32, 고정폭 메타데이터 |

수치는 전부 **빅엔디안**. 팩된 레인 내부 비트 순서는 **LSB-first**(§6) — v3의 `BitWriter`
(MSB-first)와 의도적으로 다르며, v4는 그 클래스들을 청크 경로에서 쓰지 않는다.

## 1. 목표와 명시적 비목표

목표(우선순위 순): ① 블록 기반 독립 인코딩 ② **가지치기(pruning)에 충분한** 통계 ③ 정렬·SIMD 친화
④ v3가 잘한 것 보존.

> **비목표 — 나중에 누가 다시 넣지 않도록 명시한다: v4의 통계는 가지치기 전용이다.**

청크 시간 경계, 컬럼별 min/max, 블록별 min/max는 싣는다. `sum`, `sum_sq`, `first_value`,
`last_value`, 스케치, 히스토그램은 **싣지 않고 자리도 예약하지 않는다.**

이유는 비싸서가 아니라 **쓸 데가 없어서**다. 그 필드들의 유일한 용도는 메타데이터로 집계를
*답하는* 것인데, 이 코드베이스에는 부분 집계 경로가 없다 — 집계는 읽기 경로가 만든 `Row` 위에서
돌고, 그걸 우회하면 LIMIT·페이징·gap-fill·읽기수리 순서를 동시에 건드리게 된다. 그 우회는
2026-08-03 사용자 결정으로 취소됐다. **가지치기는 범주가 다르다** — 행은 평소대로 흐르고, 통계의
효과는 "청크/블록을 안 여는 것"뿐이다. 가지치기 통계는 "안 열어도 될 걸 열었다" 방향으로만 틀릴 수
있지만, 답하기 통계는 "틀린 숫자를 반환했다" 방향으로 틀릴 수 있다. 부분 집계 경로가 언젠가
생기면 그때 v5가 추가하면 된다(§9).

## 2. 블록 모델

`blockSize = 1 << blockSizeLog2`, 헤더에서 읽으며 **v4.0은 1024**.

```
blockCount  = (rowCount + blockSize - 1) >>> blockSizeLog2
blockIndex  = rowIndex >>> blockSizeLog2
blockOffset = rowIndex & (blockSize - 1)
blockRows(k)= min(blockSize, rowCount - (k << blockSizeLog2))
```

**이 매핑은 모든 컬럼과 타임스탬프 축에서 동일하다.** 컬럼 A의 블록 k와 컬럼 B의 블록 k가 정확히
같은 행 범위를 덮는다 — 그래서 A의 블록 통계로 고른 블록 집합이 B에 번역 없이 그대로 적용된다.

블록은 **행 인덱스** 기준으로 자른다(존재 값 인덱스가 아니라). 후자가 희소 컬럼에서 약간 작지만
위의 컬럼 정렬 성질을 파괴한다.

`sectionLen == 0`인 컬럼(`ALL_NULL`, 또는 전 행 존재하는 `CONSTANT`)은 `blockCount == 0`이고 블록
테이블이 없다 — v3의 O(1) 컬럼이 O(1)로 유지되는 지점.

블록 내 값 인덱스: `rank(presenceWords, blockOffset)` — 1024 기준 최대 16회 `Long.bitCount`
인트린식, 할당 없음. v3는 컬럼 전체 RLE 런 리스트를 걸어야 했다.

**블록 독립성(규범):** 블록 k 디코드에 필요한 것은 헤더, 그 컬럼의 디렉토리 엔트리, 섹션 프리앰블
(텍스트/opaque 딕셔너리만), 블록 테이블 엔트리 k와 k+1(본문 길이용), 블록 k 본문뿐이다.

**1024인 이유:** 폭 w에서 정확히 `16w` 워드(꼬리 없음) · 모든 벡터 종이 나눠떨어짐(512비트면
128회) · presence가 정확히 2 캐시라인 · 블록 엔트리 24B가 행당 0.023B(측정된 3.39B/행의 0.8%) ·
ALP 참조 구현과 DuckDB가 1024 사용 · 실제 창이 작음(1h@1Hz = 3600행 = 3.5블록).

## 3. 헤더 (40바이트)

| off | size | 필드 |
|---|---|---|
| 0 | 1 | `version` u8 = **4** |
| 1 | 4 | `rowCount` u32 |
| 5 | 8 | `firstTimestamp` i64 (epoch ms) |
| 13 | 8 | `lastTimestamp` i64 |
| 21 | 1 | `blockSizeLog2` u8 (6..15) |
| 22 | 2 | `columnCount` u16 |
| 24 | 4 | `directoryOffset` u32 (항상 40, 명시) |
| 28 | 4 | `directoryLen` u32 |
| 32 | 4 | `tsSectionOffset` u32 |
| 36 | 4 | `tsSectionLen` u32 |

오프셋 0·1·5·13은 **v3와 의도적으로 동일** — `rowCount()`/`firstTimestamp()`/`lastTimestamp()`가
O(1) 헤더 조회로 남는다. 이게 "payload 디코드 없이 읽는 청크 시간 경계" 요구를 만족시킨다.

거부한 헤더 필드: 매직 넘버(버전 바이트가 이미 디스패치), CRC32C(SSTable 압축이 이미 체크섬),
전체 길이(`payload.remaining()`이 권위), 청크 플래그 바이트(쓸 데 없는 플래그는 유혹).

## 4. 디렉토리 · 통계 · presence

디렉토리 엔트리: `typeCode u8 | colFlags u8 | statOrder u8 | nameLen u8 | name | sectionOffset u32
| sectionLen u32 | blockCount u32 | [constLen varint + constBytes] | [min, max]`. 컬럼명 자연순 정렬,
8바이트 패딩.

`colFlags`: `0x01 ALL_PRESENT`, `0x02 ALL_NULL`, `0x04 CONSTANT`, `0x08 HAS_STATS`. `0xF0`은 0이어야
하며 아니면 손상.

**타입 코드:** `0x00` 영구 무효(제로 트랩) · `0x01 DOUBLE`(ALP/ALP-RD) · `0x02 BOOLEAN` ·
`0x03 INT32` · `0x04 INT64` · `0x05 DATE32` · `0x06 TEXT` · `0x07 OPAQUE`.

**`statOrder`** — 통계를 계산한 비교 순서. 매핑은 **`AbstractType`의 실제 비교자로 검증할 것**:
구현 중 `TimeType`이 `SIGNED_INT`가 아니라 `ComparisonType.BYTE_ORDER`(부호 없음)로 밝혀졌다.
합법 도메인(0~86399999999999 ns)에서는 두 비교의 결과가 같아서, 정상 값으로 만든 테스트는 전부
통과하고 비트 63이 선 값에서만 갈라진다 — 기대 도메인이 아니라 비교자를 따를 것.
그리고 이 타입들은 모두 **빈 값을 맨 앞으로** 정렬하므로 비교자가 길이 0을 거부하면 안 된다:
길이 0 고정폭 값이 바로 §12의 `OPAQUE` 강등 트리거이고, `pruningIsSound`가 존재 이유인 케이스를
커버하지 못하게 된다.
`0x00 NONE` · `0x01 SIGNED_INT` · `0x02 UNSIGNED_INT`
(date는 부호 없는 일수) · `0x03 IEEE754_TOTAL`(`Double.compare`: −0.0 < 0.0, NaN 최대) ·
`0x04 BYTES_UNSIGNED`. **리더는 자신이 가지치기하려는 비교자와 `statOrder`가 일치할 때만 통계를
쓸 수 있다** — 이게 float·decimal·TimeUUID·date에서 바이트 순서와 타입 순서가 다른 문제를
구조적으로 막는다. 폭 불일치로 `OPAQUE`로 강등된 컬럼은 `statOrder = NONE` 강제.

**블록 테이블 엔트리** (컬럼 타입에 따라 균일): statWidth 8(DOUBLE/INT64/ts축) → 24B
`min(8) max(8) bodyOffset(4) enc(1) flags(1) pad(2)`; statWidth 4(INT32/DATE32) → 16B;
statWidth 0(BOOLEAN/TEXT/OPAQUE) → 8B. 블록은 인덱스 순으로 연속 배치 — 본문 길이가
`entry[k+1].bodyOffset − entry[k].bodyOffset`으로 유도되어 별도 길이 필드가 없고 항상 경계 검사 가능.

**presence 4모드**(blockFlags 하위 2비트): `ALL_PRESENT`(0B) · `ALL_NULL`(0B) ·
`BITMAP`(`ceil(rows/64)*8`) · `RLE`(`u16 runCount | u8 firstRunPresent | u8 pad | varint 런들`).
**둘 다 유지하고 블록마다 정확한 크기로 고르되, 디코드 시 양쪽 모두 같은 `long[]` 비트워드로 한 번
전개한다** — RLE의 압축성은 남기고 벡터화 불가 문제만 제거. 교차점은 1024행당 약 84 전이.
(운영 형태는 부분 null 컬럼이 0개라 실제로는 어느 쪽도 안 쓰인다.)

## 5. 블록 인코딩과 결정성

`blockEncoding`: `0x01 EMPTY` · `0x02 CONSTANT` · `0x03 PRESENCE_ONLY` · `0x10 FOR_BITPACK` ·
`0x11 DELTA_FOR_BITPACK` · `0x20 ALP` · `0x21 ALP_RD` · `0x30 BITPACK1` · `0x40 DICT` · `0x41 RAW`.

**공유 예외 영역**(blockFlags 비트 4): `excCount u16 | pad | (position u16, value W바이트)*`,
위치 오름차순·중복 없음, **예외 위치의 레인 값은 0**(지정된 필러 — 바이트 결정성). ALP 내부
디테일이 아니라 공유 설비로 둔 것이 핵심 결정 — 정수 컬럼이 이상치 하나 때문에 1024값을 10→31비트로
넓히지 않아도 되고, 타임스탬프 축이 간격 내성을 얻어 DoD를 지울 수 있다.

**폭 선택:** `cost(w) = ceil(n*w/64)*8 + (예외 있으면 16 + roundUp8(excCount*(2+W)))`를 65버킷
히스토그램 + 접미합으로 O(n+65)에 전부 계산 후 argmin, **동점은 최소 w**.

### 결정성 규칙 (위반 = 모든 청크가 매 사이클 재작성)

1. **컬럼 순서**: 자연 `String` 순, `new TreeMap<>()` + `putAll` — `new TreeMap<>(map)`는 소스
   비교자를 물려받는 함정(v3에서도 같은 함정).
2. **딕셔너리 순서**: 오름차순 **부호 없는** 바이트 순, 중복 없음.
3. **인코딩 선택**: 모든 선택은 **정확한 닫힌 형태** 크기 함수의 argmin, 동점은 최소 코드/최소 폭.
   시험 인코딩·타이밍·휴리스틱 금지.
4. **고정폭 메타데이터**: 헤더·디렉토리·블록 테이블의 모든 **수치 필드**는 고정폭.
   *예외 두 곳*(구현 중 확인된 §4와의 충돌): 디렉토리의 `constLen`과, 가변폭 타입의 `min`/`max`는
   길이 접두사가 필요하므로 정규 varint를 쓴다. 이건 완화가 아니라 §4가 이미 요구하는 것이고,
   결정성은 **정규(최소 길이) 인코딩 강제 + 디코더의 비정규 거부**로 지켜진다. varint는 그 밖에서는
   데이터 본문에만
   살아남고 **정규 최소 길이**여야 하며 디코더가 비정규 인코딩을 거부한다.
5. **모든 패딩 바이트는 0x00이고 디코더가 검증한다.** 현학이 아니다 — 스크래치 버퍼를 재사용하는
   인코더는 패딩에 낡은 바이트를 흘리고, 같은 입력에서 다른 바이트를 만들고, 재인코더를
   라이브락시킨다. 그런데 **라운드트립 테스트는 전부 초록**이다. 블록당 7바이트 이하 검증이 이
   포맷에서 가장 위험한 불변식에 대한 가장 싼 트립와이어.
6. **지정된 필러**: 예외 위치 레인 값 0, 미사용 통계 필드 0, `blockRows` 초과 presence 비트 0.
7. 인코딩 결정에 **해시 순회 금지**.
8. 바이트에 영향을 주는 부동소수 연산에 **`strictfp`**(ALP).

## 6. 정렬과 SIMD

**8바이트 패딩**은 모든 섹션 경계·블록 테이블 끝·모든 블록 본문 끝에. 비용 행당 ~0.007B(0.2%).
얻는 것: 모든 팩 레인·presence 워드·고정폭 필드를 통짜 `getLong`으로 읽고, "w워드 읽어 64값 방출"
언팩 커널에 에필로그가 없다.

**32/64바이트 패딩은 하지 않는다.** payload가 Cassandra 셀 `ByteBuffer`로 도착하므로 position과
`arrayOffset`을 제어할 수 없어 **절대 정렬을 보장할 수 없고**, x86-64/AArch64에서 비정렬 벡터
로드는 전속력이며 `ByteBuffer.getLong`은 `Unsafe.getLongUnaligned` 인트린식이다.

**SIMD 친화의 실체는 패딩이 아니라 네 성질이다:** ① 분기 많은 비트스트림 없음(v3의 Chimp XOR 체인과
DoD 접두 코드는 직렬 의존) ② presence가 비트워드(`VectorMask.fromLong`이 64레인당 워드 하나) ③
블록 간 의존 없음 ④ `hasArray()` 계약이 이미 `byte[]`를 준다.

**비트 순서(규범):** 64비트 빅엔디안 워드 시퀀스. 비트 인덱스 b는 워드 `b>>>6`의 **최하위 비트부터**
센 위치 `b&63`. 폭 w의 값 i는 비트 `[i*w, (i+1)*w)`를 저차 비트 먼저 차지. 예: 값 `[1,2,3]` 폭 2 →
`1 | (2<<2) | (3<<4) = 0x39` → `00 00 00 00 00 00 00 39`.
(이 문서의 초안은 여기에 `0x31`이라고 적었다. 산술이 맞지 않을 뿐 아니라, `0x31`은 위 규칙대로
디코드하면 `[1, 0, 3]`이 되어 두 번째 값이 사라진다 — 어떤 비트 순서 규약으로도 `[1,2,3]`에서
`0x31`이 나오지 않는다. `BitPackingTest`가 닫힌 형태와 바이트를 함께 고정하고, 별도 테스트가
`0x31`을 디코드해 `[1,0,3]`임을 보이므로, 이 오타에 맞추려고 패커를 고치면 빨간불이 뜬다.)

## 7. 크기 한계

행/청크 16,777,216(유지 — 읽기 경로가 행 인덱스 배열을 만드는 한 유효) · 창당 유효 행 200,000
(`maxSamplesPerWindow`, 서비스 노브) · 컬럼 65,535 · **디렉토리 4GiB**(v3의 u16은 도달 불가능한
모순이었다) · 컬럼명 255B · **딕셔너리 65,535**(코드가 `ceil(log2(dictCount))` 비트로 팩되므로) ·
통계 값 256B 초과 시 통계 생략(잘린 max는 상한이 아니라서 — 안 싣는 게 항상 건전) ·
블록 크기 64..32,768.

## 8. 예상 크기 (운영 형태 투영, 측정 아님)

`tm_tag_point` 3,600행 8컬럼 기준 v3 실측 12,198B(3.388 B/행) → v4 추정 ~10,258B(**~2.85 B/행,
−16%**). 정직한 단서 둘: ① 이건 투영이고 v3 설계 단계 추정은 17% 낮았다 ② **이 형태엔 double
컬럼이 없어 ALP 기여가 0이다** — 이득은 `latency` 비트패킹과 DoD 제거에서 온다. ALP의 무대는
`series, ts, value double` 형태이며 그쪽에서 v3는 35% 절감에 그친다.

## 9. 버전 정책

**버전 바이트는 "아래 레이아웃 전체가 v4"라는 뜻이다.** 기능 플래그도 마이너 버전도 아니다.
인코더는 정확히 하나를 쓰고 리더는 정확히 하나를 받는다. 능력 협상도, 선택적 섹션도, 트레일러도,
예약 공간도 없다. **계층화가 첫 운영 테이블에 켜지는 순간 이 사치가 끝난다.**

v5가 필요해질 변경: 모멘트 필드(부분 집계 경로가 생긴다면) · 텍스트 블록 딕셔너리 코드 min/max
(`statWidth 0` 엔트리 크기 변경) · 청크 전역 딕셔너리 · 선언된 정렬 그레인 · 푸터 레이아웃.
반면 **새 `blockEncoding` 코드와 새 `statOrder` 코드는 버전 범프 없이 추가 가능** — FSST 텍스트
인코딩이 여기 해당할 수 있다. 이것이 v4가 사는 확장성이고, 예약 바이트가 아니라 코드 포인트로
샀다는 점이 중요하다.

## 10. 마이그레이션

1. **배포 전 전제 검증**: 모든 클러스터에서 `nodetool tieringstatus`,
   `SELECT * FROM system_views.timeseries_tiering;`, `system_schema.tables`에서 `__chunks`로 끝나는
   이름 조회. **운영 테이블에 계층화가 켜져 있으면 중단** — 이 계획은 적용되지 않고 v5식 이중 읽기가
   필요하다.
2. 벤치/테스트 청크 폐기: 계층화를 끄고 `DROP TABLE <ks>.<t>__chunks`. **콜드 행은 인코딩 시점에
   이미 삭제됐으므로 이것은 데이터 파괴다** — 재생성 가능한 데이터셋에서만 허용.
3. v4 빌드 배포.
4. **그 다음에야** 첫 운영 테이블에 계층화를 켠다.

**v3→v4 변환 도구는 만들지 않는다.** 만들려면 v3 디코더를 살려둬야 하는데, 그것이 이 창이 피하려는
호환 부담 그 자체다.

v3 payload를 만나면: `ColumnarChunkCodec.VERSION`이 4가 되고, `ChunkCodecs.unsupportedVersion`의
제거된 포맷 분기에 `3`이 `1`(gorilla)·`2`(chimp128 단일 컬럼) 옆에 추가된다. 읽기 경로는
`UnsupportedChunkFormatException`을 전파하고(절대 삼키지 않음), 재인코더는 기존 핸들러가 로그·카운트
후 **원본 행을 건드리지 않는다**.

## 11. 테스트 계획

**레이아웃 고정:** `v4LayoutGoldenVectors`(타입군별 손수 만든 입력의 기대 payload를 hex로 커밋;
다중 블록 `rowCount=2050`, presence 4모드, 예외 있는 모든 인코딩, `blockSizeLog2` 6과 15) ·
`everyFieldOffsetIsWhereTheSpecSays`(헤더 조회 오프셋 4개와 블록 엔트리 크기 3종을 수치로 단언 —
필드 재배열이 초록 라운드트립을 통과하지 못하게).

**결정성 고정:** `encodeTwiceIsByteIdentical` · `encodeIsByteIdenticalUnderShuffledInputOrder`
(TreeMap 함정) · `reencodeIsIdempotent`(`encode(decode(encode(x))) == encode(x)` — 재인코더의 지각
병합 경로 그 자체) · `paddingIsZeroEverywhere` + 거부 · `varintsAreCanonical` + 거부 ·
**`encoderIsDeterministicAcrossJitTiers`**(포크된 JVM `-XX:TieredStopAtLevel=1`로 인코딩해 바이트
비교) — **v4를 어디에도 활성화하기 전에 반드시 존재해야 하는 단 하나의 테스트.**

**통계:** `statsMatchBruteForceUnderDeclaredOrder` · **`pruningIsSound`**(무작위 술어; 통계가
"건너뛰라"고 할 때마다 실제로 0행이 만족하는지 브루트포스 — date를 부호로, float을 바이트로 다룬
실수를 잡는 유일한 테스트) · `nanAndNegativeZeroExtremaFollowDoubleCompare` ·
`constantAndAllNullColumnsCarryNoStats`.

**블록 모델:** **`blockIsIndependentlyDecodable`**(다른 블록 본문을 전부 `0xFF`로 덮고 블록 k 디코드
→ 전체 디코드와 비교 — §2 계약을 대리가 아니라 직접 시험) · `randomAccessMatchesSequentialScan`
(`rank` off-by-one은 예외 없이 *다른 행의 값*을 반환하는 조용한 오염) ·
`timestampBlockSearchMatchesLinearScan` · `projectionAndRangeSkipBlocks`(바이트 접근 카운터).

**값 충실도:** `doubleBitPatternsRoundTripExactly`(NaN 페이로드 여럿, −0.0, ±Inf, 비정규수,
MAX/MIN — NaN을 정규화하는 ALP 포팅을 잡는 테스트) · `decodedValuesSurviveCassandrasComparisonPaths`
(v3의 `hasArray()` 테스트 그대로).

**견고성:** `singleBitFlipsNeverEscapeAsUncheckedOrOom` · `truncateAtEveryPrefixLength` ·
`v1v2v3PayloadsRejectedAsUnsupportedNotCorrupt`.

**성능:** 행당 바이트 재측정(v3의 3.388 B/행을 상한 게이트로) · `scalarAndVectorDecodeAreBitIdentical`
· 재인코더 처리량 ≥50k rows/s(v3 실측 62.9k).

## 12. 규모와 최대 리스크

**약 +5,000 / −1,000줄**, 2~3 person-week. 내역: `ColumnarChunkCodec` v4 1,800~2,200 · `BitPacking`
스칼라 ~250 · `BitPackingVector` ~250 · presence ~250 · 통계+`StatOrder` ~200 · ALP/ALP-RD ~700(별도) ·
호출자 ~180 · 삭제 −600 src/−700 test · 테스트 2,000~2,500 · 문서 ~700.

**최대 리스크는 압축률이 아니라 인코더 선택의 바이트 결정성, 특히 ALP 파라미터 탐색이다.**
실패 양상이 고약하다 — JIT 티어·JVM·노드에 따라 같은 입력이 다른 바이트를 내면 **매 사이클 모든
청크가 재작성되고**, 그건 코덱 버그가 아니라 용량 문제처럼 보인다. 그리고 **평범한 단위 테스트에
전혀 안 보인다**(한 JVM·한 티어·한 입력 순서로만 도니까). 구체적 위험: `Math.pow`(정확한 10의 거듭제곱
표를 쓸 것) · JDK 11 기준선에서 비-`strictfp` · `(e,f)` 탐색의 미지정 동점 처리 · 해시 순회가 딕셔너리나
폭 결정에 새어드는 것 · 재사용 스크래치 버퍼의 낡은 바이트가 패딩에 남는 것.

**2순위: 블록 지역 `rank` off-by-one** — 예외 없이 틀린 값을 반환한다. 예제 테스트가 아니라 무작위
null 패턴 위의 속성 테스트로 막을 것.

**3순위: 인코더 CPU.** v3 재인코더는 62.9k rows/s. v4는 블록당 O(n) 폭 히스토그램과 double 블록당
ALP 탐색을 추가한다. 히스토그램은 싸고 ALP 탐색이 미지수다 — 참조 구현이 빠른 건 샘플링하기
때문이다. ≥50k rows/s로 게이트하고, ALP 탐색이 비용이면 결정성을 푸는 대신 **지정된** 샘플링 규칙을
고정할 것.

**4순위: `OPAQUE` 강등 × `statOrder` 상호작용.** 폭 비호환 값 하나가 그 청크만 `OPAQUE`로 강등시키는데
`statOrder = NONE`이 따라가지 않으면, 길이 0 값 하나 때문에 더 이상 적용되지 않는 순서로 통계가
남는다. 한 줄이고 빠뜨리기 쉽다 — 코퍼스에 길이 0 고정폭 값이 있어야 `pruningIsSound`가 잡는다.

## 관련

[[sp4-plan.md]] · [[columnar-chunks.md]](v3 스펙) · [[codec-bakeoff.md]] · [[compression.md]]
