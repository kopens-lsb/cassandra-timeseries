# 청크 포맷 v4 — 블록 기반 컬럼나 청크

상태: **구현·배선 완료. 이 저장소의 유일한 청크 포맷이다.** v3를 완전히 대체했으며 이중 읽기
경로는 없다 — v1/v2/v3 버전 바이트는 제거된 포맷으로 `UnsupportedChunkFormatException`을 던진다
(§10). 배선 머지 `5cbdf914fa`, v3 내부 삭제 머지 `6f4ce9faa7`.

검증 현황: 단위 398 / 단일 노드 통합 75 / 3-노드 클러스터 49 단언 green.
`encoderIsDeterministicAcrossJitTiers`(포크된 JVM, `-XX:TieredStopAtLevel=1`로 바이트 비교) 포함.
운영 node 41(Haswell E5-2676 v3, AVX2)이 v4 빌드로 동작 중이다. 실측 수치는 §8.

구현: [`ChunkV4Codec`](../../src/java/org/apache/cassandra/db/timeseries/ChunkV4Codec.java)(조립)을
정점으로 `ChunkV4Header` · `ChunkV4Directory` · `ChunkV4BlockTable` · `ChunkV4Section` ·
`BlockPresence` · `BlockEncodings` · `AlpBlockCodec` · `ExceptionArea` · `StatOrder` · `BitPacking`.
이 문서가 스펙이고, 각 클래스의 javadoc이 세부 레이아웃(특히 ALP 본문 — §5)의 규범을 보완한다.

## 0. v3 대비 결정 (설계 근거)

| v3 결정 | v4 |
|---|---|
| 컬럼별 `typeCode`/`flags`/`sectionLen` 디렉토리 | **유지.** 타입 코드는 여전히 *압축만* 선택 |
| `CONSTANT`/`ALL_NULL`이 O(1) 바이트 | **유지, 불변.** 운영 형태에서 8컬럼 중 4개가 0바이트인 최대 이득 |
| 미투영 컬럼 섹션 건너뛰기 | **유지+강화** — 디렉토리에 `sectionOffset`을 명시해 접두합 없이 점프 |
| 바이트 결정성 (`chunkUnchanged`) | **유지, 규칙 강화**(§5) — v4는 인코더 선택지가 늘어 틀릴 여지가 커짐 |
| `hasArray()` 계약 | **그대로 유지** |
| 손상(`IllegalArgumentException`, 건너뜀) vs 미지원 버전(`UnsupportedChunkFormatException`, 전파) | **유지**, 내부 코드까지 확장 |
| Chimp128 double | **폐기.** ALP / ALP-RD 단독 ([codec-bakeoff.md](codec-bakeoff.md)) |
| DoD 타임스탬프 비트스트림 | **폐기.** 타임스탬프는 평범한 블록형 INT64 유사 컬럼 (§4.5) |
| 컬럼 전체에 걸친 RLE null 비트맵 | **폐기.** 블록별 4모드(§4) |
| 청크 전체 순차 디코드 | **폐기.** 모든 블록이 독립 디코드·랜덤 접근 |
| 25바이트 헤더, `dirSize` u16, varint 메타데이터 | 40바이트 헤더, `directoryLen` u32, 고정폭 메타데이터 |

수치는 전부 **빅엔디안**. 팩된 레인 내부 비트 순서는 **LSB-first**(§6).

v3 구현은 트리에 존재하지 않는다 — v3 인코더/디코더 본체, `BitWriter`/`BitReader`,
`TimestampCodec`(DoD), `Chimp128Codec`은 삭제됐다. 남은 것은 버전 바이트 정책뿐이다: v1/v2/v3는
`ChunkCodecs.unsupportedVersion`이 `UnsupportedChunkFormatException`으로 거부하는 제거된
포맷이다(§10). `AlpCodec`의 계획 함수(`exponentCandidates`, `selectRdDictionary`)와 값 산술은
v4의 `AlpBlockCodec`이 호출하므로 그대로 산다.

## 1. 목표와 명시적 비목표

목표(우선순위 순): ① 블록 기반 독립 인코딩 ② **가지치기(pruning)에 충분한** 통계 ③ 정렬·SIMD 친화
④ v3가 잘한 것 보존.

> **비목표 — 나중에 누가 다시 넣지 않도록 명시한다: v4의 통계는 가지치기 전용이다.**

청크 시간 경계, 컬럼별 min/max, 블록별 min/max는 싣는다. `sum`, `sum_sq`, `first_value`,
`last_value`, 스케치, 히스토그램은 **싣지 않고 자리도 예약하지 않는다.**

이유는 비싸서가 아니라 **쓸 데가 없어서**다. 그 필드들의 유일한 용도는 메타데이터로 집계를
*답하는* 것인데, 이 코드베이스에는 부분 집계 경로가 없다 — 집계는 읽기 경로가 만든 `Row` 위에서
돌고, 그걸 우회하면 LIMIT·페이징·gap-fill·읽기수리 순서를 동시에 건드리게 된다. 그 우회(집계
푸시다운)는 취소된 방향이다([sp4-plan.md](sp4-plan.md)). **가지치기는 범주가 다르다** — 행은
평소대로 흐르고, 통계의 효과는 "청크/블록을 안 여는 것"뿐이다. 가지치기 통계는 "안 열어도 될 걸
열었다" 방향으로만 틀릴 수 있지만, 답하기 통계는 "틀린 숫자를 반환했다" 방향으로 틀릴 수 있다.
부분 집계 경로가 언젠가 생기면 그때 v5가 추가하면 된다(§9).

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
인트린식, 할당 없음. 순차 스캔은 rank를 행마다 부르지 않고 running index를 유지한다(커서가
`rankCalls() == 0`을 강제하며 테스트가 이를 단언한다 — 측정 근거는
[simd-decode-design.md](simd-decode-design.md) §10, 6.7×).

**블록 독립성(규범):** 블록 k 디코드에 필요한 것은 헤더, 그 컬럼의 디렉토리 엔트리, 섹션 프리앰블
(텍스트/opaque 딕셔너리만), 블록 테이블 엔트리 k와 k+1(본문 길이용), 블록 k 본문뿐이다.

**1024인 이유:** 폭 w에서 정확히 `16w` 워드(꼬리 없음) · 모든 벡터 종이 나눠떨어짐(512비트면
128회) · presence가 정확히 2 캐시라인 · 블록 엔트리 24B가 행당 0.023B · ALP 참조 구현과 DuckDB가
1024 사용 · 실제 창이 작음(1h@1Hz = 3600행 = 3.5블록).

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

**`statOrder`** — 통계를 계산한 비교 순서. 매핑의 권위는 `AbstractType`의 **실제 비교자**이며
`StatOrder`가 이를 구현한다. 규범 두 가지:

- `time`(`TimeType`)과 `date`(`SimpleDateType`)는 `ComparisonType.BYTE_ORDER`, 즉 **부호 없는**
  비교다 — 합법 도메인에서는 부호 있는 비교와 결과가 같고 비트 63이 선 값에서만 갈라지므로,
  기대 도메인이 아니라 비교자를 따른다. 그래서 둘 다 `UNSIGNED_INT`다.
- 이 타입들은 모두 **빈 값을 맨 앞으로** 정렬하므로 비교자가 길이 0 값을 거부하면 안 된다 —
  길이 0 고정폭 값은 `OPAQUE` 강등 트리거이기도 하다(아래, §12).

코드: `0x00 NONE` · `0x01 SIGNED_INT` · `0x02 UNSIGNED_INT`(date·time) ·
`0x03 IEEE754_TOTAL`(`Double.compare`: −0.0 < 0.0, NaN 최대) · `0x04 BYTES_UNSIGNED`.
**리더는 자신이 가지치기하려는 비교자와 `statOrder`가 일치할 때만 통계를 쓸 수 있다** — 이게
float·decimal·TimeUUID·date에서 바이트 순서와 타입 순서가 다른 문제를 구조적으로 막는다.

**모든 `OPAQUE` 컬럼은 `statOrder = NONE`이다.** 폭 불일치로 강등된 컬럼과 원래 blob인 컬럼을
디코더가 바이트만으로 구별할 수 없으므로, 규칙은 타입 코드 전체에 적용된다 — v4.0에서는 네이티브
blob 컬럼도 통계를 싣지 않는다. blob 통계가 필요해지면 v5가 별도 타입 코드로 산다(§9).

**블록 테이블 엔트리** (컬럼 타입에 따라 균일): statWidth 8(DOUBLE/INT64/ts축) → 24B
`min(8) max(8) bodyOffset(4) enc(1) flags(1) pad(2)`; statWidth 4(INT32/DATE32) → 16B;
statWidth 0(BOOLEAN/TEXT/OPAQUE) → 8B. 블록은 인덱스 순으로 연속 배치 — 본문 길이가
`entry[k+1].bodyOffset − entry[k].bodyOffset`으로 유도되어 별도 길이 필드가 없고 항상 경계 검사 가능.

**presence 4모드**(blockFlags 하위 2비트): `ALL_PRESENT`(0B) · `ALL_NULL`(0B) ·
`BITMAP`(`ceil(rows/64)*8`) · `RLE`(`u16 runCount | u8 firstRunPresent | u8 pad | varint 런들`).
**둘 다 유지하고 블록마다 정확한 크기로 고르되, 디코드 시 양쪽 모두 같은 `long[]` 비트워드로 한 번
전개한다** — RLE의 압축성은 남기고 벡터화 불가 문제만 제거. 교차점은 1024행당 약 84 전이.
(운영 형태는 부분 null 컬럼이 0개라 실제로는 어느 쪽도 안 쓰인다.)

## 4.5 섹션 조립

청크 전체 배치 (`ChunkV4Codec`):

```
0                 헤더 40B (§3)
40                디렉토리, directoryLen 바이트, 8까지 패딩 (§4)
tsSectionOffset   타임스탬프 섹션 (인코더는 디렉토리 바로 뒤에 놓는다)
...               컬럼 섹션들, 디렉토리 순서, 0바이트 컬럼은 생략
```

**타임스탬프 축은 구조적으로 컬럼이다**: `INT64`, `SIGNED_INT`, `ALL_PRESENT`이고, 다른 8바이트
컬럼과 같은 섹션 인코더/리더를 쓴다. 축을 구별하는 것은 위치뿐이다 — 디렉토리 엔트리가 아니라
헤더의 `tsSectionOffset`/`tsSectionLen`이 가리킨다(정렬할 이름이 없고 모든 리더가 필요로 하므로).
따라서 축에는 청크 수준 min/max 통계가 없다 — 헤더의 `firstTimestamp`/`lastTimestamp`가 그
극값이고, 이것이 청크 시간 경계를 섹션에 손대지 않는 O(1) 조회로 만든다(§3).

섹션 하나의 내부 배치 (`ChunkV4Section`):

```
sectionOffset (8의 배수, 디렉토리가 지정)
  [프리앰블]    TEXT/OPAQUE만: dictCount u16 | pad u16 | dictBytesLen u32 | 엔트리들, 8까지 패딩
  블록 테이블   blockCount개의 고정폭 엔트리 (§4)
  본문 0        presence 바이트들, 그 다음 블록 payload
  본문 1        ... 인덱스 순 연속, 각각 8의 배수
```

- **프리앰블은 모든 `TEXT`/`OPAQUE` 섹션에 존재한다** — 딕셔너리가 없으면 `dictCount == 0`인
  8바이트다. 존재 여부는 바이트가 아니라 **타입 코드**(리더가 디렉토리에서 이미 안다)로 결정된다 —
  리더는 아무것도 파싱하기 전에 블록 테이블 위치를 알아야 하므로 "바이트에서 탐지 가능한 선택"일
  수 없다. `dictBytesLen`은 8 패딩 **전** 엔트리 길이다(패딩은 유도 가능하고, 경계 검사가 원하는
  것은 패딩 전 값이다).
- **딕셔너리 엔트리는 부호 없는 바이트 오름차순, 중복 없음**(§5 규칙 2 — `BYTES_UNSIGNED`의
  비교). 코드는 그 순서에서의 인덱스다. 최대 65,535개(§7 — 코드가 `ceil(log2(dictCount))` 비트로
  팩되므로).
- **딕셔너리 채택 여부는 섹션 수준의 정확한 크기 비교다**: 프리앰블+테이블+본문 합계를 딕셔너리
  있는 쪽/없는 쪽 양쪽 다 계산해 작은 쪽, 동점은 딕셔너리 없음.
- **본문 = presence 그 다음 payload.** 이것이 §2의 블록 독립성 계약을 참으로 만든다.
  `PRESENCE_ONLY`는 payload 절반이 비어 본문이 정확히 presence뿐인 경우의 이름이다.
- **본문 길이는 유도된다, 저장하지 않는다**: 블록 k는
  `entry[k+1].bodyOffset − entry[k].bodyOffset`, 마지막 블록은 `sectionLen − entry[last].bodyOffset`.
  `bodyOffset`은 **섹션 상대** 오프셋이다. 저장된 길이는 다음 오프셋과 어긋나 디코더를 옆 블록으로
  침범시킬 수 있지만, 유도된 길이는 어긋날 대상이 없다 — 오프셋의 단조성·경계만 검증하면 된다.
  모든 본문이 8의 배수이므로(presence 모드도, 모든 `BlockEncodings` payload도) 다음 본문은 패딩
  바이트 없이 8-정렬로 시작한다.
- **0바이트 섹션**: 존재 값이 없는 컬럼과, 전 행 존재하는 `CONSTANT` 컬럼은 프리앰블·테이블·본문
  전부 없음. 디렉토리가 `sectionLen == 0`을 요구하는 정확히 그 두 경우다(§2).

## 5. 블록 인코딩과 결정성

`blockEncoding`: `0x01 EMPTY` · `0x02 CONSTANT` · `0x03 PRESENCE_ONLY` · `0x10 FOR_BITPACK` ·
`0x11 DELTA_FOR_BITPACK` · `0x20 ALP` · `0x21 ALP_RD` · `0x30 BITPACK1` · `0x40 DICT` · `0x41 RAW`.
`0x00`은 영구 미할당 — 0으로 채워진 엔트리가 첫 바이트에서 파싱 불가가 되게 하는 가장 싼 감지기.

**공유 예외 영역**(blockFlags 비트 4): `excCount u16 | pad u16 | pad u32 |
(position u16, value W바이트)*`, 8까지 패딩. 위치는 엄격 오름차순(중복 불가), **예외 위치의 레인
값은 0**(지정된 필러 — 바이트 결정성, 디코더가 검증). ALP 내부 디테일이 아니라 공유 설비로 둔 것이
핵심 결정 — 정수 컬럼이 이상치 하나 때문에 1024값을 10→31비트로 넓히지 않아도 되고, 타임스탬프
축이 간격 내성을 얻어 DoD 없이 블록형 컬럼이 된다.

**예외 영역의 존재 신호는 인코딩마다 다르다.** `FOR_BITPACK`/`DELTA_FOR_BITPACK`은 blockFlags
비트 4를 쓴다. **`ALP`/`ALP_RD`는 비트 4를 항상 0으로 두고 본문 헤더의 `flags u8` 비트 0으로
알린다** — `DoubleBlockCodec` 심(seam)은 코덱이 예외 개수를 `Choice`로 되돌릴 통로가 없고 디코드
시 비트 4를 받지도 않기 때문이다. 길이 산술만으로 대신할 수는 없다: 본문 길이가 정확히 영역
크기만큼 줄어든 손상은 "예외 없는 더 짧은 본문"으로 **정합하게** 파싱되어 예외 행이 필러 0으로
조용히 디코드된다. 그래서 flags 비트와 파생 길이는 서로를 검증하며, 어긋나면 손상이다.
ALP 본문 레이아웃(헤더·프레임·레인·딕셔너리)은 `AlpBlockCodec` javadoc이 규범이다.

**폭 선택:** `cost(w) = ceil(n*w/64)*8 + (예외 있으면 EXCEPTION_AREA_OVERHEAD +
roundUp8(excCount*(2+W)))`를 65버킷 히스토그램 + 접미합으로 O(n+65)에 전부 계산 후 argmin,
**동점은 최소 w**. 고정항 `BitPacking.EXCEPTION_AREA_OVERHEAD`는 숫자를 다시 적지 않고
`ExceptionArea.HEADER_BYTES`(= 8, 위 레이아웃의 헤더)를 참조하며, `ExceptionAreaTest`가 두 값의
일치를 단언한다 — 같은 상수가 두 곳에서 어긋나지 않게.

### 결정성 규칙 (위반 = 모든 청크가 매 사이클 재작성)

1. **컬럼 순서**: 자연 `String` 순, `new TreeMap<>()` + `putAll` — `new TreeMap<>(map)`는 소스
   비교자를 물려받는 함정.
2. **딕셔너리 순서**: 오름차순 **부호 없는** 바이트 순, 중복 없음.
3. **인코딩 선택**: 모든 선택은 **정확한 닫힌 형태** 크기 함수의 argmin, 동점은 최소 코드/최소 폭.
   시험 인코딩·타이밍·휴리스틱 금지.
4. **고정폭 메타데이터**: 헤더·디렉토리·블록 테이블의 모든 **수치 필드**는 고정폭.
   *예외 두 곳*: 디렉토리의 `constLen`과, 가변폭 타입의 `min`/`max`는 길이 접두사가 필요하므로
   정규 varint를 쓴다 — §4가 요구하는 것이고, 결정성은 **정규(최소 길이) 인코딩 강제 + 디코더의
   비정규 거부**로 지켜진다. varint는 그 밖에서는 데이터 본문에만 나타나며 같은 정규성 규칙을
   따른다.
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

**SIMD 친화의 실체는 패딩이 아니라 네 성질이다:** ① 분기 많은 비트스트림 없음(직렬 의존하는 XOR
체인·접두 코드 없음) ② presence가 비트워드(`VectorMask.fromLong`이 64레인당 워드 하나) ③
블록 간 의존 없음 ④ `hasArray()` 계약이 이미 `byte[]`를 준다.
(벡터 커널 자체는 측정 후 보류 확정 — [simd-decode-design.md](simd-decode-design.md).)

**비트 순서(규범):** 64비트 빅엔디안 워드 시퀀스. 비트 인덱스 b는 워드 `b>>>6`의 **최하위 비트부터**
센 위치 `b&63`. 폭 w의 값 i는 비트 `[i*w, (i+1)*w)`를 저차 비트 먼저 차지. 예: 값 `[1,2,3]` 폭 2 →
`1 | (2<<2) | (3<<4) = 0x39` → `00 00 00 00 00 00 00 39`. `BitPackingTest`가 닫힌 형태와 바이트를
함께 골든 벡터로 고정한다.

## 7. 크기 한계

행/청크 16,777,216(`MAX_ROWS` — 읽기 경로가 행 인덱스 배열을 만드는 한 유효) · 창당 유효 행
200,000(`maxSamplesPerWindow`, 서비스 노브) · 컬럼 65,535 · 디렉토리 4GiB(u32) · 컬럼명 255B ·
**딕셔너리 65,535**(코드가 `ceil(log2(dictCount))` 비트로 팩되므로) · 통계 값 256B 초과 시 통계
생략(잘린 max는 상한이 아니라서 — 안 싣는 게 항상 건전. 절대 자르지 말 것) · 블록 크기 64..32,768.

## 8. 실측 크기·성능

- **스케일 벤치마크** (2026-08-04, 호스트 234 = Xeon Silver 4114T): `tm_tag_point` 형태 20M행 ·
  500시리즈 — 비계층 237.8 MB → **계층 v4 33.3 MB (7.1×)**, v3 청크(48.3 MB) 대비 −31%. 집계
  질의는 비계층 대비 **4.0~6.1× 빠름**(count/time_bucket/percentile/variance/gapfill/integral),
  re-tier 108k rows/s. 세부: [tiering-benchmark.md](tiering-benchmark.md).
- **블록 수준 ALP vs RAW** (`DoubleBlockCodecTest.sizeAgainstRawOnTheProductionDistributions`,
  실행마다 출력): 운영형 소수 2자리 워크 **0.0712**(게이트 0.15) · near-constant 게이트 0.05 ·
  full-precision 가우시안 게이트 0.85. 배경: [codec-bakeoff.md](codec-bakeoff.md).
- **인코더 처리량** (호스트 237 = Xeon Silver 4210R, `ChunkEncodeBench`): 운영 형태
  **684k rows/s**, 전 컬럼 double 최악 케이스 221k — 재인코딩 게이트 50k의 13.7×/4.4×.
- 형태 의존성은 v3와 동일하게 남는다: 이득의 최대 몫은 CONSTANT/ALL_NULL 접기와 비트패킹이고,
  접을 컬럼이 없는 `(series, ts, value double)` 형태에서는 이득이 훨씬 작다 —
  [compression.md](compression.md).

## 9. 버전 정책

**버전 바이트는 "아래 레이아웃 전체가 v4"라는 뜻이다.** 기능 플래그도 마이너 버전도 아니다.
인코더는 정확히 하나를 쓰고 리더는 정확히 하나를 받는다. 능력 협상도, 선택적 섹션도, 트레일러도,
예약 공간도 없다. 이후의 레이아웃 변경은 v5 버전 바이트와 이중 읽기 경로를 요구한다.

v5가 필요해질 변경: 모멘트 필드(부분 집계 경로가 생긴다면) · 텍스트 블록 딕셔너리 코드 min/max
(`statWidth 0` 엔트리 크기 변경) · blob 통계(별도 타입 코드) · 청크 전역 딕셔너리 · 선언된 정렬
그레인 · 푸터 레이아웃. 반면 **새 `blockEncoding` 코드와 새 `statOrder` 코드는 버전 범프 없이
추가 가능** — FSST 텍스트 인코딩이 여기 해당할 수 있다. 이것이 v4가 사는 확장성이고, 예약 바이트가
아니라 코드 포인트로 샀다는 점이 중요하다.

## 10. 제거된 포맷 (v1 / v2 / v3)

v1(Gorilla) · v2(Chimp128 단일 컬럼) · v3(컬럼 지향 순차 디코드)는 **제거된 포맷**이다.
`ChunkCodecs.unsupportedVersion`이 버전 바이트를 보고 `UnsupportedChunkFormatException`을 만들며,
읽기 경로는 이를 **전파한다 — 절대 삼키지 않는다**. 손상(`IllegalArgumentException`)과의 구분이
핵심이다: 손상은 산발적이라 해당 청크만 경고 후 건너뛰지만, 포맷 불일치는 체계적이라 건너뛰면
모든 읽기에서 과거 데이터가 조용히 잘려 나간다. 재인코더가 미지원 버전을 만나면 로그·카운트 후
**원본 행을 건드리지 않는다**.

v3→v4 변환 도구는 없다 — 만들려면 v3 디코더를 살려둬야 하고, 그것이 바로 이 대체가 피한 호환
부담이다. 대체는 계층화가 운영 테이블에 켜지기 전에 완료되어 현장에 v3 payload가 존재하지 않는다.

## 11. 테스트 (현재 상태 — 전부 존재, green)

**레이아웃 고정:** `wholeChunkGoldenVector` + `goldenVectorDecodesToWhatItWasBuiltFrom`(청크 전체
hex 골든 벡터) · `everyFieldOffsetIsWhereTheSpecSays`(헤더 오프셋과 블록 엔트리 크기 3종을 수치로
단언) · 계층별 골든 벡터(`goldenSectionFor*`, `goldenVectorFor*` — 섹션·블록 인코딩·presence).

**결정성 고정:** `encodeTwiceIsByteIdentical` · `encodeIsByteIdenticalUnderShuffledInputOrder`
(TreeMap 함정) · `reencodeIsIdempotent`(`encode(decode(encode(x))) == encode(x)`) ·
`paddingIsZeroEverywhereAndVerifiedOnDecode` · `nonCanonicalVarintsAreRejected`(디렉토리·presence·
블록 각각) · **`encoderIsDeterministicAcrossJitTiers`**(포크된 JVM `-XX:TieredStopAtLevel=1`로
인코딩해 바이트 비교 — v4 활성화 전 필수 조건이었고, 존재하며 green).

**통계:** `statsMatchBruteForceUnderDeclaredOrder` · **`pruningIsSound`**(무작위 술어; 통계가
"건너뛰라"고 할 때마다 실제로 0행이 만족하는지 브루트포스) · `timePruningIsSoundAndCostsNoSection` ·
`nanAndNegativeZeroExtremaFollowDoubleCompare` · `constantAndAllNullColumnsCarryNoStats` ·
`aZeroLengthFixedWidthValueDowngradesToOpaqueAndDropsTheStatOrder`.

**블록 모델:** **`blockIsIndependentlyDecodable`**(다른 블록 본문을 전부 `0xFF`로 덮고 블록 k 디코드
→ 전체 디코드와 비교) · `randomAccessMatchesSequentialScan`(섹션)과
`randomAccessMatchesSequentialScanAndNeverRanksPerRow`(청크 — `rankCalls() == 0` 강제) ·
`aRunningValueIndexAgreesWithRankAtEveryOffsetAndSeek` · `projectionSkipsUnprojectedSectionsByteForByte`.

**값 충실도:** `doubleBitPatternsRoundTripExactly`(NaN 페이로드 여럿, −0.0, ±Inf, 비정규수,
MAX/MIN) · `decodedValuesSurviveCassandrasComparisonPaths`(`hasArray()` 계약).

**견고성:** `singleBitFlipsNeverEscapeAsUncheckedOrOom` · `truncateAtEveryPrefixLength`(전 계층) ·
`v1v2v3PayloadsRejectedAsUnsupportedNotCorrupt` · `aCorruptRowCountCannotReserveMemory`.

**통합·분산:** `TieredStorageColumnsTest`(실 형태 행당 바이트를 실행마다 측정, < 9 B/행 게이트)
등 단일 노드 통합 75 단언 · `TieredStorageDistributedTest`(3-노드 클러스터) 49 단언.

**성능(마이크로벤치, `Chunk{BitUnpack,Presence,BlockDecode,Read,Encode}Bench`):** 인코더 게이트
측정 완료(§8). 스칼라/벡터 동등성 테스트는 벡터 커널 보류로 대상이 없다
([simd-decode-design.md](simd-decode-design.md) §10).

## 12. 리스크 대장 (해소 상태)

1. **인코더 바이트 결정성, 특히 ALP 파라미터 탐색** — 설계 시점의 최대 리스크. 실패 양상: JIT
   티어·JVM·노드에 따라 같은 입력이 다른 바이트를 내면 매 사이클 모든 청크가 재작성되고, 코덱
   버그가 아니라 용량 문제처럼 보이며, 한 JVM·한 티어로 도는 평범한 단위 테스트에 안 보인다.
   **해소:** §11의 결정성 테스트군 + `encoderIsDeterministicAcrossJitTiers`. 지켜야 할 규율은
   §5에 규범으로 남아 있다(`Math.pow` 대신 정확한 10의 거듭제곱 표, `strictfp`, 명시된 동점 규칙,
   해시 순회 금지, 스크래치 버퍼 패딩 검증). 새 인코딩 코드를 추가할 때 같은 규율이 적용된다.
2. **블록 지역 `rank` off-by-one** — 예외 없이 *다른 행의 값*을 반환하는 조용한 오염.
   **해소:** 무작위 null 패턴 속성 테스트(`rankMatchesBruteForceAtEveryOffset`,
   `aRunningValueIndexAgreesWithRankAtEveryOffsetAndSeek`) + 청크 수준 무작위 접근 대조.
3. **인코더 CPU** — 블록당 O(n) 폭 히스토그램과 double 블록당 3중 ALP 플래닝 패스의 비용.
   **측정으로 종결:** 684k/221k rows/s ≥ 50k 게이트(§8) — 비문제로 판명.
4. **`OPAQUE` 강등 × `statOrder` 상호작용** — 강등 시 `statOrder = NONE`이 따라가지 않으면 더
   이상 적용되지 않는 순서로 통계가 남는 한 줄짜리 실수. **해소:** 규칙이 더 강하게 구현됐고
   (모든 `OPAQUE`가 `NONE`, §4 — 디렉토리가 양쪽에서 강제) 전용 테스트가 있다. 코퍼스에 길이 0
   고정폭 값이 포함되어 `pruningIsSound`가 이 케이스를 커버한다.

## 관련

[[sp4-plan.md]] · [[columnar-chunks.md]](v3 — 제거된 포맷) · [[codec-bakeoff.md]] ·
[[compression.md]] · [[simd-decode-design.md]] · [[tiering-benchmark.md]]
