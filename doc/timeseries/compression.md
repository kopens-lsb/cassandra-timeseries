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

# 압축 — 컬럼별로 무엇이, 왜, 얼마나 줄어드는가

계층형 저장의 용량 절감이 **어디서 오는지**를 설명합니다. 바이트 포맷 자체는
[chunk-format-v4.md](chunk-format-v4.md), 전체 벤치마크는
[tiering-benchmark.md](tiering-benchmark.md), double 코덱 선정 근거는
[codec-bakeoff.md](codec-bakeoff.md)에 있습니다 — 이 문서는 그 셋을 잇는 설명서입니다.

## 1. 압축은 두 층입니다

```
행 저장 (핫 데이터)              청크 저장 (콜드 데이터)
────────────────────            ─────────────────────────
행 직렬화                        ① 컬럼별 블록 인코딩 (v4)  ← 이 문서의 주제
  └ SSTable 압축 (Zstd/LZ4)       └ ② SSTable 압축 (그 위에 그대로)
```

**행 저장도 이미 압축돼 있습니다.** 운영 노드의 `tm_tag_point`는 Zstd로 압축률 0.190(5.3×)을
받고 있고, 디스크 실측은 행당 ≈9~12B입니다. 청크가 이기는 이유는 Zstd보다 잘 짜서가 아니라
**Zstd가 볼 수 없는 구조를 먼저 없애기 때문**입니다 — 행 직렬화는 행마다 클러스터링·타임스탬프·
셀 헤더를 반복하는데, 범용 압축기는 그 반복을 부분적으로만 걷어냅니다. 컬럼 지향 인코딩은
반복 자체를 표현에서 제거한 뒤 남은 것에 다시 SSTable 압축을 받습니다.

## 2. 컬럼별로 무슨 일이 일어나는가

청크 하나는 창(`chunk_window`) 하나의 행들을 컬럼 단위로 다시 묶은 것입니다. 컬럼은 1024행
블록으로 잘리고, 블록마다 독립적으로 인코딩이 정확한 크기 비교(argmin)로 선택됩니다
([포맷 §5](chunk-format-v4.md)):

| 컬럼 내용 | 인코딩 | 행당 비용 | 왜 |
| --- | --- | --- | --- |
| **값이 창 내내 일정** (`quality`=192, `error_code`=0, 빈 `attribute`) | CONSTANT — 디렉토리에 값 1회 | **0 B** | 데이터 섹션 자체가 없음. 블록 단위 상수도 별도 코드로 O(1) |
| **아무도 안 쓰는 컬럼** (숫자 태그의 `value_boolean`) | ALL_NULL | **0 B** | 섹션 없음, 플래그 하나로 끝 |
| **타임스탬프 축** (규칙적 간격) | 델타 + FOR 비트팩 | 간격이 일정하면 **~0.02 B** (블록 테이블 고정비뿐) | 델타가 상수면 팩 폭이 0비트. 빠진 샘플 하나는 폭을 넓히지 않고 예외 영역에 10B로 격리 |
| `double` 센서값 (십진 양자화된 워크·주기 신호) | ALP / ALP-RD | **~0.6~0.8 B** | 소수 2자리 워크 실측: RAW 대비 0.0712 (`DoubleBlockCodecTest`, 실행마다 출력) |
| `boolean` | 1비트 팩 | **0.125 B** | 8행 = 1바이트 |
| `int`/`bigint`/`timestamp`/`date` | FOR / 델타-FOR 비트팩 | 값 변화폭의 log₂에 비례 | 근처를 맴도는 카운터·상태코드는 1~2 B, 이상치는 예외 영역으로 격리 |
| `text` 카디널리티 낮음 | 섹션 딕셔너리 (최대 65,535 항목) | 항목 수에 로그 비례 | 같은 문자열을 창에 한 번만 저장 |
| 고엔트로피 (`latency` 균등 난수, 랜덤 blob) | 비트팩 최소 폭 / RAW | 정보량 그대로 (1..999 균등 = 10비트 = 1.25 B) | 압축할 구조가 없으면 정직하게 그대로 — 여기가 하한을 정합니다 |

**행 저장에서 행마다 내던 고정비**(클러스터링 직렬화, 셀 타임스탬프, 헤더)는 창당 1회의 공유
타임스탬프 축과 40바이트 헤더로 대체됩니다. 이 고정비 제거가 절반, 컬럼별 인코딩이 절반입니다.

## 3. 실측 — 세 개의 숫자와 각각의 조건

밀도를 밝히지 않은 행당 바이트는 의미가 없습니다(행이 몇 개 없는 창은 전부 헤더입니다).
세 측정 모두 조건이 다르고, 어느 것을 인용할지는 용도에 달렸습니다.

| 측정 | 조건 | 결과 | 출처 |
| --- | --- | --- | --- |
| **단위 측정** | `tm_tag_point` 형태, 창당 3,600행(1초 간격), 전 컬럼 사용, 인코딩만(SSTable 압축 전) | 실행마다 로그로 출력, **< 9 B/행 게이트**(운영 행 저장 실측이 기준선) | `TieredStorageColumnsTest.realShapeBytesPerRow...` |
| **스케일 벤치마크** | 같은 형태 20,000,000행, 시리즈 500개, 디스크 실측 (호스트 234 = Xeon Silver 4114T) | 237.8 MB → **33.3 MB (7.1×)**, 집계 질의 4.0~6.1× 빠름 | [tiering-benchmark.md](tiering-benchmark.md) 재측정 절 |
| **반례 형태** | `(series, ts, value double)` — 고엔트로피 double 하나뿐 | 디스크 **35% 절감에 그침** | [tiering-benchmark.md](tiering-benchmark.md) |

스케일 벤치마크의 디스크 수치는 ①층(컬럼 인코딩) 위에 ②층(SSTable 압축)을 한 번 더 받은
값입니다. v3 청크 시절의 트레이드오프("저장 절감의 대가로 질의 0~30% 감속")는 v4에는 없습니다 —
저장 7.1×와 집계 가속이 같은 측정에서 함께 나옵니다.

### 3.1 7.1×는 어디서 왔는가 — 컬럼별 분해

운영 형태의 일반 컬럼 8개가 내는 비용:

| 컬럼 | 내용 | 청크에서의 비용 |
| --- | --- | --- |
| `quality` | 항상 192 | **0** (CONSTANT) |
| `error_code` | 항상 0 | **0** (CONSTANT) |
| `attribute` | 항상 빈 맵 | **0** (CONSTANT) |
| `value_numeric` 또는 `value_boolean` | static `type`에 따라 한쪽이 통째로 빔 | **0** (ALL_NULL) |
| `timestamp` 축 | 1초 간격 | ~0 (0비트 델타-FOR + 블록 테이블) |
| `value_boolean` (boolean 태그) | 상태 비트 | 0.125 B |
| `value` (text) | 판독값의 문자열 사본 | 딕셔너리, 소액 |
| `latency` | 1..999 균등 난수 | **행당 1.25 B (10비트 팩) — 전체의 과반** |

**8개 컬럼 중 4개가 0바이트**입니다. 이것이 "컬럼이 여럿이고 그중 상당수가 상수이거나 빈"
산업 태그 테이블에서 7.1×가 나오는 이유이고, 접을 컬럼이 없는 반례 형태에서 35%에 그치는
이유입니다. 남는 비용은 고엔트로피 컬럼이 정합니다 — `latency`가 없었다면 이 형태는 그 몫만큼
더 내려갑니다.

## 4. 내 테이블은 얼마나 줄어들까 — 추정 규칙

컬럼마다 아래를 답하면 대략의 하한이 나옵니다:

1. **창 안에서 값이 사실상 일정한가?** (품질 플래그, 단위, 설정값) → 0 B
2. **이 타입의 태그에선 아예 비는 컬럼인가?** (polymorphic 스키마의 다른 타입 값 컬럼) → 0 B
3. **천천히 변하거나 십진 양자화된 숫자인가?** (센서 판독, 카운터) → 0.6~2 B
4. **매 행이 사실상 난수인가?** (지연시간, 해시, UUID) → 정보량 그대로 — **이 컬럼들이 최종
   크기를 정합니다**

그리고 두 가지 전제:

- **밀도**: 창당 행 수가 수백 이상이어야 헤더·블록 테이블이 희석됩니다. `chunk_window`를 태그의
  수집 주기에 맞추십시오 (1Hz 태그면 1h 창 = 3,600행: 적정).
- **static 컬럼은 계산에서 빼십시오** — 청크화 대상이 아니며 파티션당 1회만 저장됩니다.

## 5. 실측하는 법

추정보다 재는 것이 낫습니다. 두 가지 방법:

**① 단위 측정** — 형태만 같은 테이블로 인코더를 직접 돌립니다:

```
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.timeseries.tiering.TieredStorageColumnsTest
# 로그에 행당 바이트가 남습니다 (realShapeBytesPerRowIsMeasured...)
```

**② 실물 측정** — 운영과 같은 노드에서 짧은 창의 시험 테이블에 계층화를 걸어 봅니다. 검증된
조합은 `hot_window 10m / chunk_window 5m / interval 1m` — 백필 → flush → 청크 생성 →
`SELECT count(*)` 정합 확인까지 5분 안에 돕니다. 그 뒤:

```sql
-- 청크가 실제로 차지하는 크기
SELECT count(*) FROM <ks>.<t>__chunks;
```
```bash
du -sh <data_dir>/<ks>/<t>-*/        # 베이스
du -sh <data_dir>/<ks>/<t>__chunks-*/  # 청크
```

> **주의: 베이스 테이블 축소는 `gc_grace_seconds`를 기다립니다.** 재인코딩의 레인지 딜리트
> 톰스톤이 gc_grace(운영 기본 1일)를 지나야 컴팩션이 공간을 회수합니다. 켠 직후 `du`가 안
> 줄었다고 실패가 아닙니다 — 청크 테이블 크기와 [벤치마크](tiering-benchmark.md)의 최종
> 수치를 보십시오.
