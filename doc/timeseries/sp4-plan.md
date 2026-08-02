# SP4: 최신 시계열 기술 적용 (Compressed Query · Vectorized · SIMD · 쓰기 3라운드)

## Context

2026-08-03 세션에서 시계열 스택(TimeSeriesMemtable + TSCS + 계층화)의 읽기/쓰기 처리량을 실측·최적화했다.
쓰기는 93.6k → 152.9k rows/s로 SkipList 베이스라인(145k)을 넘겼고(2차 배치 커밋 전), 실측 중
**tiered `latest` 62 ops/s** (시간 경계 없는 LIMIT 1이 태그의 모든 청크를 디코딩)라는 병목과,
"저장은 컬럼나인데 실행은 행 단위"라는 구조적 낭비를 확인했다. 사용자 결정:

- SP4 범위: **4기둥 전부** — ① Compressed Query ② Vectorized 집계 ③ SIMD 디코드 ④ 쓰기 최적화 3라운드
- 청크 포맷: **v3 호환 확장만** (v4 개정 없음 — ALP 등 코덱 교체는 범위 밖)
- 분산: **단일노드(레플리카 로컬) 우선**, 분산 부분집계는 다음 단계
- Phase 0으로 오늘의 미커밋 작업(2차 최적화 배치 + 꼬리 인덱스) 마무리·커밋 포함

## Phase 0 — 오늘 작업 마무리 (선행, 반드시 첫 번째)

현재 워킹 트리에 미커밋 상태로 존재:
- min 가드 + `certainNewAppends` 카운터 (T4: 121,952 rows/s, +30%)
- Option B(역순 long 스토어, 비교 반전) + 셀 조립 경량화(ArrayCell/BulkIterator/O(1) minDeletionTime)
  + 창 산술 원시화(fold·언박싱·lastShard 캐시·accumulate) (T5: 152,943 rows/s)
- 꼬리 해시 인덱스 (T6: **155,644 rows/s** — 최종, SkipList 베이스라인 145,406 대비 +7%,
  보호 테스트 Columnar/Differential/StreamingRead green)
- 테스트: 신규 2건 + DESC 단언 반전 + 오프힙 사고 회귀 핀 재지정(복합/text 클러스터링) + 창 산술 동등성
- 도구: docker/rwbench-read.py (재시도·시차 연결·control_connection_timeout)

할 일: T6 결과 확인 → 남은 테스트 배터리 5클래스(ReadPath/Offheap/Flush/Heap/Options) green 확인 →
비교 리포트(doc/timeseries/rw-throughput-benchmark.md 확장 + 신규 memtable-write-tuning.md) + README 갱신 →
커밋 · master/6.0.0 푸시.

리포트에 반드시 기록: 구성별 비교표(기본/TS스택/tiered), tiered latest 62 ops/s, 계층화 저장 -48%
(base 75.2MB + chunks 48.3MB vs 237.9MB), retier 62.9k rows/s, ALTER 순서 함정(TSCS 먼저 또는 단일 ALTER),
콜드 노드 python 드라이버 connect 타임아웃(2s) 증상.

## Phase 1 — Compressed Query (v3 호환 확장)

서베이 확정 사항(2023-26 업계 관행): 존맵/메타집계는 TimescaleDB(_ts_meta sparse index)·
ClickHouse(minmax skip index)·Parquet(column stats)의 표준 패턴. **`__chunks` 섀도 테이블에
stats 컬럼을 추가하면 청크 payload 포맷은 아예 무변경** — 이미 `samples`/`window_start`가 있어
count(*)·시간 프루닝은 반쯤 완성돼 있음.

정찰 확정 — latest 62 ops/s의 실제 원인: `ChunkRowSource.windows()`가 키당 시간범위
`[MIN,MAX)`의 **모든 청크 행을 무조건 나열**(ASC로 읽어 메모리에서 reverse, :161-223, :186),
LIMIT 계산은 그 뒤 다운스트림. 창별 lazy-decode는 이미 있음(1a240126ab).

1. **tiered latest / LIMIT 푸시다운** (3층, 얕은 것부터):
   - `ChunkRowSource.windowSelect`(:161-162)에 `ORDER BY window_start DESC` + LIMIT/lazy 페이징
     — 지배 비용 제거. `rowsFor/windows()`를 ArrayList → 이터레이터로.
   - `ChunkReadSupport.rowsFromChunk` 하강 분기(:178-189)의 전체 물질화+reverse를,
     `decoded`가 이미 랜덤 액세스(`ByteBuffer[rowCount]`)임을 이용해 역방향 커서로.
   - 소비자 측 중단은 이미 동작(`ChunkMergeUnfilteredIterator.close` → `ChunkRows.close`).
2. **존 맵 = `__chunks` 테이블 컬럼** (payload 무변경): min_ts/max_ts + 컬럼별 min/max를
   `ChunkTables.chunkTableMetadata`(:191-207)/DDL(:225-267)에 추가, 기록은 두 writer
   (`TieredStorageService`:571-573/784-789, `ColdWindowChunkFlush`:453/696-700), 소비는
   `windowSelect` 확장 + 스킵 판단을 `ChunkRows.computeNext`(:333-335, payload() 호출 전 —
   `payloadReads` 카운터로 직접 테스트 가능). **주의**: `ensureChunkTable`은 CREATE IF NOT
   EXISTS만 있음 — 기존 섀도 테이블에 대한 ALTER 경로 신설 필요.
3. **청크 메타 집계**: `samples`(행 수)가 **이미 존재하는데 읽기 경로가 안 씀** — count는
   거의 공짜. sum/sumsq/first/last를 존맵과 같은 컬럼 확장으로. 쿼리 모양 인식은
   `TransparentReads.maybeWrap`(:123-262 — ReadQuery+ColumnFilter를 보는 유일한 지점)에서.
4. **압축 표현 술어**: CONSTANT(디렉토리 constBytes) 청크당 1회 평가, ALL_NULL 즉시 제외.
5. **(S-비용 즉효)** `__chunks` SSTable 압축 Zstd+큰 chunk_length — 측정 후 채택.
6. (선택) payload 내 stats가 필요해지면: **마지막 데이터 섹션 뒤 트레일러 슬롯** — 현행
   디코더가 끝 검사를 안 해(:568-570) 구버전 리더가 무시하는 유일한 전방호환 공간.

호환·불변식: 구버전 청크(stats null)는 기존 경로 폴백, 재인코딩 불필요. 인코더
바이트 결정성(`TieredStorageService`:771-773 `chunkUnchanged`), hasArray 계약(§7),
읽기수리 순서(`DataResolver`:328-337), 손상-스킵 vs 미지원버전-전파 정책 유지.

## Phase 1.5 — ALP double 코덱 (v3 확장, 저장·디코드의 최대 지렛대)

SIGMOD 2024, DuckDB가 Chimp128을 **삭제하고 대체**한 코덱 — 압축률 Chimp128 대비 ≈24%↑,
디코드 1–2 자릿수 빠름(블록 기반이라 SIMD와 정합). v3의 per-chunk 컬럼 type code에
`DOUBLE_ALP` 추가로 **v3 호환** — 결정성 규칙(ALP 시도→불리하면 Chimp128 폴백, 결정적 선택)
유지. codec-bakeoff의 "near-constant double RLE" 미결 과제도 흡수. Java 포팅 필요(참조 C++).
Phase 3(SIMD)의 실질 전제 — Chimp의 branchy 스트림은 벡터화 불가, ALP 블록은 가능.

## Phase 2 — Vectorized 집계 (단일노드)

정찰 확정 — **핸드오프 지점이 이미 존재**: `ColumnarChunkCodec.decodeColumn`이 컬럼별
`double[]`(:750)/`long[]`(:769)/`boolean[]`(:760) + `long[] timestamps`(:547) +
`boolean[] presence`(:540-542)를 만든 뒤 `ByteBuffer[]`로 박싱(:679/:692/:705/:718)한다 —
그 박싱과 행 조립(`ChunkReadSupport.nextRow`의 BTreeRow+BufferCell, :225-234)이 순수 오버헤드.

1. **벡터 뷰 API**: `ColumnarChunkCodec.columns(payload, projection)` 신설 — 박싱 전 원시
   배열을 그대로 노출. 기존 `cursor()` 경로는 무변경 유지.
1b. **커널 인터페이스** (사용자/ChatGPT 제안 검토 반영): `DoubleVectorKernel` —
   `min/max/sum(double[] values, int offset, int len)` + `filterGreaterThan(..., int[] selection)`
   (selection vector 방식). 보완 4가지 필수: ① 혼합 컬럼용 presence-마스크 변형
   (`ALL_PRESENT` 청크는 밀집 커널 — 운영 스키마 지배 케이스), ② memtable ColumnWalk는
   double을 raw bits `long[]`로 보유 — rawBits 변형/변환 패스, ③ 구현 2단:
   `ScalarKernel` 기본(밀집 min/max/sum은 C2 자동 벡터화로 이미 상당) →
   `VectorApiKernel`은 P3에서 플래그 뒤 교체(진짜 이득은 자동 벡터화 안 되는
   filter/compress·마스크 연산), ④ 행 경로 대비 동등성은 ULP 허용오차 기준 + 문서화
   (부동소수 누적 순서 차이).
2. **집계 소비자**: `ChunkReadSupport`의 형제 클래스(집계 전용) — 순수 집계 쿼리일 때
   `TransparentReads.maybeWrap`(:257-261)에서 분기. 1024 배치 폴드, presence 마스크 적용.
3. **memtable 슬롯 직접 집계**: Walk 원시 배열을 같은 집계 소비자 인터페이스로
   (captureWalk 계약 — 락 안 캡처, 캡처 후 락 프리 — 준수). 핫+콜드 연산자 통일.
4. **memtable 슬롯 방문자**: 시임은 단 한 곳 — `TimeSeriesStreamingIterator.SliceRows.computeNext`
   (:276/:304)의 `walk.assembleRow(slot)` 호출. 그 위의 슬라이스 이진탐색·overflow 병합·
   superseded 재확인은 집계-불가지 슬롯 선택이라 그대로 재사용. `walk.forEachSlot(slices,
   reversed, consumer)` 방문자 신설(원시 배열 + 슬롯 인덱스 전달), overflow 행과 width==0
   객체 컬럼만 기존 행 경로. 계약: superseded(:1415) 재확인, overflowGet(:1427) 우선,
   deletionInfo 직접 적용, 객체 셀은 ensureOnHeap 경유. `rowsAssembled` 카운터로 효과 실측.
5. **공짜 인접 수확**: `GroupMaker.SelectorGroupMaker.executeSelector`(:227-239)가
   `time_bucket`을 행마다 재평가+직렬화 비교 — 집계 select 목록과 **행당 2회 평가** 중복.
   고정폭 버킷 + long 클러스터링이면 정수 나눗셈으로 축약.

집계 함수 분류 (정찰로 확정, continuous-aggregates-design.md의 분해성 표와 일치):
- **메타-폴더블**: min/max/sum/count/avg, first/last(동반 ts 필요), delta/rate(경계 4스칼라)
- **보조상태 폴더블**: variance·stddev(n,Σx,Σx²), corr/covar/regr 6종(6모멘트),
  histogram(동일 버킷 구성 시 counts 합), integral/TWA·counter_delta/rate(내부값+경계값 —
  창 경계 사다리꼴/리셋 시맨틱 주의), derivative(baseTs 재기준화 후 병합)
- **원시값 필요**: percentile(정확 CONT), approx_count_distinct(HLL 스케치 자체는 병합
  가능하나 현재 estimate만 반환 — 스케치 상태 노출은 후속)

분산 부분집계(다음 단계로 분리 — 사용자 결정): 시임은 확보됨 —
AggregationSpecification 직렬화가 이미 레플리카에 도달(DataLimits :1184/:1223),
GroupByAwareCounter(:830)가 그룹 경계에서 부분 상태 방출 후보, 코디네이터 병합은
DataResolver.resolveInternal(:303-343)이 Row-형태 전제라 최난관. CL=ONE/다이제스트 일치
경로부터가 최저비용 진입.

## Phase 3 — SIMD 디코드 (ALP 이후)

- 전제 1: 운영 노드 41 CPU 플래그 확인(AVX2 유무) — 벤치 호스트(X5670)는 SSE4.2뿐.
- 전제 2: **Phase 1.5(ALP) 선행** — Lucene 9.10+/ES 8.13+ 출하 경험상 branchy 스트림 없이
  블록 bit-unpack이어야 벡터화 이득이 실재(단독 비트 언패킹은 수십 % 수준).
- Java Vector API(인큐베이터, --add-modules) 플래그 뒤에서: ALP 섹션 디코드, 타임스탬프
  블록 delta+bitpack(신규 type code), null 비트맵 전개. Chimp128 XOR 체인은 대상 아님.
- 스칼라 폴백 유지, 동등성 테스트(비트 단위 동일 출력) 필수.

## Phase 4 — 쓰기 최적화 3라운드

이번 세션 에이전트 명세 확보분 (명세 문서: 세션 산출물 참조):
1. flush 스트리밍 뷰(A): flushView()가 TimeSeriesStreamingIterator로 스트리밍 — EncodingStats
   2차 순회는 죽은 일임을 확인(작성자 미소비), StreamingFlushView 파사드 명세 확보.
2. 완전 정렬 수정: DESC 테이블 배열을 raw-오름차순으로 — consolidate 완전 제거.
   방향 분기 ~8곳 + flush 빌더 반전, 별도 리뷰 필수 (고위험·고수익).
3. 경합 계측: synchronized → ReentrantLock tryLock + contended/uncontended 카운터 (TrieMemtable 방식).
4. TSCS 컴팩션 성능 분석 (사용자 요청): 창 동결 비용, UCS 위임 효율 — 분석 에이전트부터.
5. 기타 명세 확보분: C4(ArrayClustering), 이중 스킵리스트 탐색 제거, dataSize 융합(2d),
   per-shard 통계, O(1) flush 사이징.

## SP4 이후 후보 (이번 범위 밖, 서베이 확보분)

- Continuous aggregates 실행(기존 DRAFT·watermark 설계가 업계 방향과 일치 확인) — Phase 1의
  청크 stats를 refresh 입력으로 쓰면 콜드 구간 롤업이 디코드 0회 (설계 시너지)
- 청크 블룸 필터로 콜드 구간 값/텍스트 검색(현재 non-static SAI 거부 제한의 부분 해제)
- memtable→청크 직행 flush 완성(InfluxDB 3 패턴; stage 4의 일반화, write amp 3-4× 제거)
- 적응형 freeze_after(QuestDB dynamic commit lag 패턴; 지각 분포 관측 기반)
- FSST 텍스트 인코딩(value 컬럼의 문자열 사본이 최대 섹션 — BtrBlocks/DuckDB 패턴)
- 관망: Elf(ALP로 대체됨)·Camel·DeXOR(논문 단계), FCBench(코덱 검증 방법론)
- SSTable 블록 수준 통계 인덱스(TSIndex — 사용자 제안 2026-08-03): 원리·필드 구성은 P1의
  청크 통계와 동일하되 원본 SSTable 계층에 적용. 청크 통계가 콜드(대부분)를 커버하므로
  후순위 — 비계층화 테이블/핫 윈도우의 집계 스캔이 프로파일상 실병목으로 확인되면 착수.
  주의: 새 SSTable 컴포넌트는 io/sstable 포맷 수술 + 업스트림 머지 마찰. SSTable 단위
  min/max ts 스킵은 StatsMetadata로 기존 동작.

## 검증

- 각 Phase: 기존 TimeSeries* 테스트 배터리 green + 신규 기능별 차등/동등성 테스트
- 벤치: docker 24c/48g 표준 하네스 재사용 — Phase 1은 tiered 읽기 3패턴 재측정(특히 latest),
  Phase 2는 집계 쿼리 시간(scale-test.sh 쿼리 스위트), Phase 4는 wbench 쓰기 rows/s
- 성능 회귀 게이트: 쓰기 ≥152k 유지, 집계는 tiering-benchmark 대비 개선 폭 기록

## 실행 순서와 마일스톤

| 순서 | 작업 | 게이트(벤치/검증) |
|---|---|---|
| 0 | Phase 0: 오늘 작업 마무리·리포트·커밋·푸시 | 배터리 8클래스 green, 쓰기 ≥150k |
| 1 | P1-1 latest/LIMIT 푸시다운 (windowSelect DESC+LIMIT → lazy 이터레이터 → 역방향 커서) | tiered latest 62 → 수천 ops/s |
| 2 | P1-2·3 존맵+메타집계 (`__chunks` 컬럼 확장 + ALTER 경로 + 두 writer + maybeWrap 인식) | 콜드 무필터 집계 디코드 0회 확인 |
| 3 | P2 벡터 뷰 + 청크/슬롯 직접 집계 + time_bucket 이중평가 제거 | 집계 쿼리 스위트(scale-test) 개선 폭 |
| 4 | P1.5 ALP 코덱 (Java 포팅, 결정성 폴백, FCBench식 검증) | 압축률·디코드 시간 vs Chimp128 |
| 5 | P3 SIMD (운영 CPU 확인 후, ALP 섹션 + ts 블록 언패킹) | 동등성 + 디코드 벤치 |
| 6 | P4 쓰기 3라운드 (A→계측→완전 정렬 수정→TSCS 컴팩션 분석) | wbench ≥152k 유지·개선 |

각 단계 독립 커밋·독립 벤치. 2·3단계는 상호 독립이라 순서 교환 가능.

## 근거 자료

이 계획의 file:line 수준 삽입 지점들은 2026-08-03 분석(쓰기 경로 핫스팟, 역순 long 타당성,
flush 경로 설계, 창 산술 미시비용, TrieMemtable 비교, TSDB 최신 기술 서베이, 청크 포맷·읽기
경로 정찰, 집계 실행 경로 정찰)에서 나왔다. 실측 배경:
[memtable-write-tuning.md](memtable-write-tuning.md) ·
[rw-throughput-benchmark.md](rw-throughput-benchmark.md).
벤치 하네스: `docker/rwbench-read.py` + 24코어/48G 표준 컨테이너 절차.
