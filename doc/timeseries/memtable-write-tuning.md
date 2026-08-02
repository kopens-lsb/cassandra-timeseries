# TimeSeriesMemtable 쓰기 성능 튜닝 기록

2026-08-03 · master 기준 · 벤치: 24코어/48G 컨테이너, 운영 형태 `tm_tag_point` 1,000만 행
(500 태그 × 12 로더, 100행 unlogged batch), generational ZGC, 힙 16G. 하네스와 절차는
[rw-throughput-benchmark.md](rw-throughput-benchmark.md)와 동일.

시계열 스택(memtable `timeseries` + TSCS)의 쓰기가 기본 memtable 대비 **-36%**로 측정된
데서 출발해, 원인 분석 → 설정 튜닝 → 코드 수정 3라운드로 **베이스라인을 +7% 넘어서는**
지점까지 간 기록이다. 결론 요약:

| 단계 | rows/s | 베이스라인 대비 |
|---|---|---|
| SkipList 베이스라인 (기본 memtable + UCS) | 145,406* | — |
| 시계열 스택, 수정 전 | 93,585 | **-36%** |
| 설정 튜닝 최고치 (`memtable_flush_writers: 4` + `memtable_heap_space: 8GiB`) | 99,096 | -32% |
| + min 경계 가드 (수정 1) | 121,952 | -16% |
| + 역순 long 스토어·셀 조립 경량화·창 산술 원시화 (수정 2) | 152,943 | **+5.2%** |
| + 꼬리 해시 인덱스 (수정 3) | **155,644** | **+7.0%** |

\* 베이스라인은 2,000만 행 적재의 지속 처리량. 이후 단계는 동일 조건의 1,000만 행 측정.
수정 후에는 설정 튜닝(flush writer 증설)의 효과가 **사라진다** — 병목이 flush 대기가 아니라
파티션 락 안의 CPU였다는 뜻이다.

## 1. 원인 — DESC 클러스터링이 두 고속 경로를 동시에 꺼버렸다

운영 스키마는 `CLUSTERING ORDER BY (timestamp DESC)`다. 컬럼나 파티션
([TimeSeriesColumnarPartition.java](../../src/java/org/apache/cassandra/db/memtable/TimeSeriesColumnarPartition.java))은
두 가지 최적화를 전제로 설계됐는데, DESC가 둘 다 무효화했다:

1. **원시 long 클러스터링 스토어**: 클러스터링이 단일 timestamp/bigint일 때 `long[]`으로
   저장한다 — 그런데 판별이 `subtype(0) == TimestampType.instance` 정체성 비교여서
   `ReversedType(TimestampType)` 래퍼가 걸러졌다. DESC 테이블은 행마다 클러스터링 객체를
   클론(~120B 힙)하고, 모든 비교가 comparator 디스패치(의존 캐시 미스 3-4회)로 갔다.
2. **순서 append 고속 경로**: "지금까지의 최대보다 확실히 위" 검사만 있었다. 오름차순
   시각 유입은 DESC 비교기상 **내림차순**이라 이 검사에 한 번도 걸리지 않았고, 행마다
   `findSlot`(정렬 접두부 이진탐색 + **미정렬 꼬리 최대 1,024개 선형 탐색**)을 파티션 락
   안에서 수행했다. 평균 ~512회 비교 × 100행 배치 = 배치당 ~5만 회 비교를 락 잡고 실행.

배치들이 96개씩 비동기로 날아가므로 도착 순서가 섞이고, 순서가 섞인 행은 min/max 경계
검사로도 못 걸러 여전히 꼬리를 긁는다. `sortedCount`는 1에 얼어붙고 1,025행마다
`consolidate()`(배열 전체 재구축)가 무한 반복됐다.

## 2. 설정 튜닝 — 한계 +6%

| 변형 | rows/s | 판정 |
|---|---|---|
| 로더 12 → 16 | 92,333 | 무효 — 서버가 병목 (동시성 부족 아님) |
| `memtable_flush_writers: 4` + `memtable_heap_space: 8GiB` | 99,096 | +5.9% |
| `memtable_allocation_type: offheap_objects` | 93,858 | 중립 (쓰기 기준) |
| GC 전환 (G1) | — | 기존 실측: 쓰기 -3%, 제외 |

오프힙은 쓰기엔 중립이지만 읽기 회귀 확인을 겸했다: 2026-08-02 운영 사고 경로였던
페이지드 리드가 오프힙에서 10,651 ops/s·오류 0으로 정상 동작함을 재확인.

## 3. 코드 수정 3라운드

각 수정은 차등(differential) 테스트 + 뮤테이션 검증(가드 제거 시 red 확인)을 거쳤고,
읽기 경로 보호를 위해 Columnar/Differential/ReadPath/StreamingRead/OffheapReadPath
클래스를 통과시켰다.

### 수정 1 — min 경계 가드 (+30%)

`apply()`의 certainly-new 검사에 대칭 조건 추가: min/max는 superseded 슬롯까지 포함한
전체 경계이므로, **min보다 엄격히 아래인 클러스터링은 어떤 슬롯과도 충돌 불가** — 비교
1회로 `findSlot` ~512회를 대체한다. 오름차순 유입(운영 패턴)의 순서 유지 도착이 전부
이 경로를 탄다. 관측 카운터 `certainNewAppends`로 테스트가 가드 소실을 감지한다.

### 수정 2 — 역순 long 스토어 + 셀 조립 경량화 + 창 산술 원시화 (+25%)

- **역순 long 스토어 (Option B)**: `ReversedType(Timestamp|Long)`도 long 스토어를 쓰되,
  키를 변환하지 않고 **비교만 반전**한다(`compareKeys`). ReversedType은 비교만 감싸고
  직렬화는 8바이트 원형 그대로임을 확인. 저장/복원 지점 수정 0곳 — 변환 방식(~x)이
  요구했을 복원 누락(조용한 오염) 위험 자체가 없다. 행당 클러스터링 클론 제거, 모든 슬롯
  비교가 `Long.compare`로. 부수 효과: 오프힙에서 min/max 경계가 native가 아닌 heap
  클러스터링이 되어 8/2 사고 계열 위험이 오히려 줄었다. 주의: 기존 오프힙 회귀 테스트
  2건이 "DESC=객체 클러스터링" 전제로 사고를 감시하고 있었으므로, 복합 클러스터링과
  text DESC 스키마로 재지정해 감시를 유지했다.
- **셀 조립 경량화** (flush·읽기 공통): `BufferCell`+`ByteBuffer.allocate` →
  `ArrayCell`+`byte[]`(셀당 ~50B↓), `ArrayList`+`BTree.build(Collection)`(4.0부터
  deprecated) → 스크래치 배열+풀링된 `BulkIterator`(행당 ~70B↓), 4-인자
  `BTreeRow.create`의 셀 전체 재순회 → 슬롯 불변식(균일 ts/ttl/ldt·톰스톤 없음)으로
  `minDeletionTime` O(1) 산출.
- **창 산술 원시화**: `windowStartFor`가 TWCS의 `Pair<Long,Long>`를 행마다 만들던 것을
  포크 소유 파일에서 원시 연산으로(동등성 테스트로 TWCS와 비트 단위 일치 고정 — 절단
  방향의 버그 호환성 포함), `singleWindowOf`는 요소별 창 계산 대신 라우팅 타임스탬프
  min/max 접기 후 **업데이트당 2회만** 창 계산(windowOf 단조성으로 정확성 증명),
  박싱 반환 → `NO_SINGLE_WINDOW` 센티널(`Long.MIN_VALUE+1`, 실산술 도달 불가 증명),
  `maxTimestamp`는 이터레이터 할당 없는 `Row.accumulate`로, `shardFor`는 volatile
  마지막 샤드 캐시(단일 참조 — (창,샤드) 필드 쌍은 찢긴 읽기로 오배정 위험).

### 수정 3 — 꼬리 해시 인덱스 (+2%)

미정렬 꼬리를 `HashMap<Long,Integer>`(클러스터링 키 → 최신 슬롯)로 색인 — 도착 순서가
섞여 min/max 경계로 못 거른 행의 `findSlot`이 선형 탐색 대신 맵 1회 조회가 된다.
작가 전용 구조(파티션 락 아래에서만 접근, 리더는 못 봄), `consolidate()`에서 통째로
비움(슬롯 번호가 바뀌므로), 꼬리 상한(1,024)만큼만 커진다. 재기록은 엔트리를 덮어쓴
뒤 이전 슬롯을 supersede하므로 맵의 엔트리는 항상 유일한 live 후보다.

## 4. 남은 격차와 다음 단계

155.6k는 베이스라인 +7%지만 `consolidate()`(1,025행마다 배열 재구축)는 여전히 남아 있다.
후속 명세가 확보된 항목 (SP4 Phase 4):

1. **flush 스트리밍 뷰**: `flushView()`의 전체 물질화(행당 ~1.8KB, flush당 힙 4.9GiB)를
   스트리밍 이터레이터로. flush의 `EncodingStats` 2차 순회는 **작성자가 소비하지 않는
   죽은 일**임이 확인됨(SSTable writer는 memtable 수준 stats만 사용).
2. **완전 정렬 수정**: DESC 테이블의 배열을 raw-오름차순으로 유지 — `consolidate` 자체가
   사라진다. 방향 분기 ~8곳 + flush 빌더 반전이 걸리는 고위험·고수익 수술로, 별도 리뷰 필수.
3. **경합 계측**: TrieMemtable 방식(tryLock + contended/uncontended 카운터) — 남은 병목이
   락 경합인지 CPU인지를 측정으로 가른다.

참고: TrieMemtable의 샤드 단위 쓰기 락은 파티션 단위인 이 memtable보다 **굵어서** 이
워크로드에는 역효과 — 채택하지 않는다(비교 분석으로 확인).

## 5. 재현

[rw-throughput-benchmark.md](rw-throughput-benchmark.md)의 절차로 컨테이너를 만들고,
테이블에 시계열 스택을 **한 번의 ALTER로** 적용한 뒤 적재한다:

```sql
ALTER TABLE scale.tm_tag_point WITH compaction = {
  'class':'TimeSeriesCompactionStrategy', 'window_size':'1h', 'freeze_after':'1h'}
  AND memtable = 'timeseries' AND gc_grace_seconds = 0;
```

**ALTER 순서 함정**: `memtable = 'timeseries'`를 TSCS 적용 **전에** 따로 ALTER하면 그
시점의 memtable은 "TSCS가 아니라서" SkipList로 폴백하고, 다음 flush 때에야 교체된다
(경고 로그 1줄, NoSpamLogger라 재발해도 시간당 1줄). 한 문장으로 묶거나 TSCS를 먼저.
