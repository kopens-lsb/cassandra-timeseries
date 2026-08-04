# TimeSeriesMemtable 쓰기 경로 — 현재 설계와 실측

시계열 스택(memtable `timeseries` + TSCS)의 쓰기 경로가 지금 어떻게 동작하는지와, 그 실측치다.
설계 전반은 [timeseries-memtable.md](timeseries-memtable.md), 하네스와 절차는
[rw-throughput-benchmark.md](rw-throughput-benchmark.md) 참고.

## 1. 실측 (24코어 컨테이너, 2026-08-03)

측정 환경: **24코어/48G 컨테이너**, 운영 형태 `tm_tag_point` 1,000만 행(500 태그 × 12 로더,
100행 unlogged batch), generational ZGC, 힙 16G. **234(Xeon Silver 4114T)에서의 쓰기 경로
재측정은 아직 없다** — 아래 수치의 호스트는 전부 24코어 컨테이너다. (청크 포맷 v4 전환은 쓰기
경로와 무관하다 — 벤치는 계층화를 켜지 않았고, memtable/TSCS 동작은 v4에서 바뀌지 않았다.)

| 구성 | rows/s | 비고 |
|---|---|---|
| 기본 memtable(SkipList) + UCS | 145,406 | 2,000만 행 적재의 지속 처리량 |
| **시계열 스택 (memtable `timeseries` + TSCS)** | **155,644** | **베이스라인 +7%** (1,000만 행, 동일 조건) |

시계열 스택은 쓰기에서 기본 스택보다 빠르며, 행당 힙은 5.5배 작다
([timeseries-memtable.md §4.3](timeseries-memtable.md)). 병목은 flush 대기가 아니라 **파티션 락
안의 CPU**다 — `memtable_flush_writers` 증설이 처리량을 바꾸지 않는 것이 그 증거다(§3).

## 2. 쓰기 경로의 성능 구조

컬럼나 파티션([TimeSeriesColumnarPartition.java](../../src/java/org/apache/cassandra/db/memtable/TimeSeriesColumnarPartition.java))의
쓰기 경로는 다음으로 구성된다. 운영 스키마의 기본형인 `CLUSTERING ORDER BY (timestamp DESC)`
테이블에서도 전부 동작한다.

- **min/max 경계 가드**: 파티션의 전체 경계(superseded 슬롯 포함) 밖에 있는 클러스터링은 어떤
  슬롯과도 충돌할 수 없으므로 비교 1회로 슬롯 탐색을 건너뛴다. 오름차순 시각 유입(운영 패턴)의
  순서 유지 도착이 전부 이 경로를 탄다. 관측 카운터 `certainNewAppends`로 테스트가 가드 소실을
  감지한다.
- **역순 long 클러스터링 스토어**: 단일 `timestamp`/`bigint` 클러스터링은 `ReversedType` 래퍼가
  있어도 `long[]`으로 저장한다 — 키를 변환하지 않고 **비교만 반전**한다(`compareKeys`).
  행당 클러스터링 객체 클론이 없고, 모든 슬롯 비교가 `Long.compare`다.
- **경량 셀 조립** (flush·읽기 공통): `ArrayCell`+`byte[]` 기반 조립, 스크래치 배열+풀링된
  `BulkIterator`, 슬롯 불변식(균일 ts/ttl/ldt·톰스톤 없음)을 이용한 `minDeletionTime` O(1) 산출.
- **창 산술 원시화**: `windowStartFor`는 박싱 없는 원시 연산(TWCS와 비트 단위 동등성 테스트로
  고정), `singleWindowOf`는 라우팅 타임스탬프 min/max 접기 후 업데이트당 2회만 창 계산,
  `shardFor`는 volatile 마지막 샤드 캐시.
- **꼬리 해시 인덱스**: 미정렬 꼬리를 `HashMap<Long,Integer>`(클러스터링 키 → 최신 슬롯)로
  색인해, 도착 순서가 섞여 경계 가드로 못 거른 행의 슬롯 탐색이 선형 탐색 대신 맵 1회 조회다.
  작가 전용 구조(파티션 락 아래에서만 접근)이고 `consolidate()`에서 통째로 비운다.

각 항목은 차등(differential) 테스트 + 뮤테이션 검증(가드 제거 시 red 확인)으로 고정돼 있고,
읽기 경로는 Columnar/Differential/ReadPath/StreamingRead/OffheapReadPath 테스트 클래스가 지킨다.

## 3. 설정 가이드 (측정으로 확인된 것)

| 설정 | 효과 |
|---|---|
| `memtable_flush_writers` 증설, `memtable_heap_space` 확대 | **효과 없음** — 병목이 flush 대기가 아니라 파티션 락 안 CPU |
| 로더(클라이언트) 동시성 증설 | 효과 없음 — 서버가 병목 |
| `memtable_allocation_type: offheap_objects` | 쓰기 중립. 읽기(페이지드 리드 포함)도 정상 동작 확인 — 10,651 ops/s·오류 0 |
| GC를 G1으로 전환 | 쓰기 -3% — 권장하지 않음 |

## 4. 남은 병목과 후속 항목 (SP4 Phase 4)

`consolidate()`(미정렬 꼬리 1,025행마다 배열 전체 재구축)는 여전히 남아 있다. 명세가 확보된
후속 항목:

1. **flush 스트리밍 뷰**: `flushView()`의 전체 물질화(행당 ~1.8KB, flush당 힙 4.9GiB)를
   스트리밍 이터레이터로. flush의 `EncodingStats` 2차 순회는 작성자가 소비하지 않는 죽은
   일임이 확인됨(SSTable writer는 memtable 수준 stats만 사용).
2. **완전 정렬**: DESC 테이블의 배열을 raw-오름차순으로 유지 — `consolidate` 자체가 사라진다.
   방향 분기 ~8곳 + flush 빌더 반전이 걸리는 고위험·고수익 수술로, 별도 리뷰 필수.
3. **경합 계측**: TrieMemtable 방식(tryLock + contended/uncontended 카운터)으로 남은 병목이
   락 경합인지 CPU인지 측정으로 가른다.

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
시점의 memtable은 "TSCS가 아니라서" 기본 memtable로 폴백하고, 다음 flush 때에야 교체된다
(경고 로그 1줄, NoSpamLogger라 재발해도 시간당 1줄). 한 문장으로 묶거나 TSCS를 먼저.
