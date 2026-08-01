# 시계열 전용 Memtable 설계

**목표:** 창 단위로 샤딩된 컬럼 지향 memtable을 추가해 flush가 이미 창 정렬된 SSTable을
내보내게 하고, 그 결과 컴팩션 총량과 flush 비용을 줄인다. 테이블별 옵트인이며 기본
memtable을 대체하지 않는다.

**배경:** 2026-08-02 프로덕션(192.168.0.41) 실측에서 `tm_tag_point` 계열 창이 파킹됐다.
원인은 `SSTableWriter`가 파티션 하나를 한 번에 받아야 해서, 창 경계로 쪼개려면 파티션
전체를 힙에 버퍼링해야 한다는 것(`WindowRoutingIterator.maxBufferedBytesPerPartition`,
64 MiB)이었다. memtable은 가변 맵이라 이 제약이 없다 — 쓰기 시점에 행 하나씩 O(1)로
해당 창에 넣으면 버퍼링 자체가 발생하지 않는다.

---

## 1. 확장 지점

Cassandra 5.x 이후 memtable은 플러그인이다(`src/java/org/apache/cassandra/db/memtable/Memtable_API.md`).
`Memtable` 인터페이스의 `put` · `rowIterator` · `partitionIterator` · `getFlushSet`을 구현하고
`Memtable.Factory`를 제공하면 되며, 통계·커밋로그 구간 추적·메모리 관리는
`AbstractAllocatorMemtable`이 제공한다. **업스트림 파일을 수정하지 않는다.**

```sql
ALTER TABLE pp.tm_tag_point WITH memtable = {'class': 'TimeSeriesMemtable'};
```

## 2. 구조

```
TimeSeriesMemtable  (extends AbstractAllocatorMemtable)
  └── NavigableMap<Long windowStart, WindowShard>
                              └── ConcurrentMap<DecoratedKey, PartitionColumns>
                                        ├── long[]   clusteringTimestamps
                                        ├── long[]   rowWriteTime          (fast path)
                                        ├── int[]    rowLocalDeletionTime  (fast path)
                                        ├── 숫자·불리언 컬럼별 원시 배열
                                        ├── Object[] 폴백 (text · blob · frozen 컬렉션)
                                        ├── 오버플로 맵 (셀별 타임스탬프가 필요한 행만)
                                        └── 톰스톤 리스트 (행 · 범위 · 파티션 삭제)
```

`window_size`는 **테이블의 TSCS 옵션에서 읽는다.** 별도 설정을 두지 않는다 — 두 곳에
따로 두면 어긋나 창이 맞지 않는다. TSCS가 아닌 테이블에는 이 memtable을 허용하지 않는다.

창 배정 기준은 **쓰기 타임스탬프**이며, 이는 `TimeSeriesCompactionStrategy`의 창 분류
기준과 동일하다. 기준이 다르면 flush 결과가 전략이 기대하는 창에 들어가지 않는다.

## 3. 저장 모델

### 3.1 행 단위 fast path, 셀 단위 slow path

Cassandra는 셀 단위 LWW를 보장해야 한다. `INSERT` 후 특정 컬럼만 `UPDATE`하면 셀마다
쓰기 타임스탬프가 달라진다. 이를 순진하게 지원하려면 컬럼마다 `long[]`이 필요하고,
8컬럼이면 행당 64바이트가 타임스탬프로만 소모되어 원시 배열의 이득을 상쇄한다.

- **fast path**: 한 행의 모든 셀이 같은 쓰기 타임스탬프 → 행당 `long` 하나. 산업 태그
  수집은 사실상 전부 이 경우다.
- **slow path**: 같은 (파티션, 클러스터링)에 다른 시각의 쓰기가 겹치면 **그 행만**
  오버플로 맵으로 승격해 셀별 타임스탬프·TTL을 갖는다.

`localDeletionTime`(TTL)도 같은 구조를 따른다.

### 3.2 정렬

시계열 쓰기는 대부분 시간 순이지만 보장되지 않는다. **append 후 지연 정렬**한다: 들어온
클러스터링이 직전보다 작으면 dirty 플래그만 세우고, 읽기 또는 flush 시점에 한 번 정렬한다.
정상 유입에서는 정렬이 발생하지 않는다.

### 3.3 원시 배열로 담지 않는 것

- **톰스톤**(행 삭제 · 범위 삭제 · 파티션 삭제): 파티션별 리스트, 읽기 시 병합
- **static 행**: 파티션당 하나, 기존 객체 표현
- **비frozen 컬렉션 · counter**: **미지원**

마지막 항목은 팩토리가 스키마를 검사해 판정하고, 지원 불가면 **기본 memtable로 폴백**한다.
`TieringPolicy`의 스키마 적합성 판정과 같은 패턴이다. 범용 memtable을 다시 만들지 않는
것이 이 설계가 감당 가능한 이유다.

### 3.4 동시성

Cassandra는 flush 시작 시 memtable을 **교체**한다. 새 쓰기는 새 memtable로 가고 flush
대상은 배리어 이후 불변이므로, "쓰기 중 flush 스냅샷" 문제를 직접 풀 필요가 없다.
책임 범위는 **동시 `put`끼리**이며 파티션 단위 락 또는 CAS로 처리한다.

## 4. 읽기 경로

`SortedTablePartitionWriter`는 `Unfiltered` 객체 자체는 보관하지 않지만(즉시 직렬화)
`clustering()`은 `firstClustering` · `lastClustering`으로 **보관한다**(코드 확인함,
`SortedTablePartitionWriter.java:137,145`). 질의 경로는 더 제약이 크다 — 병합
이터레이터가 여러 소스의 현재 행을 동시에 붙들고 비교한다.

| 경로 | 방식 | 근거 |
| --- | --- | --- |
| flush | flyweight `Row` 재사용, **`clustering()`은 매번 새 불변 객체** | 소비자가 하나이고 즉시 직렬화 |
| 질의 (SELECT) | 실제 `BTreeRow` 생성 (현행 유지) | 병합 이터레이터가 참조를 붙듦 |

질의 경로를 바꾸지 않으므로 읽기 회귀 위험이 없다. 이득은 flush 경로에서 나온다.

## 5. Flush 경로

`getFlushSet`은 창 샤드별로 나뉜 뷰를 반환하고, **샤드 하나당 SSTable 하나**가 나온다.
이미 창 정렬이 끝나 있으므로 `TimeWindowSplittingMultiWriter`가 신규 flush 경로에서는
필요 없다. 레거시 SSTable 분할과 스트리밍 수신 경로에는 그대로 남는다.

### 5.1 콜드 창 청크 flush (마지막 태스크)

계층화가 켜진 테이블에서 flush 대상 창이 `hot_window`보다 오래되었으면, 행을 베이스
SSTable에 쓰는 대신 청크로 인코딩한다. 재인코더의 read → encode → range-delete 왕복이
사라진다. 대량 백필·아카이브 적재에서 이득이 크다.

**내구성 순서 규칙 (반드시 이 순서):**

1. 청크를 청크 테이블에 **내구성 있게** 쓴다 (청크 테이블의 커밋로그에 들어간 뒤)
2. 커버리지 원장(`<base>__chunk_coverage`)을 넓힌다
3. 그 다음에야 베이스 SSTable에서 해당 행을 제외한다

순서를 지키지 않으면 청크 쓰기 실패 시 데이터가 사라진다 — 베이스 memtable의 커밋로그
구간은 flush 완료 시점에 해제되기 때문이다. 이 순서는 기존 계층화 재인코더의 불변식과
동일하다.

**핫 데이터는 청크화하지 않는다.** 청크는 불변이고 콜드 경계 아래로는 쓰기가 거부되므로,
방금 들어온 데이터를 청크로 만들면 지각 백필과 갱신이 불가능해지고 `hot_window` 설정이
의미를 잃는다.

## 6. 검증

memtable 결함은 **조용한 데이터 손실**로 나타난다. 통과 여부가 아니라 **기준 구현과
같은 답을 내는지**로 판정한다.

| 방법 | 내용 |
| --- | --- |
| **차등 테스트** | 같은 연산 시퀀스를 `TrieMemtable`과 `TimeSeriesMemtable`에 적용해 ① 읽기 결과 ② flush된 SSTable의 논리적 내용(행 · 셀 · 쓰기시각 · TTL · 톰스톤)이 완전히 일치하는지 비교 |
| **속성 기반 퍼즈** | 순서 뒤집힌 타임스탬프, 같은 셀 재기록, 부분 UPDATE, TTL, 행/범위/파티션 삭제, 창 경계 걸침을 무작위 조합. `test/harry/` 활용 |
| **flyweight 오염 검출** | flyweight 재사용을 끈 결과와 켠 결과의 flush 출력이 동일한지. 불일치는 `clustering()` 규칙 위반을 뜻한다 |
| **커밋로그 재생** | 쓰기 도중 강제 종료 → 재시작 → 손실 0 |
| **3노드 dtest** | 부트스트랩 · 스트리밍 · repair 정상 동작 |
| **도커 통합 테스트** | 릴리스 게이트 편입 |

**측정 기준** (실측으로 판정하며, 미달이면 미달로 기록한다):

- flush 처리량 (rows/s)
- 행당 힙 점유
- **GB 유입당 컴팩션된 바이트** — 창 샤딩의 실제 이득이 드러나는 지표
- 차등 테스트 불일치 **0건**

## 7. 범위 밖 (YAGNI)

- 범용 memtable을 만들지 않는다 — 비frozen 컬렉션 · counter 테이블은 기본 memtable로 폴백
- 자체 내구성 메커니즘 없음 (`writesAreDurable()` = false, 커밋로그 그대로 사용)
- `streamFromMemtable` 미지원 — 스트리밍은 기존 경로
- 질의 읽기 경로를 바꾸지 않는다
- 기본 memtable을 교체하지 않는다 — 테이블별 옵트인

## 8. 구현 순서

각 단계가 독립적으로 검증 가능하고, 앞 단계가 프로덕션에서 확인된 뒤 다음을 얹는다.

| # | 내용 | 완료 기준 |
| --- | --- | --- |
| 1 | 창 샤딩 + 기존 객체 저장 (원시 배열 없음) | 차등 테스트 0건 불일치, flush가 창당 SSTable 1개 |
| 2 | flush flyweight (`clustering()` 규칙 포함) | flyweight on/off 출력 동일, flush 처리량 실측 |
| 3 | 원시 컬럼 저장 (fast/slow path) | 차등 테스트 0건, 행당 힙 실측 |
| 4 | 콜드 창 청크 flush (§5.1 순서 규칙) | 재인코더 경유 결과와 청크 내용 일치, 강제 종료 후 손실 0 |

1단계만으로도 파킹 원천 제거와 컴팩션 총량 감소를 얻는다. 3단계 이후가 메모리 이득이다.
