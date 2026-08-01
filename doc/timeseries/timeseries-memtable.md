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

# 시계열 전용 Memtable: `TimeSeriesMemtable`

`TimeSeriesCompactionStrategy`(TSCS)로 컴팩션되는 테이블을 위한 memtable 구현입니다. 행이
**쓰이는 시점**에 그 행이 속할 TSCS 창을 정해 해당 창의 샤드에 넣습니다 — 지금까지는 이 배정이
**flush 시점**에 `WindowRoutingIterator`가 하던 일이었습니다. 테이블별 옵트인이며, 기본
memtable(`SkipListMemtable`/`TrieMemtable`)을 대체하지 않습니다.

구현: [`TimeSeriesMemtable`](../../src/java/org/apache/cassandra/db/memtable/TimeSeriesMemtable.java).
관련 배경(파킹, 라우팅 버퍼 오버플로): [timeseries-compaction.md §5](timeseries-compaction.md#5-파킹된-창--진척을-못-내는-창).

## 1. 왜 필요한가 — flush 시점 라우팅의 한계

창 경계 분할 자체는 **이 memtable 없이도 이미 동작합니다.** `TimeSeriesCompactionStrategy`가 flush
writer로 `TimeWindowSplittingMultiWriter`를 설치하므로, 메모테이블 하나가 여러 창에 걸쳐 있어도
flush 결과는 창당 SSTable로 쪼개집니다.

문제는 그 분할 방식입니다. `SSTableWriter`는 파티션 하나를 한 번에 받고 그 파티션의 키를 한 번만
받아들입니다. 창 경계로 쪼개려면 파티션 전체를 힙에 올려 창별로 다시 나눠야 하는데,
`WindowRoutingIterator.maxBufferedBytesPerPartition`(기본 64 MiB)를 넘는 파티션을 만나면 라우팅이
분할을 포기하고 파티션을 통째로 씁니다. 그 결과가 창을 걸친 SSTable이고, 모양이 안 바뀌므로
split-refreeze가 수렴하지 못해 그 창이 **파킹**됩니다. 2026-08-02 운영 노드에서 실제로 관측된
패턴입니다(상세: [prod-tscs-settings.md](prod-tscs-settings.md)).

`TimeSeriesMemtable`은 이 라우팅 자체를 없앱니다. memtable은 가변 맵이므로 행 하나가 들어올 때마다
그 행의 창을 계산해 해당 샤드에 O(1)로 넣으면 그만이고, 파티션 크기와 무관하게 버퍼링이 필요
없습니다. 창 크기는 **테이블의 TSCS `window_size` 옵션에서 읽습니다** — 별도 설정을 두지
않습니다. 두 곳에 따로 두면 어긋나 flush 결과가 전략이 기대하는 창과 맞지 않게 되기 때문입니다.

## 2. 켜는 법 — 두 단계입니다, 이 부분에서 흔히 틀립니다

`memtable` 테이블 속성은 **클래스 이름이 아니라 설정 키를 가리키는 문자열**입니다. 그래서
`ALTER TABLE ... WITH memtable = 'TimeSeriesMemtable'`처럼 클래스 이름을 바로 쓰면 그 이름의
설정 키가 없다는 이유로 동작하지 않습니다(§2.3 참고). 두 단계가 모두 필요합니다.

### 2.1 1단계 — 모든 노드의 `cassandra.yaml`에 설정 키를 추가합니다

```yaml
memtable:
  configurations:
    timeseries:
      class_name: TimeSeriesMemtable
```

**클러스터의 모든 노드에** 이 항목이 있어야 합니다. 한 노드에만 넣으면 그 노드에서 flush할 때는
`TimeSeriesMemtable`을 쓰고 다른 노드는 폴백하는, 노드마다 다르게 동작하는 상태가 됩니다(§2.3).

`cassandra.yaml`은 기동 시 읽히므로 **이 변경은 노드 재시작이 필요합니다.** 이미 떠 있는 노드에
파일만 고쳐 넣어서는 반영되지 않습니다.

### 2.2 2단계 — 테이블별로 켭니다

1단계가 전체 노드에 끝난 뒤에만 의미가 있습니다.

```sql
ALTER TABLE ks.tbl WITH memtable = 'timeseries';
```

값 `'timeseries'`는 1단계에서 정한 설정 **키**이지, `TimeSeriesMemtable` 클래스 이름이 아닙니다.
키 이름은 자유롭게 정할 수 있습니다 — 위 예제와 다른 이름을 썼다면 여기도 그 이름을 씁니다.

### 2.3 설정 키가 없거나 틀리면 — 조용히 저하될 뿐, 깨지지 않습니다

`ALTER TABLE`이 가리키는 설정 키가 `cassandra.yaml`에 없으면(1단계를 빠뜨렸거나 노드마다
다르게 설정된 경우) `MemtableParams.getWithFallback`이 처리합니다: `ERROR` 로그를 한 번 남기고
**기본 memtable로 폴백**합니다. `ALTER TABLE` 자체는 성공하고 쓰기도 계속되지만, 그 노드에서는
`TimeSeriesMemtable`의 이득을 전혀 얻지 못합니다. 즉 실패 방식이 "동작 안 함"이 아니라
"조용히 저하됨"이라, 노드별로 1단계를 빠뜨리면 알아채기 어렵습니다 — §5의 확인 절차로 노드마다
직접 확인하십시오.

## 3. 지원되는 스키마, 지원되지 않는 스키마

판정 로직은 `TimeSeriesMemtable.unsupportedReason(TableMetadata)`에 있으며, 다음을 모두
만족해야 지원됩니다:

| 조건 | 이유 |
| --- | --- |
| 컴팩션이 `TimeSeriesCompactionStrategy` | 창 크기(`window_size`)를 여기서 읽습니다. 다른 전략에는 배정할 창이 없습니다 |
| 클러스터링 컬럼이 정확히 1개 | 창 배정 기준이 그 컬럼 하나입니다 |
| 그 클러스터링 컬럼이 `timestamp` 타입 | 창은 시간 폭이므로 시간 값이어야 배정할 수 있습니다 |
| `counter` 컬럼 없음 | counter는 삭제 후 재삽입이 불가능해서, 행을 창 사이로 옮길 수 있는 이 memtable의 flush 경로가 counter 셀에는 안전하지 않습니다 |
| 비frozen(멀티셀) 컬럼 없음 | 멀티셀 컬럼은 행 전체를 대표하는 단일 쓰기 타임스탬프가 없어, 창을 어느 기준으로 정할지가 성립하지 않습니다 |

**`frozen` 컬렉션은 지원됩니다** — 단일 셀이라 위 제약에 걸리지 않습니다. 운영 `tm_tag_point`의
`attribute frozen<map<text,text>>`가 이 모양입니다.

지원되지 않는 테이블에 `memtable = 'timeseries'`를 걸면 §2.3과 같은 방식으로 처리됩니다: 팩토리가
스키마를 보고 거부 사유를 판정한 뒤, **기본 memtable로 폴백하고 테이블당 한 번(시간당) 경고를
남깁니다.** `ALTER TABLE` 자체는 거부되지 않고, 쓰기도 정상적으로 계속됩니다.

**왜 던지지 않고 폴백하는가.** memtable은 쓰기 경로 위에 있습니다. 지원하지 않는 스키마를 만났을
때 예외를 던지면 그 테이블에 대한 **모든 쓰기**가 실패합니다 — 최적화 하나를 놓치는 대가로 그
테이블 전체를 장애로 만드는 셈입니다. 대신 폴백하면 성능 이득만 놓치고 데이터 경로는
그대로입니다. 계층형 저장의 `TieringPolicy`가 지원하지 않는 스키마를 다루는 방식과 같은
패턴입니다.

## 4. 무엇이 좋아지고, 무엇이 좋아지지 않는가

과장하기 쉬운 지점이라 명확히 구분합니다.

### 4.1 이미 되던 것 — 이 memtable이 추가하는 게 아닙니다

**창 경계 분할은 이미 동작합니다.** `TimeSeriesCompactionStrategy`가 flush writer로
`TimeWindowSplittingMultiWriter`를 설치하므로, `TimeSeriesMemtable` 없이도 메모테이블은 이미
창별 SSTable로 나뉘어 flush됩니다. 이 memtable을 켜도 "창 정렬된 SSTable이 나온다"는 결과 자체는
달라지지 않습니다 — 그건 이미 있던 능력입니다.

### 4.2 실제로 더해지는 것 세 가지

| 항목 | 이 memtable 이전 | 이후 |
| --- | --- | --- |
| 파티션 크기 상한 | `WindowRoutingIterator.maxBufferedBytesPerPartition`(64 MiB)을 넘으면 분할을 포기하고 파티션을 통째로 써서 창을 걸친 SSTable이 나오고, 그 창이 파킹됩니다 | 상한이 없습니다 — 행 단위로 O(1) 배정하므로 파티션 전체를 버퍼링할 필요가 없습니다 |
| flush 시 라우팅 비용 | 매 행을 창별로 분류하는 작업이 flush마다 일어납니다 | 라우팅 자체가 없습니다 — 쓰기 시점에 이미 창별로 나뉘어 있습니다 |
| flush 시 힙 스파이크 | 창 경계를 걸친 큰 파티션마다 버퍼링 비용이 듭니다(동시 writer 수만큼 곱해집니다) | 버퍼링이 없으므로 이 스파이크가 없습니다 |

가장 크게 얻는 것은 **파킹의 원인 제거**입니다 — 64 MiB를 넘는 파티션이 있는 테이블에서 창이
파킹되는 근본 원인이 사라집니다.

### 4.3 이 단계가 개선하지 않는 것 — 오해하기 쉬운 지점

**행당 메모리 점유와 컴팩션 총량은 이 단계로 거의 달라지지 않습니다.** 이 memtable은 저장
구조를 바꾸지 않습니다(기존 파티션 표현을 그대로 재사용해 창별로 나눌 뿐입니다). 행 단위 메모리
절감은 컬럼을 원시 배열(fast/slow path)로 저장하는 이후 단계의 몫이고, 컴팩션 총량 감소는 콜드
창을 flush 시점에 바로 청크로 인코딩하는 이후 단계의 몫입니다. 두 단계 모두 **아직 구현되어
있지 않습니다.** 이 memtable만 켠 상태에서 힙 사용량이나 컴팩션 바이트 총량의 개선을 기대하지
마십시오 — 얻는 것은 파킹 제거와 flush 경로의 라우팅·버퍼링 비용 제거입니다.

## 5. 확인 — 실제로 쓰이고 있는지 보는 법

**1) 스키마에 설정이 반영됐는지**

```sql
SELECT memtable FROM system_schema.tables
 WHERE keyspace_name = 'ks' AND table_name = 'tbl';
```

`timeseries`(또는 1단계에서 정한 설정 키)가 나오면 `ALTER TABLE`은 적용된 것입니다. 다만 이
결과만으로는 **실제로 그 memtable이 쓰이고 있다는 보장이 안 됩니다** — §2.3처럼 폴백된 경우에도
이 컬럼의 값은 그대로 `timeseries`로 남습니다(스키마에 저장된 의도이지, 노드가 실제로 무엇을
썼는지가 아닙니다). 그래서 아래 로그 확인이 함께 필요합니다.

**2) 폴백 경고가 없는지**

```bash
grep "cannot be used" system.log
```

`memtable = 'timeseries' is set on ks.tbl but cannot be used: <사유>. Falling back to the
default memtable ...` 형태의 줄이 보이면, 1단계(해당 노드의 `cassandra.yaml`)가 빠졌거나
§3의 스키마 조건을 만족하지 않는다는 뜻입니다. 노드마다 별도로 확인하십시오 — 1단계는
노드 단위 설정이라 일부 노드에만 빠져 있을 수 있습니다.

**두 확인이 모두 통과해야 그 테이블의 그 노드가 실제로 `TimeSeriesMemtable`을 쓰고 있는
것입니다.**
