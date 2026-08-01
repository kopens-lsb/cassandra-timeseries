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

# 시계열 컴팩션 (TSCS): `TimeSeriesCompactionStrategy`

SSTable을 **고정 시간 창**으로 분류해, 최근 창은 평소대로 컴팩션하고 닫힌 창은 **창당 1개로 동결**하며
보존 기간이 지난 창은 **컴팩션 없이 통째로 삭제**하는 컴팩션 전략입니다. 구현은
[`TimeSeriesCompactionStrategy`](../../src/java/org/apache/cassandra/db/compaction/TimeSeriesCompactionStrategy.java).

계층형 저장([tiered-storage.md](tiered-storage.md))과 **완전히 독립**입니다 — 이쪽은 컴팩션 전략,
저쪽은 테이블 확장입니다. 하나만 켜도 되고, TSCS를 먼저 켜는 편이 위험이 낮습니다
([프로덕션 체크리스트 §3.5](production-rollout.md)).

## 1. 설정

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d',
  'freeze_after': '2d',
  'retention': '3650d'
};
```

| 옵션 | 기본값 | 뜻 |
| --- | --- | --- |
| `window_size` | `1h` | 창 하나의 폭. SSTable은 **최대 타임스탬프**로 창에 배정됩니다 |
| `freeze_after` | `2h` | 창이 닫힌 뒤 이만큼 지나면 동결 대상. 지각 데이터가 들어올 여유 구간입니다 |
| `retention` | 없음 | 이보다 오래된 창은 통째로 삭제. **설정하지 않으면 아무것도 만료되지 않습니다** (§4) |
| `max_future_window` | `1d` | 이보다 먼 미래의 창은 모든 자동 경로에서 제외 (§6) |
| `timestamp_resolution` | `MICROSECONDS` | 셀 타임스탬프 단위. 기본값을 바꿀 일은 거의 없습니다 |

`window_size`는 **조회 패턴**에 맞추십시오. 대시보드가 하루 단위로 본다면 `1d`가 자연스럽고, 창이
지나치게 잘면 SSTable 수와 창 관리 비용만 늘어납니다. 10년 보존이면 `1h` 창은 87,600개가 됩니다.

## 2. 창의 일생

```
        [현재 창]          [닫힘, freeze_after 이내]      [동결됨]        [만료]
  ───────────────────┼──────────────────────────┼──────────────┼──────────────
   UCS에 위임              여전히 UCS               창당 1 SSTable    통째로 삭제
   (평소대로 컴팩션)        (지각 데이터 흡수)         이후 손대지 않음   (컴팩션 없음)
```

- **현재 창과 갓 닫힌 창**은 내부 `UnifiedCompactionStrategy`에 **위임**합니다. 쓰기가 활발한 구간은
  업스트림 UCS가 하던 그대로 동작합니다.
- **동결(freeze)**: 닫힌 지 `freeze_after`가 지난 창은 SSTable 1개로 합쳐집니다. 읽기 증폭이 창당
  1개로 떨어지고, 그 뒤로는 다시 컴팩션되지 않습니다.
- **만료**: `retention`보다 오래된 창은 `TimeSeriesCompactionTask`가 **파일째 삭제**합니다. 데이터를
  읽지도, 다시 쓰지도 않습니다 — 대용량 이력에서 TTL 기반 회수보다 훨씬 쌉니다.

## 3. 지각 데이터 격리

엣지 장비가 통신 두절 뒤 며칠치를 밀어 넣어도, 그 행들이 **현재 창을 오염시키지 않습니다.**

- **flush 시점**: 메모테이블이 여러 창에 걸치면 창 경계에서 **쪼개서** 씁니다
  ([`TimeWindowSplittingMultiWriter`](../../src/java/org/apache/cassandra/db/compaction/timeseries/TimeWindowSplittingMultiWriter.java)).
  결과적으로 모든 SSTable이 정확히 한 창에 속합니다.
- **스트리밍 시점**: bootstrap·repair·rebuild로 **받는 노드**에서도 같은 분할이 일어납니다. 이 경로는
  3노드 테스트로 검증돼 있습니다
  (`TimeSeriesCompactionDistributedTest.streamedSSTablesAreSplitOnWindowBoundaries`).
- **레거시**: 이전 전략에서 넘어와 여러 창에 걸치는 SSTable은 배경에서 창별로 분할 재기록됩니다
  (`SplitRefreezeCompactionTask`).

## 4. ⚠️ 만료는 `retention`이 담당합니다 — TTL이 아닙니다

가장 놓치기 쉬운 부분입니다.

동결은 실제 `CompactionController`로 돌기 때문에, **동결하는 그 순간에 이미 만료된** TTL 데이터는
`retention` 없이도 회수됩니다. **보장은 거기까지입니다.** 창이 SSTable 1개로 줄면 다시는 동결 후보가
되지 않으므로(후보 선정이 SSTable 2개 미만인 창을 건너뜁니다), **동결 이후에 만료되는 데이터는 이
전략이 회수하지 않습니다.**

즉 `window_size 1h` / `freeze_after 2h`에 TTL 30일이면, 창은 T+3시간에 동결되고 그 안의 데이터는
T+30일이 지나도 **디스크에 남습니다.**

**`retention`을 목표 보존 기간으로 설정하십시오.** 만료된 창을 통째로 삭제하므로 TTL 회수보다
효율적이기까지 합니다.

## 5. 파킹된 창 — 진척을 못 내는 창

동결과 분할이 서로를 되돌리며 무한 반복하는 것을 막기 위해, 전략은 **연속으로 모양이 바뀌지 않는
창을 파킹**하고 더 이상 후보로 뽑지 않습니다.

증상은 **그 시간대의 SSTable이 합쳐지지 않고 남는 것**입니다 — 그 범위 읽기의 증폭이 내려가지 않고
디스크도 회수되지 않습니다. 파킹된 창은 백로그와 `getEstimatedRemainingTasks()`에서도 빠지므로
**할 일이 없는 테이블과 겉모습이 똑같습니다.**

확인 경로는 **JMX 하나뿐입니다** (`nodetool` 서브커맨드는 없습니다). 테이블 MBean
`org.apache.cassandra.db:type=Tables,keyspace=<ks>,table=<table>`:

| 속성 | 내용 |
| --- | --- |
| `ParkedTimeSeriesWindows` | 파킹된 창 → 그 창이 물고 있는 SSTable 목록 |
| `FarFutureTimeSeriesSSTables` | `max_future_window` 밖이라 모든 자동 경로에서 제외된 SSTable |

**둘 다 비어 있는 것이 정상입니다.**

> 계층화는 이것과 무관하게 계속 돕니다 — 재인코더는 동결 이벤트가 아니라 자체 스케줄러로 동작합니다.

## 6. 먼 미래 타임스탬프

시계 오류나 `USING TIMESTAMP` 오용으로 먼 미래 타임스탬프가 들어오면, 창 맵이 끝없이 늘어날 수
있습니다. `max_future_window`(기본 1일) 밖의 창은 **모든 자동 경로에서 제외**됩니다 — UCS 위임에서도,
동결에서도, 만료에서도.

데이터는 **삭제되지 않고 읽힙니다.** 다만 그 SSTable들은 자동으로는 영원히 손대지 않으므로 쌓입니다.
`FarFutureTimeSeriesSSTables`가 비어 있지 않다면 원인(대개 잘못된 클라이언트)을 찾아 고치고,
`nodetool compact` 같은 수동 경로로 정리하십시오.

## 7. 알아둘 것

- **전략 변경은 전체 재컴팩션을 유발합니다.** 기존 대용량 테이블에서는 상당한 IO 이벤트이니 비피크
  시간대에, 노드 하나씩 진행하십시오.
- **되돌릴 수 있습니다.** `ALTER TABLE ... WITH compaction = {'class':'UnifiedCompactionStrategy', ...}`로
  데이터 손실 없이 원복되며, 다시 전체 재컴팩션이 일어납니다.
- **인스턴스는 자기 몫만 봅니다.** `CompactionStrategyManager`가 repair 상태·디스크별로 SSTable을
  나눠 주므로, 창 상태는 매 호출마다 파생되고 완전하다고 가정하지 않습니다. JBOD에서는 한 창이
  디스크마다 SSTable을 가질 수 있습니다.
- **분산 테스트 범위**: 스트리밍 분할, 노드별 동결 수렴과 무한루프 가드, 만료 통삭제, 지각 백필
  격리가 3노드 jvm-dtest로 덮여 있습니다(`TimeSeriesCompactionDistributedTest`). 실제 운영 부하에서는
  아직 돌려본 적이 없습니다.
