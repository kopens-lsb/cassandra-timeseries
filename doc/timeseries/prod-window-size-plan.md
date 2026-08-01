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

# 프로덕션 `window_size` 조정 계획 (pp 키스페이스, 2026-08-02 실측 기준)

대상 노드 192.168.0.41, TSCS 적용 테이블 **75개**. 아래 표는 실측에 근거한 것이며,
바꿀 필요가 없는 테이블은 "변경 없음"으로 명시했습니다.

## 0. 먼저 알아야 할 것 — `window_size`가 해결하지 못하는 문제가 섞여 있습니다

파킹된 창의 원인이 **두 종류**입니다. 섞어서 다루면 안 됩니다.

| 원인 | 대상 | `window_size`로 해결? |
| --- | --- | --- |
| **쓰기 타임스탬프 오염** (`USING TIMESTAMP`에 µs 대신 ms) | `tm_tag_point`, `tm_tag_point_archive`, `tm_tag_point_snapshot` | **불가능** |
| TSCS 전환 전 레거시 SSTable (21~35일 걸침) | `tm_asset_*` 일부 | 가능하나 실익 적음 |

### 타임스탬프 오염 (해결 불가 쪽)

`sstablemetadata` 실측:

```
Minimum timestamp: 01/22/1970 00:39:21 (1784361462000)
Maximum timestamp: 07/30/2026 16:27:07 (1785396427002000)
```

`1784361462000`은 **밀리초로 읽으면 2026-07-16**입니다. 과거에 `USING TIMESTAMP`로 ms 값을
넣어 카산드라가 µs로 해석한 결과이고, 그 셀들의 쓰기 타임스탬프가 1000배 작아졌습니다.

TSCS는 **쓰기 타임스탬프로 창을 정합니다.** 따라서 그 SSTable은 1970년부터 2026년까지
**약 56년, 창 20,000개**에 걸칩니다. `window_size`를 1년으로 키워도 56개 창입니다 —
어떤 값으로도 한 창에 들어가지 않으므로 split-refreeze가 수렴할 수 없고, 무진척 가드가 파킹합니다.

오염 범위 (Data.db 기준):

| 테이블 | 오염 SSTable | 오염 용량 |
| --- | --- | --- |
| `tm_tag_point` | **7 / 20** | 4.3 GB / 5.8 GB |
| `tm_tag_point_archive` | 1 / 22 | 0.7 GB / 6.2 GB |
| `tm_tag_point_snapshot` | 1 / 7 | 0.2 GB / 0.9 GB |
| `tm_asset_*` 전체 | **0** | — |

**영향**: 데이터 자체는 정상적으로 읽힙니다. 다만 그 셀들은 쓰기 타임스탬프가 비정상적으로
작아 **이후의 어떤 정상 쓰기에도 무조건 집니다**(같은 셀을 다시 쓰면 조용히 덮어써집니다).
파킹은 이 문제의 증상일 뿐이고, 근본 해결은 오염 데이터를 걷어내는 것입니다(재적재 또는 삭제).

## 1. 변경 대상 — 3개 테이블

`window_size`를 바꾸는 이유는 **파킹 해소가 아닙니다**(위 §0 참고). 보존 기간 대비 창 개수가
지나치게 많아서입니다. 창 하나당 동결 SSTable 하나가 남으므로, 창 개수가 그대로 파일 수가 됩니다.

| 테이블 | 현재 `window_size` | 현재 창 개수 | **권장 `window_size`** | 변경 후 창 개수 | 현재 `retention` | **변경 후 `retention`** | TTL |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tm_tag_point` | `1d` | **3,651** | **`7d`** | 522 | `3651d` | **`3657d`** | 3650d (10년) |
| `tm_tag_point_archive` | `1d` | 366 | **`7d`** | 53 | `366d` | **`372d`** | 365d (1년) |
| `tm_tag_point_snapshot` | `1d` | 94 | **`7d`** | 14 | `94d` | **`100d`** | 93d |

### ⚠️ `retention`은 반드시 같이 바꿔야 합니다

`retention`은 **`TTL + window_size`** 여야 합니다. 만료 판정이
`windowStart <= now - retention - window_size` 이므로, `window_size`만 키우고 `retention`을
그대로 두면 **TTL이 지난 데이터가 창째로 삭제되지 않고 남습니다.**

현재 값들은 모두 이 규칙을 지키고 있습니다(예: `tm_asset_alarm_timeline` TTL 30d + window 7d
= retention 37d). 바꿀 때도 지켜야 합니다.

### 적용 CQL

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy',
  'window_size': '7d', 'freeze_after': '2d', 'retention': '3657d',
  'min_threshold': '4', 'max_threshold': '32',
  'scaling_parameters': 'T8', 'target_sstable_size': '512MiB'
};

ALTER TABLE pp.tm_tag_point_archive WITH compaction = {
  'class': 'org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy',
  'window_size': '7d', 'freeze_after': '2d', 'retention': '372d',
  'min_threshold': '4', 'max_threshold': '32',
  'scaling_parameters': 'T8', 'target_sstable_size': '512MiB'
};

ALTER TABLE pp.tm_tag_point_snapshot WITH compaction = {
  'class': 'org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy',
  'window_size': '7d', 'freeze_after': '2d', 'retention': '100d',
  'min_threshold': '4', 'max_threshold': '32',
  'scaling_parameters': 'T8', 'target_sstable_size': '512MiB'
};
```

**컴팩션 전략 옵션 변경은 해당 테이블 전체 재컴팩션을 유발합니다.** `tm_tag_point`는 7.5 GB이고,
현재 노드 CPU가 포화 상태(load 45/48코어)이므로 **부하가 낮은 시간대에, 한 노드씩** 적용하십시오.

## 2. 변경하지 않는 테이블 — 72개

| 그룹 | 개수 | 현재 설정 | 판단 |
| --- | --- | --- | --- |
| `tm_asset_*` 계열 | 68 | `window_size 7d`, `freeze_after 14d` | **변경 없음.** 재기동 이후 대부분 파킹이 재발하지 않았고, 데이터도 0.2 GB 수준이라 실익이 없습니다 |
| `tm_asset_data`, `tm_asset_data_based_timestamp` | 2 | `1d` / `2d` / retention `11d` | **변경 없음.** TTL 10d에 창 11개 — 적정합니다 |
| `tm_blob`, `tm_blob_object` | 2 | `1d` / `2d` / retention `94d`,`32d` | **변경 없음.** TTL과 정합합니다 |

`tm_asset_*` 68개 중 64개는 `TTL = 0`이라 `retention`이 없는 것이 **정상입니다** —
만료시킬 대상이 없습니다. 나머지 4개(`tm_asset_alarm_timeline*`)는 TTL과 retention이
`TTL + window_size` 규칙을 이미 만족합니다.

## 3. 순서

1. **오염 데이터 처리 방침을 먼저 정하십시오** (§0). 이걸 정하지 않으면
   `tm_tag_point` 계열의 파킹은 `window_size`를 어떻게 바꾸든 남습니다.
   - 데이터를 걷어내기로 했다면 → 그 뒤에 `window_size`를 바꾸는 것이 순서상 낫습니다
     (재컴팩션을 한 번만 하면 됩니다).
2. 부하가 낮은 시간대에 §1의 `ALTER TABLE` 3건 적용.
3. 적용 후 확인:
   - `nodetool compactionstats` — 재컴팩션이 진행되고 백로그가 다시 줄어드는지
   - `system.log`에서 `Parking window`, `window-routing buffer` 발생 테이블
   - JMX `org.apache.cassandra.db:type=Tables,keyspace=pp,table=<t>` 의
     `ParkedTimeSeriesWindows` — 비어 있는 것이 정상

## 4. 참고 — 파킹된 창을 그대로 둘 때의 실제 비용

지금 실측으로는 **체감 피해가 없습니다.**

- 읽기당 SSTable 수: p50 1.00, p95 **1.00**, p99 2.00 (`nodetool tablehistograms pp.tm_tag_point`)
- 읽기 지연: p50 642 µs, p99 2.3 ms
- 파킹된 창도 `retention` 만료 대상에서는 **빠지지 않습니다** — 때가 되면 통째로 삭제됩니다
  (`isParked()`는 동결·분할 후보 선정에서만 참조되고 만료 경로에는 없습니다)

즉 이 조정은 급한 장애 대응이 아니라 **정리 작업**입니다.
