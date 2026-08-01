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

# 프로덕션 TSCS `window_size` 점검 결과 (pp 키스페이스, 2026-08-02 실측)

대상 노드 192.168.0.41, TSCS 적용 테이블 **75개**.

**결론부터: 75개 전부 변경하지 않는다.** 파킹된 창은 `window_size`가 아니라 쓰기
타임스탬프 오염 때문이었고, 오염 데이터를 걷어내 해소했다. 이 문서는 그 판단 과정과
근거를 남긴 것이다 — 나중에 같은 증상을 보고 `window_size`부터 손대지 않도록.

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

## 1. 결론 — `window_size`는 바꾸지 않는다

처음에는 `tm_tag_point` 계열을 `1d` → `7d`로 올리자고 적었다. 10년 보존에 창 3,651개는
많아 보였기 때문이다. **실측이 그 근거를 지지하지 않아 철회한다.**

| 근거 | 값 |
| --- | --- |
| 읽기당 SSTable (`nodetool tablehistograms pp.tm_tag_point`) | p50 **1.00**, p95 **1.00**, p99 2.00 |
| 읽기 지연 | p50 642 µs, p99 2.3 ms |

창이 3,651개여도 읽기가 전혀 나빠지지 않는다. 오히려 `1d`가 낫다:

- **만료 granularity가 곱다** — 하루 단위로 회수된다
- **동결 SSTable이 작다** — 하루치 ≈ 120 MB. `7d`면 ≈ 850 MB가 되어 스트리밍·repair·컴팩션
  중 디스크 여유 요구가 함께 커진다

`window_size`를 바꾸면 해당 테이블 전체가 재컴팩션된다. 얻는 것이 없는 재컴팩션은 하지 않는다.

**`tm_asset_*` 68개는 이미 `7d`이고, `tm_asset_data*`·`tm_blob*`도 TTL과 정합한다.
즉 75개 테이블 전부 변경 없음이다.**

### 파킹은 어떻게 해소했는가

`window_size`가 아니라 **오염 데이터 제거**로 해소했다(2026-08-02 04:12).

```sql
TRUNCATE pp.tm_tag_point;
TRUNCATE pp.tm_tag_point_archive;
TRUNCATE pp.tm_tag_point_snapshot;
```

결과: Load 449.08 → 434.61 GiB, 이후 `Parking window` **0건**,
`window-routing buffer` 오버플로 **0건**. 현재 유입되는 쓰기의 타임스탬프는 정상(16자리 µs)이라
재발하지 않는다.

## 2. 변경하지 않는 테이블 — 72개

| 그룹 | 개수 | 현재 설정 | 판단 |
| --- | --- | --- | --- |
| `tm_asset_*` 계열 | 68 | `window_size 7d`, `freeze_after 14d` | **변경 없음.** 재기동 이후 대부분 파킹이 재발하지 않았고, 데이터도 0.2 GB 수준이라 실익이 없습니다 |
| `tm_asset_data`, `tm_asset_data_based_timestamp` | 2 | `1d` / `2d` / retention `11d` | **변경 없음.** TTL 10d에 창 11개 — 적정합니다 |
| `tm_blob`, `tm_blob_object` | 2 | `1d` / `2d` / retention `94d`,`32d` | **변경 없음.** TTL과 정합합니다 |

`tm_asset_*` 68개 중 64개는 `TTL = 0`이라 `retention`이 없는 것이 **정상입니다** —
만료시킬 대상이 없습니다. 나머지 4개(`tm_asset_alarm_timeline*`)는 TTL과 retention이
`TTL + window_size` 규칙을 이미 만족합니다.

## 3. 같은 증상을 다시 만났을 때

1. **`window_size`부터 의심하지 말 것.** 먼저 파킹 메시지의 창 범위를 본다:

   ```
   grep "Parking window" system.log | grep -oE '\[[0-9]+\.\.[0-9]+\]'
   ```

   범위가 **수십 년**이면 타임스탬프 오염이다(§0). 며칠~몇 주면 전략 전환 전 레거시
   SSTable이고, 이 경우에만 `window_size`가 선택지가 된다.

2. 오염 여부는 `sstablemetadata`의 최소 타임스탬프로 확정한다. 16자리(µs)가 정상이고,
   13자리면 ms를 µs로 해석한 것이다.

3. `window_size`를 바꾸기로 했다면 **`retention`을 반드시 같이 바꾼다** —
   `retention = TTL + window_size`. 만료 판정이
   `windowStart <= now - retention - window_size`이므로, `window_size`만 키우면
   TTL이 지난 데이터가 창째로 삭제되지 않고 남는다.

4. 어느 쪽이든 컴팩션 전략 옵션 변경은 해당 테이블 **전체 재컴팩션**을 유발한다.
   부하가 낮은 시간대에, 노드 하나씩.

## 4. 참고 — 파킹된 창을 그대로 둘 때의 실제 비용

지금 실측으로는 **체감 피해가 없습니다.**

- 읽기당 SSTable 수: p50 1.00, p95 **1.00**, p99 2.00 (`nodetool tablehistograms pp.tm_tag_point`)
- 읽기 지연: p50 642 µs, p99 2.3 ms
- 파킹된 창도 `retention` 만료 대상에서는 **빠지지 않습니다** — 때가 되면 통째로 삭제됩니다
  (`isParked()`는 동결·분할 후보 선정에서만 참조되고 만료 경로에는 없습니다)

즉 이 조정은 급한 장애 대응이 아니라 **정리 작업**입니다.
