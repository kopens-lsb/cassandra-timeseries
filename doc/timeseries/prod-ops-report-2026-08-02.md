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

# 운영 투입 보고서 — 2026-08-02, 단일 노드

cassandra-timeseries 6.0.0을 실 운영 노드(448 GiB, `pp` 키스페이스, 유입 6~24k rows/s)에
투입하고 12시간 이상 연속 관찰한 기록입니다. 배포 5회, 실사고 2건(둘 다 당일 수정·재배포),
데이터 손실 0건.

## 1. 최종 상태 (14:03 기준)

| 항목 | 값 |
| --- | --- |
| 노드 | UN, Native/Gossip active |
| 예외 · 로그 ERROR · 드롭 메시지 | **0 · 0 · 0** |
| 쓰기 지연 (tm_tag_point) | **0.085 ~ 0.095 ms** |
| 데이터 신선도 (최신 행 나이) | 0~1초 (일시 피크 ≤147초, 자체 회복) |
| 파킹된 창 · far-future | **0 · 0** (JMX 확인) |
| 계층화 | `tm_tag_point` 가동 — 청크 81,128행, 매시 주기 정상 발동 |
| 캐너리 (`tstest.tier_probe`) | 전 스택 반복 검증 중 — 청크 245행, 지속 성장 |
| 힙 | 2.1~15.7 GB 진동 (GenZGC 소프트맥스 게으른 사용 — 15.7 피크 후 3.0으로 회수 확인) |

## 2. 오늘 투입된 것

| 기능 | 상태 | 실측 |
| --- | --- | --- |
| 시계열 CQL 함수 21종 + gap-fill | 가동 | 25/25 실데이터 검증, 회귀 자기상관 slope=1·r²=1 |
| TSCS (`tm_tag_point` 등 75개) | 가동 | 수명주기 분 단위 압축 검증: 창 분할·TTL·retention 통삭제가 공식과 초 단위 일치 |
| **TimeSeriesMemtable** (창 샤딩 + 컬럼 저장) | `tm_tag_point` 가동 | 행당 힙 **511 → 92 B (5.53×)**, 24k rows/s 하 무사고 |
| **계층화** (hot 3h / chunk 1h) | `tm_tag_point` 가동 | 첫 주기 2,438청크, 정합성 7,104 = 7,104 |
| **콜드 창 청크 직접 flush** (4단계) | 가동 | 백필 3,000행 → flush 즉시 청크 25행, 재인코더 미경유 |
| 읽기 경로 최적화 (증분 스냅샷·샤드 가지치기·지연 청크 디코드) | 가동 | 지연 디코드: DESC LIMIT가 창 10개 중 1~2개만 디코드 (카운터 증명) |
| SAI n-gram LIKE | 검증 후 시험 인덱스 제거 | 인덱스 결과 = 전수 스캔, min-gram 가드 동작 |

## 3. 사고 2건 — 전말과 재발 방지

### 3.1 콜드 flush AIOOBE (09:38, 피해 없음)

static 전용 행만 담긴 SSTable의 커버 범위는 빈 클러스터링 bound인데, 인코더의 교차 가드가
그 첫 성분을 읽다 `ArrayIndexOutOfBoundsException`. **설계된 안전망이 동작해** 해당 flush는
행으로 폴백됐고 데이터 손실은 없었습니다 — 대가는 그 테이블에서 4단계 최적화가 조용히 꺼진 것.

수정(`032aa87ad7`): 빈 커버 범위는 "아무 창과도 안 겹침"으로 건너뜀. 첫 수정안(무제한으로
해석)은 가드를 뒤집어 청크화를 영구 거부하는 반대 오류였는데, **회귀 테스트가 출하 전에
잡았습니다.** 뮤테이션 검증 포함 재배포 완료.

### 3.2 페이징 읽기 장애 (11:36~11:47, 읽기 에러 11분)

읽기 경로 최적화(`mayContainRowsIn` 샤드 가지치기)가 **93건 테스트 전부 통과한 채** 배포됐지만,
프로덕션에서 페이징 읽기가 `UnsupportedOperationException`으로 전멸. 3분 만에 롤백.

근본 원인(재현으로 확정): 테스트는 인프로세스 `execute()` 경로였는데 프로덕션 읽기는
**네이티브 프로토콜 + 페이징**이고, 이 노드는 `memtable_allocation_type: offheap_objects`라
memtable 클러스터링이 `NativeClustering` — `Slice.make`가 요구하는 팩토리를 지원하지 않아
UOE. 페이징은 2페이지부터 가지치기 분기에 진입시키는 방아쇠였을 뿐입니다.

수정(`9c5d95d670`): bound를 만들지 않고 comparator로 직접 비교 + **fail-open**(판단 불가면
가지치기 포기, 절대 예외 없음). offheap 재현 테스트(수정 전 3/3 에러 → 후 초록)와 페이징
E2E가 추가됐고, **이후 memtable 읽기 경로 변경엔 이 테스트들이 게이트**입니다. 재배포 후
90분째 무사고.

교훈: `execute()` 테스트는 프로덕션 읽기 경로를 대변하지 못한다. 읽기 경로의 최적화는
실패 방식이 "느려짐"이 아니라 "장애"가 되지 않도록 fail-open이 기본이어야 한다.

### 부수 발견·수정

- `start.sh`의 OOM 보호가 매 기동마다 조용히 실패 (`pgrep`이 래퍼 PID를 집음 + 검증 없이 성공
  보고). 수정본을 만들었으나 **07:58 플랫폼(ppctl) 재동기화가 되돌림** — 플랫폼 소스 템플릿에
  넣어야 하며, 그 전까지 기동 후 `oom_score_adj` 확인·수동 -1000이 운영 절차입니다.
- 라우팅 버퍼 경고의 NoSpamLogger 키가 상수라 테이블들이 분당 1건을 나눠 쓰던 관측성 결함
  수정 (키에 테이블명 포함).
- counter 쓰기 경로 조사: 이미 최적 (캐시 1 GiB에 7 MiB 사용·적중 88.7%, Pending 0) —
  **손대지 않음**이 결론.

## 4. 판단이 뒤집힌 것들 (기록 가치)

- **파킹 원인**: "타임스탬프 오염 + 파티션 크기" 두 갈래로 진단했다가, 파킹 조건을 코드로
  추적해 **라우팅 버퍼 초과 단일 원인**으로 정정. 오염은 창 범위를 56년으로 보이게 한
  교란 변수였고 별도 문제로 처리(TRUNCATE, 15.2 GB 회수).
- **`window_size` 상향 권고 철회**: 읽기당 SSTable p95 1.00개 실측 앞에서 근거 상실.
  75개 테이블 전부 변경 없음이 결론.
- **"지연 청크 디코드 미구현"**: 벤치마크 문서가 낡은 것 — 이미 전날 구현돼 있었음.
  문서 정정 + 카운터 증명 테스트로 고정.
- **읽기 지연 최적화 효과**: 판정 보류. 배포 시점의 디스크 상태가 아침 기준선과 달라
  (읽기당 SSTable 1개 → 7~10개, 재인코딩 톰스톤·컴팩션 백로그) memtable 개선분을 분리
  측정할 수 없음. 컴팩션 안정 후 재측정 예정 — 기대치에 숫자를 맞추지 않습니다.

## 5. 운영자 절차 (이 노드에서 실제로 쓴 것)

```bash
# 배포: 검증 → drain → stop.sh → lib-backup 으로 백업 → 교체 → start.sh → oom 확인
python3 -c "import zipfile;z=zipfile.ZipFile('x.jar');print(len(z.namelist()),z.testzip())"
nodetool drain && bash bin/stop.sh
mv lib/apache-cassandra-timeseries-6.0.0.jar lib-backup/...jar.$(date +%Y%m%d-%H%M%S)
\cp -f /tmp/new.jar lib/apache-cassandra-timeseries-6.0.0.jar && bash bin/start.sh
cat /proc/$(pgrep -f '[o]rg.apache.cassandra.service.CassandraDaemon' | head -1)/oom_score_adj  # -1000

# 롤백 (실측 3분): lib-backup 의 최신 타임스탬프 jar 로 위 절차 역방향
```

감시 항목 (5분 주기 스크립트, JMX 포함):
노드 상태·예외·ERROR·드롭 / 쓰기 지연·신선도 / `cf_fail`(콜드 flush 실패)·`fallback`(memtable
폴백)·청크 증가 / JMX `ParkedTimeSeriesWindows`·`FarFutureTimeSeriesSSTables`(비어야 정상).

## 6. 남은 항목

- 읽기 지연 재측정 (컴팩션 안정 후) — §4
- `start.sh` OOM 수정을 플랫폼 소스에 반영 (플랫폼 쪽 작업)
- `tm_asset_oee` 계열 5개: `memtable='timeseries'`가 걸려 있으나 UCS라 폴백 중 — TSCS 전환
  시 자동 적용
- 3노드 소크(1k rows/s × 3일), CA1 롤업, GitHub kopens-oss 퍼블리시
