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

# 운영 투입 보고서 — 2026-08-02 (대체됨)

이 문서는 2026-08-02 단일 노드(41) 첫 투입의 시점 기록이었고, 이후 상태가 바뀌어 **더 이상
현재를 기술하지 않습니다.** 특히 당시 가동했던 계층화와 그 청크는 청크 포맷 **v4** 전환
(2026-08-04 배포) 때 폐기됐습니다 — 현재 계층화는 운영 테이블에 켜져 있지 않습니다.

당시 사고·발견에서 나온 지속 가치가 있는 내용은 전부 현행 문서로 옮겨져 있습니다:

| 내용 | 현행 문서 |
| --- | --- |
| 노드 41 배포·롤백 절차, jar 파일명, HA 워치독, `start.sh` OOM 보호, 상시 감시 항목 | [production-rollout.md §0.5–0.6](production-rollout.md) |
| TSCS 운영 설정(75개 테이블), 파킹 진단, 운영 실측 | [prod-tscs-settings.md](prod-tscs-settings.md) |
| TSCS 수명주기 검증(창 분할·TTL·retention 통삭제) | [timeseries-compaction.md §7](timeseries-compaction.md) |
| memtable 폴백 확인 절차, 콜드 flush 폴백 안전망, 읽기 경로 게이트(오프힙·페이징 E2E) | [timeseries-memtable.md](timeseries-memtable.md) |
| 계층화 투입 체크리스트와 검증 배터리 | [production-rollout.md](production-rollout.md) |

당시 사고 2건의 교훈 요약(둘 다 당일 수정, 데이터 손실 0): ① `execute()` 인프로세스 테스트는
네이티브 프로토콜 + 페이징이라는 실제 운영 읽기 경로를 대변하지 못한다 — memtable 읽기 변경은
오프힙 재현 + 페이징 E2E 테스트가 게이트다. ② 읽기 경로 최적화의 실패 방식은 "느려짐"이어야지
"장애"여서는 안 된다 — 판단 불가면 최적화를 포기하는 fail-open이 기본이다.
