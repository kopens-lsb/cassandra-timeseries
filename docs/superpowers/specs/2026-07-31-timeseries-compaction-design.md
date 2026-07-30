# TimeSeriesCompactionStrategy (TSCS) — 설계

날짜: 2026-07-31 · 저장소: cassandra-timeseries (Apache Cassandra 6.0.0 포크)
상태: 사용자 승인된 브레인스토밍 결과 (접근법 A)

## 1. 목표

TWCS의 시간 정렬·통째 만료와 UCS의 정교한 창 내부 컴팩션을 결합한 시계열 전용 전략.
사용자가 선정한 4대 목표:

1. **보존 만료 회수 효율** — 보존기간 지난 창의 SSTable을 컴팩션 없이 통째 삭제.
2. **지각 데이터 격리** — store-and-forward 백필이 현재 창 컴팩션을 오염시키지 않고,
   소속 창에 국소적으로 편입된다.
3. **읽기 성능** — 닫힌 창은 동결(freeze) 컴팩션으로 **창당 SSTable 1개**로 수렴.
   파티션+시간범위 조회의 읽기 증폭 최소화.
4. **계층화 연동** — 창 동결 시 이벤트를 발행해 Gorilla 재인코더(계층화 서브프로젝트 2)의
   트리거가 된다. 결합은 이벤트 훅 수준(느슨한 결합) — 재인코더 부재/실패가 컴팩션을
   막지 않고, TSCS는 단독으로도 완결적 가치를 가진다.

비목표: 컴팩션이 직접 청크를 쓰는 것(관례 위반·복구 복잡성으로 기각), TWCS 개조(기각),
UCS 본체 수정(머지 부담으로 기각).

## 2. 아키텍처 (승인된 접근법 A)

**창 파티셔닝 외피 + 창 내부 UCS 위임.** TSCS는 SSTable을 `max timestamp` 기준 시간 창으로
분류하는 외피 전략이고, 창 내부의 컴팩션 선택은 UCS의 컨트롤러(스케일링 파라미터, 샤딩,
목표 크기)에 위임한다. TWCS가 창 내부를 STCS에 위임한 구조의 UCS 세대 버전.

```
시간축:  ... | W-3 (만료→통삭제) | W-2 (동결: 1 SSTable) | W-1 (동결 대기) | W0 현재 창 (UCS T4) |
지각:    창 밖 데이터는 flush/컴팩션 시 창 경계로 스플릿 → 소속 창의 '지각 버킷'으로 격리
이벤트:  W가 동결될 때마다 WindowFrozenListener.onWindowFrozen(ks, table, windowStart, sstable)
```

### CQL 표면

```sql
CREATE TABLE metrics (...) WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1h',          -- 창 폭 (고정 길이; 계층화 chunk_window와 정렬 권장)
  'freeze_after': '2h',         -- 창 상한 경과 후 이 시간이 지나면 동결 대상 (지각 유예)
  'scaling_parameters': 'T4',   -- 현재 창 내부 UCS 위임 파라미터 (UCS 문법 그대로)
  'target_sstable_size': '1GiB',-- UCS 위임
  'retention': '30d'            -- 선택: 창 상한이 now-retention 이전이면 통째 삭제
};
```

`retention` 미지정 시 통삭제는 TTL 만료(전 셀 만료 SSTable)에만 의존한다. TTL과 `retention`
병행 시 먼저 도래하는 쪽이 적용된다.

## 3. 창 상태 기계

각 창은 SSTable 집합에서 유도되는 상태를 가진다(별도 영속 메타데이터 없음 — 재시작 안전):

```
CURRENT   : windowEnd > now.  UCS 위임 컴팩션.
CLOSING   : windowEnd <= now < windowEnd + freeze_after.  UCS 위임 유지 (지각 유예).
FREEZING  : now >= windowEnd + freeze_after 이고 창 내 SSTable > 1 (또는 미동결 마크).
            → 동결 컴팩션 태스크: 창 내 전 SSTable을 단일 SSTable로 메이저 컴팩션.
FROZEN    : 창 내 SSTable == 1 이고 동결 컴팩션 산출물.  이벤트 발행 완료 상태.
EXPIRED   : windowEnd <= now - retention (또는 전 셀 TTL 만료).  → 통째 삭제.
```

- 상태는 매 컴팩션 라운드에 SSTable 메타데이터(min/max timestamp)로 재계산 — TWCS와 동일한
  무상태 원칙. FROZEN 판정은 "창 경계 안에 완전히 포함된 단일 SSTable" 사실로 판단하며,
  동결 이벤트의 중복 발행은 리스너가 멱등하게 처리한다(재인코더는 원래 멱등 설계).
- 동결 컴팩션은 노드당 동시 1개(설정 가능)로 스로틀 — 대량 백로그(부트스트랩 직후 등)가
  IO를 삼키지 않게 한다. FREEZING 대상 선택은 오래된 창 우선.

## 4. 지각 데이터 격리

- **flush 스플릿**: memtable flush 산출 SSTable이 창 경계를 걸치면 창 경계에서 분할해 쓴다
  (Cassandra의 flush writer는 분할 지점 지정을 지원 — 구현 시 확인, 불가하면 flush 후
  1회 스플릿 컴팩션으로 대체). 결과: **모든 SSTable은 정확히 한 창에 속한다.** 이 불변식이
  통째 삭제와 창당 1 SSTable을 가능하게 하는 토대다.
- FROZEN 창에 지각 SSTable이 도착하면: 그 창은 FREEZING으로 되돌아가 [동결 SSTable + 지각
  SSTable] → 새 단일 SSTable로 국소 재동결하고 이벤트를 재발행한다(재인코더가 청크를
  갱신). 현재 창 컴팩션과 완전히 독립적인 태스크다.
- 리페어/스트리밍으로 유입되는 SSTable도 같은 규칙(창 분류 → 해당 창 재동결)을 탄다.
  스트리밍 SSTable이 창을 걸치면 수신 후 스플릿한다.

## 5. 이벤트 훅 인터페이스

```java
package org.apache.cassandra.db.compaction.timeseries;

public interface WindowFrozenListener
{
    /** 창이 단일 SSTable로 동결(또는 재동결)될 때마다 호출. 멱등해야 한다. */
    void onWindowFrozen(TableMetadata table, long windowStartMillis, SSTableReader frozen);
}
```

- TSCS는 리스너 레지스트리(정적 등록)를 가진다. 리스너 예외는 로깅만 하고 컴팩션 결과에
  영향을 주지 않는다(격리 원칙). v1 기본 리스너 없음 — 서브프로젝트 2의 재인코더가 첫 소비자.
- 이벤트 유실 대비: 리스너는 폴백으로 "FROZEN인데 청크 없는 창" 스캔을 자체 수행할 수 있어야
  한다(재인코더의 워터마크 스캔이 그 역할). 이벤트는 지연 단축 최적화이지 유일한 트리거가 아니다.

## 6. 만료 회수

- `retention` 지정 시: 창 상한 < now − retention 인 창의 SSTable 전부를 컴팩션 없이 삭제
  (TWCS의 만료 창 삭제와 동일 메커니즘). §4의 "SSTable은 정확히 한 창" 불변식 덕에 안전.
- 주의: 계층화(§계층형 스펙)와 병용 시 `retention`은 **핫 로우의 보존**이다. 재인코더가 이미
  청크화한 창은 레인지 딜리트로 비워지므로 보통 만료 전에 사라진다. retention은 재인코더
  장애 시의 안전망 겸 계층화 미사용 테이블의 1차 보존 장치다.

## 7. UCS 위임 세부

- 창 분류 후, CURRENT/CLOSING 창의 SSTable 집합만 UCS 컨트롤러에 공급해 컴팩션 후보를 받는다.
  UCS의 샤딩/스케일링 로직은 수정 없이 재사용(위임 객체로 UCS 인스턴스 보유 — TWCS가 창 내부
  STCS 인스턴스를 쓰는 패턴).
- FREEZING 동결 컴팩션은 UCS를 거치지 않는 전용 메이저 태스크.
- 업스트림 머지 리스크: UCS Controller 시그니처 변경 시 위임부 1곳만 영향 — CLAUDE.md의
  재발 충돌 지점 목록에 추가한다.

## 8. 오류·엣지 처리

- 동결 컴팩션 실패: 창은 FREEZING으로 남고 다음 라운드 재시도. 부분 산출물은 표준 컴팩션
  트랜잭션 롤백에 맡긴다.
- 시계 역행/이상 타임스탬프(멀리 미래 데이터): 미래 창은 CURRENT 취급로 방치하지 않고
  "far-future 가드"(예: now + 1일 초과 창은 경고 + 즉시 동결 대상 제외)로 로깅 — 오입력이
  창을 무한 생성하지 않게 한다.
- `window_size` 변경(ALTER): 새 창 폭은 새 데이터부터 적용. 기존 동결 SSTable은 재작성하지
  않는다(경계 불일치 창은 FROZEN 판정에서 "구 경계 정합"도 허용).
- 카운터 테이블·비시계열 테이블에 대한 오용: 클러스터링 첫 컬럼이 timestamp 계열이 아니면
  CREATE 시 경고(차단은 하지 않음 — max timestamp 기반이라 동작 자체는 가능).

## 9. 테스트 전략

- 단위: 창 분류(경계 정확성, 걸침 스플릿), 상태 기계 전이, 만료 선택, 동결 후보 선택,
  far-future 가드 — 기존 `CompactionsTest`/`TimeWindowCompactionStrategyTest` 패턴.
- 수명주기: 쓰기→flush→창 닫힘→동결→지각 유입→재동결→만료 통삭제 전 과정을 시뮬레이션
  시간으로 검증. 이벤트 발행 횟수·멱등성 확인(목 리스너).
- jvm-dtest: 3노드에서 리페어 후 유입 SSTable의 창 편입, 스트리밍(부트스트랩) 경로.
- 도커 통합: logs/metrics 테이블에 TSCS 적용 후 기존 통합 테스트 전체 + 만료 통삭제 확인.
- 스케일: 기존 1억 건 하네스에서 UCS 대비 — 만료 회수 시간, 닫힌 창 조회 지연(SSTable 수),
  쓰기 처리량. 결과는 `doc/timeseries/tscs-benchmark.md`.

## 10. 서브프로젝트 분해

| 순서 | 내용 | 독립 가치 |
| --- | --- | --- |
| T1 | 창 분류 + CURRENT 위임 + 만료 통삭제 (동결 없음) | "시간을 아는 UCS" — 만료 효율 즉시 확보 |
| T2 | 동결 상태 기계 + 동결 컴팩션 + 이벤트 훅 | 창당 1 SSTable 읽기 최적 + 계층화 트리거 준비 |
| T3 | 지각 격리(flush/스트리밍 스플릿, 국소 재동결) | 백필 현실 대응 완성 |

각 단계는 자체 plan→구현 사이클. T1만으로도 배포 가치가 있다.
