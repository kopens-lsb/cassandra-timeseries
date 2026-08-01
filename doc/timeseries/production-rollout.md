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

# 계층형 저장 프로덕션 투입 체크리스트

계층화를 실제 운영 테이블에 처음 켜기 전에 확인할 것들입니다. **계층화는 아직 어디에도 적용된 적이
없습니다** — 이 문서는 첫 투입을 위한 것입니다.

---

## 0. 되돌릴 수 없는 지점 두 개

투입 전에 이 둘을 이해하고 시작하십시오.

### 0.1 구버전 빌드로 켜면 노드가 죽습니다

`ChunkTables.ensureChunkTable`은 초기 구현에서 청크 테이블을 **프로그래매틱 스키마 변경**으로
만들었습니다. TCM은 스키마 변경을 **CQL 텍스트로** 직렬화해 되읽는데, 그 경로는 `cql()` 기본값인
문자열 `"null"`을 기록했습니다. 결과:

- 피어가 그 로그 엔트리를 파싱하지 못해 **클러스터 전체의 스키마 전파가 그 지점에서 멈춥니다**
- 그 엔트리를 쓴 **노드 자신도 재시작 시 메타데이터 로그 재생에 실패해 기동하지 못합니다**

**고친 빌드로도 이미 쓰인 엔트리는 복구되지 않습니다.** 메타데이터 로그는 append-only라 해당 노드는
데이터 디렉토리를 비우고 재구축해야 합니다.

> **따라서: 계층화를 켤 노드는 반드시 이 수정이 포함된 빌드여야 합니다.** 배포 전에
> `nodetool version`과 jar 빌드 시점을 확인하십시오. 확신이 없으면 **켜지 마십시오** — 확인 비용보다
> 복구 비용이 비교할 수 없이 큽니다.

### 0.2 계층화는 일방통행입니다

재인코더는 청크를 쓴 뒤 **원본 행을 삭제합니다.** 그 시점부터 콜드 데이터의 유일한 사본은
`<테이블>__chunks`입니다.

- 청크를 읽을 줄 모르는 빌드(업스트림 카산드라 포함)로 롤백하면 **그 데이터는 읽히지 않습니다**
- 청크 테이블을 `DROP`하면 그 데이터는 **영구히 사라집니다** (원본 행은 이미 없습니다)

투입 전에 백업 정책이 `<테이블>__chunks`를 함께 포함하는지 확인하십시오.

---

## 0.5 기존 6.0.0에 올리기 — jar 하나만 교체하면 됩니다

업스트림(`cassandra-6.0`)과 실제로 비교한 결과입니다:

| 항목 | 포크가 바꿨나 | 교체 필요 |
| --- | --- | --- |
| `lib/apache-cassandra-6.0.0.jar` | **예** — 포크 코드 전부가 여기 들어 있습니다 | **예** |
| `bin/` (`nodetool`, `cassandra`, `cqlsh` …) | 아니오 | 아니오 |
| `lib/` 의 다른 jar | 아니오 — Lucene도 업스트림 SAI가 이미 씁니다 | 아니오 |
| `conf/cassandra.yaml` | 주석 처리된 가이드레일 설명 7줄뿐 | 아니오 |

`nodetool`은 셸 스크립트이고 `retier`/`tieringstatus` 클래스는 메인 jar 안에 있습니다
(`org/apache/cassandra/tools/nodetool/Retier.class` 확인). 그래서 스크립트는 건드릴 필요가 없습니다.

```bash
nodetool drain                                   # memtable flush + 커밋로그 정리
systemctl stop cassandra
# 기존 jar 교체 (이름이 같으므로 그대로 덮어쓰기)
cp apache-cassandra-6.0.0.jar "$CASSANDRA_HOME/lib/"
systemctl start cassandra
nodetool version && nodetool status              # 올라왔는지, 링에 붙었는지
```

**주의 두 가지:**

1. **`lib/`에 6.0.0 jar이 하나뿐인지 확인하십시오.** 이름이 다른 6.0.0 jar이 남아 있으면 둘 다
   클래스패스에 올라갑니다 — `ls $CASSANDRA_HOME/lib/apache-cassandra*.jar`.
2. **롤링으로 하십시오.** 온디스크 포맷과 CQL 문법은 업스트림 그대로이고 새 기능은 전부
   옵트인(테이블 확장 / 컴팩션 전략)이므로, **jar만 바꾼 시점에는 동작이 전혀 달라지지 않습니다.**
   계층화나 TSCS는 그 뒤에 테이블별로 켜면 됩니다. 이 성질 덕분에 바이너리 교체와 기능 활성화를
   분리해서 각각 검증할 수 있습니다.

---

## 1. 스키마 적합성 확인

계층화가 거부하는 형태가 하나라도 있으면 그 테이블은 계층화가 **통째로 멈추고**, 콜드 청크 만료도
함께 멈춥니다. 켜기 전에 확인하십시오 ([전체 규칙](tiered-storage.md#1-대상-스키마--시간으로-클러스터링된-아무-테이블)):

```sql
DESCRIBE TABLE pp.tm_tag_point;
```

| 확인 항목 | 요구 |
| --- | --- |
| 클러스터링 | **정확히 1개**, `timestamp` 타입 (ASC/DESC 무관) |
| `counter` 컬럼 | **없어야 함** — 삭제된 카운터는 다시 쓸 수 없습니다 |
| 비frozen 컬렉션 (일반 컬럼) | **없어야 함** — `frozen<...>`은 무방 |
| 보조 인덱스 | **static 컬럼에 걸린 것만** 허용 |
| 머티리얼라이즈드 뷰 | 이 테이블 위에 **없어야 함** |
| `transactional_mode` | **`off`여야 함** (§3 참고) |

---

## 2. TTL을 `cold_window`로 넘기기

`tm_tag_point`는 현재 `default_time_to_live = 5356800`(62일)입니다. 계층화를 켤 때 이 관계를 반드시
정리해야 합니다:

- **청크로 옮겨진 데이터에는 TTL이 적용되지 않습니다.** 재인코더는 셀의 `WRITETIME`만 읽고 `TTL`은
  읽지 않으며, 청크 포맷에 TTL 자리가 없습니다. 청크화된 데이터의 유일한 만료 장치는 `cold_window`입니다.
- **`hot_window >= TTL`이면 아무것도 압축되지 않습니다** — TTL이 먼저 지웁니다. WARN만 남고 조용히
  아무 일도 일어나지 않습니다.

> **⚠️ `default_time_to_live` 변경은 기존 행에 소급 적용되지 않습니다.** TTL은 **쓰기 시점에** 셀에
> 박힙니다. 62일 → 10년으로 바꿔도 **이미 저장된 행은 원래의 62일 만료를 그대로 들고 있습니다.**
> 옵션만 바꾸고 안심하면 기존 데이터는 예정대로 사라집니다.
>
> **그런데 계층화가 이걸 구해 줍니다.** `hot_window`가 남은 TTL보다 짧으면(예: `hot_window 7d` vs
> 62일 TTL), 기존 행은 TTL이 터지기 전에 청크로 옮겨지고 **그 순간 TTL이 벗겨집니다** — 이후로는
> `cold_window`가 지배합니다. 따라서 **기존 데이터를 살리려면 계층화를 먼저 켜십시오.** TTL 옵션
> 변경은 그다음에 해도 되고, 신규 행에만 영향을 줍니다.
>
> TTL 상한은 20년(`Attributes.MAX_TTL` = 630,720,000초)이므로 10년(315,360,000초)은 허용됩니다.

따라서 `hot_window < TTL`이어야 하고, `cold_window`가 실제 목표 보존 기간이 됩니다:

```
hot_window   : 대시보드가 실제로 때리는 구간 (예: 7d ~ 30d)
             → 이 안쪽 질의는 계층화 전과 완전히 동일한 속도 (병합 자체를 건너뜁니다)
default_ttl  : hot_window 보다 길게 (또는 계층화 도입과 함께 제거)
cold_window  : 목표 보존 기간 (예: 10y)
```

---

## 3. 애플리케이션 영향 — 코드 확인이 필요한 두 가지

### 3.1 파티션 키 없는 집계는 0을 돌려줍니다

레인지 스캔은 청크를 병합하지 않습니다. 전부 계층화된 테이블에서:

```sql
SELECT count(*) FROM pp.tm_tag_point;        -- → 태그 수 (실제 행 수 아님)
```

**클러스터링 행을 하나도 못 봅니다.** static 컬럼이 있으면 태그마다 static 행 하나가 남아 **태그
수**가 나오고(실측 3,000만 행 테이블에서 `600`), static이 없으면 `0`이 나옵니다. `600` 같은 값은
그럴듯해서 오답인 걸 알아채기 더 어렵습니다. 클라이언트 경고가 나가지만 대부분의 드라이버는 이를
노출하지 않습니다. 애플리케이션·배치·모니터링 쿼리에 파티션 키 없는 집계나 전체 스캔이 있다면
투입 전에 찾아 두십시오.

### 3.2 콜드 구간 쓰기는 거부됩니다

`hot_window` 이전 구간에 대한 `DELETE`, `UPDATE ... SET col = null`, 파티션 삭제, `INSERT`의 null
바인딩은 **거부**됩니다(`InvalidRequestException`). 콜드 데이터는 설계상 불변입니다 — 청크만 가리는
툼스톤은 `gc_grace` 이후 무력화되어 값이 되살아나기 때문입니다.

과거 데이터를 정정·삭제하는 배치가 있다면 계층화와 양립하지 않습니다.

### 3.2.1 ⚠️ 백필: `null` 바인딩이 거부됩니다

콜드 구간 쓰기 가드가 무엇을 막는지 정확히 보면:

```java
if (!row.deletion().isLive())   → 거부   // 행 삭제
else if (hasCellTombstone(row)) → 거부   // 셀 툼스톤
```

카산드라에서 **컬럼을 생략하면** 아무것도 쓰지 않지만, **`null`을 명시적으로 바인딩하면 툼스톤**이
됩니다. 따라서:

| 백필 형태 | 결과 |
| --- | --- |
| `INSERT INTO t (tag_id, timestamp, value, value_boolean) VALUES (...)` — 안 쓰는 컬럼 **생략** | ✅ 허용. 다음 재인코딩 주기에 청크로 병합 |
| `INSERT INTO t (...모든 컬럼...) VALUES (?, ?, ?, null, ?)` — **명시적 null** | ❌ 거부 |

**이게 왜 함정인가**: 드라이버의 prepared statement는 보통 **모든 컬럼을 바인딩**합니다. 그리고
`tm_tag_point`는 태그 타입에 따라 `value_numeric`/`value_boolean` 중 하나가 **항상 비어 있습니다.**
8개 컬럼을 전부 바인딩하는 수집기라면 **모든 행이 null을 하나씩 들고 있고**, `hot_window` 이전
구간으로 들어오는 백필은 **전부 거부됩니다.**

**투입 전 확인**: 수집기가 안 쓰는 컬럼을 (a) 컬럼 목록에서 빼는지, (b) `unset`으로 두는지,
(c) `null`을 바인딩하는지. (c)라면 (a)나 (b)로 바꿔야 백필이 동작합니다.

### 3.3 `transactional_mode`을 켜지 마십시오

Accord 트랜잭션 **읽기**는 청크 병합을 거치지 않아 `hot_window` 이전 이력이 통째로 빠진 결과를
조용히 돌려줍니다. 계층화는 그런 테이블을 거부하지만 **`ALTER TABLE`을 막지는 못합니다** — 실패는
다음 사이클의 ERROR 로그로만 나타납니다.

---

## 3.5 TSCS만 먼저 적용하기 (권장 순서)

계층화와 TSCS는 **완전히 독립**입니다 — 계층화는 테이블 확장, TSCS는 컴팩션 전략입니다. 둘 다 켤
필요가 없고, **TSCS를 먼저 켜는 편이 위험이 낮습니다.**

**되돌릴 수 있기 때문입니다.** 컴팩션 전략은 언제든 되돌리면 그만이고 데이터는 그대로입니다.
계층화는 원본 행을 삭제하므로 되돌릴 수 없습니다(§0.2).

TSCS만으로 얻는 것: 창 단위 SSTable 정렬(시간 범위 조회의 지역성), **만료 창 통삭제**, 지각 백필
격리, 닫힌 창 동결(창당 1 SSTable → 읽기 증폭 감소).

### ⚠️ 만료는 TTL이 아니라 `retention`에 맡기십시오

동결된 창은 다시 컴팩션 후보로 선택되지 않습니다. 따라서:

- **동결 시점에** 이미 만료된 TTL 데이터는 그때 회수됩니다
- **동결 이후에** 만료되는 데이터는 **TTL만으로는 영원히 회수되지 않습니다**

TSCS의 `retention`을 목표 보존 기간으로 설정하십시오. 만료된 창을 **컴팩션 없이 통째로 삭제**하므로
TTL 기반 회수보다 오히려 효율적입니다.

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
  'class': 'TimeSeriesCompactionStrategy',
  'window_size': '1d',        -- 창 크기 (조회 패턴에 맞춤)
  'freeze_after': '2d',       -- 창이 닫힌 뒤 이 시간이 지나면 동결
  'retention': '3650d'        -- 보존 기간. 만료 창은 통삭제
};
```

### 투입 시 주의

1. **전략 변경은 전체 재컴팩션을 유발합니다.** 기존 대용량 테이블에서는 상당한 IO 이벤트이므로
   비피크 시간대에, 한 노드씩 진행하십시오.
2. **분산 검증이 아직 없습니다.** 계층화는 3노드 jvm-dtest를 통과했지만 **TSCS는 분산 테스트가
   없습니다.** 특히 스트리밍 창 스플릿(repair/bootstrap으로 SSTable이 도착할 때 동작)은 2대 이상에서
   실행된 적이 없습니다.
3. **되돌리기**: `ALTER TABLE ... WITH compaction = {'class': 'UnifiedCompactionStrategy', ...}`.
   데이터 손실 없이 원복되며, 다시 전체 재컴팩션이 일어납니다.

---

## 4. 투입 절차

한 번에 전체를 켜지 마십시오.

1. **테이블 하나로 시작** — 보존이 가장 급한 테이블 하나. 계층화는 테이블 확장 옵션이라 테이블마다
   독립적으로 켜고 끌 수 있습니다.
2. **`hot_window`를 넉넉하게 시작** — 처음에는 실제 필요보다 길게 잡아 압축 대상을 좁힌 뒤, 동작을
   확인하고 줄이십시오. 되돌릴 수 없는 작업이므로 처음에는 적게 건드리는 편이 낫습니다.
3. **`nodetool retier`로 수동 1회 실행** — 60초 스위프를 기다리지 말고 직접 돌려 결과를 확인합니다.
4. **검증** — 아래 §5.
5. **관측하며 확대** — 며칠 지켜본 뒤 다음 테이블로.

---

## 5. 검증 항목

```sql
-- 청크가 실제로 쓰였는가
SELECT count(*) FROM pp.tm_tag_point__chunks;

-- 계층화 사이클 상태 (에러·건너뜀 포함)
SELECT * FROM system_views.timeseries_tiering;

-- 같은 데이터가 계층화 전과 동일하게 읽히는가 (반드시 파티션 키를 걸고)
SELECT count(*) FROM pp.tm_tag_point
 WHERE tag_id = '<태그>' AND timestamp >= '<콜드 구간 시작>' AND timestamp < '<끝>';

-- static은 그대로 남아 있는가
SELECT site_id, tag_name, type FROM pp.tm_tag_point WHERE tag_id = '<태그>' LIMIT 1;
```

로그에서 확인할 것:

- `ERROR` — 스키마 거부 사유 (§1의 어느 항목에 걸렸는지 명시됩니다)
- `WARN` — TTL/`hot_window` 충돌, 건너뛴 태그

JMX (`org.apache.cassandra.db:type=Tables,keyspace=pp,table=tm_tag_point`):

| 속성 | 의미 |
| --- | --- |
| `ParkedTimeSeriesWindows` | 컴팩션이 진척을 못 내 파킹한 창 — 비어 있는 것이 정상 |
| `FarFutureTimeSeriesSSTables` | `max_future_window` 밖이라 모든 자동 경로에서 제외된 sstable |

---

## 6. 아직 검증되지 않은 것 (투입 판단에 필요한 정보)

정직하게 남깁니다:

- **실제 다중 노드 운영 클러스터에서 돌려본 적이 없습니다.** 3노드 jvm-dtest는 통과했고(재인코더의
  프라이머리 레인지 분할, 코디네이터 독립 투명 읽기, 지각 행 생존, 노드 재시작), 그 테스트가 실제
  스키마 전파 결함을 잡아냈지만, 이는 실 운영 부하와 다릅니다.
- **TSCS(시계열 컴팩션)는 분산 테스트가 없습니다.** 계층화와 독립적으로 켤 수 있으므로, 첫 투입에서는
  계층화만 켜고 컴팩션 전략은 기존 것을 유지하는 편이 변수가 적습니다.
- **질의 성능은 패턴에 따라 갈립니다** ([벤치마크](tiering-benchmark.md)). 운영 형태 실측 기준
  집계는 약 2.2× 빨라지고 gap-fill은 4.3× 빨라지지만, **행 단위 조회(`SELECT *`로 최신 N행)는 3배,
  static 컬럼만 읽는 질의는 31배 느려집니다.** 조회 패턴이 집계 중심인지 원시 행 인출 중심인지
  먼저 확인하십시오. `hot_window` 안쪽만 보는 질의는 병합을 건너뛰므로 **비용이 없습니다.**
