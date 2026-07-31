# 운영 튜닝 가이드 — 장기 보존 시계열 테이블

실제 산업 현장 테이블(`pp.tm_tag_point`, 태그 66,076개 · 태그당 3,614행/일 · 노드당 131GB)을
**5년 보존**으로 전환하는 것을 기준으로, 계층형 저장(tiered storage)과 시계열 컴팩션(TSCS)을
함께 튜닝하는 방법을 정리한다. 숫자는 전부 실측에서 나왔다.

관련 문서: [계층형 저장](tiered-storage.md) · [계층화 벤치마크](tiering-benchmark.md) ·
[코덱 bake-off](codec-bakeoff.md)

## 1. 왜 튜닝이 필요한가 — 용량 산수부터

| | 행당 | 하루 | 62일(현재) | **5년(1,825일)** |
| --- | --- | --- | --- | --- |
| 행 저장 (Zstd, 현재) | 9 B | 2.15 GB | 133 GB | **3.9 TB/노드** ❌ |
| 계층화 후 (컬럼 지향 청크) | ≈3 B | 0.72 GB | 45 GB | **1.3 TB/노드** ✅ |

압축 없이는 5년이 사실상 불가능하고(노드당 3.9TB), 계층화하면 1.3TB로 들어온다. 컴팩션 여유분까지
감안해 **노드당 2TB**를 권장한다. 더 줄이려면 노드를 늘리거나(RF=3 · 6노드 → 노드당 절반),
오래된 구간은 원시 대신 샘플링 테이블만 보관한다.

## 2. 적용 순서 — 계층화를 먼저, 컴팩션을 나중에

**권장 순서는 `계층화 → 컴팩션`이다.** 컴팩션 전략 변경(`ALTER ... WITH compaction`)은 테이블
전체를 재작성하므로, 131GB 상태에서 하는 것보다 계층화로 원본이 작아진(≈4GB) 뒤에 하는 편이
부담이 30배 적다.

### 2.1 계층화 정책 (CQL 한 줄)

```sql
ALTER TABLE pp.tm_tag_point WITH extensions = {
  'timeseries_tiering': '{"hot_window":"2d","chunk_window":"1d","cold_window":"1825d","interval":"1h","codec":"auto","consistency":"LOCAL_QUORUM"}'
};
```

> `extensions`는 스키마상 blob 맵이지만 이 포크는 **평문 JSON 문자열**을 받아 UTF-8로 저장한다.
> `0x`로 시작하는 값만 hex 블롭으로 해석하므로 기존 hex 표기도 그대로 동작한다.

```bash
nodetool retier pp tm_tag_point     # 즉시 1사이클 (청크 테이블이 이때 자동 생성됨)
```

### 2.2 원본 테이블 컴팩션 (TSCS)

```sql
ALTER TABLE pp.tm_tag_point WITH compaction = {
    'class'              : 'TimeSeriesCompactionStrategy',
    'window_size'        : '1d',      -- = chunk_window (필수 일치)
    'freeze_after'       : '2h',      -- 지각 백필 유예
    'scaling_parameters' : 'T4',      -- 핫 구간이 2일치뿐 → T8보다 낮춰도 충분
    'target_sstable_size': '256MiB',  -- 테이블이 30배 작아졌으므로 축소
    'max_future_window'  : '1d'       -- 미래 타임스탬프 오입력 격리
};
```

**TTL(62일)은 그대로 둔다.** 계층화가 정상 동작하면 원본 행은 2일 만에 청크로 옮겨져 TTL은 발동조차
하지 않는다. TTL의 역할은 **재인코더가 멈췄을 때의 안전망** — 이걸 빼면 계층화 장애 시 원본이
무한 증가한다. 같은 이유로 TSCS `retention`을 TTL보다 짧게 거는 것도 운영 테이블에서는 피한다
(장애 시 지금보다 빨리 데이터가 사라진다).

### 2.3 ★ 청크 테이블 튜닝 — 5년치가 여기 쌓인다

계층화가 자동 생성한 `<테이블>__chunks`는 기본값으로 만들어지므로, 장기 보존이라면 반드시 조정한다.

```sql
ALTER TABLE pp.tm_tag_point__chunks WITH
    compaction = {
        'class'              : 'UnifiedCompactionStrategy',
        'scaling_parameters' : 'T2',        -- 사실상 write-once → 병합 최소화
        'target_sstable_size': '2GiB',      -- 대용량·저변경 테이블은 큰 SSTable이 유리
        'base_shard_count'   : '8',
        'expired_sstable_check_frequency_seconds': '3600'
    }
    AND compression = {
        'class'              : 'ZstdCompressor',
        'chunk_length_in_kb' : '64',        -- 청크 페이로드가 ≈11KB → 64KB 블록이 효율적
        'compression_level'  : '3'
    }
    AND gc_grace_seconds = 86400            -- cold_window 만료 톰스톤을 하루 만에 회수
    AND caching = {'keys':'ALL','rows_per_partition':'NONE'};
```

## 3. 값을 고르는 규칙 (테이블 무관)

시간 축이 이렇게 겹친다:

```
쓰기 ─▶ [현재 창: UCS 위임] ─window_size─▶ [닫힘] ─freeze_after─▶ [동결: 창당 1 SSTable]
                                                                        │
                                              ────── hot_window ────────┴─▶ [청크 압축, 원본 삭제]
                                                                                    │
                                                        ────── cold_window ─────────┴─▶ 삭제
        (재인코더가 멈추면) ────── TTL 또는 TSCS retention ──▶ 원본 정리 = 안전망
```

| 옵션 | 정하는 기준 |
| --- | --- |
| `window_size` = `chunk_window` | **반드시 같게.** 창 하나가 청크 하나가 되도록. 청크당 **1,000~10,000 샘플**이 되는 폭을 고른다 — 태그당 3,614행/일이면 `1d`, 초당 1건이면 `1h`, 10초당 1건이면 `6h` |
| `freeze_after` | 지각(백필) 데이터가 도착하는 최대 지연보다 크게 |
| `hot_window` | `freeze_after`보다 크고(데이터가 안정된 뒤 압축), **TTL보다 훨씬 작게**. 권장: 그 테이블 TTL의 10% 이하 |
| `cold_window` | 실제로 원하는 보존 기간. 미설정이면 영구 보관 |
| `interval` | `chunk_window`보다 짧게. 하루 창이면 `1h`면 충분 |
| `codec` | `auto` 고정 — 창마다 Gorilla/Chimp128 중 작은 쪽을 자동 선택 |
| 안전망(TTL 또는 TSCS `retention`) | 재인코더가 며칠 멈춰도 버틸 만큼 넉넉히. 정상 동작 시엔 발동하지 않는다 |

### 튜닝 항목별 근거

| 항목 | 기본→권장 | 이유 |
| --- | --- | --- |
| 원본 `scaling_parameters` | T8 → **T4** | 계층화 후 원본은 `hot_window`만큼(2일치 ≈4GB)만 유지 → 공격적 tiered 불필요 |
| 원본 `target_sstable_size` | 512 → **256MiB** | 테이블이 30배 작아지면 SSTable도 줄여야 컴팩션이 빠르다 |
| 청크 `scaling_parameters` | T4 → **T2** | 청크는 한 번 쓰고 거의 안 바뀐다(지각 병합만) → 재컴팩션 최소화가 이득 |
| 청크 `target_sstable_size` | 1 → **2GiB** | 1.3TB 테이블에 512MiB면 SSTable이 2,600개. 큰 단위가 관리·읽기 모두 유리 |
| 청크 `chunk_length_in_kb` | 16 → **64** | 청크 하나가 ≈11KB. 16KB 블록이면 블록당 1~1.5청크뿐이라 Zstd가 학습할 문맥이 부족하다 |

## 4. TTL과 계층화의 관계 (반드시 이해할 것)

- 재인코더가 만드는 청크는 **새로 쓰는 행**이라 원본의 TTL을 물려받지 않는다. 청크로 옮겨진
  데이터의 수명은 오직 `cold_window`가 정한다 — 이것이 "압축해서 보존 연장"의 메커니즘이다.
- 따라서 계층화를 켜는 순간 그 테이블의 보존 정책은 **TTL이 아니라 `hot_window` + `cold_window`
  조합**으로 넘어간다. 되돌리려면 정책을 제거해야 하지만, 이미 청크가 된 데이터는 계속
  `cold_window` 규칙을 따른다.
- **`hot_window`가 테이블 TTL보다 크거나 같으면 압축이 한 번도 일어나지 않는다.** 재인코더가 오기
  전에 TTL이 데이터를 지워버리기 때문이다. 가장 위험한 조용한 실패이므로 정책 적용 시 경고한다.
- 행 단위 TTL(`USING TTL`)을 섞어 쓰는 테이블은 테이블 기본값으로 판정하되, 실제 행 TTL이 더 짧을
  수 있다.

## 5. 확인과 운영

```sql
SELECT * FROM system_views.timeseries_tiering;   -- 정책 + 실행 통계 (창/행/바이트/지각 병합/만료)
SELECT count(*) FROM pp.tm_tag_point WHERE tag_id='TAG_...';  -- 압축 후에도 같은 값 (투명 읽기)
```
```bash
nodetool tieringstatus                                  # 테이블별 정책·마지막 실행·누적 통계
nodetool retier <ks> <table>                            # 수동 1사이클 (동기)
nodetool tablestats pp.tm_tag_point pp.tm_tag_point__chunks
```

**대용량(1TB+) 운영 주의**

- **리페어**: 전체 리페어는 며칠 걸린다. `nodetool repair -pr` 서브레인지 또는 auto_repair
  스케줄러를 쓴다. 청크 테이블은 변경이 거의 없어 실제 diff는 작다.
- **디스크**: 컴팩션 여유분 포함 목표 용량의 1.5배를 확보한다. 청크 테이블의 `T2` + 2GiB 설정이
  컴팩션 순간 공간 사용을 크게 줄여준다.
- **정책 변경**: `cold_window`는 언제든 `ALTER`로 바꿀 수 있다. 일단 켜고 실측 압축률을 본 뒤
  조정해도 된다.

## 6. 적용 전 점검 목록

- [ ] `window_size` == `chunk_window` 인가
- [ ] `hot_window` > `freeze_after` 이고, 테이블 TTL의 10% 이하인가
- [ ] 안전망(TTL 또는 TSCS `retention`)이 `hot_window`보다 충분히 큰가
- [ ] 청크당 샘플 수가 1,000~10,000 범위인가 (태그당 수집 주기 × `chunk_window`로 계산)
- [ ] `cold_window` × 하루 압축 후 용량 ≤ 노드 디스크의 2/3 인가
- [ ] 청크 테이블 튜닝(2.3)을 첫 사이클 후에 적용했는가
- [ ] 일반 컬럼에 보조 인덱스가 걸려 있지 않은가 (static 컬럼 인덱스는 안전)
