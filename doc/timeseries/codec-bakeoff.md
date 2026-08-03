# Chunk codec bake-off: Gorilla (v1) vs Chimp128 (v2)

> ## 최신 결론 (2026-08-03) — **ALP가 유일한 double 인코딩**
>
> Chimp128도 v3 컬럼 지향 청크에서 **은퇴**했습니다. double 컬럼은 이제
> [`AlpCodec`](../../src/java/org/apache/cassandra/db/timeseries/AlpCodec.java) 하나만 씁니다
> (타입 코드 `0x07`, 십진 ALP / ALP-RD 두 변형). 타입 코드 `0x01`은 `0x00`(Gorilla)과 같은
> 이유로 **영구 예약**됩니다. `Chimp128Codec`은 v2 단일 컬럼 포맷으로 남아 있고, 아래 크기 비교의
> 기준선으로도 계속 쓰입니다(`AlpCodecTest`).
>
> **실측 (3,600 값/컬럼 = 1시간 × 1초 간격, `AlpCodecTest`가 매 실행마다 출력):**
>
> | 분포 | 변형 | ALP B/값 | Chimp128 B/값 | ALP/Chimp |
> | --- | --- | ---: | ---: | ---: |
> | 운영 `value_numeric` 소수 2자리 워크 | ALP | **0.752** | 1.351 | **0.56×** |
> | 운영 `value_numeric` 정수 워크 | ALP | **0.752** | 1.905 | **0.40×** |
> | near-constant (997행마다 1회 변화) | ALP | **0.013** | 1.130 | **0.011×** |
> | quantized-walk-0.1 | ALP | 0.752 | 1.269 | 0.59× |
> | quantized-sine | ALP | 1.377 | 3.179 | 0.43× |
> | 소수 3자리 압력 워크 | ALP | 0.753 | 1.330 | 0.57× |
> | 단조 증가 정수 카운터 | ALP | 1.627 | 2.320 | 0.70× |
> | uniform [0,1) double | ALP-RD | 6.893 | 7.220 | 0.96× |
> | random 64비트 패턴 | ALP-RD | 8.002 | 8.251 | 0.97× |
> | **full-precision 가우시안 워크** | ALP-RD | 6.627 | **6.532** | **1.015×** |
> | **full-precision 사인** | ALP-RD | 7.038 | **6.773** | **1.039×** |
>
> **알려진 손해**: 굵게 표시한 두 줄 — 소수점이 잘리지 **않은**, 매끄럽게 변하는 double —
> 에서는 Chimp128이 1.5~3.9% 더 작습니다. 튜닝 실패가 아니라 구조적입니다. Chimp는 각 값을
> 비슷한 최근 값과 XOR해 **순차 상관**을 먹지만, ALP-RD는 비트 패턴의 정적 분포만 모델링하고
> 모든 값을 같은 자리에서 자릅니다. ALP-RD 사전을 8개 이상으로 키워도 개선되지 않습니다 —
> 그런 계열의 하위 ~53 맨티사 비트는 순차 모델 없이는 사실상 비압축이고, 그것이 ALP-RD의
> 하한(53 bits/값)을 정합니다.
>
> 이 두 형태는 **측정된 운영 분포에는 없습니다**(`docker/scale-workload.py` — 운영 판독값은
> 전부 십진 양자화되어 있습니다). 순차 상관이 없는 full-precision 데이터에서는 오히려 ALP-RD가
> 더 작습니다. `AlpCodecTest.fullPrecisionColumnsStayWithinTheRecordedMarginOfChimp128`이 이
> 손해를 **수치로 고정**해 두므로, 몇 %에서 조용히 커지는 일은 없습니다.
>
> **재현**: `.build/sh/ai-ci-test org.apache.cassandra.db.timeseries.AlpCodecTest` — 위 표는
> 테스트 표준출력에 그대로 찍힙니다.


> ## 결론 (2026-08-01, SP4 Task 1.5) — **Chimp128이 유일한 코덱**
>
> 이 bake-off 이후 **Gorilla는 삭제됐고 Chimp128이 유일한 double 코덱**이 됐습니다. 정책의
> `codec` 옵션(`auto`/`gorilla`/`chimp128`)도 함께 제거됐습니다.
>
> **이유**: 아래 측정에서 Gorilla가 이긴 유일한 구간은 **상수(near-constant) 계열**(5배 우세)
> 뿐입니다. 그런데 컬럼 지향 청크 포맷(v3, SP4 Task 1)이 **CONSTANT 플래그**로 상수 컬럼을
> 어떤 코덱보다 먼저, 행 수와 무관하게 **O(1) 바이트**로 처리합니다 — 값을 디렉토리에 한 번만
> 저장하고 데이터 섹션은 0바이트입니다. 즉 Gorilla의 유일한 강점이 코덱 레이어에 도달하기도
> 전에 사라졌습니다. 남는 위험은 "거의 변하지 않지만 완전한 상수는 아닌" double 계열인데,
> 그 경우에도 Gorilla를 되살리는 대신 **double용 RLE 경로**를 추가하는 편이 낫습니다(RLE는
> 그 패턴에서 Gorilla보다도 작습니다).
>
> 아래의 측정치·표·판정문은 당시 기록 그대로 보존합니다(`ChunkCodecsTest#bakeoff`는 Gorilla와
> 함께 삭제되어 더 이상 재현되지 않습니다). Chimp128 단독의 크기 회귀 기준은
> `Chimp128CodecTest#sizeRegressionBaselines`가 이어받았습니다.

## 당시 기록 (2026-07-31)

Source: `org.apache.cassandra.db.timeseries.ChunkCodecsTest#bakeoff` · 100,000 samples/pattern,
seed 17, single JVM run (JIT warm-up not isolated -- see the timing caveat below). Promotion
criteria: [chimp128-codec design spec](../../docs/superpowers/specs/2026-07-31-chimp128-codec-design.md),
§1 ("목표·판정 기준"). Numbers below are the actual measured `BAKEOFF` output, unedited.

## Raw output

```
BAKEOFF constant               gorilla=0.250 B/s chimp=1.250 B/s ratio=0.20 enc(g/c)=18/39 ms dec(g/c)=9/7 ms
BAKEOFF quantized-walk-0.1     gorilla=4.669 B/s chimp=1.451 B/s ratio=3.22 enc(g/c)=59/16 ms dec(g/c)=6/8 ms
BAKEOFF quantized-sine         gorilla=6.469 B/s chimp=2.503 B/s ratio=2.58 enc(g/c)=4/28 ms dec(g/c)=9/6 ms
BAKEOFF full-precision-walk    gorilla=8.359 B/s chimp=6.637 B/s ratio=1.26 enc(g/c)=5/87 ms dec(g/c)=9/10 ms
BAKEOFF random-bits            gorilla=8.375 B/s chimp=8.375 B/s ratio=1.00 enc(g/c)=5/14 ms dec(g/c)=5/8 ms
```

(`B/s` in the printf label is a leftover from the spec's fragment -- the values are
**bytes/sample**, not bytes/second; `ratio` = gorilla/chimp, i.e. >1 means chimp is smaller.)

## Table

| pattern | gorilla (B/sample) | chimp (B/sample) | size ratio (g/c) | chimp vs gorilla | enc ms (g/c) | dec ms (g/c) |
|---|---:|---:|---:|---:|---:|---:|
| constant | 0.250 | 1.250 | 0.20 | **+400% (5.0x larger)** | 18 / 39 | 9 / 7 |
| quantized-walk-0.1 | 4.669 | 1.451 | 3.22 | **-68.9% (smaller)** | 59 / 16 | 6 / 8 |
| quantized-sine | 6.469 | 2.503 | 2.58 | **-61.3% (smaller)** | 4 / 28 | 9 / 6 |
| full-precision-walk | 8.359 | 6.637 | 1.26 | -20.6% (smaller) | 5 / 87 | 9 / 10 |
| random-bits | 8.375 | 8.375 | 1.00 | 0.0% (identical) | 5 / 14 | 5 / 8 |

## Promotion verdict (spec §1 criteria)

Spec §1 requires **both** of the following to promote chimp128 to the default codec:

1. **>=30% bytes/sample reduction vs gorilla on quantized patterns** (quantized-walk-0.1,
   quantized-sine).
2. **No more than +-10% regression (+ small fixed slack) on the constant pattern**, relative to
   the spec's cited 0.253 B/sample (measured 0.250 in this run).

Measured: criterion 1 **passes decisively** -- quantized-walk-0.1 is 68.9% smaller and
quantized-sine is 61.3% smaller, both more than double the 30% bar. Criterion 2 **fails badly** --
the constant pattern is not within +-10%, it is **5.0x larger (a ~400% regression)**.

**Verdict: chimp128 (v2) is NOT promoted to the default chunk codec.** It stays available as an
explicit opt-in via `ChunkCodecs.Codec.CHIMP128` (or direct `Chimp128Codec` use); `GorillaCodec`
(v1) remains the recommended default for workloads that mix constant/setpoint stretches with
quantized movement, which is the common industrial-sensor case this repo targets (see the README's
Gorilla row: constant series compress to 0.25 B/sample, a 64x reduction, precisely the case chimp
regresses). Because `ChunkCodecs.encode` takes an explicit `Codec` enum, callers that know their
series is reliably quantized and rarely constant (e.g. periodic vibration/temperature readings
that never truly flatline) can deliberately choose `CHIMP128` for a real 60-69% size win.

### Why chimp loses on the constant pattern (not a bug)

This is a real, honest loss, not an implementation defect -- it falls directly out of the
normative algorithm in the design spec (§2, left branch, `xorC == 0` case). Chimp128 always
references its best-matching prior sample through a 7-bit ring index (`i % 128`), even when that
best match happens to be the immediately preceding value. So an exact repeat costs
`1 (branch flag) + 7 (ring index) + 1 (exact-match flag)` = **9 bits**, versus Gorilla's dedicated
1-bit "value unchanged" fast path when the raw XOR against the previous value is zero. For a
long run of identical values, timestamp DoD is 1 bit in both codecs, so per-sample cost is 2 bits
(gorilla) vs 10 bits (chimp) -- a 5x blow-up that matches the measured ratio almost exactly
(1.25 / 0.25 = 5.0). Chimp128's 128-deep ring is optimized for *periodic* recurrence (a value seen
somewhere in the last 128 samples, not necessarily the previous one), which is exactly why it wins
big on quantized-sine; it simply does not special-case "same as literally the last sample" the way
Gorilla's single-window XOR scheme does.

### full-precision-walk and random-bits (informational, not gating)

Neither pattern is part of the spec §1 promotion gate (that only covers quantized + constant), but
both are worth recording. full-precision-walk (unquantized Gaussian random walk) still favors
chimp by 20.6% -- its ring-candidate search occasionally finds a better XOR partner than "always
compare to the immediately preceding sample" even without quantization. random-bits (uniform random
`double` bits, the encoding worst case for both codecs) comes out identical at 8.375 bytes/sample
for both -- neither scheme can compress incompressible data, and both converge to essentially the
8-byte raw value plus a small fixed per-sample control-bit tax.

### Timing caveat

The enc/dec millisecond columns are single-run, same-JVM, no-warm-up wall-clock timings taken
inside one JUnit test method (see `ChunkCodecsTest#bakeoff`) -- they include JIT warm-up noise and
should be read as rough orders of magnitude, not a rigorous throughput benchmark. They are recorded
here only because the source printf emits them; no conclusion in this document rests on their
precise values.

## Reproducing

```bash
.build/sh/ai-ci-test --reuse org.apache.cassandra.db.timeseries.ChunkCodecsTest
```

The `BAKEOFF` lines are on the test's standard output, visible directly in the ant/junit console
log (`showoutput=true` for `testclasslist`/`testsome`) or duplicated into
`build/test/output/TEST-org.apache.cassandra.db.timeseries.ChunkCodecsTest.xml`'s
`Standard Output` section when running via `ant testsome`.
