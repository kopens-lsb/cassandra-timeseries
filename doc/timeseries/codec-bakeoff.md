# Chunk codec bake-off: Gorilla (v1) vs Chimp128 (v2)

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
