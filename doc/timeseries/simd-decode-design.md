# SIMD 디코드 설계 — 실제로 할 일은 "SIMD 구현"이 함의하는 것보다 훨씬 적다

상태: **설계. 구현 미착수. 전제 0 미해결**(§8). SP4 Phase 3([sp4-plan.md](sp4-plan.md) §Phase 3)의
후속 설계이며, 그 계획의 "Java Vector API 뒤에서 ALP·타임스탬프·null 비트맵을 벡터화한다"는
한 줄을 **범위를 좁히는 방향으로** 대체한다.

## 0. 결론 먼저

> **이 작업의 올바른 결과물은 대부분 SIMD가 아니다.** 측정 가능한 이득의 거의 전부는
> width-specialized **스칼라** 커널과 두 건의 알고리즘 수정에서 나오고, vector kernel은
> 게이트를 통과하지 못하면 **삭제하는 것이 성공**이다.

이 결론을 강제하는 발견이 셋이다.

### 0.1 Vector API는 하드 의존성이 될 수 없다

[`build.xml:47-48`](../../build.xml)이 지원 JDK를 선언한다.

```xml
<property name="java.default" value="11" />
<property name="java.supported" value="11,17,21" />
```

`jdk.incubator.vector`는 **JDK 16 이전에 존재하지 않는다.** 따라서 `src/java` 안의 import 한
줄이 JDK 11 빌드를 깬다 — 런타임 fallback을 아무리 잘 써도 소용없다. 컴파일이 먼저 죽는다.
(이 저장소는 CLAUDE.md 기준 Java 21로 빌드하지만, `java.supported`는 upstream과 함께 관리되는
값이고 이 한 줄을 위해 그것을 좁히는 것은 upstream 병합 비용을 영구히 늘린다.)

필요한 구조:

- **별도 source set `src/java-vector`** — JDK 21에서만 컴파일되는 `<javac>` 타깃, 결과물은
  선택적 모듈/디렉토리.
- **reflective holder** — `Class.forName`으로 커널을 찾고 `catch (Throwable)`로 스칼라에
  떨어지는 홀더. 이 패턴은 이미 저장소에 있다:
  [`src/java/org/apache/cassandra/utils/FastByteOperations.java:148-178`](../../src/java/org/apache/cassandra/utils/FastByteOperations.java)
  의 `BestHolder.getBest()` — `Class.forName(UNSAFE_COMPARER_NAME)` → 실패 시
  `PureJavaOperations`. 새 패턴을 발명하지 말고 이것을 그대로 따른다.

즉 vector kernel의 **최소 비용이 이미 "빌드 시스템 특수 케이스 + 리플렉션 홀더"** 다. 이 비용은
커널이 아무리 빨라도 사라지지 않으며, §6의 게이트가 존재하는 이유이기도 하다.

### 0.2 종단 이득의 상한이 약 1%다

[tiering-benchmark.md:41-53](tiering-benchmark.md)의 단일 파티션 50,000행 질의는 계층화 경로에서
**66~125 ms** 다(`variance`/`stddev` 66 ms ~ `OHLC + change + p95` 125 ms. 호스트: 2 × Xeon
X5670, Westmere, SSE4.2 — [rw-throughput-benchmark.md:30](rw-throughput-benchmark.md)).

그 안에서 bit unpacking이 차지하는 비중은 **0.3~1.8%** 다. 따라서 **완벽한 4배 커널의 종단
기여는 0.25~1.4%** 다. 4배가 아니라 무한대여도 2% 를 넘지 못한다.

이 숫자가 설계 전체를 지배한다. "몇 % 를 위해 인큐베이터 의존성과 빌드 특수 케이스와 두 번째
구현을 들인다"는 거래는 대부분의 경우 성립하지 않으며, 성립하는지 여부는 **미리 적어둔
임계값**(§6)으로만 판정한다.

### 0.3 디코드 최대 이득 두 개는 SIMD가 아니다

1. **RLE bit-setting 수정** — `BlockPresence.setBits`가 present run을 **행마다** read-modify-write
   하던 문제. 1024행 블록의 단일 present run이 16개 워드를 채우는 데 1024회 종속 반복을 썼다.
   head partial word masked OR + `Arrays.fill` + tail partial word masked OR 로 교체하면
   O(rows) → O(words). 이 함수 기준 10~60배.
2. **running rank 규칙** — `BlockPresence.rank`는 O(offset/64)다. seek에는 맞고 sequential scan
   에는 틀렸다. 순차 스캔이 행마다 호출하면 O(n · n/64)가 된다. 커서는 running value index를
   들고 다니며 행의 presence bit만큼 증가시켜야 한다.

둘 다 벡터 명령을 한 개도 쓰지 않는다. 둘 다 §0.2의 상한과 무관하게 이득이 실재한다.

## 1. 이득 순위

| # | 항목 | SIMD? | 판정 |
|---|---|---|---|
| 1 | RLE word-at-a-time expansion | 아니오 | **최대 이득. 먼저 한다.** |
| 2 | v4 cursor의 running rank | 아니오 | **미작성 코드에 대한 설계 제약. 지금이 유일하게 싼 시점.** |
| 3 | width-specialized **스칼라** unpack kernel | 아니오 | **이득의 대부분.** |
| 4 | vector unpack kernel | 예 | 진짜로 벡터화됨. AVX2로 충분. 게이트 대상. |
| 5 | ALP scaled-integer reconstruct | 부분 | AVX-512DQ에서만. 스칼라 dense/exception 분리가 선행. |
| 6 | ALP-RD recombination | 예 | 우아하지만 **운영에 없는 데이터 형태**를 위한 것. |
| 7 | FOR / delta-FOR | — | C2가 이미 한다. **아무것도 하지 않는다.** |
| 8 | presence BITMAP / ALL_PRESENT / ALL_NULL | — | 이미 최적. |
| 9 | vector `rank` | 불가 | `AVX512-VPOPCNTDQ` 필요. 호스트 237(Cascade Lake)에 **없다.** |
| 10 | 본질적 직렬 구간 | 불가 | RLE run walking, canonical varint read, dictionary expansion, Chimp XOR chain. |

### 1.3 (#3) "C2가 이미 unpack loop을 auto-vectorize한다"는 **거짓이다**

이 주장이 이 설계에서 가장 자주 나오는 오답이고, 코드를 보면 바로 반증된다.
[`BitPacking.unpack`](../../src/java/org/apache/cassandra/db/timeseries/BitPacking.java) `:239-254`:

```java
for (int i = 0; i < count; i++)
{
    long v = cur >>> bitOffset;
    bitOffset += width;
    if (bitOffset >= 64)
    {
        bitOffset -= 64;
        wordIndex++;
        cur = wordIndex < words ? src.getLong(base + (wordIndex << 3)) : 0L;
        if (bitOffset != 0)
            v |= cur << (width - bitOffset);
    }
    dst[i] = v & mask;
}
```

`cur`, `bitOffset`, `wordIndex`가 **반복 간에 이월(loop-carried)** 되고, 그 위에 분기가 둘
얹힌다. C2의 SuperWord는 loop-carried dependency와 control flow가 있는 루프를 건드리지 못한다.
따라서 이 루프는 지금 **한 줄도 벡터화되어 있지 않다.**

폭 `w`를 컴파일 타임 상수로 고정한 width-specialized 커널을 만들면 `bitOffset` 수열이 **정적**이
되고(주기 `lcm(w,64)/w` 값마다 반복), 워드 경계 분기가 **펼쳐져 사라진다.** 그 시점에서야 비로소
자동 벡터화의 여지가 생기고, 그전에도 이미 분기 제거와 상수 shift만으로 상당한 이득이 있다.

Lucene의 출하 경험이 정확히 이것이다: **생성된 스칼라 커널이 이득의 대부분**이고, vector API는
그 위에 **추가로 1.3~2배**를 얹는다. 즉 #3을 건너뛰고 #4로 가면 vector kernel은 잘못된 기준선
(현재의 branchy 스칼라 루프)과 비교되어 실제보다 훨씬 좋아 보인다. §6 게이트 A가 비교 대상을
**specialized scalar** 로 못박는 이유다.

### 1.5 (#5) ALP scaled-integer reconstruct는 AVX-512DQ에서만 벡터화된다

`value = (double) encoded * factor` 형태의 재구성에서 핵심은 `long` → `double` 변환이다. JIT가
내는 `L2D`는 **`vcvtqq2pd`** 로 컴파일되는데 이 명령은 **AVX-512DQ** 에서만 존재한다. AVX2에는
`long` 벡터 → `double` 벡터 변환 명령 자체가 없다(32비트 정수 변환만 있다).

그러므로 #5는 하드웨어 조건부다. 다만 **스칼라 dense/exception 분리는 조건과 무관하게 먼저
한다** — exception 처리 분기를 dense 루프 밖으로 빼는 것은 어떤 ISA에서도 이득이고, 벡터화의
전제이기도 하다.

### 1.6 (#6) ALP-RD recombination은 우아하지만 대상 데이터가 없다

left/right part를 8-entry dictionary로 합치는 연산은 벡터로 **한 개의 `vpermq`** 다. 설계로서는
가장 깔끔하다. 그러나 ALP-RD가 선택되는 데이터는 full-precision, 십진 양자화되지 않은 double
계열이고, [codec-bakeoff.md](codec-bakeoff.md)가 기록하듯 **측정된 운영 분포에는 그런 컬럼이
없다**(`docker/scale-workload.py` — 운영 판독값은 전부 십진 양자화). 즉 존재하지 않는 워크로드를
위해 두 번째 구현을 들이는 항목이다. 하지 않는다.

## 2. 결정적 API 제약 — 커널은 `long[]`에서 로드해야 한다

JDK 21 기준:

- `Vector.fromByteBuffer(...)` — **삭제됨**(JDK 19에서 deprecate, 이후 제거).
- `Vector.fromMemorySegment(...)` — **preview API.** `--enable-preview`가 필요하고, 그것은
  §7의 금지 목록에 있다(preview 플래그는 클래스 파일에 minor version을 새겨서 정확히 그 JDK
  빌드에서만 실행되게 만든다 — 운영 노드에 배포할 수 없는 산출물이 된다).

남는 것은 **`fromArray(SPECIES, long[], offset)`** 뿐이다. 결과적으로 커널의 입력은
`ByteBuffer`가 아니라 `long[]` + `wordOffset`이어야 하고, 이는 API 설계 결정이 아니라 **제약**
이다. 청크 읽기 경로가 `ByteBuffer`를 들고 있으므로, 커널 진입 전에 워드 배열로 올리는 지점이
어디인지가 설계의 실질 내용이 된다.

> **250줄을 쓰기 전에 30분짜리 compile spike로 이것부터 검증한다.** `fromArray` 하나만 쓰는
> 최소 클래스를 `src/java-vector`에 두고 JDK 21로 컴파일 + 리플렉션 로드가 되는지 확인한다.
> 이 30분이 실패하면 §1의 #4는 그 자리에서 끝나고 #1~#3만 남는다 — 그것도 정상적인 결과다.

## 3. 가장 어려운 규칙: 벡터 경로는 **영원히 디코드 전용**

**`pack`, `chooseWidth`, `BlockPresence.encode`, `BlockPresence.chooseMode`, 그리고 모든 ALP
planning은 단일 구현 스칼라로 남는다.** 예외 없음. "인코드도 벡터화하면 flush가 빨라진다"는
제안은 미래에 반드시 다시 나오고, 그때 이 절을 인용해서 거절한다.

이유는 성능이 아니라 **실패 모드의 비대칭성** 이다.

| | 디코드 불일치 | 인코드 불일치 |
|---|---|---|
| 증상 | 틀린 값을 반환 | 같은 입력이 다른 바이트를 생성 |
| 탐지 | **differential test가 즉시 잡는다** | round-trip test는 **전부 green으로 통과한다** |
| 운영 영향 | 질의 결과 오류(눈에 보임) | `chunkUnchanged`가 영원히 "달라졌다"고 보고 → **re-encoder livelock** |

인코드 쪽 두 번째 구현은 바이트 결정성([chunk-format-v4.md](chunk-format-v4.md) §5 rule 5)을
깨는 가장 값싼 방법이고, 그 깨짐은 테스트를 통과한 채로 배포되어 컴팩션이 멈추지 않는 형태로
드러난다. 디코드 쪽 두 번째 구현은 틀리면 시끄럽게 틀린다. 그래서 한쪽만 허용한다.

## 4. 정확성 — differential property test

오라클은 **기존 스칼라 `BitPacking.unpack`** 이고, 그 스칼라 자체는 이미 스펙의 golden vector에
핀되어 있다. 그 위에 다음 곱집합 전체를 도는 differential property test를 둔다.

| 축 | 값 |
|---|---|
| width | **65개 전부** (0..64) |
| count | 0, 1, 63, 64, 65, 127, 128, 1023, 1024, 1025, 4096 |
| value pattern | adversarial (all-zero, all-ones, 최상위 비트만, 경계값, 난수) |
| wordOffset | 0, non-zero |
| kernel | Scalar, vector @ `SPECIES_128` / `SPECIES_256` / `SPECIES_512` / `SPECIES_PREFERRED` |

**양쪽 모두 직접 인스턴스화로 강제한다** — 홀더가 고른 구현을 쓰면, 벡터 커널이 로드되지 않은
환경에서 테스트가 조용히 **스칼라 대 스칼라**를 비교하며 green이 된다. 그 green은 정보가 0이고
정보가 0인 green은 위험하다.

같은 이유로 **`Assume.assumeTrue`로 skip하는 테스트는 없는 것보다 나쁘다.** JDK 21 테스트 JVM
인자에 **`-Dcassandra.test.require_vector_kernel=true`** 를 넣어, 커널이 없으면 skip이 아니라
**fail** 하게 한다. 그래야 커널이 CI에서 조용히 사라지지 않는다.

### 4.1 실행 구성 3종

| 구성 | 무엇을 증명하는가 | 무엇을 증명하지 못하는가 |
|---|---|---|
| AVX 없는 벤치 호스트 (2 × Xeon X5670, SSE4.2) | **모든 lane arithmetic의 bit-identity** — Vector API는 SIMD가 없으면 스칼라로 폴백해 실행되므로 정확성은 전부 검증된다 | 속도, AVX-512 코드 경로 |
| 호스트 237 (Cascade Lake) | AVX-512 경로 + 실측 속도 | AVX2-only 하드웨어에서의 거동 |
| 호스트 237 + `-XX:UseAVX=2` | **AVX2-only 하드웨어의 대역**(node 41이 그럴 수 있다) | — |

세 번째 구성이 있는 이유는 §8이다. 운영 노드가 AVX2까지만이면 게이트는 그 조건에서 통과해야
한다.

## 5. 게이트 — 순차적이며, 하나라도 실패하면 vector 경로를 삭제한다

**임계값은 측정 전에 적는다. 측정 후에 적으면 그것은 게이트가 아니라 사후 합리화다.**

| 게이트 | 대상 | 임계값 |
|---|---|---|
| **A** | kernel microbench vs **specialized scalar** (branchy 스칼라 아님) | ≥ **2.0×** |
| **B** | block decode 전체 | ≥ **1.25×** |
| **C** | chunk read | ≥ **1.10×** |
| **D** | query battery ([tiering-benchmark.md](tiering-benchmark.md)와 동일 질의) | ≥ **1.05×** |
| **E** | 이득이 **AVX-512DQ에서만** 존재하고 node 41에 그 플래그가 없다면 | **무조건 삭제** |

### 5.1 SKU 함정 — Silver 4210R

호스트 237의 Xeon Silver 4210R은 **512비트 FMA 포트가 하나뿐**이고 AVX-512 실행 시 **주파수가
내려간다**(downclocking). 즉 SPECIES_512가 SPECIES_256보다 이론상 2배여도 실측은 그렇지 않고,
경우에 따라 **더 느리다.** 게다가 downclock은 같은 코어의 **다른 스레드**(= Cassandra의 나머지
전부)까지 느리게 만든다 — microbench에는 절대 나타나지 않고 게이트 C/D에서만 나타나는 종류의
손해다.

> **규칙: SPECIES_256이 SPECIES_512의 ~20% 이내면 256으로 고정한다.** `SPECIES_PREFERRED`를
> 그냥 쓰지 않는다.

## 6. 예상 결과 — 미리 적어둔다

**A 통과, B 애매, C·D 실패.** 그 경우 **vector kernel과 빌드 배선을 삭제하고 스칼라 작업만
남긴다.**

이것은 실패가 아니라 **성공이다.** 결과물:

- 약 **4.5일**
- 인큐베이터 의존성 **없음**
- 빌드 특수 케이스 **없음**
- 시작 시 경고 **없음**
- 그리고 **얻을 수 있었던 이득의 거의 전부**(§0.3의 #1·#2 + §1의 #3)

이 문단이 미리 적혀 있는 이유는, 4일을 쓴 뒤에는 "그래도 커널은 남기자"는 결론이 반드시
매력적으로 보이기 때문이다. 그 시점의 판단은 이미 투입한 비용에 오염되어 있으므로, 삭제 결정은
투입 전에 내려두고 게이트 결과만 기계적으로 적용한다.

## 7. 전제 0 (미해결) — node 41의 CPU 플래그를 모른다

**벡터 코드를 한 줄이라도 쓰기 전에**, 운영 노드에서:

```
grep -m1 flags /proc/cpuinfo
```

확인할 것: `avx2`, `avx512f`, `avx512dq`, `avx512vpopcntdq`.

- `avx512dq`가 없으면 §1의 #5는 자동 탈락.
- `avx512vpopcntdq`는 호스트 237에도 없으므로 #9는 어차피 탈락.
- `avx2`조차 없으면 **이 문서의 §1 #4~#6 전체가 탈락**하고 스칼라 작업만 남는다.

[sp4-plan.md:125](sp4-plan.md)가 이 전제를 이미 "전제 1"로 적어두었고, **2026-08-03 현재도 여전히
확인되지 않았다.**

## 8. 하지 말 것

명시적으로 금지한다 — 나중에 누가 "좋은 아이디어"로 다시 제안하지 않도록.

1. **스칼라 경로를 느리게 만드는 어떤 변경도** 하지 않는다. 스칼라가 모든 JDK·모든 호스트의
   기본 경로다.
2. **쓰기 쪽 두 번째 구현**을 만들지 않는다(§3).
3. **SIMD를 돕기 위한 포맷 변경**을 하지 않는다. [chunk-format-v4.md](chunk-format-v4.md) §6이
   정렬 문제를 이미 정리했다.
4. "vector-friendly 변형"을 위한 **새 `blockEncoding`** 을 만들지 않는다. 인코딩이 하나 늘면
   디코더 경로가 하나 늘고, 그것은 §3의 비대칭성을 인코드 쪽으로 되가져온다.
5. **vectorized `rank`** 를 시도하지 않는다(§1 #9).
6. 버전 독립적인 **`conf/jvm-server.options`에 `--add-modules`** 를 넣지 않는다. 그 파일은 JDK
   11에서도 읽히고, 거기 들어간 `--add-modules jdk.incubator.vector`는 JDK 11 노드의 **시작을
   막는다.**
7. 게이트 C·D 통과 전에 **모듈을 기본 활성화**하지 않는다.
8. 모듈이 없을 때 **`StartupCheck`·WARN·예외**를 내지 않는다. 없는 것이 정상 상태다. 부재를
   알리는 로그 한 줄은 모든 노드의 모든 재시작에 붙는 노이즈이고, 운영자가 고칠 수 있는 문제를
   가리킨다는 잘못된 신호를 준다.
9. **`--enable-preview`** 를 어디에도 쓰지 않는다(§2).
10. **CPU 이름이 옆에 적혀 있지 않은 호스트의 벤치마크 숫자를 인용하지 않는다.** 이 문서의 모든
    수치에는 호스트가 붙어 있다. 그 규율이 깨지면 §0.2의 상한 계산이 무의미해진다.

## 9. 작업 순서

1. **전제 0** — node 41 `/proc/cpuinfo` 확인(§7). *(미완)*
2. **#1 RLE word-at-a-time expansion** — `BlockPresence.setBits`. *(완료: differential property
   test 포함)*
3. **#2 running rank 규칙** — `BlockPresence.rank` javadoc에 규범으로 명시. v4 block cursor를
   쓸 때 지킨다. *(규칙 명시 완료, 커서 미작성)*
4. **#3 width-specialized 스칼라 커널** — 65폭. 게이트 A의 비교 기준선이 된다.
5. **compile spike 30분**(§2) — 실패하면 여기서 종료.
6. **#4 vector unpack kernel** + differential test(§4) + 게이트 A~E(§5).
7. 게이트 실패 시 **6번의 산출물과 빌드 배선을 삭제**하고 4번까지를 유지한다(§6).
