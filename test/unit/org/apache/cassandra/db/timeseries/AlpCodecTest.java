/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link AlpCodec}, the only double encoding of the columnar (v3) chunk format.
 * <p>
 * The first two tests are the load-bearing ones. ALP is <b>not</b> backstopped by a fallback codec:
 * chimp128 was retired from this path, so if the exception mechanism and ALP-RD do not between them
 * carry every 64-bit pattern exactly, doubles are silently corrupted at rest with nothing to catch
 * it. Everything else here -- sizes, variant choice, determinism -- is secondary to that.
 * <p>
 * Sizes are asserted as ratios against {@link Chimp128Codec#encodeValues}, the encoding ALP
 * replaced, so a future regression in the encoder shows up as a failing number rather than as an
 * unnoticed drift. The measured values are printed too; see {@code doc/timeseries/codec-bakeoff.md}.
 */
public class AlpCodecTest
{
    /**
     * Bit patterns chosen because they are the ones a scaled-integer codec gets wrong if its
     * acceptance test is sloppy. Each is a raw {@code long}, never a {@code double} literal, so the
     * test itself cannot lose a signalling NaN on the way in.
     */
    private static final long[] EDGE_CASE_BITS =
    {
        0x0000000000000000L,   // +0.0
        0x8000000000000000L,   // -0.0  -- decodes numerically equal to +0.0, so only a BIT test rejects it
        0x7FF0000000000000L,   // +Infinity
        0xFFF0000000000000L,   // -Infinity
        0x7FF8000000000000L,   // canonical quiet NaN
        0xFFF8000000000000L,   // canonical quiet NaN, sign bit set
        0x7FF8000000000001L,   // quiet NaN, payload 1
        0x7FFFFFFFFFFFFFFFL,   // quiet NaN, all payload bits set
        0xFFFFFFFFFFFFFFFFL,   // quiet NaN, all payload bits set, sign bit set
        0x7FF0000000000001L,   // SIGNALLING NaN -- longBitsToDouble is permitted to quiet this one
        0xFFF0000000000001L,   // signalling NaN, sign bit set
        0x7FF4000000000000L,   // signalling NaN, high payload bit
        0x0000000000000001L,   // Double.MIN_VALUE, the smallest subnormal
        0x8000000000000001L,   // -Double.MIN_VALUE
        0x000FFFFFFFFFFFFFL,   // largest subnormal
        0x0010000000000000L,   // Double.MIN_NORMAL
        0x7FEFFFFFFFFFFFFFL,   // Double.MAX_VALUE
        0xFFEFFFFFFFFFFFFFL,   // -Double.MAX_VALUE
    };

    /** Ordinary values that must survive alongside the pathological ones. */
    private static final double[] EDGE_CASE_VALUES =
    {
        1.0, -1.0, 0.1, -0.1, -0.01, 0.5, 0.25, 1.0 / 3.0, Math.PI, Math.E,
        20.76, 157.0, 1000.0, 1e18, 1e-18, 1e300, 1e-300, 123456789.123456789,
        9007199254740992.0,    // 2^53, the largest integer every larger double skips past
        9007199254740994.0,    // 2^53 + 2
        (double) Long.MAX_VALUE,
        Float.MIN_VALUE,
    };

    /**
     * The single most important test in the codec. Every edge-case pattern must come back
     * bit-identical, both as a column of its own and injected into a column ALP would otherwise
     * encode densely -- the latter is what actually exercises the exception machinery, because a
     * column that is <em>all</em> exceptions never builds a packed body at all.
     */
    @Test
    public void everyEdgeCaseBitPatternRoundTripsExactly()
    {
        long[] all = edgeCaseColumn();
        assertRoundTrip("all edge cases in one column", all);

        // One value per column, so nothing can round-trip by accident through a neighbour.
        for (long bits : all)
            assertRoundTrip("single value " + hex(bits), new long[]{ bits, bits });

        // And injected at the head, the middle and the tail of a dense decimal column, which is
        // where a mis-ordered exception position or an off-by-one in the packed body would show.
        long[] dense = sensorWalkTwoDecimals(200, 7);
        for (long bits : all)
        {
            long[] mixed = dense.clone();
            mixed[0] = bits;
            mixed[97] = bits;
            mixed[mixed.length - 1] = bits;
            assertRoundTrip("edge case " + hex(bits) + " injected into a decimal column", mixed);
        }
    }

    /**
     * NaN is the pattern most likely to be lost, because it is the one Java itself is allowed to
     * rewrite: {@code Double.doubleToLongBits} canonicalises every payload, and
     * {@code longBitsToDouble} is specified as permitted to quiet a signalling NaN. Sweeping every
     * payload bit position in both signs proves the value pipeline never passes a pattern through
     * either of those.
     */
    @Test
    public void everyNaNPayloadRoundTripsExactly()
    {
        for (int signBit = 0; signBit <= 1; signBit++)
        {
            for (int payloadBit = 0; payloadBit < 52; payloadBit++)
            {
                long nan = (signBit == 1 ? Long.MIN_VALUE : 0L) | 0x7FF0000000000000L | (1L << payloadBit);
                assertRoundTrip("NaN payload " + hex(nan),
                                new long[]{ nan, bits(1.5), nan, bits(2.5), nan });
            }
        }
    }

    @Test
    public void generatedDistributionsRoundTripExactly()
    {
        for (Map.Entry<String, long[]> distribution : distributions(3600).entrySet())
            assertRoundTrip(distribution.getKey(), distribution.getValue());

        // Shapes the distribution table does not cover.
        assertRoundTrip("all NaN, one payload", constantColumn(400, bits(Double.NaN)));
        assertRoundTrip("all NaN, differing payloads", nanPayloadColumn(400));
        assertRoundTrip("single value", new long[]{ bits(20.76) });
        assertRoundTrip("two values", new long[]{ bits(-0.0), bits(0.0) });
        for (int n = 1; n <= 8; n++)
            assertRoundTrip("tiny column n=" + n, sensorWalkTwoDecimals(n, 3));
        // Longer than the sampling stride's first bucket, so the (e, f) search really samples.
        assertRoundTrip("50k rows", sensorWalkTwoDecimals(50_000, 5));
    }

    /**
     * Two replicas re-encoding the same window must converge, because
     * {@code TieredStorageService.chunkUnchanged} decides "nothing changed" by comparing payload
     * bytes. Anything order-, identity- or timing-dependent in the encoder breaks that and the
     * chunk is rewritten every cycle forever.
     */
    @Test
    public void encodingIsByteIdenticalAcrossRunsAndFreshInputs()
    {
        for (Map.Entry<String, long[]> distribution : distributions(1000).entrySet())
        {
            long[] values = distribution.getValue();
            byte[] first = AlpCodec.encode(values, values.length);
            byte[] second = AlpCodec.encode(values, values.length);
            // A separately-built array holding the same numbers: the "other node" case, where
            // nothing but the values themselves is shared.
            byte[] onAnotherNode = AlpCodec.encode(Arrays.copyOf(values, values.length), values.length);

            assertArrayEquals(distribution.getKey() + ": re-encoding must be byte-identical", first, second);
            assertArrayEquals(distribution.getKey() + ": a fresh input array must encode identically",
                              first, onAnotherNode);
        }
    }

    /**
     * The variant choice must be a strict total order, never a coin flip: ALP-RD is used only when
     * it is <b>strictly</b> smaller, so an exact tie always resolves to decimal ALP.
     */
    @Test
    public void variantChoiceIsStrictAndDeterministic()
    {
        // Decimal-like data: ALP must win outright, and the section must say so.
        for (String name : new String[]{ "production 2-decimal sensor walk", "production integral walk",
                                         "near-constant", "quantized sine" })
        {
            long[] values = distributions(3600).get(name);
            assertEquals(name + " must choose decimal ALP",
                         AlpCodec.SUBFORMAT_ALP, AlpCodec.choose(values, values.length).subFormat());
            assertEquals(name + " section must declare decimal ALP",
                         AlpCodec.SUBFORMAT_ALP, AlpCodec.encode(values, values.length)[0]);
        }

        // High-entropy bit patterns: nothing is decimal, so ALP-RD must win.
        long[] random = distributions(3600).get("random 64-bit patterns");
        assertEquals("random bit patterns must choose ALP-RD",
                     AlpCodec.SUBFORMAT_ALP_RD, AlpCodec.choose(random, random.length).subFormat());
        assertEquals("random bit patterns section must declare ALP-RD",
                     AlpCodec.SUBFORMAT_ALP_RD, AlpCodec.encode(random, random.length)[0]);

        // The choice is reproducible: same input, same answer, every time.
        for (Map.Entry<String, long[]> distribution : distributions(500).entrySet())
        {
            long[] values = distribution.getValue();
            byte expected = AlpCodec.choose(values, values.length).subFormat();
            for (int repeat = 0; repeat < 5; repeat++)
                assertEquals(distribution.getKey() + ": variant choice must not vary between calls",
                             expected, AlpCodec.choose(values, values.length).subFormat());
        }
    }

    /**
     * The variant choice compares <em>predicted</em> sizes so the loser is never encoded. That is
     * only sound if a plan's predicted size is exactly what it produces -- otherwise the encoder
     * could pick the larger of the two while believing it picked the smaller.
     */
    @Test
    public void aPlanPredictsItsExactEncodedSize()
    {
        List<long[]> columns = new ArrayList<>(distributions(1000).values());
        columns.add(edgeCaseColumn());
        columns.add(nanPayloadColumn(64));
        for (int n = 1; n <= 8; n++)
            columns.add(sensorWalkTwoDecimals(n, 3));

        for (long[] values : columns)
        {
            int n = values.length;
            AlpCodec.Plan alp = AlpCodec.planAlp(values, n);
            AlpCodec.Plan rd = AlpCodec.planRd(values, n);
            assertEquals("decimal ALP plan size must match its output", alp.sizeInBytes, alp.encode(values, n).length);
            assertEquals("ALP-RD plan size must match its output", rd.sizeInBytes, rd.encode(values, n).length);
            // ...and the codec really does emit the smaller of the two.
            assertEquals("encode() must emit the smaller plan",
                         Math.min(alp.sizeInBytes, rd.sizeInBytes), AlpCodec.encode(values, n).length);
        }
    }

    /**
     * The recorded compression numbers, versus the chimp128 value stream ALP replaced. Chimp128 is
     * kept alive precisely so this comparison stays reproducible -- see
     * {@link Chimp128Codec#encodeValues}.
     * <p>
     * The bounds are deliberately loose enough to survive an unrelated encoder tweak and tight
     * enough that losing the frame-of-reference packing, the exception path or the {@code (e, f)}
     * search would trip them. The exact measurements are printed on stdout.
     */
    @Test
    public void productionShapedColumnsCompressFarBetterThanChimp128()
    {
        Map<String, Double> ratios = reportRatios(3600);

        // Every shape the production table actually produces (docker/scale-workload.py: value_numeric
        // is a bounded random walk that is either integral or rounded to 2 decimals, alongside
        // constant-in-practice columns). ALP must beat chimp128 by a wide margin on all of them.
        assertRatioBelow(ratios, "production 2-decimal sensor walk", 0.70);
        assertRatioBelow(ratios, "production integral walk", 0.55);
        assertRatioBelow(ratios, "near-constant", 0.05);
        assertRatioBelow(ratios, "quantized 0.1 walk", 0.70);
        assertRatioBelow(ratios, "quantized sine", 0.60);
        assertRatioBelow(ratios, "3-decimal pressure walk", 0.70);
        assertRatioBelow(ratios, "monotonic integral counter", 0.80);
    }

    /**
     * The honest other half of the size story, recorded rather than hidden.
     * <p>
     * On <b>unquantized</b> doubles that vary smoothly -- a Gaussian random walk, an unrounded sine
     * -- chimp128 is slightly <em>smaller</em> than ALP-RD, by a few percent. This is structural,
     * not a tuning miss: chimp XORs each value against a similar recent one, so it profits from
     * <em>sequential</em> correlation, while ALP-RD models only the static distribution of bit
     * patterns and cuts every value at the same place. Raising the ALP-RD dictionary size does not
     * help; the low ~53 mantissa bits of such a series are close to incompressible without a
     * sequential model, which is what sets ALP-RD's floor here.
     * <p>
     * None of these shapes occur in the measured production distribution (all of its numeric
     * readings are decimal-quantised), and on uncorrelated full-precision data ALP-RD is the
     * smaller of the two. This test pins the size of the loss so that it stays a known few percent
     * instead of quietly growing.
     */
    @Test
    public void fullPrecisionColumnsStayWithinTheRecordedMarginOfChimp128()
    {
        Map<String, Double> ratios = reportRatios(3600);

        assertRatioBelow(ratios, "full-precision gaussian walk", 1.05);
        assertRatioBelow(ratios, "full-precision sine", 1.08);
        // Where full-precision values are NOT sequentially correlated, chimp's XOR has nothing to
        // exploit and ALP-RD is the smaller of the two.
        assertRatioBelow(ratios, "uniform [0,1) doubles", 1.00);
        assertRatioBelow(ratios, "random 64-bit patterns", 1.00);
    }

    // ---------------------------------------------------------------------------------------
    // Corruption
    // ---------------------------------------------------------------------------------------

    /**
     * A section that stops short must be reported as corruption, not as an
     * {@link IndexOutOfBoundsException} escaping from the bit reader: the read path skips a chunk
     * on {@link IllegalArgumentException} and only on that.
     */
    @Test
    public void everyTruncationOfASectionIsRejectedAsCorruption()
    {
        for (long[] values : new long[][]{ sensorWalkTwoDecimals(64, 3), randomBitPatterns(64, 3), edgeCaseColumn() })
        {
            byte[] section = AlpCodec.encode(values, values.length);
            int count = values.length;
            for (int cut = 1; cut <= section.length; cut++)
            {
                byte[] truncated = Arrays.copyOf(section, section.length - cut);
                assertThatThrownBy(() -> AlpCodec.decode(ByteBuffer.wrap(truncated), count))
                    .as("truncated by " + cut + " of " + section.length + " bytes")
                    .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @Test
    public void corruptHeaderFieldsAreRejectedBeforeAnyAllocation()
    {
        long[] decimal = sensorWalkTwoDecimals(64, 3);
        byte[] alp = AlpCodec.encode(decimal, decimal.length);
        assertEquals("fixture must be a decimal ALP section", AlpCodec.SUBFORMAT_ALP, alp[0]);

        assertRejected("unknown sub-format", withByte(alp, 0, (byte) 0x7F), decimal.length);
        assertRejected("exponent above the maximum", withByte(alp, 1, (byte) 100), decimal.length);
        assertRejected("factor above the exponent", withByte(alp, 2, (byte) (alp[1] + 1)), decimal.length);
        assertRejected("bit width beyond the format maximum",
                       withByte(alp, 3, (byte) (AlpCodec.MAX_BIT_WIDTH + 1)), decimal.length);
        // A count no section of this many rows could carry, encoded as a large but int-positive
        // varint so it survives the cast -- the same trap the dictionary-size guard is tested with
        // in ColumnarChunkCodecTest: a value that goes negative would be caught by the array
        // allocator anyway and would not prove the bound check works.
        assertRejected("exception count beyond the row count", withVarLongAt(alp, 4 + varLen(alp, 4), 0x20000000L),
                       decimal.length);

        long[] random = randomBitPatterns(64, 3);
        byte[] rd = AlpCodec.encode(random, random.length);
        assertEquals("fixture must be an ALP-RD section", AlpCodec.SUBFORMAT_ALP_RD, rd[0]);

        assertRejected("left bit width of zero", withByte(rd, 1, (byte) 0), random.length);
        assertRejected("left bit width beyond 16", withByte(rd, 1, (byte) 17), random.length);
        assertRejected("dictionary size of zero", withByte(rd, 2, (byte) 0), random.length);
        assertRejected("dictionary size beyond 8", withByte(rd, 2, (byte) 9), random.length);
    }

    /**
     * A code the dictionary does not have must be rejected rather than indexing past it. The code
     * field is {@code ceil(log2(dictionarySize))} bits wide, so a dictionary of 5..7 entries leaves
     * codes that are expressible but invalid; this builds exactly that case.
     */
    @Test
    public void anOutOfRangeAlpRdDictionaryCodeIsRejected()
    {
        // Exactly three distinct high 16 bits over otherwise-random low bits, so the widest cut wins
        // with a 3-entry dictionary -- and 3 entries need 2-bit codes, leaving code 3 expressible
        // but invalid. That gap is the thing under test; a power-of-two dictionary would not have one.
        int[] highParts = { 0x3FF0, 0x4030, 0xBFE8 };
        long[] values = new long[400];
        Random random = new Random(4);
        for (int i = 0; i < values.length; i++)
            values[i] = ((long) highParts[i % 7 == 0 ? (i / 7) % 3 : 0] << 48) | (random.nextLong() & 0xFFFFFFFFFFFFL);

        AlpCodec.RdPlan plan = AlpCodec.planRd(values, values.length);
        assertEquals("fixture must cut at the full 16 bits", 16, plan.leftBits);
        assertEquals("fixture must produce a 3-entry dictionary", 3, plan.dictionary.length);
        assertTrue("a 3-entry dictionary must leave an expressible-but-invalid code",
                   (1 << AlpCodec.codeBitsFor(plan.dictionary.length)) > plan.dictionary.length);

        byte[] section = plan.encode(values, values.length);
        // Setting the body's leading bits turns the first row's code into the largest value its
        // width can express, which is necessarily >= the dictionary size for this fixture.
        int bodyStart = section.length - packedBodyBytes(plan, values.length);
        byte[] corrupted = section.clone();
        corrupted[bodyStart] = (byte) 0xFF;
        assertThatThrownBy(() -> AlpCodec.decode(ByteBuffer.wrap(corrupted), values.length))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dictionary size");
    }

    /**
     * No single-bit corruption anywhere in a section may escape as an exception type the read path
     * does not catch. It is entirely fine for a flipped bit to decode to different values -- most
     * of the section is packed data with no redundancy -- but not to throw something that kills the
     * whole query instead of skipping one chunk.
     */
    @Test
    public void singleBitCorruptionOnlyEverSurfacesAsIllegalArgument()
    {
        long[] values = sensorWalkTwoDecimals(50, 11);
        values[3] = 0x7FF0000000000001L;   // force an exception into the section as well
        for (byte[] section : new byte[][]{ AlpCodec.encode(values, values.length),
                                            AlpCodec.planRd(values, values.length).encode(values, values.length) })
        {
            for (int position = 0; position < section.length; position++)
            {
                for (int bit = 0; bit < 8; bit++)
                {
                    byte[] corrupted = section.clone();
                    corrupted[position] ^= (byte) (1 << bit);
                    try
                    {
                        AlpCodec.decode(ByteBuffer.wrap(corrupted), values.length);
                    }
                    catch (IllegalArgumentException expected)
                    {
                        // the one type the read path is allowed to see
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Distributions and helpers
    // ---------------------------------------------------------------------------------------

    /**
     * The shapes this codec is measured on. The first two reproduce {@code docker/scale-workload.py}'s
     * model of the production {@code value_numeric} column, which is a bounded random walk that is
     * either integral (tags typed {@code int}/{@code long}) or rounded to two decimals (tags typed
     * {@code double}/{@code float}); the rest are the reference patterns from the original codec
     * bake-off plus the two full-precision shapes where chimp128 still edges ALP out.
     */
    private static Map<String, long[]> distributions(int n)
    {
        long seed = 20260801L;
        Map<String, long[]> out = new LinkedHashMap<>();
        out.put("production 2-decimal sensor walk", sensorWalkTwoDecimals(n, seed));
        out.put("production integral walk", integralWalk(n, seed));
        out.put("near-constant", nearConstant(n));
        out.put("quantized 0.1 walk", quantizedWalk(n, seed));
        out.put("quantized sine", quantizedSine(n));
        out.put("3-decimal pressure walk", pressureWalk(n, seed));
        out.put("monotonic integral counter", monotonicCounter(n, seed));
        out.put("full-precision gaussian walk", fullPrecisionWalk(n, seed));
        out.put("full-precision sine", fullPrecisionSine(n));
        out.put("uniform [0,1) doubles", uniformDoubles(n, seed));
        out.put("random 64-bit patterns", randomBitPatterns(n, seed));
        return out;
    }

    private static long[] sensorWalkTwoDecimals(int n, long seed)
    {
        Random random = new Random(seed);
        double[] steps = { -0.01, 0.0, 0.0, 0.01 };
        double value = 10.0 + random.nextDouble() * 80.0;
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
        {
            value = clamp(value + steps[random.nextInt(4)], 0.0, 100.0);
            out[i] = bits(Math.round(value * 100.0) / 100.0);
        }
        return out;
    }

    private static long[] integralWalk(int n, long seed)
    {
        Random random = new Random(seed);
        double[] steps = { -1.0, 0.0, 0.0, 1.0 };
        double value = random.nextInt(80) + 10;
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
        {
            value = clamp(value + steps[random.nextInt(4)], 0.0, 1000.0);
            out[i] = bits(value);
        }
        return out;
    }

    /** A setpoint that is not quite constant, so the CONSTANT flag never fires and the codec sees it. */
    private static long[] nearConstant(int n)
    {
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
            out[i] = bits(i % 997 == 0 ? 192.5 : 192.0);
        return out;
    }

    private static long[] quantizedWalk(int n, long seed)
    {
        Random random = new Random(seed);
        double[] steps = { -0.1, 0.0, 0.0, 0.1 };
        double value = 50.0;
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
        {
            value += steps[random.nextInt(4)];
            out[i] = bits(Math.round(value * 10.0) / 10.0);
        }
        return out;
    }

    private static long[] quantizedSine(int n)
    {
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
            out[i] = bits(Math.round(Math.sin(i / 50.0) * 1000.0) / 100.0);
        return out;
    }

    private static long[] pressureWalk(int n, long seed)
    {
        Random random = new Random(seed);
        double value = 101325.0;
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
        {
            value += (random.nextInt(3) - 1) * 0.001;
            out[i] = bits(Math.round(value * 1000.0) / 1000.0);
        }
        return out;
    }

    private static long[] monotonicCounter(int n, long seed)
    {
        Random random = new Random(seed);
        double value = 0.0;
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
        {
            value += random.nextInt(5);
            out[i] = bits(value);
        }
        return out;
    }

    private static long[] fullPrecisionWalk(int n, long seed)
    {
        Random random = new Random(seed);
        double value = 50.0;
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
        {
            value += random.nextGaussian();
            out[i] = bits(value);
        }
        return out;
    }

    private static long[] fullPrecisionSine(int n)
    {
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
            out[i] = bits(Math.sin(i / 50.0) * 37.42);
        return out;
    }

    private static long[] uniformDoubles(int n, long seed)
    {
        Random random = new Random(seed);
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
            out[i] = bits(random.nextDouble());
        return out;
    }

    private static long[] randomBitPatterns(int n, long seed)
    {
        Random random = new Random(seed);
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
            out[i] = random.nextLong();
        return out;
    }

    private static long[] constantColumn(int n, long value)
    {
        long[] out = new long[n];
        Arrays.fill(out, value);
        return out;
    }

    /** All NaN, but a different payload every row -- so it is not CONSTANT and must reach the codec. */
    private static long[] nanPayloadColumn(int n)
    {
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
            out[i] = 0x7FF8000000000000L | ((i + 1) & 0xFFFFFFFFFFFFFL);
        return out;
    }

    private static long[] edgeCaseColumn()
    {
        long[] out = new long[EDGE_CASE_BITS.length + EDGE_CASE_VALUES.length];
        System.arraycopy(EDGE_CASE_BITS, 0, out, 0, EDGE_CASE_BITS.length);
        for (int i = 0; i < EDGE_CASE_VALUES.length; i++)
            out[EDGE_CASE_BITS.length + i] = bits(EDGE_CASE_VALUES[i]);
        return out;
    }

    private static double clamp(double value, double low, double high)
    {
        return value < low ? low : Math.min(value, high);
    }

    private static long bits(double value)
    {
        return Double.doubleToRawLongBits(value);
    }

    private static String hex(long bits)
    {
        return String.format("0x%016X", bits);
    }

    /**
     * Encodes, decodes and compares <b>bit patterns</b>, never {@code double}s: {@code assertEquals}
     * on doubles would pass for {@code 0.0} against {@code -0.0} and fail for every NaN against
     * itself, which is the opposite of what this codec has to guarantee.
     */
    private static void assertRoundTrip(String what, long[] values)
    {
        int n = values.length;
        byte[] section = AlpCodec.encode(values, n);
        long[] decoded = AlpCodec.decode(ByteBuffer.wrap(section), n);
        assertEquals(what + ": row count", n, decoded.length);
        for (int i = 0; i < n; i++)
            if (decoded[i] != values[i])
                throw new AssertionError(what + ": row " + i + " expected " + hex(values[i]) +
                                         " but decoded " + hex(decoded[i]));
    }

    /** ALP bytes / chimp128 bytes per distribution, printed and returned. */
    private static Map<String, Double> reportRatios(int n)
    {
        Map<String, Double> ratios = new LinkedHashMap<>();
        System.out.printf("ALP vs chimp128, %d values/column%n", n);
        System.out.printf("  %-34s %-7s %9s %9s %9s %9s %8s%n",
                          "pattern", "variant", "alp B", "chimp B", "alp B/val", "chimp B/val", "alp/chimp");
        for (Map.Entry<String, long[]> distribution : distributions(n).entrySet())
        {
            long[] values = distribution.getValue();
            byte[] section = AlpCodec.encode(values, n);
            int chimp = chimp128Bytes(values);
            double ratio = section.length / (double) chimp;
            ratios.put(distribution.getKey(), ratio);
            System.out.printf("  %-34s %-7s %9d %9d %9.3f %9.3f %8.3f%n", distribution.getKey(),
                              AlpCodec.choose(values, n).subFormat() == AlpCodec.SUBFORMAT_ALP ? "ALP" : "ALP-RD",
                              section.length, chimp, section.length / (double) n, chimp / (double) n, ratio);
        }
        return ratios;
    }

    /** What the retired DOUBLE_CHIMP section would have cost for the same values. */
    private static int chimp128Bytes(long[] rawBits)
    {
        double[] values = new double[rawBits.length];
        for (int i = 0; i < rawBits.length; i++)
            values[i] = Double.longBitsToDouble(rawBits[i]);
        BitWriter bits = new BitWriter();
        Chimp128Codec.encodeValues(bits, values, values.length);
        return bits.sizeInBytes();
    }

    private static void assertRatioBelow(Map<String, Double> ratios, String pattern, double bound)
    {
        Double ratio = ratios.get(pattern);
        assertTrue("no measurement recorded for " + pattern, ratio != null);
        assertTrue(pattern + ": ALP is " + String.format("%.4f", ratio) + "x the size of chimp128, bound is " +
                   bound + "x", ratio < bound);
    }

    private static void assertRejected(String what, byte[] section, int count)
    {
        assertThatThrownBy(() -> AlpCodec.decode(ByteBuffer.wrap(section), count))
            .as(what).isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] withByte(byte[] section, int index, byte value)
    {
        byte[] copy = section.clone();
        copy[index] = value;
        return copy;
    }

    /** Overwrites the varint at {@code index} with a 5-byte one encoding {@code value}. */
    private static byte[] withVarLongAt(byte[] section, int index, long value)
    {
        byte[] copy = section.clone();
        for (int i = 0; i < 4; i++)
        {
            copy[index + i] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        copy[index + 4] = (byte) (value & 0x7F);
        return copy;
    }

    /** Length in bytes of the LEB128 varint starting at {@code index}. */
    private static int varLen(byte[] section, int index)
    {
        int length = 1;
        while ((section[index + length - 1] & 0x80) != 0)
            length++;
        return length;
    }

    private static int packedBodyBytes(AlpCodec.RdPlan plan, int count)
    {
        long bodyBits = (long) count * (AlpCodec.codeBitsFor(plan.dictionary.length) + 64 - plan.leftBits);
        return (int) ((bodyBits + 7) >>> 3);
    }
}
