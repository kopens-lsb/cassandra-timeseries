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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import static org.apache.cassandra.db.timeseries.ChunkV4HeaderTest.hex;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests {@link AlpBlockCodec}, the chunk-format-v4 §5 double encodings {@code 0x20 ALP} and
 * {@code 0x21 ALP_RD}.
 *
 * <p>The load-bearing test is {@link #everyEdgeCaseBitPatternRoundTripsExactly}, for the reason
 * the retired v3 container's suite ({@code AlpCodecTest}, deleted with the v3 framing it tested)
 * established and which v4 inherits: <b>doubles have no fallback codec</b>. If decimal ALP's exception path and ALP-RD do not between them carry every
 * 64-bit pattern exactly, a double column is silently corrupted at rest with nothing to catch it.
 * Everything else here -- sizes, framing, the variant choice -- is secondary to that.
 *
 * <p>Everything else is about <b>byte determinism</b>, and it is tested the way §12 says the failure
 * arrives: not as a wrong value but as identical input producing different bytes on two replicas, at
 * which point {@code chunkUnchanged} reports a difference forever and every chunk in the cluster is
 * rewritten every cycle -- a failure that presents as a capacity problem rather than as a codec bug.
 * So the golden vectors are hand-derived from the field lists in {@link AlpBlockCodec}'s javadoc and
 * §6's bit order, never captured from the encoder; the encodes run into pre-dirtied buffers; and the
 * exact size of every candidate is asserted against the bytes it actually writes.
 *
 * <p>Sizes are reported as ratios against {@code RAW}, the universal fallback the argmin would
 * otherwise pick, and printed, so a future regression in the encoder shows up as a number.
 */
public class DoubleBlockCodecTest
{
    private static final int ALP = ChunkV4BlockTable.ENC_ALP;
    private static final int ALP_RD = ChunkV4BlockTable.ENC_ALP_RD;

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

    // -----------------------------------------------------------------------------------------
    // golden vectors
    // -----------------------------------------------------------------------------------------

    /**
     * {@code 0x20 ALP} over {@code 10.0, 11.0, 12.0, 13.0}.
     *
     * <p>Field by field: {@code exponent 0}, {@code factor 0} (these are integers, so no scaling is
     * needed and the search's (score asc, e asc, f asc) order reaches {@code (0, 0)} first);
     * {@code width 2}; {@code flags 0} because there is no exception area; four pad bytes; then the
     * frame of reference {@code 10} as an {@code i64}. The lane is the residuals {@code 0, 1, 2, 3}
     * at two bits, LSB-first per §6, i.e. {@code 0 | 1<<2 | 2<<4 | 3<<6 = 0xE4} in one big-endian
     * word. {@code 16 + 8 = 24} bytes.
     */
    private static final String GOLDEN_ALP_HEX =
        "00" + "00" + "02" + "00" + "00000000" +
        "000000000000000A" +
        "00000000000000E4";

    /**
     * {@code 0x20 ALP} over {@code 1.0, 2.0, NaN, 4.0} -- the exception path.
     *
     * <p>{@code flags} is now {@code 0x01}: an {@link ExceptionArea} follows. The reference is 1 and
     * the residuals are {@code 0, 1, <filler 0>, 3}, so the lane word is
     * {@code 0 | 1<<2 | 0<<4 | 3<<6 = 0xC4} -- the NaN's slot carries §5 rule 6's specified filler
     * rather than a truncated residual. The area is {@code excCount 1 | pad u16 | pad u32}, then
     * {@code position 2} beside the <b>verbatim 64-bit pattern</b>, padded from 10 to 16.
     * {@code 16 + 8 + 24 = 48} bytes.
     */
    private static final String GOLDEN_ALP_EXCEPTION_HEX =
        "00" + "00" + "02" + "01" + "00000000" +
        "0000000000000001" +
        "00000000000000C4" +
        "0001" + "0000" + "00000000" +
        "0002" + "7FF8000000000000" + "000000000000";

    /**
     * {@code 0x21 ALP_RD} over eight quiet NaNs with payloads 1..8.
     *
     * <p>Nothing here is decimal, so every value would be an ALP exception; ALP-RD instead cuts each
     * pattern at {@code leftBits 16}, where all eight share the high half {@code 0x7FF8}. A
     * one-entry dictionary needs no code at all ({@code codeBits 0}), so there is no code lane, and
     * the payload is the header, the dictionary padded from 2 bytes to 8, and the 48-bit low parts.
     *
     * <p>The low parts are {@code 1..8} at 48 bits, LSB-first, three words per four values:
     * {@code 1 | 2<<48}, then {@code 2>>>16 | 3<<32}, then {@code 3>>>32 | 4<<16}, and again for
     * {@code 5..8}. {@code 8 + 8 + 48 = 64} bytes, against 104 for ALP and 72 for RAW.
     */
    private static final String GOLDEN_ALP_RD_HEX =
        "10" + "01" + "00" + "00" + "00000000" +
        "7FF8" + "000000000000" +
        "0002000000000001" + "0000000300000000" + "0000000000040000" +
        "0006000000000005" + "0000000700000000" + "0000000000080000";

    /**
     * {@code 0x21 ALP_RD} with a two-entry dictionary: the same payloads, alternating between the
     * two canonical NaN high halves {@code 0x7FF8} and {@code 0xFFF8}.
     *
     * <p>The dictionary is written in <b>ascending</b> order, so {@code 0x7FF8} takes code 0 and
     * {@code 0xFFF8} code 1 -- not the frequency order {@link AlpCodec#selectRdDictionary} selects
     * in, which is what v3 wrote. The codes are then {@code 0,1,0,1,0,1,0,1} at one bit, LSB-first,
     * which is {@code 0xAA}. {@code 8 + 8 + 8 + 48 = 72} bytes.
     */
    private static final String GOLDEN_ALP_RD_DICT2_HEX =
        "10" + "02" + "00" + "00" + "00000000" +
        "7FF8" + "FFF8" + "00000000" +
        "00000000000000AA" +
        "0002000000000001" + "0000000300000000" + "0000000000040000" +
        "0006000000000005" + "0000000700000000" + "0000000000080000";

    @Test
    public void goldenVectorForDecimalAlp()
    {
        long[] values = { bits(10.0), bits(11.0), bits(12.0), bits(13.0) };
        // The lane word, derived from §6 rather than copied from the packer.
        assertEquals(0xE4L, 0L | (1L << 2) | (2L << 4) | (3L << 6));

        AlpBlockCodec.AlpPlan plan = AlpBlockCodec.planAlp(values, 4);
        assertEquals(0, plan.exponent);
        assertEquals(0, plan.factor);
        assertEquals(2, plan.width);
        assertEquals(10L, plan.reference);
        assertEquals(0, plan.exceptionCount);

        assertArrayEquals(hex(GOLDEN_ALP_HEX), encode(ALP, values));
        assertEquals(24, GOLDEN_ALP_HEX.length() / 2);
        assertRoundTrip("golden ALP", ALP, values);
        // A size decision, not a preference: ALP-RD would need 48 bytes for the same four values.
        assertEquals(48L, AlpBlockCodec.INSTANCE.payloadLength(ALP_RD, values, 4));
    }

    @Test
    public void goldenVectorForDecimalAlpWithAnException()
    {
        long[] values = { bits(1.0), bits(2.0), bits(Double.NaN), bits(4.0) };
        assertEquals(0xC4L, 0L | (1L << 2) | (0L << 4) | (3L << 6));

        AlpBlockCodec.AlpPlan plan = AlpBlockCodec.planAlp(values, 4);
        assertEquals(1, plan.exceptionCount);
        assertEquals(1L, plan.reference);
        assertEquals(2, plan.width);

        byte[] encoded = encode(ALP, values);
        assertArrayEquals(hex(GOLDEN_ALP_EXCEPTION_HEX), encoded);
        assertEquals(48, encoded.length);
        // The flag says the area is there; the length has to agree, and the decoder checks both.
        assertEquals(AlpBlockCodec.FLAG_HAS_EXCEPTIONS, encoded[3]);
        assertRoundTrip("golden ALP with exception", ALP, values);
    }

    @Test
    public void goldenVectorsForAlpRd()
    {
        long[] oneEntry = nanPayloads(8);
        AlpBlockCodec.RdPlan plan = AlpBlockCodec.planRd(oneEntry, 8);
        assertEquals(16, plan.leftBits);
        assertArrayEquals(new int[]{ 0x7FF8 }, plan.dictionary);
        assertEquals(0, plan.codeBits);
        assertEquals(0, plan.exceptionCount);
        assertArrayEquals(hex(GOLDEN_ALP_RD_HEX), encode(ALP_RD, oneEntry));
        assertEquals(64, GOLDEN_ALP_RD_HEX.length() / 2);
        assertRoundTrip("golden ALP-RD", ALP_RD, oneEntry);
        // Every value would be an ALP exception, which is what makes ALP-RD the cheaper variant.
        assertEquals(104L, AlpBlockCodec.INSTANCE.payloadLength(ALP, oneEntry, 8));

        long[] twoEntries = new long[8];
        for (int i = 0; i < twoEntries.length; i++)
            twoEntries[i] = (i % 2 == 0 ? 0x7FF8000000000000L : 0xFFF8000000000000L) | (i + 1);
        AlpBlockCodec.RdPlan dict2 = AlpBlockCodec.planRd(twoEntries, 8);
        assertArrayEquals("ascending, not frequency order", new int[]{ 0x7FF8, 0xFFF8 }, dict2.dictionary);
        assertEquals(1, dict2.codeBits);
        assertEquals(0xAAL, 0L | (1L << 1) | (1L << 3) | (1L << 5) | (1L << 7));
        assertArrayEquals(hex(GOLDEN_ALP_RD_DICT2_HEX), encode(ALP_RD, twoEntries));
        assertRoundTrip("golden ALP-RD, two entries", ALP_RD, twoEntries);
    }

    /**
     * A block with nothing the decimal path can represent still encodes as {@code 0x20 ALP} if asked:
     * width 0, reference 0, no lane, and every value in the exception area. The argmin will not
     * choose it -- ALP-RD is far smaller -- but a plan that produced a sentinel reference or an
     * un-encodable block would be a hole in the seam's "exact length, always" contract.
     */
    @Test
    public void anAllExceptionAlpBlockIsStillExactlySized()
    {
        long[] values = nanPayloads(4);
        AlpBlockCodec.AlpPlan plan = AlpBlockCodec.planAlp(values, 4);
        assertEquals(4, plan.exceptionCount);
        assertEquals(0, plan.width);
        // §5 rule 6: an unused field is zero, not a leftover sentinel from the measuring loop.
        assertEquals(0L, plan.reference);
        assertEquals(64, plan.payloadLength);
        assertRoundTrip("all-exception ALP", ALP, values);
    }

    // -----------------------------------------------------------------------------------------
    // bit exactness -- the test the whole codec exists to pass
    // -----------------------------------------------------------------------------------------

    /**
     * Every edge-case pattern must come back bit-identical under <b>both</b> encodings: on its own,
     * as a whole column, and injected into a dense decimal column -- the last is what actually
     * exercises the exception machinery, because a column that is <em>all</em> exceptions never
     * builds a packed lane at all.
     */
    @Test
    public void everyEdgeCaseBitPatternRoundTripsExactly()
    {
        long[] all = edgeCaseColumn();
        assertBothVariantsRoundTrip("all edge cases in one column", all);

        for (long pattern : all)
            assertBothVariantsRoundTrip("single value " + hexBits(pattern), new long[]{ pattern, pattern });

        long[] dense = sensorWalkTwoDecimals(200, 7);
        for (long pattern : all)
        {
            long[] mixed = dense.clone();
            mixed[0] = pattern;
            mixed[97] = pattern;
            mixed[mixed.length - 1] = pattern;
            assertBothVariantsRoundTrip("edge case " + hexBits(pattern) + " injected into a decimal column", mixed);
        }
    }

    /**
     * NaN is the pattern most likely to be lost, because it is the one Java itself is allowed to
     * rewrite: {@code Double.doubleToLongBits} canonicalises every payload and
     * {@code longBitsToDouble} is specified as permitted to quiet a signalling NaN. Sweeping every
     * payload bit position in both signs proves the value pipeline never passes a pattern through a
     * {@code double} as a way of carrying it.
     */
    @Test
    public void everyNanPayloadBitSurvivesBothVariants()
    {
        for (int bit = 0; bit < 52; bit++)
        {
            for (long sign : new long[]{ 0L, 0x8000000000000000L })
            {
                long pattern = sign | 0x7FF0000000000000L | (1L << bit);
                assertBothVariantsRoundTrip("NaN payload bit " + bit, new long[]{ pattern, bits(1.0), pattern });
            }
        }
    }

    /**
     * {@code -0.0} is the quietest of the failures: it decodes numerically equal to {@code +0.0}, so
     * a round-trip test that compared doubles would pass while the bits were being rewritten. The
     * decimal path must <em>refuse</em> it -- {@code AlpCodec}'s acceptance test is bit equality, and
     * {@code 0.0 * 10^e * 10^-f} scales to {@code +0.0} -- and pay eight bytes for it.
     */
    @Test
    public void signedZerosStayDistinctAndBecomeAlpExceptions()
    {
        long[] values = { bits(0.0), bits(-0.0), bits(0.0), bits(-0.0) };
        AlpBlockCodec.AlpPlan plan = AlpBlockCodec.planAlp(values, 4);
        assertEquals("the two -0.0 rows are exceptions, the two +0.0 rows are not", 2, plan.exceptionCount);

        long[] decoded = decode(ALP, encode(ALP, values), 4);
        assertArrayEquals(values, decoded);
        assertEquals(0x8000000000000000L, decoded[1]);
        assertNotEquals(decoded[0], decoded[1]);
        assertBothVariantsRoundTrip("signed zeros", values);
    }

    // -----------------------------------------------------------------------------------------
    // the seam's contract: exact sizes, never a trial encode
    // -----------------------------------------------------------------------------------------

    /**
     * {@link BlockEncodings.DoubleBlockCodec#payloadLength} must equal the encoded length exactly,
     * for both codes, on every shape. It is not an estimate and it is not a bound: the section lays
     * out every following block's {@code bodyOffset} from it before a byte exists, so a payload one
     * byte longer than the number scored corrupts the rest of the section.
     */
    @Test
    public void payloadLengthIsTheEncodedLengthForEveryCase()
    {
        for (Map.Entry<String, long[]> distribution : distributions(257).entrySet())
        {
            long[] values = distribution.getValue();
            for (int encoding : new int[]{ ALP, ALP_RD })
            {
                long declared = AlpBlockCodec.INSTANCE.payloadLength(encoding, values, values.length);
                assertTrue(distribution.getKey() + ": payloadLength " + declared, declared >= 0);
                assertEquals(distribution.getKey() + ": §6 alignment", 0, declared % 8);

                byte[] out = new byte[(int) declared];
                ByteBuffer dst = ByteBuffer.wrap(out);
                int written = AlpBlockCodec.INSTANCE.encode(encoding, values, values.length, dst);
                assertEquals(distribution.getKey() + " 0x" + Integer.toHexString(encoding), declared, written);
                assertEquals(declared, dst.position());
            }
        }
    }

    /** A length request the codec does not answer is a negative number, never a guess. */
    @Test
    public void payloadLengthDeclinesWhatIsNotItsOwn()
    {
        long[] values = { bits(1.0), bits(2.0) };
        // §5 gives a block with no present value exactly one encoding, and it is not this one.
        assertTrue(AlpBlockCodec.INSTANCE.payloadLength(ALP, new long[0], 0) < 0);
        assertTrue(AlpBlockCodec.INSTANCE.payloadLength(ALP_RD, new long[0], 0) < 0);
        for (int foreign : new int[]{ ChunkV4BlockTable.ENC_RAW, ChunkV4BlockTable.ENC_FOR_BITPACK,
                                      ChunkV4BlockTable.ENC_CONSTANT, ChunkV4BlockTable.ENC_DICT })
            assertTrue("0x" + Integer.toHexString(foreign),
                       AlpBlockCodec.INSTANCE.payloadLength(foreign, values, 2) < 0);
        assertThatThrownBy(() -> AlpBlockCodec.INSTANCE.payloadLength(ALP, values, 3))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AlpBlockCodec.INSTANCE.encode(ChunkV4BlockTable.ENC_RAW, values, 2,
                                                               ByteBuffer.allocate(64)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // byte determinism
    // -----------------------------------------------------------------------------------------

    /**
     * §5 rule 5: the same input packed into a scratch buffer full of stale bytes must produce the
     * same bytes as into a fresh one. The failure this catches leaves every round-trip test green
     * while making identical input produce different bytes on two replicas.
     */
    @Test
    public void encodeTwiceIntoDirtyBuffersIsByteIdentical()
    {
        for (Map.Entry<String, long[]> distribution : distributions(300).entrySet())
        {
            long[] values = distribution.getValue();
            for (int encoding : new int[]{ ALP, ALP_RD })
            {
                byte[] fresh = encode(encoding, values);
                assertArrayEquals(distribution.getKey(), fresh, encodeInto(encoding, values, (byte) 0xFF));
                assertArrayEquals(distribution.getKey(), fresh, encodeInto(encoding, values, (byte) 0x5A));
            }
        }
    }

    /** {@code encode(decode(encode(x))) == encode(x)}: the re-encoder's late-merge path (§11). */
    @Test
    public void reencodeIsIdempotent()
    {
        for (Map.Entry<String, long[]> distribution : distributions(511).entrySet())
        {
            long[] values = distribution.getValue();
            for (int encoding : new int[]{ ALP, ALP_RD })
            {
                byte[] first = encode(encoding, values);
                long[] decoded = decode(encoding, first, values.length);
                assertArrayEquals(distribution.getKey(), values, decoded);
                assertArrayEquals(distribution.getKey(), first, encode(encoding, decoded));
            }
        }
    }

    /**
     * The parameter search is a pure function of the input: the same block must yield the same
     * {@code (e, f)}, the same {@code leftBits} and the same dictionary however many times it is
     * planned, and whatever else the thread has planned in between.
     *
     * <p>That "in between" is not idle: the codec keeps its histogram and its lanes in per-thread
     * scratch, and scratch that leaked state between blocks would be a byte-determinism break that
     * only ever appeared on the second block of a section.
     */
    @Test
    public void theParameterSearchIsAFunctionOfTheInputAlone()
    {
        Random random = new Random(31337L);
        long[][] blocks = new long[8][];
        for (int i = 0; i < blocks.length; i++)
            blocks[i] = randomishDoubles(random, 1 + random.nextInt(400));

        AlpBlockCodec.AlpPlan[] alp = new AlpBlockCodec.AlpPlan[blocks.length];
        AlpBlockCodec.RdPlan[] rd = new AlpBlockCodec.RdPlan[blocks.length];
        for (int i = 0; i < blocks.length; i++)
        {
            alp[i] = AlpBlockCodec.planAlp(blocks[i], blocks[i].length);
            rd[i] = AlpBlockCodec.planRd(blocks[i], blocks[i].length);
        }
        // Interleaved, so a scratch buffer that carried a previous block's state would show.
        for (int repeat = 0; repeat < 3; repeat++)
        {
            for (int i = blocks.length - 1; i >= 0; i--)
            {
                AlpBlockCodec.AlpPlan again = AlpBlockCodec.planAlp(blocks[i], blocks[i].length);
                assertEquals("block " + i + " exponent", alp[i].exponent, again.exponent);
                assertEquals("block " + i + " factor", alp[i].factor, again.factor);
                assertEquals("block " + i + " width", alp[i].width, again.width);
                assertEquals("block " + i + " reference", alp[i].reference, again.reference);
                assertEquals("block " + i + " length", alp[i].payloadLength, again.payloadLength);

                AlpBlockCodec.RdPlan rdAgain = AlpBlockCodec.planRd(blocks[i], blocks[i].length);
                assertEquals("block " + i + " leftBits", rd[i].leftBits, rdAgain.leftBits);
                assertArrayEquals("block " + i + " dictionary", rd[i].dictionary, rdAgain.dictionary);
                assertEquals("block " + i + " length", rd[i].payloadLength, rdAgain.payloadLength);
            }
        }
    }

    /** The ALP-RD dictionary is ascending and duplicate-free (§5 rule 2), at every size. */
    @Test
    public void theRdDictionaryIsAscendingAndDuplicateFree()
    {
        for (Map.Entry<String, long[]> distribution : distributions(1024).entrySet())
        {
            AlpBlockCodec.RdPlan plan = AlpBlockCodec.planRd(distribution.getValue(),
                                                             distribution.getValue().length);
            assertTrue(distribution.getKey(), plan.dictionary.length >= 1);
            assertTrue(distribution.getKey(), plan.dictionary.length <= AlpCodec.RD_MAX_DICTIONARY);
            for (int i = 1; i < plan.dictionary.length; i++)
                assertTrue(distribution.getKey() + ": entry " + i + " of " + Arrays.toString(plan.dictionary),
                           plan.dictionary[i] > plan.dictionary[i - 1]);
            for (int entry : plan.dictionary)
                assertTrue(distribution.getKey(), entry >= 0 && entry < (1 << plan.leftBits));
        }
    }

    // -----------------------------------------------------------------------------------------
    // the variant choice
    // -----------------------------------------------------------------------------------------

    /**
     * §5 rule 3's tie-break, on a tie constructed to be exact rather than found by luck.
     *
     * <p>Four two-decimal values interleaved with four canonical NaNs. Decimal ALP scales the four
     * decimals at {@code (e 2, f 0)} into {@code 1001..1007}, a 3-bit lane over eight slots, and pays
     * a four-entry exception area: {@code 16 + 8 + 48 = 72}. ALP-RD cuts at 16 bits, where the values
     * have exactly two high halves, and pays {@code 8 + 8 + 8 + 48 = 72}. {@code RAW} is
     * {@code roundUp8(8 * 9) = 72} as well, so all three candidates cost the same and the argmin's
     * lowest-code-wins rule has to decide -- {@code 0x20} beats {@code 0x21} beats {@code 0x41}.
     *
     * <p>It is a determinism rule, not a preference. A replica resolving this the other way writes a
     * byte-different chunk, {@code chunkUnchanged} reports a difference, and every chunk in the
     * cluster is rewritten every cycle.
     */
    @Test
    public void anExactThreeWayTieResolvesToTheLowestCodePoint()
    {
        long nan = 0x7FF8000000000000L;
        long[] values = { nan, bits(10.01), nan, bits(10.03), nan, bits(10.05), nan, bits(10.07) };

        assertEquals(72L, AlpBlockCodec.INSTANCE.payloadLength(ALP, values, 8));
        assertEquals(72L, AlpBlockCodec.INSTANCE.payloadLength(ALP_RD, values, 8));
        assertEquals(72, (8 * (BlockPresence.varIntLength(8) + 8) + 7) / 8 * 8);

        BlockEncodings.Choice choice = choose(values);
        assertEquals(ALP, choice.encoding);
        assertEquals(72, choice.payloadLength);
        assertTrue(ChunkV4BlockTable.ENC_ALP < ChunkV4BlockTable.ENC_ALP_RD);
        assertTrue(ChunkV4BlockTable.ENC_ALP_RD < ChunkV4BlockTable.ENC_RAW);
        assertRoundTripThroughTheArgmin("the exact tie", values);
    }

    /**
     * The tie-break <em>inside</em> ALP-RD: two {@code leftBits} cuts that cost exactly the same, in
     * which case the narrowest wins -- the rule v3's {@code AlpCodec.planRd} applied.
     *
     * <p>This one needs constructing rather than finding, and it is worth the trouble because no
     * size or round-trip test can reach it: both cuts decode to the same values and differ only in
     * bytes, which is precisely the failure mode §12 describes. 64 patterns alternating between two
     * high halves that are identical above bit 48. At {@code leftBits 15} they are one value, so the
     * dictionary is one entry and no code is stored at all; at 16 they separate, buying a one-bit
     * narrower low part and paying for a second dictionary entry and a one-bit code lane. The two
     * cancel exactly.
     */
    @Test
    public void anExactTieBetweenTwoRdCutsResolvesToTheNarrowestCut()
    {
        long[] values = new long[64];
        for (int i = 0; i < values.length; i++)
            values[i] = (i % 2 == 0 ? 0x7FF8000000000000L : 0x7FF9000000000000L) | (i + 1);

        // header + one dictionary entry padded to 8 + no code lane + a 49-bit low part
        assertEquals(408, 8 + 8 + BitPacking.packedLength(64, 49));
        // header + two entries padded to 8 + a 1-bit code lane + a 48-bit low part
        assertEquals(408, 8 + 8 + BitPacking.packedLength(64, 1) + BitPacking.packedLength(64, 48));

        AlpBlockCodec.RdPlan plan = AlpBlockCodec.planRd(values, values.length);
        assertEquals("the narrowest cut takes the tie", 15, plan.leftBits);
        assertEquals(408, plan.payloadLength);
        assertArrayEquals(new int[]{ 0x3FFC }, plan.dictionary);
        assertEquals(408L, AlpBlockCodec.INSTANCE.payloadLength(ALP_RD, values, values.length));
        assertRoundTrip("the leftBits tie", ALP_RD, values);
    }

    /** And when one variant is strictly smaller, it wins -- in both directions. */
    @Test
    public void theArgminPicksWhicheverVariantIsSmaller()
    {
        long[] decimal = sensorWalkTwoDecimals(1024, 7);
        assertEquals(ALP, choose(decimal).encoding);
        assertTrue(AlpBlockCodec.INSTANCE.payloadLength(ALP, decimal, decimal.length)
                   < AlpBlockCodec.INSTANCE.payloadLength(ALP_RD, decimal, decimal.length));

        long[] real = fullPrecisionWalk(1024, 13);
        assertEquals(ALP_RD, choose(real).encoding);
        assertTrue(AlpBlockCodec.INSTANCE.payloadLength(ALP_RD, real, real.length)
                   < AlpBlockCodec.INSTANCE.payloadLength(ALP, real, real.length));

        // ALP-RD carries exceptions here, so the flag path is exercised on the winning variant too.
        AlpBlockCodec.RdPlan plan = AlpBlockCodec.planRd(real, real.length);
        assertTrue("a real-double block should overflow the 8-entry dictionary", plan.exceptionCount > 0);
        assertEquals(AlpBlockCodec.FLAG_HAS_EXCEPTIONS, encode(ALP_RD, real)[2]);
        assertRoundTripThroughTheArgmin("full-precision gaussian", real);
    }

    // -----------------------------------------------------------------------------------------
    // size
    // -----------------------------------------------------------------------------------------

    /**
     * ALP's bytes against {@code RAW}'s on the production distributions, printed and bounded.
     *
     * <p>{@code RAW} is the right baseline because it is what the argmin picks for a double block
     * with no codec installed -- it is the encoding this class replaces, exactly as the retired
     * {@code AlpCodecTest} measured v3's ALP against the chimp128 it replaced. The bounds have
     * headroom over the measured values on purpose: this test exists so that a regression shows up
     * as a number in the log, not so that an improvement fails the build.
     */
    @Test
    public void sizeAgainstRawOnTheProductionDistributions()
    {
        int n = 1024;
        Map<String, Double> ratios = new LinkedHashMap<>();
        int raw = (n * (BlockPresence.varIntLength(8) + 8) + 7) / 8 * 8;
        System.out.printf("chunk v4 ALP vs RAW, %d values/block%n", n);
        System.out.printf("  %-30s %-7s %9s %9s %10s %9s%n",
                          "distribution", "variant", "alp B", "raw B", "alp B/val", "alp/raw");
        for (Map.Entry<String, long[]> distribution : distributions(n).entrySet())
        {
            long[] values = distribution.getValue();
            BlockEncodings.Choice choice = choose(values);
            double ratio = choice.payloadLength / (double) raw;
            ratios.put(distribution.getKey(), ratio);
            String variant = choice.encoding == ALP ? "ALP"
                             : choice.encoding == ALP_RD ? "ALP-RD"
                             : "0x" + Integer.toHexString(choice.encoding);
            System.out.printf("  %-30s %-7s %9d %9d %10.4f %9.4f%n", distribution.getKey(), variant,
                              choice.payloadLength, raw, choice.payloadLength / (double) n, ratio);
        }
        assertRatioBelow(ratios, "2-decimal sensor walk", 0.15);
        assertRatioBelow(ratios, "integral walk", 0.15);
        assertRatioBelow(ratios, "near-constant setpoint", 0.05);
        assertRatioBelow(ratios, "full-precision gaussian walk", 0.85);
        // Even the two shapes ALP cannot compress must not cost more than the fallback it displaced.
        assertRatioBelow(ratios, "all NaN, distinct payloads", 0.80);
        assertRatioBelow(ratios, "random 64-bit patterns", 0.95);
    }

    // -----------------------------------------------------------------------------------------
    // corruption
    // -----------------------------------------------------------------------------------------

    @Test
    public void truncateAtEveryPrefixLength()
    {
        for (int encoding : new int[]{ ALP, ALP_RD })
        {
            byte[] full = encode(encoding, corpusFor(encoding));
            int count = corpusFor(encoding).length;
            for (int length = 0; length < full.length; length++)
            {
                byte[] truncated = Arrays.copyOf(full, length);
                try
                {
                    decode(encoding, truncated, count);
                    fail("a " + length + "-byte payload parsed a " + full.length + "-byte 0x" +
                         Integer.toHexString(encoding) + " block");
                }
                catch (IllegalArgumentException expected)
                {
                    // the only permitted outcome
                }
            }
            // And the declared length staying long while the bytes run out is the block table's
            // corruption rather than the body's, which must also be a clean reject.
            byte[] short8 = Arrays.copyOf(full, full.length - 8);
            assertThatThrownBy(() -> AlpBlockCodec.INSTANCE.decode(encoding, ByteBuffer.wrap(short8), full.length,
                                                                   count, new long[count]))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void singleBitFlipsNeverEscapeAsUncheckedOrOom()
    {
        for (int encoding : new int[]{ ALP, ALP_RD })
        {
            long[] values = corpusFor(encoding);
            byte[] full = encode(encoding, values);
            int accepted = 0;
            for (int bit = 0; bit < full.length * 8; bit++)
            {
                byte[] flipped = full.clone();
                flipped[bit >>> 3] ^= (byte) (1 << (bit & 7));
                try
                {
                    decode(encoding, flipped, values.length);
                    accepted++;
                }
                catch (IllegalArgumentException expected)
                {
                    // the only permitted outcome
                }
                catch (RuntimeException unexpected)
                {
                    fail("0x" + Integer.toHexString(encoding) + " bit " + bit + " escaped as " + unexpected);
                }
            }
            // The verbatim values and the lane carry no redundancy, so flips there are undetectable
            // by construction; a run where nothing was accepted would mean nothing was parsed.
            if (accepted == 0)
                fail("every bit flip was rejected, which means 0x" + Integer.toHexString(encoding) +
                     " is not being parsed");
        }
    }

    /**
     * The header's exception flag and the derived body length have to agree, in both directions.
     * That agreement is what stops a body length corrupted downwards by exactly an area's size from
     * parsing as a shorter, self-consistent block whose exception rows silently come back as
     * whatever their filler slot decodes to.
     */
    @Test
    public void aMisdeclaredExceptionFlagIsRejected()
    {
        byte[] withArea = hex(GOLDEN_ALP_EXCEPTION_HEX);
        // Claimed absent, present in the bytes.
        byte[] clearedFlag = withArea.clone();
        clearedFlag[3] = 0;
        assertThatThrownBy(() -> decode(ALP, clearedFlag, 4)).isInstanceOf(IllegalArgumentException.class);
        // Claimed present, absent from the bytes.
        byte[] withoutArea = hex(GOLDEN_ALP_HEX);
        byte[] setFlag = withoutArea.clone();
        setFlag[3] = AlpBlockCodec.FLAG_HAS_EXCEPTIONS;
        assertThatThrownBy(() -> decode(ALP, setFlag, 4)).isInstanceOf(IllegalArgumentException.class);

        byte[] rd = hex(GOLDEN_ALP_RD_HEX);
        byte[] rdSetFlag = rd.clone();
        rdSetFlag[2] = AlpBlockCodec.FLAG_HAS_EXCEPTIONS;
        assertThatThrownBy(() -> decode(ALP_RD, rdSetFlag, 8)).isInstanceOf(IllegalArgumentException.class);
    }

    /** §9 grows the format with new encoding codes, not with reserved bits; so these must be zero. */
    @Test
    public void reservedFlagBitsAreRejected()
    {
        for (int bit = 1; bit < 8; bit++)
        {
            byte[] alp = hex(GOLDEN_ALP_HEX);
            alp[3] = (byte) (1 << bit);
            assertThatThrownBy(() -> decode(ALP, alp, 4)).isInstanceOf(IllegalArgumentException.class);

            byte[] rd = hex(GOLDEN_ALP_RD_HEX);
            rd[2] = (byte) (1 << bit);
            assertThatThrownBy(() -> decode(ALP_RD, rd, 8)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void paddingIsZeroEverywhereAndIsVerified()
    {
        // ALP: the four header pad bytes.
        assertPaddingIsCheckedAt(ALP, GOLDEN_ALP_HEX, 4, new int[]{ 4, 5, 6, 7 });
        // ALP with an area: the same, plus the area's two pad fields and its trailing pad to 8.
        assertPaddingIsCheckedAt(ALP, GOLDEN_ALP_EXCEPTION_HEX, 4,
                                 new int[]{ 4, 5, 6, 7, 26, 27, 28, 29, 30, 31, 42, 43, 44, 45, 46, 47 });
        // ALP-RD: the header's pad byte and pad word, and the dictionary's pad to 8.
        assertPaddingIsCheckedAt(ALP_RD, GOLDEN_ALP_RD_HEX, 8,
                                 new int[]{ 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15 });
    }

    @Test
    public void headerFieldsOutsideTheirDomainAreRejected()
    {
        byte[] alp = hex(GOLDEN_ALP_HEX);
        // exponent past 18, and a factor above its exponent
        assertThatThrownBy(() -> decode(ALP, withByte(alp, 0, (byte) 19), 4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> decode(ALP, withByte(alp, 1, (byte) 1), 4))
            .isInstanceOf(IllegalArgumentException.class);
        // a lane wider than a frame of reference over +/-2^53 can ever need
        assertThatThrownBy(() -> decode(ALP, withByte(alp, 2, (byte) (AlpCodec.MAX_BIT_WIDTH + 1)), 4))
            .isInstanceOf(IllegalArgumentException.class);
        // a frame of reference outside +/-2^53
        byte[] wildReference = alp.clone();
        ByteBuffer.wrap(wildReference).putLong(8, Long.MAX_VALUE);
        assertThatThrownBy(() -> decode(ALP, wildReference, 4)).isInstanceOf(IllegalArgumentException.class);

        byte[] rd = hex(GOLDEN_ALP_RD_HEX);
        for (byte leftBits : new byte[]{ 0, (byte) (AlpCodec.RD_MAX_LEFT_BITS + 1), (byte) 0xFF })
            assertThatThrownBy(() -> decode(ALP_RD, withByte(rd, 0, leftBits), 8))
                .isInstanceOf(IllegalArgumentException.class);
        for (byte size : new byte[]{ 0, (byte) (AlpCodec.RD_MAX_DICTIONARY + 1), (byte) 0xFF })
            assertThatThrownBy(() -> decode(ALP_RD, withByte(rd, 1, size), 8))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * §5 rule 2 on the ALP-RD dictionary: swapping two entries decodes to the same set of left parts
     * and re-encodes to different bytes, which is the second byte form the ascending rule removes; a
     * duplicated entry would give one left part two codes.
     */
    @Test
    public void anOutOfOrderOrDuplicatedRdDictionaryIsRejected()
    {
        byte[] golden = hex(GOLDEN_ALP_RD_DICT2_HEX);
        int at = AlpBlockCodec.RD_HEADER_BYTES;

        byte[] swapped = golden.clone();
        System.arraycopy(golden, at + 2, swapped, at, 2);
        System.arraycopy(golden, at, swapped, at + 2, 2);
        assertThatThrownBy(() -> decode(ALP_RD, swapped, 8)).isInstanceOf(IllegalArgumentException.class);

        byte[] duplicated = golden.clone();
        System.arraycopy(golden, at, duplicated, at + 2, 2);
        assertThatThrownBy(() -> decode(ALP_RD, duplicated, 8)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * A dictionary code the dictionary does not define is corruption, not an array index:
     * {@code codeBits} rounds up to a power of two, so a five-entry dictionary leaves codes 5..7
     * expressible.
     */
    @Test
    public void outOfRangeRdCodesAreRejected()
    {
        byte[] golden = hex(GOLDEN_ALP_RD_DICT2_HEX);
        // Two entries, one code bit -- so no code is out of range. Shrinking the declared size to
        // one leaves the lane naming code 1, which is now undefined.
        byte[] shrunk = withByte(golden, 1, (byte) 1);
        assertThatThrownBy(() -> decode(ALP_RD, shrunk, 8)).isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // integration with the block and the section
    // -----------------------------------------------------------------------------------------

    /**
     * The codec reached through {@link BlockEncodings}, which is how the encoder actually uses it,
     * including the {@code blockFlags} bit 4 invariant: an ALP block never sets it, and a set bit is
     * corruption rather than a variant spelling.
     */
    @Test
    public void theBlockLayerRoutesToTheCodecAndLeavesBlockFlagsBitFourClear()
    {
        long[] values = sensorWalkTwoDecimals(256, 3);
        BlockEncodings.Choice choice = choose(values);
        assertEquals(ALP, choice.encoding);
        assertEquals("an ALP block declares no exceptions to the block table", 0, choice.exceptionCount);
        assertTrue(!choice.hasExceptions());
        assertRoundTripThroughTheArgmin("routed decimal block", values);

        byte[] body = new byte[choice.payloadLength];
        BlockEncodings.encodeFixed(choice, ChunkV4Directory.TYPE_DOUBLE, values, values.length, 0,
                                   AlpBlockCodec.INSTANCE, new BlockEncodings.Scratch(1024), ByteBuffer.wrap(body));
        assertThatThrownBy(() -> BlockEncodings.decodeFixed(ALP, true, ByteBuffer.wrap(body), body.length,
                                                            ChunkV4Directory.TYPE_DOUBLE, values.length, 0, null,
                                                            AlpBlockCodec.INSTANCE, new BlockEncodings.Scratch(1024),
                                                            new long[values.length]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * A double column with holes, through the whole section: the codec sees only the present values
     * of each block, absent rows are presence and never values, and the section is byte-identical on
     * a second encode.
     */
    @Test
    public void mixedNullDoubleSectionsRoundTripThroughTheSection()
    {
        int rowCount = 2050;
        long[] values = new long[rowCount];
        boolean[] present = new boolean[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = bits(Math.round((20.0 + Math.sin(i / 50.0) * 5.0) * 100.0) / 100.0);
            present[i] = i % 7 != 3;
        }

        ChunkV4Section.Encoded encoded =
            ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_DOUBLE, values, present, rowCount, 10, null,
                                       AlpBlockCodec.INSTANCE);
        assertEquals(ALP, encoded.blockTable.get(0).blockEncoding);
        assertTrue("an ALP block leaves blockFlags bit 4 clear", !encoded.blockTable.get(0).hasExceptions());

        ChunkV4Section section = ChunkV4Section.read(ByteBuffer.wrap(encoded.bytes), 0, encoded.sectionLen(),
                                                     ChunkV4Directory.TYPE_DOUBLE, rowCount, 10, null,
                                                     AlpBlockCodec.INSTANCE);
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(1024);
        long[] words = new long[BlockPresence.wordCount(1024)];
        long[] block = new long[1024];
        for (int k = 0; k < section.blockCount(); k++)
        {
            section.decodeFixedBlock(k, words, block, scratch);
            int valueIndex = 0;
            int start = k << 10;
            for (int i = 0; i < section.blockRows(k); i++)
            {
                boolean bit = ((words[i >>> 6] >>> (i & 63)) & 1L) != 0;
                assertEquals("row " + (start + i), present[start + i], bit);
                if (bit)
                    assertEquals("row " + (start + i), values[start + i], block[valueIndex++]);
            }
        }
        assertArrayEquals(encoded.bytes,
                          ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_DOUBLE, values, present, rowCount, 10,
                                                     null, AlpBlockCodec.INSTANCE).bytes);
    }

    /**
     * §12's largest risk, pinned as a shape rather than as a comment: the codec is reachable only as
     * a constant. There is no public constructor to make a second one with different parameters and
     * no setter to turn it off, because a codec that some replicas have and others do not produces
     * different bytes for identical input.
     */
    @Test
    public void theCodecIsABuildTimeConstant()
    {
        assertTrue(AlpBlockCodec.INSTANCE instanceof BlockEncodings.DoubleBlockCodec);
        assertEquals("no public constructor: the codec cannot be configured into existence",
                     0, AlpBlockCodec.class.getConstructors().length);
        assertTrue("the codec is final, so it cannot be subclassed into a second byte format",
                   java.lang.reflect.Modifier.isFinal(AlpBlockCodec.class.getModifiers()));
    }

    // -----------------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------------

    private static byte[] encode(int encoding, long[] values)
    {
        long length = AlpBlockCodec.INSTANCE.payloadLength(encoding, values, values.length);
        assertTrue("payloadLength " + length, length >= 0 && (length & 7) == 0);
        byte[] out = new byte[(int) length];
        ByteBuffer dst = ByteBuffer.wrap(out);
        assertEquals(length, AlpBlockCodec.INSTANCE.encode(encoding, values, values.length, dst));
        return out;
    }

    private static byte[] encodeInto(int encoding, long[] values, byte fill)
    {
        long length = AlpBlockCodec.INSTANCE.payloadLength(encoding, values, values.length);
        byte[] out = new byte[(int) length];
        Arrays.fill(out, fill);
        AlpBlockCodec.INSTANCE.encode(encoding, values, values.length, ByteBuffer.wrap(out));
        return out;
    }

    private static long[] decode(int encoding, byte[] payload, int count)
    {
        long[] dst = new long[Math.max(count, 1)];
        AlpBlockCodec.INSTANCE.decode(encoding, ByteBuffer.wrap(payload), payload.length, count, dst);
        return Arrays.copyOf(dst, count);
    }

    private static void assertRoundTrip(String what, int encoding, long[] values)
    {
        byte[] encoded = encode(encoding, values);
        long[] decoded = decode(encoding, encoded, values.length);
        for (int i = 0; i < values.length; i++)
            if (decoded[i] != values[i])
                throw new AssertionError(what + " (0x" + Integer.toHexString(encoding) + "): row " + i +
                                         " expected " + hexBits(values[i]) + " but decoded " + hexBits(decoded[i]));
        assertArrayEquals(what + ": re-encode", encoded, encode(encoding, decoded));
    }

    private static void assertBothVariantsRoundTrip(String what, long[] values)
    {
        assertRoundTrip(what, ALP, values);
        assertRoundTrip(what, ALP_RD, values);
    }

    /** Encodes through {@link BlockEncodings#chooseFixed}, i.e. exactly as the section does. */
    private static void assertRoundTripThroughTheArgmin(String what, long[] values)
    {
        BlockEncodings.Choice choice = choose(values);
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(Math.max(values.length, 1));
        byte[] body = new byte[choice.payloadLength];
        BlockEncodings.encodeFixed(choice, ChunkV4Directory.TYPE_DOUBLE, values, values.length, minOf(values),
                                   AlpBlockCodec.INSTANCE, scratch, ByteBuffer.wrap(body));
        long[] decoded = new long[values.length];
        BlockEncodings.decodeFixed(choice.encoding, choice.hasExceptions(), ByteBuffer.wrap(body), body.length,
                                   ChunkV4Directory.TYPE_DOUBLE, values.length, minOf(values), null,
                                   AlpBlockCodec.INSTANCE, scratch, decoded);
        for (int i = 0; i < values.length; i++)
            if (decoded[i] != values[i])
                throw new AssertionError(what + ": row " + i + " expected " + hexBits(values[i]) + " but decoded " +
                                         hexBits(decoded[i]));
    }

    private static BlockEncodings.Choice choose(long[] values)
    {
        BlockEncodings.Choice choice = new BlockEncodings.Choice();
        BlockEncodings.chooseFixed(ChunkV4Directory.TYPE_DOUBLE, values, values.length, minOf(values), null,
                                   AlpBlockCodec.INSTANCE, new BlockEncodings.Scratch(Math.max(values.length, 1)),
                                   choice);
        return choice;
    }

    private static long minOf(long[] values)
    {
        long min = values[0];
        for (long value : values)
            if (BlockEncodings.compareValues(value, min, ChunkV4Directory.TYPE_DOUBLE) < 0)
                min = value;
        return min;
    }

    /**
     * Every listed byte must be zero in the golden vector and must be rejected when set -- the second
     * half is what proves the check exists rather than that the vector happens to be clean.
     */
    private static void assertPaddingIsCheckedAt(int encoding, String golden, int count, int[] offsets)
    {
        byte[] clean = hex(golden);
        for (int at : offsets)
        {
            assertEquals("golden pad byte " + at, 0, clean[at]);
            byte[] corrupt = withByte(clean, at, (byte) 1);
            assertThatThrownBy(() -> decode(encoding, corrupt, count))
                .as("pad byte " + at + " of 0x" + Integer.toHexString(encoding))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static byte[] withByte(byte[] payload, int index, byte value)
    {
        byte[] copy = payload.clone();
        copy[index] = value;
        return copy;
    }

    private static void assertRatioBelow(Map<String, Double> ratios, String pattern, double bound)
    {
        Double ratio = ratios.get(pattern);
        assertTrue("no measurement recorded for " + pattern, ratio != null);
        assertTrue(pattern + ": ALP is " + String.format("%.4f", ratio) + "x the size of RAW, bound is " + bound + 'x',
                   ratio < bound);
    }

    /** The corpus each variant's corruption sweep runs on: small, and carrying an exception area. */
    private static long[] corpusFor(int encoding)
    {
        return encoding == ALP
               ? new long[]{ bits(1.0), bits(2.0), bits(Double.NaN), bits(4.0) }
               : nanPayloads(8);
    }

    // -- distributions, keeping the retired AlpCodecTest's vocabulary so reports stay comparable --

    private static Map<String, long[]> distributions(int n)
    {
        Map<String, long[]> out = new LinkedHashMap<>();
        out.put("2-decimal sensor walk", sensorWalkTwoDecimals(n, 7));
        out.put("integral walk", integralWalk(n, 11));
        out.put("near-constant setpoint", nearConstant(n));
        out.put("full-precision gaussian walk", fullPrecisionWalk(n, 13));
        out.put("all NaN, distinct payloads", nanPayloads(n));
        out.put("random 64-bit patterns", randomBitPatterns(n, 23));
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

    /** A setpoint that is not quite constant, so the CONSTANT encoding never fires and ALP sees it. */
    private static long[] nearConstant(int n)
    {
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
            out[i] = bits(i % 997 == 0 ? 192.5 : 192.0);
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

    /** All NaN, but a different payload every row -- so it is not CONSTANT and must reach the codec. */
    private static long[] nanPayloads(int n)
    {
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
            out[i] = 0x7FF8000000000000L | ((i + 1) & 0xFFFFFFFFFFFFFL);
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

    private static long[] randomishDoubles(Random random, int n)
    {
        long[] out = new long[n];
        for (int i = 0; i < n; i++)
        {
            switch (random.nextInt(4))
            {
                case 0:
                    out[i] = bits(Math.round(random.nextGaussian() * 1000.0) / 100.0);
                    break;
                case 1:
                    out[i] = bits(random.nextInt(10000));
                    break;
                case 2:
                    out[i] = random.nextLong();
                    break;
                default:
                    out[i] = bits(random.nextGaussian());
            }
        }
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

    private static String hexBits(long bits)
    {
        return String.format("0x%016X", bits);
    }
}
