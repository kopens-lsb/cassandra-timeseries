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
 * Pins the block encodings of chunk-format-v4 §5: the payload layout of every code this phase
 * implements, and the argmin that chooses between them.
 *
 * <p>The golden vectors are hand-computed from §5's field lists and §6's bit order, never captured
 * from the encoder, because a layout change that moves a field passes a green round trip and makes
 * every chunk another build wrote unreadable. The argmin tests are about determinism rather than
 * compression: a replica that broke a tie the other way emits a byte-different chunk, and every
 * chunk in the cluster is then rewritten every cycle.
 */
public class BlockEncodingsTest
{
    // -----------------------------------------------------------------------------------------
    // golden vectors
    // -----------------------------------------------------------------------------------------

    /** {@code 0x30 BITPACK1} over 8 booleans {@code T F T T F F F T}: bits 0,2,3,7 set = 0x8D. */
    private static final String GOLDEN_BITPACK1_HEX = "000000000000008D";

    /** {@code 0x02 CONSTANT} on a statWidth 0 type: {@code varint 2 | "hi"}, padded to 8. */
    private static final String GOLDEN_CONSTANT_TEXT_HEX = "02" + "6869" + "0000000000";

    /** {@code 0x41 RAW} over {@code "ab"}, {@code ""}, {@code "xyz"}: 3+1+4 = 8 bytes exactly. */
    private static final String GOLDEN_RAW_HEX = "02" + "6162" + "00" + "03" + "78797A";

    /** {@code 0x40 DICT} over dictionary {@code [a,b,c]}: codes 2,0,1,2 at 2 bits = 0x92. */
    private static final String GOLDEN_DICT_HEX = "0000000000000092";

    @Test
    public void goldenVectorForFrameOfReferenceBitpacking()
    {
        // The lane word, derived from §6 rather than copied from the packer.
        assertEquals(0xE4L, 0L | (1L << 2) | (2L << 4) | (3L << 6));
        long[] values = { 10, 11, 12, 13 };
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        assertEquals(ChunkV4BlockTable.ENC_FOR_BITPACK, encoded.choice.encoding);
        assertEquals(2, encoded.choice.width);
        assertEquals(0, encoded.choice.exceptionCount);
        assertArrayEquals(hex("02" + "00000000000000" + "00000000000000E4"), encoded.bytes);
        assertEquals(16, encoded.bytes.length);
        assertArrayEquals(values, decodeFixed(ChunkV4Directory.TYPE_INT64, encoded, values.length, null));
    }

    /**
     * The same encoding with the shared exception area, and the reason §5 made it shared. Seven
     * values need two bits and one needs forty.
     *
     * <p>The argmin scores the two-bit lane at {@code 8 lane + 8 area header + 16 area body} = 32
     * against the forty-bit lane's 40, so the exception wins outright. It used to win it as a tie:
     * while {@code EXCEPTION_AREA_OVERHEAD} was 16 both candidates scored 40 and the §5 smallest-w
     * rule decided it. Reconciling the constant with the layout turned the tie into a strict win and
     * left the chosen width, and therefore every byte below, unchanged -- but that had to be
     * recomputed rather than assumed, since a width move here would silently rewrite the vector.
     *
     * <p>The <em>emitted</em> body is 40 bytes either way: the cost function is what picks the
     * width, and the width it picked is the one the bytes are packed at.
     */
    @Test
    public void goldenVectorForFrameOfReferenceWithAnException()
    {
        long[] values = { 0, 1, 2, 3, 0, 1, 2, 1L << 39 };
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        assertEquals(ChunkV4BlockTable.ENC_FOR_BITPACK, encoded.choice.encoding);
        assertEquals(2, encoded.choice.width);
        assertEquals(1, encoded.choice.exceptionCount);

        // The two scores the width chooser compared, stated as numbers so a change to either the
        // lane sizing or the exception term shows up here rather than as a moved golden vector.
        assertEquals(32L, BitPacking.widthCost(8, 2, 1, 8));
        assertEquals(40L, BitPacking.widthCost(8, 40, 0, 8));

        // lane: the outlier's slot carries the specified filler 0, the rest pack at two bits.
        assertEquals(0x24E4L, 0L | (1L << 2) | (2L << 4) | (3L << 6) | (0L << 8) | (1L << 10) | (2L << 12));
        assertArrayEquals(hex("02" + "00000000000000" +
                              "00000000000024E4" +
                              "0001" + "0000" + "00000000" +
                              "0007" + "0000008000000000" + "000000000000"),
                          encoded.bytes);
        assertEquals(40, encoded.bytes.length);
        assertArrayEquals(values, decodeFixed(ChunkV4Directory.TYPE_INT64, encoded, values.length, null));
    }

    /**
     * {@code 0x11 DELTA_FOR_BITPACK} over 64 timestamps one second apart. Every delta is 1,000, so
     * {@code deltaBase} absorbs all of them and the lane is zero bits wide: the whole block is its
     * 24-byte header. This is the shape §0 deleted the delta-of-delta bitstream for.
     */
    @Test
    public void goldenVectorForDeltaFrameOfReference()
    {
        long[] values = new long[64];
        for (int i = 0; i < values.length; i++)
            values[i] = 1_700_000_000_000L + 1000L * i;
        assertEquals(0x18BCFE56800L, 1_700_000_000_000L);

        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        assertEquals(ChunkV4BlockTable.ENC_DELTA_FOR_BITPACK, encoded.choice.encoding);
        assertEquals(0, encoded.choice.width);
        assertArrayEquals(hex("0000018BCFE56800" + "00000000000003E8" + "00" + "00000000000000"), encoded.bytes);
        assertEquals(24, encoded.bytes.length);
        assertArrayEquals(values, decodeFixed(ChunkV4Directory.TYPE_INT64, encoded, values.length, null));

        // The frame-of-reference alternative on the same input is 136 bytes: a 16-bit lane over 64
        // values plus its header. Asserted so the choice above is shown to be a size decision.
        assertEquals(8 + BitPacking.packedLength(64, 16), 136);
    }

    @Test
    public void goldenVectorForBooleanBitpacking()
    {
        long[] values = { 1, 0, 1, 1, 0, 0, 0, 1 };
        assertEquals(0x8DL, 1L | (1L << 2) | (1L << 3) | (1L << 7));
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_BOOLEAN, values, null, null);
        assertEquals(ChunkV4BlockTable.ENC_BITPACK1, encoded.choice.encoding);
        assertArrayEquals(hex(GOLDEN_BITPACK1_HEX), encoded.bytes);
        assertArrayEquals(values, decodeFixed(ChunkV4Directory.TYPE_BOOLEAN, encoded, values.length, null));
    }

    @Test
    public void goldenVectorsForTheVariableWidthEncodings()
    {
        byte[][] constant = { bytes("hi"), bytes("hi"), bytes("hi") };
        EncodedVariable encodedConstant = encodeVariable(ChunkV4Directory.TYPE_TEXT, constant, null, null);
        assertEquals(ChunkV4BlockTable.ENC_CONSTANT, encodedConstant.choice.encoding);
        assertArrayEquals(hex(GOLDEN_CONSTANT_TEXT_HEX), encodedConstant.bytes);

        byte[][] raw = { bytes("ab"), bytes(""), bytes("xyz") };
        EncodedVariable encodedRaw = encodeVariable(ChunkV4Directory.TYPE_OPAQUE, raw, null, null);
        assertEquals(ChunkV4BlockTable.ENC_RAW, encodedRaw.choice.encoding);
        assertArrayEquals(hex(GOLDEN_RAW_HEX), encodedRaw.bytes);
        assertEquals(8, encodedRaw.bytes.length);

        byte[][] dictionary = { bytes("a"), bytes("b"), bytes("c") };
        byte[][] values = { bytes("c"), bytes("a"), bytes("b"), bytes("c") };
        assertEquals(2, BlockEncodings.dictionaryCodeBits(3));
        assertEquals(0x92L, 2L | (0L << 2) | (1L << 4) | (2L << 6));
        EncodedVariable encodedDict = encodeVariable(ChunkV4Directory.TYPE_TEXT, values, null, dictionary);
        assertEquals(ChunkV4BlockTable.ENC_DICT, encodedDict.choice.encoding);
        assertArrayEquals(hex(GOLDEN_DICT_HEX), encodedDict.bytes);

        assertVariableRoundTrip(ChunkV4Directory.TYPE_TEXT, constant, encodedConstant, null, null);
        assertVariableRoundTrip(ChunkV4Directory.TYPE_OPAQUE, raw, encodedRaw, null, null);
        assertVariableRoundTrip(ChunkV4Directory.TYPE_TEXT, values, encodedDict, null, dictionary);
    }

    @Test
    public void zeroByteEncodingsCarryNoBody()
    {
        // 0x01 EMPTY: the only encoding for a block with no present value.
        Encoded empty = encodeFixed(ChunkV4Directory.TYPE_INT64, new long[0], null, null);
        assertEquals(ChunkV4BlockTable.ENC_EMPTY, empty.choice.encoding);
        assertEquals(0, empty.bytes.length);

        // 0x02 CONSTANT on a type with a block-table min: the value is the min, the body is nothing.
        Encoded constant = encodeFixed(ChunkV4Directory.TYPE_INT64, new long[]{ 42, 42, 42 }, null, null);
        assertEquals(ChunkV4BlockTable.ENC_CONSTANT, constant.choice.encoding);
        assertEquals(0, constant.bytes.length);
        assertArrayEquals(new long[]{ 42, 42, 42 },
                          decodeFixed(ChunkV4Directory.TYPE_INT64, constant, 3, null));

        // 0x03 PRESENCE_ONLY: reachable exactly where CONSTANT is not free, i.e. statWidth 0.
        byte[] columnConstant = { 1 };
        Encoded presenceOnly = encodeFixed(ChunkV4Directory.TYPE_BOOLEAN, new long[]{ 1, 1, 1 }, columnConstant, null);
        assertEquals(ChunkV4BlockTable.ENC_PRESENCE_ONLY, presenceOnly.choice.encoding);
        assertEquals(0, presenceOnly.bytes.length);
        assertArrayEquals(new long[]{ 1, 1, 1 },
                          decodeFixed(ChunkV4Directory.TYPE_BOOLEAN, presenceOnly, 3, columnConstant));

        // ... and on a type that has a min, CONSTANT wins the 0-byte tie because 0x02 < 0x03.
        Encoded tie = encodeFixed(ChunkV4Directory.TYPE_INT64, new long[]{ 42, 42 },
                                  BlockEncodings.toFixedBytes(42, ChunkV4Directory.TYPE_INT64), null);
        assertEquals(ChunkV4BlockTable.ENC_CONSTANT, tie.choice.encoding);
    }

    // -----------------------------------------------------------------------------------------
    // the argmin
    // -----------------------------------------------------------------------------------------

    /**
     * §5 rule 3's tie-break, on a tie constructed to be exact rather than found by luck. A one-value
     * boolean block costs 8 bytes three different ways: {@code CONSTANT} is
     * {@code roundUp8(varint(1) + 1)}, {@code BITPACK1} is {@code ceil(1*1/64)*8}, {@code RAW} is
     * {@code roundUp8(1 * (varint(1) + 1))}. The lowest code point takes it.
     */
    @Test
    public void anExactThreeWayTieResolvesToTheLowestCodePoint()
    {
        assertEquals(8, BitPacking.packedLength(1, 1));
        assertEquals(8, BlockEncodings.constantBodyLength(1));

        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_BOOLEAN, new long[]{ 1 }, null, null);
        assertEquals(8, encoded.choice.payloadLength);
        assertEquals(ChunkV4BlockTable.ENC_CONSTANT, encoded.choice.encoding);
        assertTrue(ChunkV4BlockTable.ENC_CONSTANT < ChunkV4BlockTable.ENC_BITPACK1);
        assertTrue(ChunkV4BlockTable.ENC_BITPACK1 < ChunkV4BlockTable.ENC_RAW);

        // A DICT/RAW tie for the variable path: four one-byte values are 8 bytes either way.
        byte[][] dictionary = { bytes("a"), bytes("b"), bytes("c") };
        byte[][] values = { bytes("c"), bytes("a"), bytes("b"), bytes("c") };
        BlockEncodings.Choice choice = new BlockEncodings.Choice();
        BlockEncodings.chooseVariable(ChunkV4Directory.TYPE_TEXT, values, 4, null, dictionary, choice);
        assertEquals(8, choice.payloadLength);
        assertEquals(ChunkV4BlockTable.ENC_DICT, choice.encoding);
        BlockEncodings.chooseVariable(ChunkV4Directory.TYPE_TEXT, values, 4, null, null, choice);
        assertEquals(8, choice.payloadLength);
        assertEquals(ChunkV4BlockTable.ENC_RAW, choice.encoding);
    }

    /** The same input must yield the same code, width and length every time it is scored. */
    @Test
    public void theArgminIsAFunctionOfTheInputAlone()
    {
        Random random = new Random(31337L);
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(1024);
        BlockEncodings.Choice first = new BlockEncodings.Choice();
        BlockEncodings.Choice again = new BlockEncodings.Choice();
        for (int trial = 0; trial < 400; trial++)
        {
            int typeCode = FIXED_TYPES[random.nextInt(FIXED_TYPES.length)];
            long[] values = randomValues(random, typeCode, 1 + random.nextInt(200));
            long min = minOf(values, values.length, typeCode);
            for (int repeat = 0; repeat < 3; repeat++)
            {
                BlockEncodings.chooseFixed(typeCode, values, values.length, min, null, null, scratch,
                                           repeat == 0 ? first : again);
                if (repeat > 0)
                {
                    assertEquals(first.encoding, again.encoding);
                    assertEquals(first.width, again.width);
                    assertEquals(first.exceptionCount, again.exceptionCount);
                    assertEquals(first.payloadLength, again.payloadLength);
                }
            }
        }
    }

    /**
     * {@link BlockEncodings#compareValues} is both the statistic's order and the order
     * {@code FOR_BITPACK}'s subtraction assumes, so it has to be the column's declared
     * {@link StatOrder}. Compared against the serialized-byte comparators rather than against
     * itself; {@code date} signed instead of unsigned is the mistake this catches.
     */
    @Test
    public void valueOrderAgreesWithTheDeclaredStatOrder()
    {
        Random random = new Random(11L);
        for (int trial = 0; trial < 2000; trial++)
        {
            for (int typeCode : new int[]{ ChunkV4Directory.TYPE_INT32, ChunkV4Directory.TYPE_INT64,
                                           ChunkV4Directory.TYPE_DATE32, ChunkV4Directory.TYPE_DOUBLE })
            {
                long left = randomValue(random, typeCode);
                long right = randomValue(random, typeCode);
                StatOrder order = statOrderOf(typeCode);
                int expected = Integer.signum(order.compare(BlockEncodings.toFixedBytes(left, typeCode),
                                                            BlockEncodings.toFixedBytes(right, typeCode)));
                assertEquals(typeCode + ": " + left + " vs " + right, expected,
                             Integer.signum(BlockEncodings.compareValues(left, right, typeCode)));
            }
        }
        // The case that makes the pairing load-bearing: the same four bytes under the two 4-byte
        // types disagree about which value is smaller.
        long dateLow = 0x7FFFFFFFL;
        long dateHigh = 0x80000000L;
        assertTrue(BlockEncodings.compareValues(dateLow, dateHigh, ChunkV4Directory.TYPE_DATE32) < 0);
        assertTrue(BlockEncodings.compareValues((int) dateLow, (int) dateHigh, ChunkV4Directory.TYPE_INT32) > 0);
    }

    // -----------------------------------------------------------------------------------------
    // round trip
    // -----------------------------------------------------------------------------------------

    @Test
    public void roundTripOverRandomisedFixedWidthBlocks()
    {
        Random random = new Random(2718L);
        for (int trial = 0; trial < 1500; trial++)
        {
            int typeCode = FIXED_TYPES[random.nextInt(FIXED_TYPES.length)];
            int count = random.nextInt(300);
            long[] values = randomValues(random, typeCode, count);
            byte[] columnConstant = count > 0 && random.nextInt(4) == 0
                                    ? BlockEncodings.toFixedBytes(values[0], typeCode) : null;
            if (columnConstant != null)
                Arrays.fill(values, values[0]);

            Encoded encoded = encodeFixed(typeCode, values, columnConstant, null);
            assertEquals(0, encoded.bytes.length % 8);
            assertArrayEquals(values, decodeFixed(typeCode, encoded, count, columnConstant));

            // encode twice into differently dirtied buffers, then re-encode what came back.
            assertArrayEquals(encoded.bytes, encodeFixedInto(typeCode, values, columnConstant, (byte) 0xFF));
            assertArrayEquals(encoded.bytes, encodeFixedInto(typeCode, values, columnConstant, (byte) 0x5A));
            long[] decoded = decodeFixed(typeCode, encoded, count, columnConstant);
            assertArrayEquals(encoded.bytes, encodeFixed(typeCode, decoded, columnConstant, null).bytes);
        }
    }

    @Test
    public void roundTripOverRandomisedVariableWidthBlocks()
    {
        Random random = new Random(1618L);
        for (int trial = 0; trial < 800; trial++)
        {
            int typeCode = random.nextBoolean() ? ChunkV4Directory.TYPE_TEXT : ChunkV4Directory.TYPE_OPAQUE;
            int count = random.nextInt(60);
            byte[][] values = new byte[count][];
            for (int i = 0; i < count; i++)
                values[i] = randomBytes(random, random.nextInt(6));
            byte[][] dictionary = random.nextBoolean() ? distinctSorted(values, count) : null;
            byte[] columnConstant = count > 0 && random.nextInt(5) == 0 ? values[0] : null;
            if (columnConstant != null)
            {
                Arrays.fill(values, columnConstant);
                dictionary = random.nextBoolean() ? distinctSorted(values, count) : null;
            }

            EncodedVariable encoded = encodeVariable(typeCode, values, columnConstant, dictionary);
            assertEquals(0, encoded.bytes.length % 8);
            assertVariableRoundTrip(typeCode, values, encoded, columnConstant, dictionary);
            assertArrayEquals(encoded.bytes, encodeVariable(typeCode, values, columnConstant, dictionary).bytes);
        }
    }

    /**
     * The one input class the frame-of-reference encodings must not get wrong quietly: a block whose
     * span exceeds a signed long, where {@code v - min} wraps. Two's complement makes the residual
     * the correct unsigned magnitude and the addition wraps back, so the round trip is exact.
     */
    @Test
    public void framesOfReferenceSurviveWraparound()
    {
        long[] values = { Long.MIN_VALUE, 0, Long.MAX_VALUE, -1, 1 };
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        assertArrayEquals(values, decodeFixed(ChunkV4Directory.TYPE_INT64, encoded, values.length, null));

        long[] ascending = { Long.MIN_VALUE, Long.MAX_VALUE };
        Encoded delta = encodeFixed(ChunkV4Directory.TYPE_INT64, ascending, null, null);
        assertArrayEquals(ascending, decodeFixed(ChunkV4Directory.TYPE_INT64, delta, 2, null));
    }

    /**
     * The gap {@code DELTA_FOR_BITPACK}'s exception area exists for: one missing sample in an
     * otherwise regular timestamp axis. The exception carries the value, not the delta, which is
     * what re-bases every delta after it.
     */
    @Test
    public void aTimestampGapCostsOneExceptionRatherThanAWiderLane()
    {
        long[] values = new long[256];
        for (int i = 0; i < values.length; i++)
            values[i] = 1_700_000_000_000L + 1000L * i;
        values[128] += 86_400_000L;                 // a day-long hole
        for (int i = 129; i < values.length; i++)
            values[i] += 86_400_000L;

        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        assertEquals(ChunkV4BlockTable.ENC_DELTA_FOR_BITPACK, encoded.choice.encoding);
        assertEquals(1, encoded.choice.exceptionCount);
        assertArrayEquals(values, decodeFixed(ChunkV4Directory.TYPE_INT64, encoded, values.length, null));
        // Cheaper than the widened lane it replaces: a 27-bit delta over 255 slots would be 856
        // bytes of header plus lane, and this is a fraction of it.
        assertTrue(encoded.bytes.length < 8 + 24 + BitPacking.packedLength(255, 27));
    }

    // -----------------------------------------------------------------------------------------
    // the ALP seam
    // -----------------------------------------------------------------------------------------

    /** With no codec installed a double block still encodes, through the universal RAW fallback. */
    @Test
    public void doubleBlocksFallBackToRawWithoutAnAlpCodec()
    {
        long[] values = new long[8];
        for (int i = 0; i < values.length; i++)
            values[i] = Double.doubleToRawLongBits(1.5 * i);
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_DOUBLE, values, null, null);
        assertEquals(ChunkV4BlockTable.ENC_RAW, encoded.choice.encoding);
        assertArrayEquals(values, decodeFixed(ChunkV4Directory.TYPE_DOUBLE, encoded, values.length, null));

        // Frame of reference is never offered for doubles: the block table's min is ordered by
        // Double.compare and the raw bits of a negative double ascend as the value descends.
        assertThatThrownBy(() -> BlockEncodings.decodeFixed(ChunkV4BlockTable.ENC_FOR_BITPACK, false,
                                                            ByteBuffer.wrap(new byte[16]), 16,
                                                            ChunkV4Directory.TYPE_DOUBLE, 4, 0, null, null,
                                                            new BlockEncodings.Scratch(64), new long[4]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** An ALP code inside a payload this build cannot have written is corruption, not a future format. */
    @Test
    public void anAlpCodeWithoutACodecIsCorruption()
    {
        for (int encoding : new int[]{ ChunkV4BlockTable.ENC_ALP, ChunkV4BlockTable.ENC_ALP_RD })
            assertThatThrownBy(() -> BlockEncodings.decodeFixed(encoding, false, ByteBuffer.wrap(new byte[32]), 32,
                                                                ChunkV4Directory.TYPE_DOUBLE, 4, 0, null, null,
                                                                new BlockEncodings.Scratch(64), new long[4]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The seam with its real occupant: {@link AlpBlockCodec} displaces {@code RAW} on decimal data,
     * through the same argmin as everything else.
     *
     * <p>The exhaustive behaviour of the codec -- bit-exactness, framing, the variant choice -- is
     * {@code DoubleBlockCodecTest}'s. What belongs here is only that this layer routes to it and that
     * the routing is a size decision.
     */
    @Test
    public void theRealAlpCodecDisplacesRawForADoubleBlock()
    {
        long[] values = new long[256];
        for (int i = 0; i < values.length; i++)
            values[i] = Double.doubleToRawLongBits(20.0 + (i % 97) * 0.01);

        Encoded withoutCodec = encodeFixed(ChunkV4Directory.TYPE_DOUBLE, values, null, null);
        assertEquals(ChunkV4BlockTable.ENC_RAW, withoutCodec.choice.encoding);

        Encoded withCodec = encodeFixed(ChunkV4Directory.TYPE_DOUBLE, values, null, AlpBlockCodec.INSTANCE);
        assertEquals(ChunkV4BlockTable.ENC_ALP, withCodec.choice.encoding);
        assertTrue(withCodec.bytes.length < withoutCodec.bytes.length);
        assertEquals(0, withCodec.bytes.length % 8);
        // An ALP block signals its exception area inside the payload, so it never sets bit 4.
        assertEquals(0, withCodec.choice.exceptionCount);

        long[] decoded = new long[values.length];
        BlockEncodings.decodeFixed(withCodec.choice.encoding, false, ByteBuffer.wrap(withCodec.bytes),
                                   withCodec.bytes.length, ChunkV4Directory.TYPE_DOUBLE, values.length, 0, null,
                                   AlpBlockCodec.INSTANCE, new BlockEncodings.Scratch(256), decoded);
        assertArrayEquals(values, decoded);
    }

    /**
     * The seam routes by exact size like every other candidate, and only for doubles.
     *
     * <p>This one deliberately uses a stub rather than {@link AlpBlockCodec}: it is about the routing
     * arithmetic in {@link BlockEncodings}, and a stub with a trivially predictable size keeps the
     * assertion about the argmin rather than about ALP's compression.
     */
    @Test
    public void theAlpSeamIsRoutedToWhenItIsSmaller()
    {
        BlockEncodings.DoubleBlockCodec codec = new VerbatimDoubleCodec();
        long[] values = new long[8];
        for (int i = 0; i < values.length; i++)
            values[i] = Double.doubleToRawLongBits(0.25 * i);

        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(64);
        BlockEncodings.Choice choice = new BlockEncodings.Choice();
        BlockEncodings.chooseFixed(ChunkV4Directory.TYPE_DOUBLE, values, values.length,
                                   minOf(values, values.length, ChunkV4Directory.TYPE_DOUBLE), null, codec, scratch,
                                   choice);
        // 64 bytes verbatim against RAW's roundUp8(8 * 9) = 72.
        assertEquals(ChunkV4BlockTable.ENC_ALP, choice.encoding);
        assertEquals(64, choice.payloadLength);

        ByteBuffer dst = ByteBuffer.wrap(new byte[choice.payloadLength]);
        BlockEncodings.encodeFixed(choice, ChunkV4Directory.TYPE_DOUBLE, values, values.length, 0, codec, scratch, dst);
        long[] decoded = new long[values.length];
        BlockEncodings.decodeFixed(choice.encoding, false, ByteBuffer.wrap(dst.array()), choice.payloadLength,
                                   ChunkV4Directory.TYPE_DOUBLE, values.length, 0, null, codec, scratch, decoded);
        assertArrayEquals(values, decoded);

        // The seam is never consulted for a non-double column, so a codec cannot change those bytes.
        BlockEncodings.chooseFixed(ChunkV4Directory.TYPE_INT64, new long[]{ 1, 2, 3 }, 3, 1, null, codec, scratch,
                                   choice);
        assertNotEquals(ChunkV4BlockTable.ENC_ALP, choice.encoding);
    }

    // -----------------------------------------------------------------------------------------
    // determinism traps and corruption
    // -----------------------------------------------------------------------------------------

    @Test
    public void laneHeaderPaddingMustBeZeroAndIsVerified()
    {
        long[] values = { 10, 11, 12, 13 };
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        for (int at = 1; at < 8; at++)
        {
            assertEquals("golden pad byte " + at, 0, encoded.bytes[at]);
            byte[] corrupt = encoded.bytes.clone();
            corrupt[at] = 1;
            assertThatThrownBy(() -> decodeFixed(ChunkV4Directory.TYPE_INT64, corrupt, encoded.choice, encoded.min,
                                                 4, null))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void bodyPaddingMustBeZeroAndIsVerified()
    {
        byte[][] values = { bytes("hi"), bytes("hi") };
        EncodedVariable encoded = encodeVariable(ChunkV4Directory.TYPE_TEXT, values, null, null);
        for (int at = 3; at < 8; at++)
        {
            byte[] corrupt = encoded.bytes.clone();
            corrupt[at] = 1;
            assertThatThrownBy(() -> BlockEncodings.decodeVariable(encoded.choice.encoding,
                                                                   ByteBuffer.wrap(corrupt), corrupt.length,
                                                                   ChunkV4Directory.TYPE_TEXT, 2, null, null,
                                                                   new BlockEncodings.Scratch(64), new byte[2][]))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /**
     * §5 rule 4: a non-minimal varint decodes to the same length and re-encodes shorter, so one
     * block would have two byte forms and {@code chunkUnchanged} would report a difference forever.
     * {@code 0x82 0x00} is 2 written non-minimally; the padding after it is still zero, so nothing
     * but the canonical-form check can reject this payload.
     */
    @Test
    public void nonCanonicalVarintsAreRejected()
    {
        byte[] canonical = hex("02" + "6869" + "0000000000");
        byte[] nonCanonical = hex("8200" + "6869" + "00000000");
        BlockEncodings.decodeVariable(ChunkV4BlockTable.ENC_CONSTANT, ByteBuffer.wrap(canonical), 8,
                                      ChunkV4Directory.TYPE_TEXT, 2, null, null, new BlockEncodings.Scratch(64),
                                      new byte[2][]);
        assertThatThrownBy(() -> BlockEncodings.decodeVariable(ChunkV4BlockTable.ENC_CONSTANT,
                                                               ByteBuffer.wrap(nonCanonical), 8,
                                                               ChunkV4Directory.TYPE_TEXT, 2, null, null,
                                                               new BlockEncodings.Scratch(64), new byte[2][]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** A lane slot under an exception that is not the specified filler 0 (§5 rule 6). */
    @Test
    public void aNonZeroFillerUnderAnExceptionIsRejected()
    {
        long[] values = { 0, 1, 2, 3, 0, 1, 2, 1L << 39 };
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        byte[] corrupt = encoded.bytes.clone();
        // Lane slot 7 occupies bits 14..15 of the single lane word, i.e. the low bits of byte 14.
        corrupt[14] = (byte) (corrupt[14] | 0x40);
        assertThatThrownBy(() -> decodeFixed(ChunkV4Directory.TYPE_INT64, corrupt, encoded.choice, encoded.min,
                                             8, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** The block table's exception bit and the bytes have to agree, in both directions. */
    @Test
    public void aMisdeclaredExceptionFlagIsRejected()
    {
        long[] values = { 0, 1, 2, 3, 0, 1, 2, 1L << 39 };
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(64);
        // claimed absent, present in the bytes
        assertThatThrownBy(() -> BlockEncodings.decodeFixed(encoded.choice.encoding, false,
                                                            ByteBuffer.wrap(encoded.bytes), encoded.bytes.length,
                                                            ChunkV4Directory.TYPE_INT64, 8, 0, null, null, scratch,
                                                            new long[8]))
            .isInstanceOf(IllegalArgumentException.class);

        long[] plain = { 10, 11, 12, 13 };
        Encoded noExceptions = encodeFixed(ChunkV4Directory.TYPE_INT64, plain, null, null);
        // claimed present, absent from the bytes
        assertThatThrownBy(() -> BlockEncodings.decodeFixed(noExceptions.choice.encoding, true,
                                                            ByteBuffer.wrap(noExceptions.bytes),
                                                            noExceptions.bytes.length, ChunkV4Directory.TYPE_INT64,
                                                            4, 10, null, null, scratch, new long[4]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void unknownEncodingCodesAreCorruption()
    {
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(64);
        for (int code : new int[]{ 0x00, 0x04, 0x12, 0x22, 0x31, 0x42, 0xFF })
            assertThatThrownBy(() -> BlockEncodings.decodeFixed(code, false, ByteBuffer.wrap(new byte[8]), 8,
                                                                ChunkV4Directory.TYPE_INT64, 1, 0, null, null,
                                                                scratch, new long[1]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void truncateAtEveryPrefixLength()
    {
        long[] values = { 0, 1, 2, 3, 0, 1, 2, 1L << 39 };
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        for (int length = 0; length < encoded.bytes.length; length++)
        {
            byte[] truncated = Arrays.copyOf(encoded.bytes, length);
            try
            {
                BlockEncodings.decodeFixed(encoded.choice.encoding, true, ByteBuffer.wrap(truncated), length,
                                           ChunkV4Directory.TYPE_INT64, 8, 0, null, null,
                                           new BlockEncodings.Scratch(64), new long[8]);
                fail("a " + length + "-byte payload parsed a " + encoded.bytes.length + "-byte block");
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
        }
    }

    @Test
    public void singleBitFlipsNeverEscapeAsUncheckedOrOom()
    {
        long[] values = { 0, 1, 2, 3, 0, 1, 2, 1L << 39 };
        Encoded encoded = encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, null);
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(64);
        int accepted = 0;
        for (int bit = 0; bit < encoded.bytes.length * 8; bit++)
        {
            byte[] flipped = encoded.bytes.clone();
            flipped[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try
            {
                BlockEncodings.decodeFixed(encoded.choice.encoding, true, ByteBuffer.wrap(flipped),
                                           flipped.length, ChunkV4Directory.TYPE_INT64, 8, 0, null, null, scratch,
                                           new long[8]);
                accepted++;
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
            catch (RuntimeException unexpected)
            {
                fail("block-payload bit " + bit + " escaped as " + unexpected);
            }
        }
        if (accepted == 0)
            fail("every bit flip was rejected, which means the payload is not being parsed");
    }

    @Test
    public void aRawValueLengthCannotReserveMemory()
    {
        // A varint claiming 2^31-1 bytes inside an 8-byte body: checked against the bytes that are
        // there, before the array exists.
        byte[] body = hex("FFFFFFFF07" + "000000");
        assertThatThrownBy(() -> BlockEncodings.decodeVariable(ChunkV4BlockTable.ENC_RAW, ByteBuffer.wrap(body), 8,
                                                               ChunkV4Directory.TYPE_OPAQUE, 1, null, null,
                                                               new BlockEncodings.Scratch(64), new byte[1][]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void valuesOutsideTheirTypeDomainAreRejected()
    {
        BlockEncodings.checkValueDomain(Integer.MIN_VALUE, ChunkV4Directory.TYPE_INT32);
        assertThatThrownBy(() -> BlockEncodings.checkValueDomain(1L + Integer.MAX_VALUE,
                                                                 ChunkV4Directory.TYPE_INT32))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockEncodings.checkValueDomain(-1, ChunkV4Directory.TYPE_DATE32))
            .isInstanceOf(IllegalArgumentException.class);
        // BooleanType normalises any non-zero byte to true, so a second truthy byte would be a
        // second byte form of one value.
        assertThatThrownBy(() -> BlockEncodings.checkValueDomain(2, ChunkV4Directory.TYPE_BOOLEAN))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void statBitsRoundTripThroughTheBlockTableConvention()
    {
        // Zero-extended on the way in, widened by the type's signedness on the way out -- the same
        // convention ChunkV4BlockTable.Entry documents for its extrema.
        assertEquals(0xFFFFFFFBL, BlockEncodings.toStatBits(-5, ChunkV4Directory.TYPE_INT32));
        assertEquals(-5L, BlockEncodings.fromStatBits(0xFFFFFFFBL, ChunkV4Directory.TYPE_INT32));
        assertEquals(0xFFFFFFFBL, BlockEncodings.toStatBits(0xFFFFFFFBL, ChunkV4Directory.TYPE_DATE32));
        assertEquals(0xFFFFFFFBL, BlockEncodings.fromStatBits(0xFFFFFFFBL, ChunkV4Directory.TYPE_DATE32));
        assertEquals(-5L, BlockEncodings.toStatBits(-5, ChunkV4Directory.TYPE_INT64));
        assertEquals(0, BlockEncodings.toStatBits(0, ChunkV4Directory.TYPE_TEXT));
        assertThatThrownBy(() -> BlockEncodings.toStatBits(1, ChunkV4Directory.TYPE_TEXT))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockEncodings.fromStatBits(0x1_0000_0000L, ChunkV4Directory.TYPE_INT32))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void dictionaryCodeWidthIsCeilLogTwo()
    {
        assertEquals(0, BlockEncodings.dictionaryCodeBits(1));
        assertEquals(1, BlockEncodings.dictionaryCodeBits(2));
        assertEquals(2, BlockEncodings.dictionaryCodeBits(3));
        assertEquals(2, BlockEncodings.dictionaryCodeBits(4));
        assertEquals(3, BlockEncodings.dictionaryCodeBits(5));
        assertEquals(8, BlockEncodings.dictionaryCodeBits(256));
        assertEquals(9, BlockEncodings.dictionaryCodeBits(257));
        assertEquals(16, BlockEncodings.dictionaryCodeBits(ChunkV4Section.MAX_DICTIONARY_ENTRIES));
        assertThatThrownBy(() -> BlockEncodings.dictionaryCodeBits(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockEncodings.dictionaryCodeBits(0x10000))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** A representable code the dictionary does not define is corruption, not an array index. */
    @Test
    public void outOfRangeDictionaryCodesAreRejected()
    {
        byte[][] dictionary = { bytes("a"), bytes("b"), bytes("c") };
        // one value, code 3, packed at two bits
        byte[] body = new byte[8];
        body[7] = 0x03;
        assertThatThrownBy(() -> BlockEncodings.decodeVariable(ChunkV4BlockTable.ENC_DICT, ByteBuffer.wrap(body), 8,
                                                               ChunkV4Directory.TYPE_TEXT, 1, null, dictionary,
                                                               new BlockEncodings.Scratch(64), new byte[1][]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------------

    private static final int[] FIXED_TYPES = { ChunkV4Directory.TYPE_DOUBLE, ChunkV4Directory.TYPE_BOOLEAN,
                                               ChunkV4Directory.TYPE_INT32, ChunkV4Directory.TYPE_INT64,
                                               ChunkV4Directory.TYPE_DATE32 };

    /** A stub with a trivially predictable size: 8 verbatim bytes per value, exact and 8-aligned. */
    private static final class VerbatimDoubleCodec implements BlockEncodings.DoubleBlockCodec
    {
        public long payloadLength(int encoding, long[] rawBits, int count)
        {
            return encoding == ChunkV4BlockTable.ENC_ALP ? 8L * count : -1;
        }

        public int encode(int encoding, long[] rawBits, int count, ByteBuffer dst)
        {
            for (int i = 0; i < count; i++)
                dst.putLong(rawBits[i]);
            return 8 * count;
        }

        public void decode(int encoding, ByteBuffer payload, int payloadLength, int count, long[] dst)
        {
            if (payloadLength != 8 * count)
                throw new IllegalArgumentException("bad ALP payload length " + payloadLength);
            for (int i = 0; i < count; i++)
                dst[i] = payload.getLong();
        }
    }

    private static final class Encoded
    {
        final byte[] bytes;
        final BlockEncodings.Choice choice;
        /** The block table's min, which is where FOR_BITPACK and CONSTANT keep their value. */
        final long min;

        Encoded(byte[] bytes, BlockEncodings.Choice choice, long min)
        {
            this.bytes = bytes;
            this.choice = choice;
            this.min = min;
        }
    }

    private static final class EncodedVariable
    {
        final byte[] bytes;
        final BlockEncodings.Choice choice;

        EncodedVariable(byte[] bytes, BlockEncodings.Choice choice)
        {
            this.bytes = bytes;
            this.choice = choice;
        }
    }

    private static Encoded encodeFixed(int typeCode, long[] values, byte[] columnConstant,
                                       BlockEncodings.DoubleBlockCodec alp)
    {
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(Math.max(values.length, 1));
        BlockEncodings.Choice choice = new BlockEncodings.Choice();
        long min = minOf(values, values.length, typeCode);
        BlockEncodings.chooseFixed(typeCode, values, values.length, min, columnConstant, alp, scratch, choice);
        ByteBuffer dst = ByteBuffer.wrap(new byte[choice.payloadLength]);
        BlockEncodings.encodeFixed(choice, typeCode, values, values.length, min, alp, scratch, dst);
        assertEquals(choice.payloadLength, dst.position());
        return new Encoded(dst.array(), choice, min);
    }

    private static byte[] encodeFixedInto(int typeCode, long[] values, byte[] columnConstant, byte fill)
    {
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(Math.max(values.length, 1));
        BlockEncodings.Choice choice = new BlockEncodings.Choice();
        long min = minOf(values, values.length, typeCode);
        BlockEncodings.chooseFixed(typeCode, values, values.length, min, columnConstant, null, scratch, choice);
        byte[] out = new byte[choice.payloadLength];
        Arrays.fill(out, fill);
        BlockEncodings.encodeFixed(choice, typeCode, values, values.length, min, null, scratch,
                                   ByteBuffer.wrap(out));
        return out;
    }

    private static long[] decodeFixed(int typeCode, Encoded encoded, int count, byte[] columnConstant)
    {
        return decodeFixed(typeCode, encoded.bytes, encoded.choice, encoded.min, count, columnConstant);
    }

    private static long[] decodeFixed(int typeCode, byte[] bytes, BlockEncodings.Choice choice, long min, int count,
                                      byte[] columnConstant)
    {
        long[] dst = new long[Math.max(count, 1)];
        BlockEncodings.decodeFixed(choice.encoding, choice.hasExceptions(), ByteBuffer.wrap(bytes), bytes.length,
                                   typeCode, count, min, columnConstant, null,
                                   new BlockEncodings.Scratch(Math.max(count, 1)), dst);
        return Arrays.copyOf(dst, count);
    }

    private static EncodedVariable encodeVariable(int typeCode, byte[][] values, byte[] columnConstant,
                                                  byte[][] dictionary)
    {
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(Math.max(values.length, 1));
        BlockEncodings.Choice choice = new BlockEncodings.Choice();
        BlockEncodings.chooseVariable(typeCode, values, values.length, columnConstant, dictionary, choice);
        ByteBuffer dst = ByteBuffer.wrap(new byte[choice.payloadLength]);
        BlockEncodings.encodeVariable(choice, typeCode, values, values.length, dictionary, scratch, dst);
        assertEquals(choice.payloadLength, dst.position());
        return new EncodedVariable(dst.array(), choice);
    }

    private static void assertVariableRoundTrip(int typeCode, byte[][] values, EncodedVariable encoded,
                                                byte[] columnConstant, byte[][] dictionary)
    {
        byte[][] dst = new byte[Math.max(values.length, 1)][];
        BlockEncodings.decodeVariable(encoded.choice.encoding, ByteBuffer.wrap(encoded.bytes), encoded.bytes.length,
                                      typeCode, values.length, columnConstant, dictionary,
                                      new BlockEncodings.Scratch(Math.max(values.length, 1)), dst);
        for (int i = 0; i < values.length; i++)
            assertArrayEquals("value " + i, values[i], dst[i]);
    }

    private static long minOf(long[] values, int count, int typeCode)
    {
        if (count == 0 || ChunkV4Directory.statWidth(typeCode) == 0)
            return 0;
        long min = values[0];
        for (int i = 1; i < count; i++)
            if (BlockEncodings.compareValues(values[i], min, typeCode) < 0)
                min = values[i];
        return min;
    }

    private static StatOrder statOrderOf(int typeCode)
    {
        switch (typeCode)
        {
            case ChunkV4Directory.TYPE_INT32:
            case ChunkV4Directory.TYPE_INT64:
                return StatOrder.SIGNED_INT;
            case ChunkV4Directory.TYPE_DATE32:
                return StatOrder.UNSIGNED_INT;
            case ChunkV4Directory.TYPE_DOUBLE:
                return StatOrder.IEEE754_TOTAL;
            default:
                return StatOrder.NONE;
        }
    }

    private static long randomValue(Random random, int typeCode)
    {
        switch (typeCode)
        {
            case ChunkV4Directory.TYPE_DOUBLE:
            {
                double[] specials = { 0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                                      Double.MIN_VALUE, Double.MAX_VALUE, -1.5, 1e300 };
                return random.nextInt(4) == 0
                       ? Double.doubleToRawLongBits(specials[random.nextInt(specials.length)])
                       : Double.doubleToRawLongBits(random.nextGaussian() * 1000);
            }
            case ChunkV4Directory.TYPE_BOOLEAN:
                return random.nextBoolean() ? 1 : 0;
            case ChunkV4Directory.TYPE_INT32:
                return random.nextInt();
            case ChunkV4Directory.TYPE_DATE32:
                return random.nextInt() & 0xFFFFFFFFL;
            default:
                return random.nextInt(8) == 0 ? random.nextLong() : random.nextInt(1000);
        }
    }

    private static long[] randomValues(Random random, int typeCode, int count)
    {
        long[] values = new long[count];
        for (int i = 0; i < count; i++)
            values[i] = randomValue(random, typeCode);
        return values;
    }

    private static byte[] randomBytes(Random random, int length)
    {
        byte[] out = new byte[length];
        random.nextBytes(out);
        return out;
    }

    private static byte[][] distinctSorted(byte[][] values, int count)
    {
        if (count == 0)
            return null;
        byte[][] all = Arrays.copyOf(values, count);
        Arrays.sort(all, ChunkV4Section::compareUnsigned);
        int distinct = 0;
        for (int i = 0; i < count; i++)
            if (i == 0 || ChunkV4Section.compareUnsigned(all[i], all[i - 1]) != 0)
                all[distinct++] = all[i];
        return Arrays.copyOf(all, distinct);
    }

    private static byte[] bytes(String ascii)
    {
        return ascii.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    }
}
