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
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Pins the normative bit order of chunk-format-v4 §6 and the width chooser of §5.
 *
 * <p>The round-trip tests are properties over every width and a spread of counts rather than
 * examples, because a bit packer that is wrong only where values straddle a word boundary passes
 * any example whose width divides 64. The determinism tests pack into pre-dirtied buffers, because
 * §5 rule 5's failure mode -- stale bytes leaking into padding -- leaves every round-trip test
 * green while making identical input produce different bytes.
 */
public class BitPackingTest
{
    private static final int[] COUNTS = { 0, 1, 63, 64, 65, 1023, 1024, 1025 };

    // -----------------------------------------------------------------------------------------
    // §6 bit order
    // -----------------------------------------------------------------------------------------

    /**
     * The §6 worked example: {@code [1, 2, 3]} at width 2. Value 0 takes bits 0..1, value 1 takes
     * bits 2..3, value 2 takes bits 4..5, all counted from the least significant bit of a
     * big-endian word, so the word is {@code 1 | (2 << 2) | (3 << 4)} and the lane is that word
     * serialised big-endian.
     */
    @Test
    public void goldenVectorFromSpecSection6()
    {
        // The spec's own closed form, evaluated. Asserted so the expectation below is derived from
        // §6 rather than copied from the implementation.
        assertEquals(0x39L, 1L | (2L << 2) | (3L << 4));

        byte[] packed = BitPacking.pack(new long[]{ 1, 2, 3 }, 3, 2);
        assertArrayEquals(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0x39 }, packed);
        assertEquals(8, packed.length);

        long[] out = new long[3];
        BitPacking.unpack(packed, 0, 3, 2, out);
        assertArrayEquals(new long[]{ 1, 2, 3 }, out);
    }

    /**
     * The hex literal printed beside that formula in §6 is {@code 0x31}, one nibble off from the
     * value its own closed form produces. Decoded under the rule §6 states in the preceding
     * sentence, {@code 0x31} is {@code [1, 0, 3]} -- the second value is gone. No bit-order
     * convention turns {@code [1, 2, 3]} into {@code 0x31}; it is a typo, not a second reading.
     *
     * <p>Machine-checked here so that a future reader who trusts the literal and "fixes" the packer
     * gets a red test instead of a second, silently incompatible bit order in the format.
     */
    @Test
    public void specSection6HexLiteralIsATypoForZeroX39()
    {
        long[] out = new long[3];
        BitPacking.unpack(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0x31 }, 0, 3, 2, out);
        assertArrayEquals(new long[]{ 1, 0, 3 }, out);
    }

    /**
     * Width 3 over 22 values spans exactly 66 bits, so value 21 straddles the word boundary with
     * one bit in word 0 and two in word 1. Hand-computed from §6 rather than from the packer.
     */
    @Test
    public void valuesStraddleTheWordBoundary()
    {
        long[] values = new long[22];
        for (int i = 0; i < values.length; i++)
            values[i] = i % 8;

        long[] expected = new long[2];
        for (int i = 0; i < values.length; i++)
        {
            int bit = i * 3;
            expected[bit >>> 6] |= values[i] << (bit & 63);
            if ((bit & 63) > 61)
                expected[(bit >>> 6) + 1] |= values[i] >>> (64 - (bit & 63));
        }

        byte[] packed = BitPacking.pack(values, values.length, 3);
        assertEquals(16, packed.length);
        ByteBuffer view = ByteBuffer.wrap(packed);
        assertEquals(expected[0], view.getLong(0));
        assertEquals(expected[1], view.getLong(8));

        long[] out = new long[values.length];
        BitPacking.unpack(packed, 0, values.length, 3, out);
        assertArrayEquals(values, out);
    }

    // -----------------------------------------------------------------------------------------
    // round trip
    // -----------------------------------------------------------------------------------------

    @Test
    public void roundTripEveryWidthByEveryCount()
    {
        Random random = new Random(20260803L);
        for (int width = 0; width <= BitPacking.MAX_WIDTH; width++)
        {
            for (int count : COUNTS)
            {
                long[] values = randomValues(random, count, width);
                byte[] packed = BitPacking.pack(values, count, width);
                assertEquals("width " + width + " count " + count,
                             expectedLength(count, width), packed.length);

                long[] out = new long[Math.max(count, 1)];
                BitPacking.unpack(packed, 0, count, width, out);
                for (int i = 0; i < count; i++)
                    assertEquals("width " + width + " count " + count + " index " + i, values[i], out[i]);
            }
        }
    }

    @Test
    public void roundTripThroughAByteBufferAtANonZeroPosition()
    {
        Random random = new Random(7L);
        for (int width = 0; width <= BitPacking.MAX_WIDTH; width++)
        {
            int count = 700;
            long[] values = randomValues(random, count, width);
            int length = BitPacking.packedLength(count, width);

            ByteBuffer buffer = ByteBuffer.allocate(length + 24);
            fill(buffer, (byte) 0xFF);
            buffer.position(13);
            assertEquals(length, BitPacking.pack(values, count, width, buffer));
            assertEquals(13 + length, buffer.position());

            // The bytes on either side of the lane are untouched: pack writes exactly its length.
            assertEquals((byte) 0xFF, buffer.get(12));
            for (int i = 13 + length; i < buffer.capacity(); i++)
                assertEquals((byte) 0xFF, buffer.get(i));

            buffer.position(13);
            long[] out = new long[count];
            assertEquals(length, BitPacking.unpack(buffer, count, width, out));
            assertEquals(13 + length, buffer.position());
            assertArrayEquals("width " + width, values, out);
        }
    }

    @Test
    public void widthZeroCostsNoBytesAndDecodesToZeros()
    {
        assertEquals(0, BitPacking.packedLength(1024, 0));
        byte[] packed = BitPacking.pack(new long[1024], 1024, 0);
        assertEquals(0, packed.length);

        long[] out = new long[1024];
        Arrays.fill(out, 7L);
        BitPacking.unpack(packed, 0, 1024, 0, out);
        assertArrayEquals(new long[1024], out);
    }

    @Test
    public void packedLengthIsCeilingOfBitsOverAWord()
    {
        for (int width = 0; width <= BitPacking.MAX_WIDTH; width++)
            for (int count : COUNTS)
                assertEquals("width " + width + " count " + count,
                             expectedLength(count, width), BitPacking.packedLength(count, width));

        assertEquals(8, BitPacking.packedLength(3, 2));
        assertEquals(8, BitPacking.packedLength(64, 1));
        assertEquals(16, BitPacking.packedLength(65, 1));
        assertEquals(896, BitPacking.packedLength(1024, 7));
        assertEquals(8192, BitPacking.packedLength(1024, 64));
    }

    // -----------------------------------------------------------------------------------------
    // byte determinism (§5 rule 5)
    // -----------------------------------------------------------------------------------------

    @Test
    public void encodeTwiceIsByteIdentical()
    {
        Random random = new Random(99L);
        for (int trial = 0; trial < 200; trial++)
        {
            int width = random.nextInt(BitPacking.MAX_WIDTH + 1);
            int count = 1 + random.nextInt(1200);
            long[] values = randomValues(random, count, width);
            assertArrayEquals(BitPacking.pack(values, count, width), BitPacking.pack(values, count, width));
        }
    }

    /**
     * The same input packed into a scratch buffer full of stale bytes must produce the same bytes
     * as into a fresh one. This is the §5 rule 5 hazard exactly: the tail word of a lane whose
     * count is not a multiple of 64 has bits past the last value, and an encoder that ORs into a
     * reused accumulator leaves whatever was there before -- byte-different output for identical
     * input, a re-encoder that never converges, and a round-trip test that still passes.
     */
    @Test
    public void packingIntoADirtyBufferIsByteIdenticalToAFreshOne()
    {
        Random random = new Random(4242L);
        for (int trial = 0; trial < 200; trial++)
        {
            int width = random.nextInt(BitPacking.MAX_WIDTH + 1);
            // Counts deliberately off a multiple of 64 so most lanes have a partial tail word.
            int count = 1 + random.nextInt(300);
            long[] values = randomValues(random, count, width);
            int length = BitPacking.packedLength(count, width);

            byte[] fresh = BitPacking.pack(values, count, width);

            ByteBuffer dirty = ByteBuffer.allocate(length);
            fill(dirty, (byte) 0xA5);
            dirty.position(0);
            BitPacking.pack(values, count, width, dirty);
            assertArrayEquals("width " + width + " count " + count, fresh, dirty.array());
        }
    }

    // -----------------------------------------------------------------------------------------
    // rejection
    // -----------------------------------------------------------------------------------------

    @Test
    public void packRejectsAValueWiderThanItsWidth()
    {
        assertThatThrownBy(() -> BitPacking.pack(new long[]{ 0, 4, 1 }, 3, 2))
            .isInstanceOf(IllegalArgumentException.class);
        // width 0 means every value is zero; a non-zero one is an encoder bug, not a truncation.
        assertThatThrownBy(() -> BitPacking.pack(new long[]{ 0, 0, 1 }, 3, 0))
            .isInstanceOf(IllegalArgumentException.class);
        // a negative long is a width-64 magnitude, so it is only packable verbatim
        assertThatThrownBy(() -> BitPacking.pack(new long[]{ -1L }, 1, 63))
            .isInstanceOf(IllegalArgumentException.class);
        assertArrayEquals(new byte[]{ -1, -1, -1, -1, -1, -1, -1, -1 },
                          BitPacking.pack(new long[]{ -1L }, 1, 64));
    }

    @Test
    public void widthsOutsideZeroToSixtyFourAreRejected()
    {
        assertThatThrownBy(() -> BitPacking.packedLength(10, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.packedLength(10, 65)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.pack(new long[10], 10, 65)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.unpack(new byte[64], 0, 10, 65, new long[10]))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.packedLength(-1, 8)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void truncatedLanesAreRejectedRatherThanRead()
    {
        byte[] packed = BitPacking.pack(new long[]{ 1, 2, 3, 4 }, 4, 17);
        assertEquals(16, packed.length);
        for (int prefix = 0; prefix < packed.length; prefix++)
        {
            byte[] truncated = Arrays.copyOf(packed, prefix);
            assertThatThrownBy(() -> BitPacking.unpack(truncated, 0, 4, 17, new long[4]))
                .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> BitPacking.unpack(packed, 0, 4, 17, new long[3]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void littleEndianBuffersAreRejected()
    {
        ByteBuffer little = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        assertThatThrownBy(() -> BitPacking.pack(new long[]{ 1 }, 1, 8, little))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.unpack(little, 1, 8, new long[1]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // §5 width chooser
    // -----------------------------------------------------------------------------------------

    @Test
    public void exceptionCountMatchesBruteForce()
    {
        Random random = new Random(11L);
        for (int trial = 0; trial < 50; trial++)
        {
            long[] values = skewedValues(random, 500);
            for (int width = 0; width <= BitPacking.MAX_WIDTH; width++)
            {
                int expected = 0;
                for (long v : values)
                    if (bitLength(v) > width)
                        expected++;
                assertEquals("width " + width, expected, BitPacking.exceptionCount(values, values.length, width));
            }
        }
    }

    @Test
    public void chooseWidthMatchesBruteForceArgmin()
    {
        Random random = new Random(1234L);
        int[] scratch = new int[BitPacking.WIDTH_HISTOGRAM_SIZE];
        for (int trial = 0; trial < 400; trial++)
        {
            int count = 1 + random.nextInt(1200);
            long[] values = skewedValues(random, count);
            int exceptionValueBytes = 1 << random.nextInt(4); // 1, 2, 4, 8

            int expected = bruteForceWidth(values, count, exceptionValueBytes);
            assertEquals(expected, BitPacking.chooseWidth(values, count, exceptionValueBytes));
            // The reusable-histogram overload must agree, and must be safe to call repeatedly with
            // the same scratch: it zeroes what it uses rather than trusting the caller.
            assertEquals(expected, BitPacking.chooseWidth(values, count, exceptionValueBytes, scratch));
            assertEquals(expected, BitPacking.chooseWidth(values, count, exceptionValueBytes, scratch));
        }
    }

    /**
     * An input constructed so that two widths cost exactly the same number of bytes: 64 values, 52
     * of them 8 bits wide and 12 of them 16, with 2-byte exception values.
     *
     * <p>Width 8 pays 64 lane bytes plus a 16-byte exception header plus {@code roundUp8(12*4)} =
     * 48, so 128. Width 16 pays 128 lane bytes and no exceptions, so 128. §5 says the smaller width
     * wins, and it is a determinism rule rather than a size preference: a replica resolving this
     * tie the other way writes a byte-different chunk, {@code chunkUnchanged} sees a difference, and
     * every chunk is rewritten every cycle.
     */
    @Test
    public void chooseWidthBreaksAnExactTieTowardsTheSmallerWidth()
    {
        long[] values = new long[64];
        Arrays.fill(values, 0, 52, 0xFFL);
        Arrays.fill(values, 52, 64, 0xFFFFL);

        assertEquals(128L, BitPacking.widthCost(64, 8, 12, 2));
        assertEquals(128L, BitPacking.widthCost(64, 16, 0, 2));
        assertEquals(12, BitPacking.exceptionCount(values, 64, 8));
        assertEquals(0, BitPacking.exceptionCount(values, 64, 16));

        assertEquals(8, BitPacking.chooseWidth(values, 64, 2));
        assertEquals(8, bruteForceWidth(values, 64, 2));
    }

    @Test
    public void chooseWidthIsZeroForAnAllZeroBlockAndSixtyFourForVerbatimOnes()
    {
        assertEquals(0, BitPacking.chooseWidth(new long[1024], 1024, 8));
        assertEquals(0L, BitPacking.widthCost(1024, 0, 0, 8));

        long[] wide = new long[1024];
        Arrays.fill(wide, -1L);
        assertEquals(64, BitPacking.chooseWidth(wide, 1024, 8));
    }

    @Test
    public void widthCostIsTheClosedFormFromSection5()
    {
        // no exceptions: lane bytes only
        assertEquals(8L, BitPacking.widthCost(64, 1, 0, 8));
        assertEquals(8192L, BitPacking.widthCost(1024, 64, 0, 8));
        // one exception: fixed 16 plus roundUp8(1 * (2 + 8)) = 16
        assertEquals(1024L + 16 + 16, BitPacking.widthCost(1024, 8, 1, 8));
        // the exception body rounds up to 8
        assertEquals(1024L + 16 + 24, BitPacking.widthCost(1024, 8, 2, 8));
    }

    @Test
    public void widthChooserRejectsImpossibleArguments()
    {
        assertThatThrownBy(() -> BitPacking.chooseWidth(new long[4], 4, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.chooseWidth(new long[4], 4, 9))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.chooseWidth(new long[4], 8, 8))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.chooseWidth(new long[4], 4, 8, new int[64]))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BitPacking.widthCost(10, 8, 11, 8))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // helpers -- deliberately independent of the implementation under test
    // -----------------------------------------------------------------------------------------

    private static int bruteForceWidth(long[] values, int count, int exceptionValueBytes)
    {
        long bestCost = Long.MAX_VALUE;
        int bestWidth = -1;
        for (int width = 0; width <= 64; width++)
        {
            int exceptions = 0;
            for (int i = 0; i < count; i++)
                if (bitLength(values[i]) > width)
                    exceptions++;
            long lanes = (((long) count * width + 63) / 64) * 8;
            long cost = lanes;
            if (exceptions > 0)
                cost += 16 + ((((long) exceptions * (2 + exceptionValueBytes)) + 7) / 8) * 8;
            if (cost < bestCost)
            {
                bestCost = cost;
                bestWidth = width;
            }
        }
        return bestWidth;
    }

    private static int bitLength(long value)
    {
        return 64 - Long.numberOfLeadingZeros(value);
    }

    private static int expectedLength(int count, int width)
    {
        return (int) ((((long) count * width + 63) / 64) * 8);
    }

    private static long[] randomValues(Random random, int count, int width)
    {
        long[] values = new long[count];
        if (width == 0)
            return values;
        for (int i = 0; i < count; i++)
            values[i] = random.nextLong() >>> (64 - width);
        return values;
    }

    /** Mostly narrow with a tail of outliers -- the shape the shared exception area exists for. */
    private static long[] skewedValues(Random random, int count)
    {
        long[] values = new long[count];
        int narrow = 1 + random.nextInt(20);
        int wide = narrow + random.nextInt(64 - narrow + 1);
        int outlierOdds = 1 + random.nextInt(64);
        for (int i = 0; i < count; i++)
        {
            int width = random.nextInt(outlierOdds) == 0 ? wide : narrow;
            values[i] = width == 0 ? 0 : random.nextLong() >>> (64 - width);
        }
        return values;
    }

    private static void fill(ByteBuffer buffer, byte value)
    {
        buffer.position(0);
        while (buffer.hasRemaining())
            buffer.put(value);
        buffer.position(0);
    }
}
