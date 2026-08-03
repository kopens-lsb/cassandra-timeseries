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
import static org.junit.Assert.fail;

/**
 * Pins the shared exception area of chunk-format-v4 §5: its layout at all three value widths, the
 * ordering and distinctness rules, and the zero filler.
 *
 * <p>The golden vectors are hand-computed from §5's field list rather than captured from the
 * encoder, because the area is a run of adjacent fixed-width fields -- exactly the shape where
 * swapping {@code position} with the first two bytes of a value round-trips perfectly and makes
 * every chunk written by another build unreadable.
 */
public class ExceptionAreaTest
{
    /**
     * {@code W = 8}: two exceptions at lane slots 3 and 1000. Entries occupy {@code 2 * 10 = 20}
     * bytes, padded to 24, so the area is {@code 8 + 24 = 32}.
     */
    private static final String GOLDEN_W8_HEX =
        "0002" + "0000" + "00000000" +
        "0003" + "0000000080000000" +
        "03E8" + "FFFFFFFFFFFFFFFF" +
        "00000000";

    /** {@code W = 4}: one exception, 6 bytes of entry padded to 8, so 16 bytes in all. */
    private static final String GOLDEN_W4_HEX =
        "0001" + "0000" + "00000000" +
        "0005" + "FFFFFFFF" +
        "0000";

    /** {@code W = 2}, the ALP-RD left-part width: three entries of 4 bytes, padded 12 -> 16. */
    private static final String GOLDEN_W2_HEX =
        "0003" + "0000" + "00000000" +
        "0000" + "0001" +
        "0001" + "0002" +
        "0002" + "0003" +
        "00000000";

    // -----------------------------------------------------------------------------------------
    // layout
    // -----------------------------------------------------------------------------------------

    @Test
    public void goldenVectorsForEachValueWidth()
    {
        assertArrayEquals(hex(GOLDEN_W8_HEX),
                          ExceptionArea.encode(new int[]{ 3, 1000 }, new long[]{ 0x80000000L, -1L }, 2, 8));
        assertArrayEquals(hex(GOLDEN_W4_HEX),
                          ExceptionArea.encode(new int[]{ 5 }, new long[]{ 0xFFFFFFFFL }, 1, 4));
        assertArrayEquals(hex(GOLDEN_W2_HEX),
                          ExceptionArea.encode(new int[]{ 0, 1, 2 }, new long[]{ 1, 2, 3 }, 3, 2));
        assertEquals(32, GOLDEN_W8_HEX.length() / 2);
        assertEquals(16, GOLDEN_W4_HEX.length() / 2);
        assertEquals(24, GOLDEN_W2_HEX.length() / 2);

        int[] positions = new int[8];
        long[] values = new long[8];
        ByteBuffer w8 = ByteBuffer.wrap(hex(GOLDEN_W8_HEX));
        assertEquals(2, ExceptionArea.decode(w8, 8, 1024, positions, values));
        assertEquals(32, w8.position());
        assertEquals(3, positions[0]);
        assertEquals(1000, positions[1]);
        assertEquals(0x80000000L, values[0]);
        // Zero-extended raw bits, not a number: all eight bytes set is 2^64-1 read back as -1L.
        assertEquals(-1L, values[1]);

        ByteBuffer w4 = ByteBuffer.wrap(hex(GOLDEN_W4_HEX));
        assertEquals(1, ExceptionArea.decode(w4, 4, 64, positions, values));
        assertEquals(5, positions[0]);
        assertEquals(0xFFFFFFFFL, values[0]);

        ByteBuffer w2 = ByteBuffer.wrap(hex(GOLDEN_W2_HEX));
        assertEquals(3, ExceptionArea.decode(w2, 2, 64, positions, values));
        assertArrayEquals(new long[]{ 1, 2, 3 }, Arrays.copyOf(values, 3));
    }

    @Test
    public void encodedLengthIsTheClosedFormFromSectionFive()
    {
        // header + roundUp8(count * (2 + W)), and nothing at all when there are no exceptions.
        assertEquals(8, ExceptionArea.HEADER_BYTES);
        for (int width : new int[]{ 2, 4, 8 })
        {
            assertEquals(0, ExceptionArea.encodedLength(0, width));
            for (int count = 1; count < 200; count++)
            {
                long expected = 8 + ((count * (2L + width) + 7) & ~7L);
                assertEquals("count " + count + " W " + width, expected, ExceptionArea.encodedLength(count, width));
                assertEquals(0, ExceptionArea.encodedLength(count, width) % 8);
            }
        }
        for (int bad : new int[]{ 0, 1, 3, 5, 6, 7, 9, 16 })
            assertThatThrownBy(() -> ExceptionArea.encodedLength(1, bad)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExceptionArea.encodedLength(-1, 8)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExceptionArea.encodedLength(0x10000, 8)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void valueWidthFollowsTheColumnType()
    {
        assertEquals(8, ExceptionArea.valueBytes(ChunkV4Directory.TYPE_DOUBLE));
        assertEquals(8, ExceptionArea.valueBytes(ChunkV4Directory.TYPE_INT64));
        assertEquals(4, ExceptionArea.valueBytes(ChunkV4Directory.TYPE_INT32));
        assertEquals(4, ExceptionArea.valueBytes(ChunkV4Directory.TYPE_DATE32));
        assertEquals(2, ExceptionArea.VALUE_BYTES_ALP_RD_LEFT);
        // The types that never pack into a lane have no W to invent.
        for (int type : new int[]{ ChunkV4Directory.TYPE_BOOLEAN, ChunkV4Directory.TYPE_TEXT,
                                   ChunkV4Directory.TYPE_OPAQUE, ChunkV4Directory.TYPE_INVALID, 0x42 })
            assertThatThrownBy(() -> ExceptionArea.valueBytes(type)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * §5 rule 3 in its cheapest form: the number the width chooser scores an exception area with
     * must be the number of bytes the area actually occupies.
     *
     * <p>This once pinned a <em>disagreement</em>. §5's prose gave the cost formula a fixed term of
     * 16 while giving the layout an eight-byte header, and the code followed both -- so
     * {@link BitPacking#chooseWidth} over-charged every width carrying exceptions by exactly eight
     * bytes. That was conservative and reproducible, but it could pick a width one step wider than
     * the true minimum, which is real bytes on every chunk in the cluster. The layout won, the doc's
     * formula was corrected, and this test now protects the fix in both directions: that
     * {@link BitPacking#EXCEPTION_AREA_OVERHEAD} is the layout's own header size (it is
     * {@link ExceptionArea#HEADER_BYTES} by reference, so this asserts the reference has not been
     * re-literalised), and that {@code widthCost}'s whole exception term equals
     * {@link ExceptionArea#encodedLength} at every count and every {@code W}.
     *
     * <p>A width-0 lane costs no lane bytes, so {@code widthCost(n, 0, count, W)} <em>is</em> the
     * exception term with nothing else in it -- which is what makes the comparison exact rather than
     * a subtraction that could hide an error in either operand.
     */
    @Test
    public void widthChooserOverheadIsTheLayoutsOwnHeaderSize()
    {
        assertEquals(ExceptionArea.HEADER_BYTES, BitPacking.EXCEPTION_AREA_OVERHEAD);
        assertEquals(8, BitPacking.EXCEPTION_AREA_OVERHEAD);
        for (int width : new int[]{ 2, 4, 8 })
            for (int count = 1; count < 40; count++)
                assertEquals("count " + count + " W " + width,
                             BitPacking.widthCost(64, 0, count, width),
                             ExceptionArea.encodedLength(count, width));
        // And no area at all when there are no exceptions: the cost function must not charge a
        // header for a block whose blockFlags bit 4 stays clear.
        for (int width : new int[]{ 2, 4, 8 })
            assertEquals(0L, BitPacking.widthCost(64, 0, 0, width));
    }

    // -----------------------------------------------------------------------------------------
    // ordering, distinctness, the filler
    // -----------------------------------------------------------------------------------------

    @Test
    public void positionsMustStrictlyAscendOnBothSides()
    {
        assertThatThrownBy(() -> ExceptionArea.encode(new int[]{ 5, 3 }, new long[]{ 1, 2 }, 2, 8))
            .isInstanceOf(IllegalArgumentException.class);
        // Distinctness falls out of the same comparison: one slot cannot take two values.
        assertThatThrownBy(() -> ExceptionArea.encode(new int[]{ 5, 5 }, new long[]{ 1, 2 }, 2, 8))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExceptionArea.encode(new int[]{ -1 }, new long[]{ 1 }, 1, 8))
            .isInstanceOf(IllegalArgumentException.class);

        // Swapping the two entries of the golden vector, which decodes to the same set and would
        // re-encode to different bytes -- the second byte form §5 removes.
        byte[] swapped = hex("0002" + "0000" + "00000000" +
                             "03E8" + "FFFFFFFFFFFFFFFF" +
                             "0003" + "0000000080000000" +
                             "00000000");
        assertThatThrownBy(() -> ExceptionArea.decode(ByteBuffer.wrap(swapped), 8, 1024, new int[4], new long[4]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void positionsMustLieInsideTheLane()
    {
        byte[] golden = hex(GOLDEN_W8_HEX);
        // Slot 1000 is legal in a 1024-slot lane and corruption in a 1000-slot one.
        ExceptionArea.decode(ByteBuffer.wrap(golden), 8, 1001, new int[4], new long[4]);
        assertThatThrownBy(() -> ExceptionArea.decode(ByteBuffer.wrap(golden), 8, 1000, new int[4], new long[4]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * §5 rule 6's specified filler. The check is on the raw lane, before the frame of reference is
     * folded in -- afterwards the slot holds {@code min} and the assertion silently stops testing.
     */
    @Test
    public void laneSlotsUnderAnExceptionMustBeZero()
    {
        long[] clean = new long[]{ 7, 0, 9, 0 };
        ExceptionArea.checkLaneIsZeroAtExceptions(clean, 4, new int[]{ 1, 3 }, 2);

        long[] dirty = new long[]{ 7, 0, 9, 4 };
        assertThatThrownBy(() -> ExceptionArea.checkLaneIsZeroAtExceptions(dirty, 4, new int[]{ 1, 3 }, 2))
            .isInstanceOf(IllegalArgumentException.class);
        // Any non-zero filler at all, not just a plausible residual.
        for (long filler : new long[]{ 1, -1, Long.MAX_VALUE, 0x5A5A5A5AL })
        {
            long[] lane = new long[]{ filler, 0 };
            assertThatThrownBy(() -> ExceptionArea.checkLaneIsZeroAtExceptions(lane, 2, new int[]{ 0 }, 1))
                .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> ExceptionArea.checkLaneIsZeroAtExceptions(clean, 4, new int[]{ 4 }, 1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void anAreaWithNoExceptionsHasNoBytesAndNoEncoding()
    {
        assertEquals(0, ExceptionArea.encodedLength(0, 8));
        assertThatThrownBy(() -> ExceptionArea.encode(new int[0], new long[0], 0, 8))
            .isInstanceOf(IllegalArgumentException.class);
        // An eight-byte area declaring zero exceptions would be a second byte form of "none".
        byte[] empty = hex("0000" + "0000" + "00000000");
        assertThatThrownBy(() -> ExceptionArea.decode(ByteBuffer.wrap(empty), 8, 64, new int[4], new long[4]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void valuesMustFitTheirDeclaredWidth()
    {
        assertThatThrownBy(() -> ExceptionArea.encode(new int[]{ 0 }, new long[]{ 0x1_0000_0000L }, 1, 4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExceptionArea.encode(new int[]{ 0 }, new long[]{ -1L }, 1, 4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExceptionArea.encode(new int[]{ 0 }, new long[]{ 0x10000L }, 1, 2))
            .isInstanceOf(IllegalArgumentException.class);
        // The boundary value is legal, zero-extended.
        ExceptionArea.encode(new int[]{ 0 }, new long[]{ 0xFFFFFFFFL }, 1, 4);
        ExceptionArea.encode(new int[]{ 0 }, new long[]{ -1L }, 1, 8);
    }

    // -----------------------------------------------------------------------------------------
    // padding and determinism
    // -----------------------------------------------------------------------------------------

    @Test
    public void paddingIsZeroEverywhereAndIsVerified()
    {
        // The header's two pad fields and the trailing pad to 8. Each is a byte a reused scratch
        // buffer leaks into, and none of them is read by a round-trip test.
        for (int at : new int[]{ 2, 3, 4, 5, 6, 7, 28, 29, 30, 31 })
        {
            byte[] bytes = hex(GOLDEN_W8_HEX);
            bytes[at] = 1;
            assertThatThrownBy(() -> ExceptionArea.decode(ByteBuffer.wrap(bytes), 8, 1024, new int[4], new long[4]))
                .isInstanceOf(IllegalArgumentException.class);
        }
        // The golden vector's own pad bytes are zero, so the loop above is testing the check and
        // not merely a corrupted value field.
        byte[] golden = hex(GOLDEN_W8_HEX);
        for (int at : new int[]{ 2, 3, 4, 5, 6, 7, 28, 29, 30, 31 })
            assertEquals("pad byte " + at, 0, golden[at]);
    }

    @Test
    public void encodeTwiceIsByteIdenticalIncludingPadding()
    {
        Random random = new Random(4242L);
        for (int trial = 0; trial < 300; trial++)
        {
            int width = new int[]{ 2, 4, 8 }[random.nextInt(3)];
            int lane = 1 + random.nextInt(300);
            Exceptions exceptions = randomExceptions(random, lane, width);
            byte[] first = intoDirtyBuffer(exceptions, width, (byte) 0xFF);
            byte[] second = intoDirtyBuffer(exceptions, width, (byte) 0x5A);
            assertArrayEquals(first, second);
        }
    }

    @Test
    public void roundTripOverRandomisedAreas()
    {
        Random random = new Random(90210L);
        int[] positions = new int[512];
        long[] values = new long[512];
        for (int trial = 0; trial < 500; trial++)
        {
            int width = new int[]{ 2, 4, 8 }[random.nextInt(3)];
            int lane = 1 + random.nextInt(500);
            Exceptions exceptions = randomExceptions(random, lane, width);
            byte[] bytes = ExceptionArea.encode(exceptions.positions, exceptions.values, exceptions.count, width);
            assertEquals(ExceptionArea.encodedLength(exceptions.count, width), bytes.length);

            ByteBuffer src = ByteBuffer.wrap(bytes);
            assertEquals(exceptions.count, ExceptionArea.decode(src, width, lane, positions, values));
            assertEquals(bytes.length, src.position());
            for (int i = 0; i < exceptions.count; i++)
            {
                assertEquals(exceptions.positions[i], positions[i]);
                assertEquals(exceptions.values[i], values[i]);
            }
            // encode(decode(encode(x))) == encode(x): the re-encoder's late-merge path.
            assertArrayEquals(bytes, ExceptionArea.encode(positions, values, exceptions.count, width));
        }
    }

    // -----------------------------------------------------------------------------------------
    // robustness
    // -----------------------------------------------------------------------------------------

    @Test
    public void truncateAtEveryPrefixLength()
    {
        byte[] full = hex(GOLDEN_W8_HEX);
        for (int length = 0; length < full.length; length++)
        {
            byte[] truncated = Arrays.copyOf(full, length);
            try
            {
                ExceptionArea.decode(ByteBuffer.wrap(truncated), 8, 1024, new int[4], new long[4]);
                fail("a " + length + "-byte buffer parsed a 32-byte exception area");
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
        }
        ExceptionArea.decode(ByteBuffer.wrap(full), 8, 1024, new int[4], new long[4]);
    }

    @Test
    public void singleBitFlipsNeverEscapeAsUncheckedOrOom()
    {
        byte[] full = hex(GOLDEN_W8_HEX);
        int accepted = 0;
        for (int bit = 0; bit < full.length * 8; bit++)
        {
            byte[] flipped = full.clone();
            flipped[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try
            {
                ExceptionArea.decode(ByteBuffer.wrap(flipped), 8, 1024, new int[8], new long[8]);
                accepted++;
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
            catch (RuntimeException unexpected)
            {
                fail("exception-area bit " + bit + " escaped as " + unexpected);
            }
        }
        // The verbatim values carry no redundancy, so flips there are undetectable by construction.
        if (accepted == 0)
            fail("every bit flip was rejected, which means the area is not being parsed");
    }

    /**
     * A corrupt {@code excCount} must throw against the lane length the caller derived from the
     * header, before it is used to size or index anything. The buffer here is eight bytes; a decoder
     * that trusted 65,535 would have tried to walk 655,350.
     */
    @Test
    public void aCorruptCountThrowsRatherThanReservingMemory()
    {
        byte[] bytes = hex("FFFF" + "0000" + "00000000");
        assertThatThrownBy(() -> ExceptionArea.decode(ByteBuffer.wrap(bytes), 8, 1024, new int[1024], new long[1024]))
            .isInstanceOf(IllegalArgumentException.class);
        // And when the count is inside the lane but the bytes are not there.
        byte[] plausible = hex("0400" + "0000" + "00000000");
        assertThatThrownBy(() -> ExceptionArea.decode(ByteBuffer.wrap(plausible), 8, 1024, new int[1024],
                                                      new long[1024]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void littleEndianBuffersAreRejected()
    {
        ByteBuffer little = ByteBuffer.wrap(hex(GOLDEN_W8_HEX)).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        assertThatThrownBy(() -> ExceptionArea.decode(little, 8, 1024, new int[4], new long[4]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------------

    private static final class Exceptions
    {
        final int[] positions;
        final long[] values;
        final int count;

        Exceptions(int[] positions, long[] values, int count)
        {
            this.positions = positions;
            this.values = values;
            this.count = count;
        }
    }

    private static Exceptions randomExceptions(Random random, int lane, int width)
    {
        int count = 1 + random.nextInt(Math.min(lane, 20));
        boolean[] taken = new boolean[lane];
        int chosen = 0;
        while (chosen < count)
        {
            int position = random.nextInt(lane);
            if (!taken[position])
            {
                taken[position] = true;
                chosen++;
            }
        }
        int[] positions = new int[count];
        long[] values = new long[count];
        int at = 0;
        long mask = width == 8 ? -1L : (1L << (width * 8)) - 1;
        for (int i = 0; i < lane && at < count; i++)
            if (taken[i])
            {
                positions[at] = i;
                values[at] = random.nextLong() & mask;
                at++;
            }
        return new Exceptions(positions, values, count);
    }

    private static byte[] intoDirtyBuffer(Exceptions exceptions, int width, byte fill)
    {
        byte[] scratch = new byte[ExceptionArea.encodedLength(exceptions.count, width)];
        Arrays.fill(scratch, fill);
        ByteBuffer dst = ByteBuffer.wrap(scratch);
        ExceptionArea.encode(exceptions.positions, exceptions.values, exceptions.count, width, dst);
        assertEquals(scratch.length, dst.position());
        return scratch;
    }
}
