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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Covers the four presence modes of chunk-format-v4 §4 and the block-local {@code rank} of §2.
 *
 * <p>{@code rank} is tested as a property over random null patterns at every offset, not by
 * example: §12 ranks an off-by-one here as the format's second-largest risk precisely because it
 * does not throw -- it returns another row's value, silently, for one column.
 */
public class BlockPresenceTest
{
    // -----------------------------------------------------------------------------------------
    // round trip and the shared decoded form
    // -----------------------------------------------------------------------------------------

    @Test
    public void allFourModesRoundTrip()
    {
        Random random = new Random(31L);
        int[] sizes = { 1, 7, 63, 64, 65, 100, 1023, 1024 };
        for (int blockRows : sizes)
        {
            roundTrip(BlockPresence.MODE_ALL_PRESENT, constantPresence(blockRows, true), blockRows);
            roundTrip(BlockPresence.MODE_ALL_NULL, constantPresence(blockRows, false), blockRows);
            if (blockRows < 2)
                continue;
            for (int trial = 0; trial < 20; trial++)
            {
                boolean[] present = randomPresence(random, blockRows);
                roundTrip(BlockPresence.MODE_BITMAP, present, blockRows);
                roundTrip(BlockPresence.MODE_RLE, present, blockRows);
            }
        }
    }

    /**
     * The load-bearing property of §4: whichever storage mode a block used, the decoder hands the
     * value loop the same {@code long[]}. If this ever diverges, the read path grows a second shape
     * to handle and RLE blocks stop being randomly addressable.
     */
    @Test
    public void bothStorageModesDecodeToTheSameWords()
    {
        Random random = new Random(1707L);
        for (int trial = 0; trial < 300; trial++)
        {
            int blockRows = 1 + random.nextInt(1024);
            boolean[] present = randomPresence(random, blockRows);
            long[] words = BlockPresence.toWords(present, blockRows);

            long[] viaBitmap = BlockPresence.decode(BlockPresence.MODE_BITMAP,
                                                    ByteBuffer.wrap(BlockPresence.encode(BlockPresence.MODE_BITMAP,
                                                                                         words, blockRows)),
                                                    blockRows);
            long[] viaRle = BlockPresence.decode(BlockPresence.MODE_RLE,
                                                 ByteBuffer.wrap(BlockPresence.encode(BlockPresence.MODE_RLE,
                                                                                      words, blockRows)),
                                                 blockRows);
            assertArrayEquals("blockRows " + blockRows, words, viaBitmap);
            assertArrayEquals("blockRows " + blockRows, words, viaRle);
        }
    }

    @Test
    public void zeroByteModesConsumeNothingAndSetTheRightBits()
    {
        for (int blockRows : new int[]{ 1, 63, 64, 65, 1024 })
        {
            ByteBuffer empty = ByteBuffer.allocate(0);
            long[] allPresent = BlockPresence.decode(BlockPresence.MODE_ALL_PRESENT, empty, blockRows);
            assertEquals(blockRows, BlockPresence.rank(allPresent, blockRows));
            // §5 rule 6: nothing set past the last row, or rank counts a phantom.
            assertEquals(blockRows, Long.bitCount(allPresent[0]) + totalBits(allPresent, 1));

            long[] allNull = BlockPresence.decode(BlockPresence.MODE_ALL_NULL, empty, blockRows);
            assertEquals(0, BlockPresence.rank(allNull, blockRows));
            assertEquals(0, empty.position());
        }
    }

    // -----------------------------------------------------------------------------------------
    // rank (§2)
    // -----------------------------------------------------------------------------------------

    @Test
    public void rankMatchesBruteForceAtEveryOffset()
    {
        Random random = new Random(5L);
        int[] sizes = { 1, 2, 63, 64, 65, 127, 128, 1000, 1024 };
        for (int blockRows : sizes)
        {
            for (int trial = 0; trial < 10; trial++)
            {
                boolean[] present = randomPresence(random, blockRows);
                long[] words = BlockPresence.toWords(present, blockRows);
                int expected = 0;
                for (int offset = 0; offset <= blockRows; offset++)
                {
                    assertEquals("blockRows " + blockRows + " offset " + offset,
                                 expected, BlockPresence.rank(words, offset));
                    if (offset < blockRows && present[offset])
                        expected++;
                }
                assertEquals(expected, BlockPresence.cardinality(words, blockRows));
            }
        }
    }

    @Test
    public void rankRejectsAnOffsetPastTheWords()
    {
        long[] words = new long[2];
        assertEquals(0, BlockPresence.rank(words, 128));
        assertThatThrownBy(() -> BlockPresence.rank(words, 129)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockPresence.rank(words, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Pins the rule {@code rank}'s javadoc states normatively: a sequential scan carries its own
     * value index, incremented by each row's presence bit, and calls {@code rank} only on a seek.
     * The rule is only safe to state if the running index and {@code rank} agree at <i>every</i>
     * offset, so that is what is asserted -- including that the index a seek computes is exactly the
     * one a scan would have carried to the same row, which is what makes the two interchangeable.
     *
     * <p>Written against the raw word expression the cursor will use rather than against the
     * {@code boolean[]}, because the cursor will not have a {@code boolean[]}.
     */
    @Test
    public void aRunningValueIndexAgreesWithRankAtEveryOffsetAndSeek()
    {
        Random random = new Random(4242L);
        int[] sizes = { 1, 2, 63, 64, 65, 127, 128, 129, 1000, 1024, 1025 };
        for (int blockRows : sizes)
        {
            for (int trial = 0; trial < 10; trial++)
            {
                boolean[] present = randomPresence(random, blockRows);
                long[] words = BlockPresence.toWords(present, blockRows);

                // the densified value lane a block actually stores: one entry per present row
                int[] lane = new int[BlockPresence.cardinality(words, blockRows)];
                int filled = 0;
                for (int row = 0; row < blockRows; row++)
                    if (present[row])
                        lane[filled++] = row;

                // the scan: one increment per row, no bitCount anywhere
                int valueIndex = 0;
                for (int row = 0; row < blockRows; row++)
                {
                    assertEquals("blockRows " + blockRows + " row " + row,
                                 valueIndex, BlockPresence.rank(words, row));
                    int bit = (int) ((words[row >>> 6] >>> (row & 63)) & 1L);
                    if (bit == 1)
                        assertEquals("blockRows " + blockRows + " row " + row, row, lane[valueIndex]);
                    valueIndex += bit;
                }
                assertEquals("blockRows " + blockRows, valueIndex, BlockPresence.rank(words, blockRows));
                assertEquals(lane.length, valueIndex);

                // the seek: rank() must reproduce exactly the index the scan would have carried
                for (int probe = 0; probe < 8; probe++)
                {
                    int offset = random.nextInt(blockRows + 1);
                    int scanned = 0;
                    for (int row = 0; row < offset; row++)
                        scanned += (int) ((words[row >>> 6] >>> (row & 63)) & 1L);
                    assertEquals("blockRows " + blockRows + " seek " + offset,
                                 scanned, BlockPresence.rank(words, offset));
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // RLE expansion into bit words (§4)
    // -----------------------------------------------------------------------------------------

    /**
     * The differential test for the word-at-a-time run expansion. The reference below is the naive
     * per-row {@code words[i >>> 6] |= 1L << (i & 63)} loop the decoder used to run; the production
     * path reaches the same runs through {@code decode(MODE_RLE, ...)}. Every run pattern must
     * produce a bit-identical {@code long[]}.
     *
     * <p>The entire risk of the fast path is an off-by-one at one of the two partial-word edges, so
     * the shapes below place run boundaries at every alignment: word-aligned starts and ends, starts
     * and ends one row either side of a word boundary, single-row runs at both ends of the block, a
     * run confined to one word, runs spanning many words, a run ending exactly at a non-word-aligned
     * {@code blockRows}, and the full-block run. Such an error does not throw and does not break the
     * round trip in any visible way -- it shifts one column's values by one row -- which is why it
     * has to be caught by comparing the words themselves.
     */
    @Test
    public void rleExpansionMatchesANaivePerRowReferenceAtEveryAlignment()
    {
        // { firstRunPresent, run lengths... }; the runs sum to blockRows.
        int[][] shapes = {
            { 1, 1 },                       // one row, present
            { 0, 1 },                       // one row, absent
            { 1, 1024 },                    // the full-block run
            { 1, 128 },                     // full block, word-aligned
            { 0, 1, 1022, 1 },              // single-row runs at both edges
            { 0, 63, 1, 960 },              // a single present row at the last bit of word 0
            { 0, 64, 1, 959 },              // a single present row at the first bit of word 1
            { 0, 63, 2, 959 },              // a present run straddling the word 0/1 boundary
            { 0, 63, 66, 895 },             // straddling both edges, with a whole word inside
            { 1, 64, 64, 896 },             // a word-aligned present run at the block start
            { 0, 64, 64, 896 },             // a word-aligned present run in the middle
            { 0, 64, 128, 832 },            // word-aligned, spanning two whole words
            { 0, 5, 250, 769 },             // starts and ends mid-word, spans four words
            { 0, 1, 1023 },                 // a present run reaching the exact end of the block
            { 1, 1023, 1 },                 // a present run from row 0 ending one short of the end
            { 0, 960, 64 },                 // present tail, word-aligned start, ends at blockRows
            { 0, 1, 999 },                  // blockRows 1000: present run ends in a partial word
            { 1, 999, 1 },                  // blockRows 1000: present run starts at 0, partial tail
            { 0, 64, 1 },                   // blockRows 65: the present row is alone in word 1
            { 1, 65 },                      // blockRows 65: full block over a partial tail word
            { 1, 127 },                     // blockRows 127: full block, tail word one short
            { 0, 126, 1 },                  // blockRows 127: last row of a partial tail word
            { 1, 1, 1, 1, 1, 1, 1, 1, 1 },  // alternating single rows across no boundary
            { 0, 60, 1, 1, 1, 1, 1, 1, 1 }, // alternating single rows across the word 0/1 boundary
        };
        for (int[] shape : shapes)
            assertRleExpansionMatchesReference(shape[0] == 1, Arrays.copyOfRange(shape, 1, shape.length));

        Random random = new Random(90210L);
        int[] sizes = { 1, 63, 64, 65, 127, 128, 129, 191, 192, 193, 1000, 1023, 1024, 1025, 4096 };
        for (int blockRows : sizes)
            for (int trial = 0; trial < 40; trial++)
                assertRleExpansionMatchesReference(random.nextBoolean(), randomRuns(random, blockRows));
    }

    private static void assertRleExpansionMatchesReference(boolean firstRunPresent, int[] runs)
    {
        int blockRows = 0;
        for (int run : runs)
            blockRows += run;

        // The reference: one read-modify-write per present row, deliberately naive, written here
        // rather than borrowed so that a change to the production expansion cannot change it too.
        long[] expected = new long[(blockRows + 63) / 64];
        boolean present = firstRunPresent;
        int row = 0;
        for (int run : runs)
        {
            if (present)
                for (int i = row; i < row + run; i++)
                    expected[i >>> 6] |= 1L << (i & 63);
            row += run;
            present = !present;
        }

        String description = "firstRunPresent " + firstRunPresent + " runs " + Arrays.toString(runs);
        byte[] encoded = BlockPresence.encode(BlockPresence.MODE_RLE, expected, blockRows);
        long[] actual = BlockPresence.decode(BlockPresence.MODE_RLE, ByteBuffer.wrap(encoded), blockRows);
        assertArrayEquals(description, expected, actual);

        // and the words really do describe these runs, so a reference that agreed with a broken
        // production path on a degenerate pattern could not pass unnoticed
        int expectedRuns = 0;
        for (int run : runs)
            if (run > 0)
                expectedRuns++;
        assertEquals(description, expectedRuns, ByteBuffer.wrap(encoded).getShort() & 0xFFFF);
    }

    /** Run lengths clustered around word boundaries, so runs begin and end at every alignment. */
    private static int[] randomRuns(Random random, int blockRows)
    {
        int[] candidates = { 1, 2, 3, 31, 32, 33, 63, 64, 65, 126, 127, 128, 129, 191, 192, 193, 255, 256, 257 };
        int[] runs = new int[blockRows];
        int count = 0;
        int row = 0;
        while (row < blockRows)
        {
            int remaining = blockRows - row;
            int run = random.nextInt(4) == 0 ? 1 + random.nextInt(remaining)
                                             : candidates[random.nextInt(candidates.length)];
            run = Math.min(run, remaining);
            runs[count++] = run;
            row += run;
        }
        return Arrays.copyOf(runs, count);
    }

    // -----------------------------------------------------------------------------------------
    // mode choice (§5 rule 3)
    // -----------------------------------------------------------------------------------------

    @Test
    public void modeChoiceMatchesAnExactSizeComparison()
    {
        Random random = new Random(64738L);
        for (int trial = 0; trial < 500; trial++)
        {
            int blockRows = 1 + random.nextInt(1024);
            boolean[] present = randomPresence(random, blockRows);
            long[] words = BlockPresence.toWords(present, blockRows);

            int expected = expectedMode(present, blockRows);
            int chosen = BlockPresence.chooseMode(words, blockRows);
            assertEquals("blockRows " + blockRows, expected, chosen);
            assertEquals(chosen, BlockPresence.chooseMode(present, blockRows));

            // the declared length is the length actually emitted
            assertEquals(BlockPresence.encodedLength(chosen, words, blockRows),
                         BlockPresence.encode(chosen, words, blockRows).length);
        }
    }

    @Test
    public void modeChoiceKeepsBitmapOnAnExactTie()
    {
        // 64 rows: bitmap is one word, 8 bytes. Two runs of 32 cost 4 header bytes plus two
        // one-byte varints, 6, padded to 8. Exactly equal, so the smaller mode code wins (§5 rule 3).
        int blockRows = 64;
        boolean[] present = new boolean[blockRows];
        Arrays.fill(present, 0, 32, true);
        long[] words = BlockPresence.toWords(present, blockRows);

        assertEquals(8, BlockPresence.encodedLength(BlockPresence.MODE_BITMAP, words, blockRows));
        assertEquals(8, BlockPresence.encodedLength(BlockPresence.MODE_RLE, words, blockRows));
        assertEquals(BlockPresence.MODE_BITMAP, BlockPresence.chooseMode(words, blockRows));
    }

    @Test
    public void modeChoiceTakesRleWhenRunsAreLongAndBitmapWhenTheyAreNot()
    {
        int blockRows = 1024;

        boolean[] twoRuns = new boolean[blockRows];
        Arrays.fill(twoRuns, 0, 512, true);
        long[] twoRunWords = BlockPresence.toWords(twoRuns, blockRows);
        assertEquals(128, BlockPresence.encodedLength(BlockPresence.MODE_BITMAP, twoRunWords, blockRows));
        assertEquals(8, BlockPresence.encodedLength(BlockPresence.MODE_RLE, twoRunWords, blockRows));
        assertEquals(BlockPresence.MODE_RLE, BlockPresence.chooseMode(twoRunWords, blockRows));

        boolean[] alternating = new boolean[blockRows];
        for (int i = 0; i < blockRows; i++)
            alternating[i] = (i & 1) == 0;
        long[] alternatingWords = BlockPresence.toWords(alternating, blockRows);
        assertEquals(1032, BlockPresence.encodedLength(BlockPresence.MODE_RLE, alternatingWords, blockRows));
        assertEquals(BlockPresence.MODE_BITMAP, BlockPresence.chooseMode(alternatingWords, blockRows));
    }

    @Test
    public void zeroByteModesWinWheneverTheyApply()
    {
        assertEquals(BlockPresence.MODE_ALL_PRESENT,
                     BlockPresence.chooseMode(constantPresence(1024, true), 1024));
        assertEquals(BlockPresence.MODE_ALL_NULL,
                     BlockPresence.chooseMode(constantPresence(1024, false), 1024));
    }

    @Test
    public void zeroByteModesRejectAPatternTheyCannotRepresent()
    {
        int blockRows = 64;
        boolean[] present = new boolean[blockRows];
        present[0] = true;
        long[] words = BlockPresence.toWords(present, blockRows);
        assertThatThrownBy(() -> BlockPresence.encode(BlockPresence.MODE_ALL_PRESENT, words, blockRows))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockPresence.encode(BlockPresence.MODE_ALL_NULL, words, blockRows))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // byte determinism (§5 rule 5)
    // -----------------------------------------------------------------------------------------

    @Test
    public void encodeTwiceIsByteIdenticalIncludingIntoADirtyBuffer()
    {
        Random random = new Random(808L);
        for (int trial = 0; trial < 300; trial++)
        {
            int blockRows = 1 + random.nextInt(1024);
            boolean[] present = randomPresence(random, blockRows);
            long[] words = BlockPresence.toWords(present, blockRows);
            int mode = BlockPresence.chooseMode(words, blockRows);

            byte[] first = BlockPresence.encode(mode, words, blockRows);
            assertArrayEquals(first, BlockPresence.encode(mode, words, blockRows));

            // Into a buffer full of stale bytes: the RLE tail padding is where a reused scratch
            // buffer leaks, and the leak is invisible to a round-trip test because the decoder
            // reaches the same bitmap either way -- it only shows up as a chunk that never stops
            // being rewritten.
            ByteBuffer dirty = ByteBuffer.allocate(first.length);
            while (dirty.hasRemaining())
                dirty.put((byte) 0x5A);
            dirty.position(0);
            BlockPresence.encode(mode, words, blockRows, dirty);
            assertArrayEquals("blockRows " + blockRows + " mode " + mode, first, dirty.array());
        }
    }

    /**
     * The RLE layout of §4, byte for byte: 10 rows, the first three present.
     * {@code u16 runCount | u8 firstRunPresent | u8 pad | varint runLengths[]}, padded to 8.
     */
    @Test
    public void rleLayoutIsExactlyWhatSection4Says()
    {
        int blockRows = 10;
        boolean[] present = new boolean[blockRows];
        Arrays.fill(present, 0, 3, true);
        long[] words = BlockPresence.toWords(present, blockRows);

        assertArrayEquals(new byte[]{ 0x00, 0x02, 0x01, 0x00, 0x03, 0x07, 0x00, 0x00 },
                          BlockPresence.encode(BlockPresence.MODE_RLE, words, blockRows));
        assertArrayEquals(words, BlockPresence.decode(BlockPresence.MODE_RLE,
                                                      ByteBuffer.wrap(new byte[]{ 0x00, 0x02, 0x01, 0x00,
                                                                                  0x03, 0x07, 0x00, 0x00 }),
                                                      blockRows));
    }

    // -----------------------------------------------------------------------------------------
    // corruption
    // -----------------------------------------------------------------------------------------

    @Test
    public void bitmapBitsAtOrAboveBlockRowsAreRejected()
    {
        int blockRows = 10;
        long[] withTrailingBit = { 0b111L | (1L << 10) };

        // rejected on the way in: the encoder must not be able to mint a second encoding of one
        // pattern, which is what a set trailing bit would be.
        assertThatThrownBy(() -> BlockPresence.encode(BlockPresence.MODE_BITMAP, withTrailingBit, blockRows))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockPresence.chooseMode(withTrailingBit, blockRows))
            .isInstanceOf(IllegalArgumentException.class);

        // and on the way out, where it would otherwise corrupt rank silently
        ByteBuffer payload = ByteBuffer.allocate(8);
        payload.putLong(0, withTrailingBit[0]);
        assertThatThrownBy(() -> BlockPresence.decode(BlockPresence.MODE_BITMAP, payload.duplicate(),
                                                      blockRows, new long[1]))
            .isInstanceOf(IllegalArgumentException.class);

        // the same word without the trailing bit is fine
        ByteBuffer clean = ByteBuffer.allocate(8);
        clean.putLong(0, 0b111L);
        assertArrayEquals(new long[]{ 0b111L },
                          BlockPresence.decode(BlockPresence.MODE_BITMAP, clean, blockRows));
    }

    @Test
    public void rleRunsMustSumToBlockRows()
    {
        // 3 + 5 = 8, not 10
        assertRleRejected(10, 0x00, 0x02, 0x01, 0x00, 0x03, 0x05, 0x00, 0x00);
        // 3 + 9 = 12, past the end
        assertRleRejected(10, 0x00, 0x02, 0x01, 0x00, 0x03, 0x09, 0x00, 0x00);
    }

    @Test
    public void rleZeroLengthRunIsRejected()
    {
        // A zero run decodes to the same pattern as its neighbours merged, so accepting it would
        // give one pattern two byte forms.
        assertRleRejected(10, 0x00, 0x02, 0x01, 0x00, 0x00, 0x0A, 0x00, 0x00);
    }

    @Test
    public void rleRunCountOutsideOneToBlockRowsIsRejected()
    {
        assertRleRejected(10, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00);
        assertRleRejected(10, 0x00, 0xFF, 0x01, 0x00, 0x03, 0x07, 0x00, 0x00);
        assertRleRejected(10, 0xFF, 0xFF, 0x01, 0x00, 0x03, 0x07, 0x00, 0x00);
    }

    @Test
    public void rleFirstRunPresentMustBeExactlyZeroOrOne()
    {
        // 0x02 would decode "present" under any truthiness test, through different bytes.
        assertRleRejected(10, 0x00, 0x02, 0x02, 0x00, 0x03, 0x07, 0x00, 0x00);
        assertRleRejected(10, 0x00, 0x02, 0xFF, 0x00, 0x03, 0x07, 0x00, 0x00);
    }

    @Test
    public void rlePaddingMustBeZero()
    {
        // the header's own pad byte
        assertRleRejected(10, 0x00, 0x02, 0x01, 0x01, 0x03, 0x07, 0x00, 0x00);
        // and the pad to the §6 eight-byte boundary
        assertRleRejected(10, 0x00, 0x02, 0x01, 0x00, 0x03, 0x07, 0x01, 0x00);
        assertRleRejected(10, 0x00, 0x02, 0x01, 0x00, 0x03, 0x07, 0x00, 0x01);
    }

    @Test
    public void nonCanonicalVarintsAreRejected()
    {
        // 3 written as 0x83 0x00: same value, two bytes, and therefore a second encoding of one
        // presence pattern. §5 rule 4.
        assertRleRejected(10, 0x00, 0x02, 0x01, 0x00, 0x83, 0x00, 0x07, 0x00);

        assertEquals(3L, BlockPresence.readVarInt(ByteBuffer.wrap(new byte[]{ 0x03 })));
        assertEquals(300L, BlockPresence.readVarInt(ByteBuffer.wrap(new byte[]{ (byte) 0xAC, 0x02 })));
        assertThatThrownBy(() -> BlockPresence.readVarInt(ByteBuffer.wrap(new byte[]{ (byte) 0xAC, (byte) 0x80,
                                                                                     0x00 })))
            .isInstanceOf(IllegalArgumentException.class);
        // truncated, and over-long: neither may escape as a buffer exception
        assertThatThrownBy(() -> BlockPresence.readVarInt(ByteBuffer.wrap(new byte[]{ (byte) 0x80 })))
            .isInstanceOf(IllegalArgumentException.class);
        byte[] overlong = new byte[11];
        Arrays.fill(overlong, (byte) 0x80);
        assertThatThrownBy(() -> BlockPresence.readVarInt(ByteBuffer.wrap(overlong)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void varintRoundTripsAtTheCanonicalLength()
    {
        long[] values = { 0, 1, 127, 128, 300, 1023, 1024, 32767, 32768, Integer.MAX_VALUE, Long.MAX_VALUE };
        for (long value : values)
        {
            int length = BlockPresence.varIntLength(value);
            ByteBuffer buffer = ByteBuffer.allocate(length);
            BlockPresence.writeVarInt(buffer, value);
            assertEquals("value " + value, length, buffer.position());
            buffer.position(0);
            assertEquals(value, BlockPresence.readVarInt(buffer));
            assertEquals(length, buffer.position());
        }
    }

    @Test
    public void unknownModesAndImpossibleBlockRowsAreRejected()
    {
        long[] words = { 1L };
        assertThatThrownBy(() -> BlockPresence.decode(4, ByteBuffer.allocate(8), 10, new long[1]))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockPresence.decode(-1, ByteBuffer.allocate(8), 10, new long[1]))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockPresence.encodedLength(4, words, 10))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockPresence.wordCount(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BlockPresence.wordCount(BlockPresence.MAX_BLOCK_ROWS + 1))
            .isInstanceOf(IllegalArgumentException.class);
        // the destination must be big enough for the block, whatever the payload claims
        assertThatThrownBy(() -> BlockPresence.decode(BlockPresence.MODE_ALL_PRESENT, ByteBuffer.allocate(0),
                                                      1024, new long[15]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * §11 robustness: no corrupt byte may leave this class as anything but an
     * {@link IllegalArgumentException}. Allocation is structurally bounded rather than merely
     * checked -- every array here is sized from {@code blockRows}, which comes from the header, so
     * there is no payload-derived length that could size one.
     */
    @Test
    public void singleBitFlipsNeverEscapeAsUncheckedOrOom()
    {
        Random random = new Random(2718L);
        final int blockRows = 300;
        for (int trial = 0; trial < 10; trial++)
        {
            boolean[] present = randomPresence(random, blockRows);
            long[] words = BlockPresence.toWords(present, blockRows);
            for (int mode : new int[]{ BlockPresence.MODE_BITMAP, BlockPresence.MODE_RLE })
            {
                byte[] encoded = BlockPresence.encode(mode, words, blockRows);
                for (int index = 0; index < encoded.length; index++)
                {
                    for (int bit = 0; bit < 8; bit++)
                    {
                        byte[] flipped = encoded.clone();
                        flipped[index] ^= (byte) (1 << bit);
                        try
                        {
                            BlockPresence.decode(mode, ByteBuffer.wrap(flipped), blockRows,
                                                 new long[BlockPresence.wordCount(blockRows)]);
                        }
                        catch (IllegalArgumentException expected)
                        {
                            // rejecting corruption is the contract
                        }
                        catch (Throwable unexpected)
                        {
                            fail("mode " + mode + " byte " + index + " bit " + bit + " escaped as " + unexpected);
                        }
                    }
                }
            }
        }
    }

    /** §11: every truncated prefix of an encoded block is corruption, never a short read. */
    @Test
    public void truncateAtEveryPrefixLength()
    {
        final int blockRows = 1024;
        boolean[] present = new boolean[blockRows];
        Arrays.fill(present, 0, 512, true);
        long[] words = BlockPresence.toWords(present, blockRows);

        for (int mode : new int[]{ BlockPresence.MODE_BITMAP, BlockPresence.MODE_RLE })
        {
            final int decodeMode = mode;
            byte[] encoded = BlockPresence.encode(mode, words, blockRows);
            for (int prefix = 0; prefix < encoded.length; prefix++)
            {
                final byte[] truncated = Arrays.copyOf(encoded, prefix);
                assertThatThrownBy(() -> BlockPresence.decode(decodeMode, ByteBuffer.wrap(truncated), blockRows,
                                                              new long[BlockPresence.wordCount(blockRows)]))
                    .describedAs("mode %d prefix %d", decodeMode, truncated.length)
                    .isInstanceOf(IllegalArgumentException.class);
            }
            assertArrayEquals(words, BlockPresence.decode(mode, ByteBuffer.wrap(encoded), blockRows));
        }
    }

    // -----------------------------------------------------------------------------------------
    // helpers -- deliberately independent of the implementation under test
    // -----------------------------------------------------------------------------------------

    private static void roundTrip(int mode, boolean[] present, int blockRows)
    {
        long[] words = BlockPresence.toWords(present, blockRows);
        byte[] encoded = BlockPresence.encode(mode, words, blockRows);
        assertEquals("mode " + mode + " blockRows " + blockRows,
                     BlockPresence.encodedLength(mode, words, blockRows), encoded.length);

        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        long[] decoded = new long[BlockPresence.wordCount(blockRows)];
        assertEquals(encoded.length, BlockPresence.decode(mode, buffer, blockRows, decoded));
        assertEquals(encoded.length, buffer.position());
        assertArrayEquals("mode " + mode + " blockRows " + blockRows, words, decoded);

        boolean[] back = new boolean[blockRows];
        BlockPresence.toBooleans(decoded, blockRows, back);
        for (int i = 0; i < blockRows; i++)
            assertEquals("mode " + mode + " row " + i, present[i], back[i]);
    }

    private static void assertRleRejected(int blockRows, int... payload)
    {
        assertThatThrownBy(() -> BlockPresence.decode(BlockPresence.MODE_RLE, bytes(payload), blockRows,
                                                      new long[BlockPresence.wordCount(blockRows)]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    private static ByteBuffer bytes(int... values)
    {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++)
            out[i] = (byte) values[i];
        return ByteBuffer.wrap(out);
    }

    private static int expectedMode(boolean[] present, int blockRows)
    {
        int count = 0;
        for (int i = 0; i < blockRows; i++)
            if (present[i])
                count++;
        if (count == blockRows)
            return BlockPresence.MODE_ALL_PRESENT;
        if (count == 0)
            return BlockPresence.MODE_ALL_NULL;
        int bitmap = ((blockRows + 63) / 64) * 8;
        int rle = expectedRleLength(present, blockRows);
        return rle < bitmap ? BlockPresence.MODE_RLE : BlockPresence.MODE_BITMAP;
    }

    private static int expectedRleLength(boolean[] present, int blockRows)
    {
        int bytes = 4;
        int runStart = 0;
        for (int i = 1; i <= blockRows; i++)
        {
            if (i == blockRows || present[i] != present[i - 1])
            {
                bytes += expectedVarIntLength(i - runStart);
                runStart = i;
            }
        }
        return (bytes + 7) & ~7;
    }

    private static int expectedVarIntLength(long value)
    {
        int length = 1;
        while ((value >>>= 7) != 0)
            length++;
        return length;
    }

    private static boolean[] constantPresence(int blockRows, boolean value)
    {
        boolean[] present = new boolean[blockRows];
        Arrays.fill(present, value);
        return present;
    }

    /** Densities from "one null" to "one present", plus run-heavy shapes, so both modes get chosen. */
    private static boolean[] randomPresence(Random random, int blockRows)
    {
        boolean[] present = new boolean[blockRows];
        int shape = random.nextInt(4);
        if (shape == 0)
        {
            for (int i = 0; i < blockRows; i++)
                present[i] = random.nextBoolean();
        }
        else if (shape == 1)
        {
            int odds = 1 + random.nextInt(32);
            for (int i = 0; i < blockRows; i++)
                present[i] = random.nextInt(odds) != 0;
        }
        else if (shape == 2)
        {
            // long runs: RLE territory
            boolean current = random.nextBoolean();
            int i = 0;
            while (i < blockRows)
            {
                int run = 1 + random.nextInt(Math.max(1, blockRows / 4));
                for (int j = 0; j < run && i < blockRows; j++, i++)
                    present[i] = current;
                current = !current;
            }
        }
        else
        {
            Arrays.fill(present, true);
            present[random.nextInt(blockRows)] = false;
        }
        return present;
    }

    private static int totalBits(long[] words, int from)
    {
        int total = 0;
        for (int i = from; i < words.length; i++)
            total += Long.bitCount(words[i]);
        return total;
    }
}
