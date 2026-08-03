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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static org.apache.cassandra.db.timeseries.ChunkV4HeaderTest.hex;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the section layout of chunk-format-v4 §4.5: preamble, block table, contiguous bodies.
 *
 * <p>The golden vectors are hand-assembled from the three layers' field lists rather than captured,
 * because the property that makes a body bounds-checkable -- its length being the distance to the
 * next body -- is only true while the bodies really are contiguous and in index order, and a layout
 * that quietly reordered or padded between them would round-trip perfectly inside one build.
 */
public class ChunkV4SectionTest
{
    /**
     * An {@code INT64} section: four rows, all present, values 10..13, block size 64 so one block.
     * 24-byte block table (min 10, max 13, body at 24, {@code FOR_BITPACK}, all-present) then the
     * body, which is zero presence bytes and the 16-byte frame-of-reference payload.
     */
    private static final String GOLDEN_INT64_HEX =
        "000000000000000A" + "000000000000000D" + "00000018" + "10" + "00" + "0000" +
        "02" + "00000000000000" + "00000000000000E4";

    /**
     * A {@code TEXT} section that pays for a dictionary: 64 rows alternating {@code alpha} and
     * {@code beta}. The preamble is {@code dictCount 2 | pad | dictBytesLen 11} plus the two entries
     * padded to 24; the 8-byte block table has no extrema; the body is a one-bit code per value.
     */
    private static final String GOLDEN_TEXT_DICT_HEX =
        "0002" + "0000" + "0000000B" + "05" + "616C706861" + "04" + "62657461" + "0000000000" +
        "00000020" + "40" + "00" + "0000" +
        "AAAAAAAAAAAAAAAA";

    /**
     * A {@code TEXT} section that does not: four rows {@code a, -, b, a}. The dictionary would cost
     * more than it saves, so the preamble is the eight-byte {@code dictCount 0} form, presence is a
     * bitmap and the payload is {@code RAW}.
     */
    private static final String GOLDEN_TEXT_RAW_HEX =
        "0000" + "0000" + "00000000" +
        "00000010" + "41" + "02" + "0000" +
        "000000000000000D" +
        "0161" + "0162" + "0161" + "0000";

    // -----------------------------------------------------------------------------------------
    // layout
    // -----------------------------------------------------------------------------------------

    @Test
    public void goldenSectionForAFixedWidthColumn()
    {
        long[] values = { 10, 11, 12, 13 };
        ChunkV4Section.Encoded encoded =
            ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INT64, values, null, 4, 6, null, null);
        assertArrayEquals(hex(GOLDEN_INT64_HEX), encoded.bytes);
        assertEquals(40, encoded.sectionLen());
        assertEquals(1, encoded.blockCount());
        assertEquals(0, encoded.preambleLength);

        ChunkV4Section section = read(encoded, ChunkV4Directory.TYPE_INT64, 4, 6, null);
        assertEquals(1, section.blockCount());
        assertEquals(0, section.preambleLength());
        assertNull(section.dictionary());
        assertEquals(10L, section.blockTable().entry(0).min);
        assertEquals(13L, section.blockTable().entry(0).max);
        assertEquals(24, section.blockTable().bodyOffset(0));
        assertEquals(16, section.blockTable().bodyLength(0));
        assertArrayEquals(values, decodeFixed(section, 4, 6).values);
    }

    @Test
    public void goldenSectionForTextWithADictionary()
    {
        byte[][] values = new byte[64][];
        for (int i = 0; i < values.length; i++)
            values[i] = bytes(i % 2 == 0 ? "alpha" : "beta");
        ChunkV4Section.Encoded encoded =
            ChunkV4Section.encodeVariable(ChunkV4Directory.TYPE_TEXT, values, 64, 6, null);
        assertArrayEquals(hex(GOLDEN_TEXT_DICT_HEX), encoded.bytes);
        assertEquals(40, encoded.sectionLen());
        assertEquals(24, encoded.preambleLength);
        assertEquals(2, encoded.dictionaryCount);

        ChunkV4Section section = read(encoded, ChunkV4Directory.TYPE_TEXT, 64, 6, null);
        assertEquals(24, section.preambleLength());
        assertEquals(2, section.dictionary().length);
        assertArrayEquals(bytes("alpha"), section.dictionary()[0]);
        assertArrayEquals(bytes("beta"), section.dictionary()[1]);
        assertEquals(ChunkV4BlockTable.ENC_DICT, section.blockTable().entry(0).blockEncoding);
        assertVariableEquals(values, decodeVariable(section, 64, 6).variable);
    }

    @Test
    public void goldenSectionForTextWithoutADictionary()
    {
        byte[][] values = { bytes("a"), null, bytes("b"), bytes("a") };
        ChunkV4Section.Encoded encoded =
            ChunkV4Section.encodeVariable(ChunkV4Directory.TYPE_TEXT, values, 4, 6, null);
        assertArrayEquals(hex(GOLDEN_TEXT_RAW_HEX), encoded.bytes);
        assertEquals(32, encoded.sectionLen());
        assertEquals(8, encoded.preambleLength);
        assertEquals(0, encoded.dictionaryCount);

        ChunkV4Section section = read(encoded, ChunkV4Directory.TYPE_TEXT, 4, 6, null);
        assertNull(section.dictionary());
        assertEquals(BlockPresence.MODE_BITMAP, section.blockTable().entry(0).presenceMode());
        assertEquals(ChunkV4BlockTable.ENC_RAW, section.blockTable().entry(0).blockEncoding);
        assertVariableEquals(values, decodeVariable(section, 4, 6).variable);

        // The dictionary really is the more expensive layout here, which is why it is absent: two
        // entries cost 16 preamble bytes to save nothing on a three-value body.
        assertTrue(32 < 16 + 8 + 8 + BitPacking.packedLength(3, 1));
    }

    /** §2: the two columns that cost no bytes at all, and which the directory requires to be empty. */
    @Test
    public void theTwoZeroByteSectionsAreZeroBytes()
    {
        boolean[] none = new boolean[8];
        ChunkV4Section.Encoded allNull =
            ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INT64, new long[8], none, 8, 6, null, null);
        assertEquals(0, allNull.sectionLen());
        assertEquals(0, allNull.blockCount());

        byte[] constant = BlockEncodings.toFixedBytes(7, ChunkV4Directory.TYPE_INT64);
        long[] sevens = new long[8];
        Arrays.fill(sevens, 7L);
        ChunkV4Section.Encoded allPresentConstant =
            ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INT64, sevens, null, 8, 6, constant, null);
        assertEquals(0, allPresentConstant.sectionLen());

        // A CONSTANT column with holes is not zero bytes: the section still has to carry presence.
        boolean[] some = new boolean[8];
        Arrays.fill(some, true);
        some[3] = false;
        ChunkV4Section.Encoded partial =
            ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INT64, sevens, some, 8, 6, constant, null);
        assertTrue(partial.sectionLen() > 0);
        // Its body is pure presence: CONSTANT costs nothing on a type whose block table already
        // holds a min, and takes the zero-byte tie from PRESENCE_ONLY because 0x02 < 0x03. The
        // PRESENCE_ONLY spelling is what a statWidth 0 column uses, where CONSTANT is not free.
        assertEquals(ChunkV4BlockTable.ENC_CONSTANT, partial.blockTable.get(0).blockEncoding);
        // 24 bytes of block table and an 8-byte presence bitmap: no value bytes at all.
        assertEquals(ChunkV4BlockTable.tableLength(1, 8) + 8, partial.sectionLen());

        byte[][] text = new byte[8][];
        Arrays.fill(text, bytes("x"));
        text[3] = null;
        ChunkV4Section.Encoded partialText =
            ChunkV4Section.encodeVariable(ChunkV4Directory.TYPE_TEXT, text, 8, 6, bytes("x"));
        assertEquals(ChunkV4BlockTable.ENC_PRESENCE_ONLY, partialText.blockTable.get(0).blockEncoding);

        ChunkV4Section empty = read(allNull, ChunkV4Directory.TYPE_INT64, 8, 6, null);
        assertEquals(0, empty.blockCount());
    }

    /**
     * §5's derived body length is only safe while the bodies are contiguous and in index order. This
     * asserts the property directly rather than through a green round trip.
     */
    @Test
    public void bodiesAreContiguousInIndexOrderAndEightAligned()
    {
        Random random = new Random(77L);
        for (int trial = 0; trial < 200; trial++)
        {
            Sample sample = randomFixedSample(random, 10);
            ChunkV4Section.Encoded encoded = encode(sample);
            if (encoded.sectionLen() == 0)
                continue;
            int tableEnd = encoded.preambleLength
                           + ChunkV4BlockTable.tableLength(encoded.blockCount(),
                                                           ChunkV4Directory.statWidth(sample.typeCode));
            int expected = tableEnd;
            ChunkV4Section section = read(encoded, sample.typeCode, sample.rowCount, sample.blockSizeLog2,
                                          sample.columnConstant);
            for (int k = 0; k < encoded.blockCount(); k++)
            {
                ChunkV4BlockTable.Entry entry = encoded.blockTable.get(k);
                assertEquals("block " + k + " body offset", expected, entry.bodyOffset);
                assertEquals(0, entry.bodyOffset % 8);
                expected += section.blockTable().bodyLength(k);
            }
            assertEquals("bodies fill the section exactly", encoded.sectionLen(), expected);
            assertEquals(0, encoded.sectionLen() % 8);
        }
    }

    // -----------------------------------------------------------------------------------------
    // round trip and determinism
    // -----------------------------------------------------------------------------------------

    @Test
    public void roundTripOverRandomisedFixedWidthSections()
    {
        Random random = new Random(31415L);
        for (int trial = 0; trial < 600; trial++)
        {
            Sample sample = randomFixedSample(random, 10);
            ChunkV4Section.Encoded encoded = encode(sample);
            if (encoded.sectionLen() == 0)
            {
                assertTrue(sample.presentCount() == 0
                           || (sample.columnConstant != null && sample.presentCount() == sample.rowCount));
                continue;
            }
            ChunkV4Section section = read(encoded, sample.typeCode, sample.rowCount, sample.blockSizeLog2,
                                          sample.columnConstant);
            Decoded decoded = decodeFixed(section, sample.rowCount, sample.blockSizeLog2);
            assertPresenceEquals(sample, decoded.present);
            for (int i = 0; i < sample.rowCount; i++)
                if (decoded.present[i])
                    assertEquals("row " + i, sample.values[i], decoded.values[i]);

            // encode twice, and encode(decode(encode(x))) == encode(x) -- the re-encoder's path.
            assertArrayEquals(encoded.bytes, encode(sample).bytes);
            long[] again = new long[sample.rowCount];
            for (int i = 0; i < sample.rowCount; i++)
                again[i] = decoded.present[i] ? decoded.values[i] : 0;
            assertArrayEquals(encoded.bytes,
                              ChunkV4Section.encodeFixed(sample.typeCode, again, decoded.present, sample.rowCount,
                                                         sample.blockSizeLog2, sample.columnConstant, null).bytes);
        }
    }

    @Test
    public void roundTripOverRandomisedVariableWidthSections()
    {
        Random random = new Random(27182L);
        for (int trial = 0; trial < 400; trial++)
        {
            int rowCount = 1 + random.nextInt(2100);
            int blockSizeLog2 = random.nextBoolean() ? 6 : 10;
            int typeCode = random.nextBoolean() ? ChunkV4Directory.TYPE_TEXT : ChunkV4Directory.TYPE_OPAQUE;
            byte[][] values = new byte[rowCount][];
            int distinctPool = 1 + random.nextInt(6);
            for (int i = 0; i < rowCount; i++)
                values[i] = random.nextInt(8) == 0 ? null : bytes("v" + random.nextInt(distinctPool));

            ChunkV4Section.Encoded encoded =
                ChunkV4Section.encodeVariable(typeCode, values, rowCount, blockSizeLog2, null);
            if (encoded.sectionLen() == 0)
                continue;
            ChunkV4Section section = read(encoded, typeCode, rowCount, blockSizeLog2, null);
            Decoded decoded = decodeVariable(section, rowCount, blockSizeLog2);
            assertVariableEquals(values, decoded.variable);
            assertArrayEquals(encoded.bytes,
                              ChunkV4Section.encodeVariable(typeCode, decoded.variable, rowCount, blockSizeLog2,
                                                            null).bytes);
        }
    }

    /** Two blocks of 1,024 and one of 2, the multi-block shape §11 names for the golden corpus. */
    @Test
    public void multiBlockSectionsAtBothEndsOfTheBlockSizeRange()
    {
        int rowCount = 2050;
        long[] values = new long[rowCount];
        boolean[] present = new boolean[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = 1_700_000_000_000L + 1000L * i;
            present[i] = i % 7 != 3;
        }
        for (int blockSizeLog2 : new int[]{ 6, 10, 15 })
        {
            ChunkV4Section.Encoded encoded = ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INT64, values, present,
                                                                        rowCount, blockSizeLog2, null, null);
            assertEquals(ChunkV4Header.blockCount(rowCount, blockSizeLog2), encoded.blockCount());
            ChunkV4Section section = read(encoded, ChunkV4Directory.TYPE_INT64, rowCount, blockSizeLog2, null);
            Decoded decoded = decodeFixed(section, rowCount, blockSizeLog2);
            for (int i = 0; i < rowCount; i++)
            {
                assertEquals("row " + i, present[i], decoded.present[i]);
                if (present[i])
                    assertEquals("row " + i, values[i], decoded.values[i]);
            }
        }
    }

    /**
     * §2's block-independence contract, tested directly rather than by proxy: every other body is
     * overwritten with {@code 0xFF} and block {@code k} must still decode to the same values.
     */
    @Test
    public void blockIsIndependentlyDecodable()
    {
        int rowCount = 2050;
        long[] values = new long[rowCount];
        boolean[] present = new boolean[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = 1_700_000_000_000L + 1000L * i + (i % 11);
            present[i] = i % 5 != 2;
        }
        ChunkV4Section.Encoded encoded =
            ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INT64, values, present, rowCount, 10, null, null);
        ChunkV4Section clean = read(encoded, ChunkV4Directory.TYPE_INT64, rowCount, 10, null);

        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(1024);
        for (int k = 0; k < clean.blockCount(); k++)
        {
            long[] expected = new long[1024];
            long[] expectedWords = new long[BlockPresence.wordCount(1024)];
            int count = clean.decodeFixedBlock(k, expectedWords, expected, scratch);

            byte[] scrubbed = encoded.bytes.clone();
            for (int other = 0; other < clean.blockCount(); other++)
            {
                if (other == k)
                    continue;
                int from = clean.blockTable().bodyOffset(other);
                Arrays.fill(scrubbed, from, from + clean.blockTable().bodyLength(other), (byte) 0xFF);
            }
            ChunkV4Section damaged = ChunkV4Section.read(ByteBuffer.wrap(scrubbed), 0, scrubbed.length,
                                                         ChunkV4Directory.TYPE_INT64, rowCount, 10, null, null);
            long[] actual = new long[1024];
            long[] actualWords = new long[BlockPresence.wordCount(1024)];
            assertEquals(count, damaged.decodeFixedBlock(k, actualWords, actual, scratch));
            assertArrayEquals("block " + k, Arrays.copyOf(expected, count), Arrays.copyOf(actual, count));
            assertArrayEquals(expectedWords, actualWords);
        }
    }

    /**
     * §11: a {@code rank} off-by-one returns another row's value rather than throwing, so the test
     * for it is a property. The sequential walk carries a running value index, which is what
     * {@link BlockPresence#rank}'s normative note requires of a scan; the random access uses
     * {@code rank} itself, which is what it exists for.
     */
    @Test
    public void randomAccessMatchesSequentialScan()
    {
        Random random = new Random(8675309L);
        int rowCount = 2050;
        long[] values = new long[rowCount];
        boolean[] present = new boolean[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = random.nextInt(100000);
            present[i] = random.nextInt(3) != 0;
        }
        ChunkV4Section.Encoded encoded =
            ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INT32, values, present, rowCount, 10, null, null);
        ChunkV4Section section = read(encoded, ChunkV4Directory.TYPE_INT32, rowCount, 10, null);
        Decoded sequential = decodeFixed(section, rowCount, 10);

        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(1024);
        long[] words = new long[BlockPresence.wordCount(1024)];
        long[] block = new long[1024];
        for (int probe = 0; probe < 5000; probe++)
        {
            int row = random.nextInt(rowCount);
            int k = ChunkV4Header.blockIndex(row, 10);
            int offset = ChunkV4Header.blockOffset(row, 10);
            section.decodeFixedBlock(k, words, block, scratch);
            boolean rowPresent = ((words[offset >>> 6] >>> (offset & 63)) & 1L) != 0;
            assertEquals("row " + row, present[row], rowPresent);
            if (rowPresent)
                assertEquals("row " + row, values[row], block[BlockPresence.rank(words, offset)]);
        }
        for (int i = 0; i < rowCount; i++)
            if (present[i])
                assertEquals("row " + i, values[i], sequential.values[i]);
    }

    // -----------------------------------------------------------------------------------------
    // the dictionary
    // -----------------------------------------------------------------------------------------

    /**
     * §5 rule 2. v3 sorted with {@code ByteBuffer.compareTo}, which is signed and therefore puts
     * {@code 0x80} before {@code 0x7F}; unsigned costs nothing and makes the codes order-preserving.
     */
    @Test
    public void dictionaryOrderIsUnsignedNotSigned()
    {
        byte[] low = { 0x7F };
        byte[] high = { (byte) 0x80 };
        assertTrue(ChunkV4Section.compareUnsigned(low, high) < 0);
        // The signed comparator the previous format used disagrees, which is the whole point.
        assertTrue(ByteBuffer.wrap(low).compareTo(ByteBuffer.wrap(high)) > 0);

        // Five-byte values over 64 rows, so the dictionary is decisively the cheaper layout and the
        // assertions below are about the order it was built in rather than about whether it exists.
        byte[] lowValue = { 0x7F, 0, 0, 0, 0 };
        byte[] highValue = { (byte) 0x80, 0, 0, 0, 0 };
        byte[][] values = new byte[64][];
        for (int i = 0; i < values.length; i++)
            values[i] = i % 2 == 0 ? highValue : lowValue;

        ChunkV4Section.Encoded encoded =
            ChunkV4Section.encodeVariable(ChunkV4Directory.TYPE_OPAQUE, values, 64, 6, null);
        ChunkV4Section section = read(encoded, ChunkV4Directory.TYPE_OPAQUE, 64, 6, null);
        assertEquals(2, section.dictionary().length);
        assertArrayEquals(lowValue, section.dictionary()[0]);
        assertArrayEquals(highValue, section.dictionary()[1]);
        // Order-preserving codes: the smaller value under §5 rule 2 has the smaller code.
        assertEquals(0, ChunkV4Section.dictionaryCodeOf(section.dictionary(), lowValue));
        assertEquals(1, ChunkV4Section.dictionaryCodeOf(section.dictionary(), highValue));
        assertEquals(-1, ChunkV4Section.dictionaryCodeOf(section.dictionary(), new byte[]{ 1 }));
        assertVariableEquals(values, decodeVariable(section, 64, 6).variable);
    }

    @Test
    public void anOutOfOrderOrDuplicatedDictionaryIsRejected()
    {
        // The golden dictionary section with its two entries swapped: same values, different bytes,
        // which is the second byte form §5 rule 2 removes.
        byte[] swapped = hex("0002" + "0000" + "0000000B" + "04" + "62657461" + "05" + "616C706861" + "0000000000" +
                             "00000020" + "40" + "00" + "0000" +
                             "AAAAAAAAAAAAAAAA");
        assertThatThrownBy(() -> ChunkV4Section.read(ByteBuffer.wrap(swapped), 0, swapped.length,
                                                     ChunkV4Directory.TYPE_TEXT, 64, 6, null, null))
            .isInstanceOf(IllegalArgumentException.class);

        // A duplicated entry would give one value two codes.
        byte[] duplicated = hex("0002" + "0000" + "0000000A" + "04" + "62657461" + "04" + "62657461" + "000000000000" +
                                "00000020" + "40" + "00" + "0000" +
                                "AAAAAAAAAAAAAAAA");
        assertThatThrownBy(() -> ChunkV4Section.read(ByteBuffer.wrap(duplicated), 0, duplicated.length,
                                                     ChunkV4Directory.TYPE_TEXT, 64, 6, null, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void preamblePaddingAndUnusedFieldsMustBeZero()
    {
        byte[] golden = hex(GOLDEN_TEXT_DICT_HEX);
        for (int at : new int[]{ 2, 3, 19, 20, 21, 22, 23 })
        {
            assertEquals("pad byte " + at, 0, golden[at]);
            byte[] corrupt = golden.clone();
            corrupt[at] = 1;
            assertThatThrownBy(() -> ChunkV4Section.read(ByteBuffer.wrap(corrupt), 0, corrupt.length,
                                                         ChunkV4Directory.TYPE_TEXT, 64, 6, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        }
        // dictCount 0 with a non-zero dictBytesLen describes nothing.
        byte[] inconsistent = hex(GOLDEN_TEXT_RAW_HEX).clone();
        ByteBuffer.wrap(inconsistent).putInt(4, 3);
        assertThatThrownBy(() -> ChunkV4Section.read(ByteBuffer.wrap(inconsistent), 0, inconsistent.length,
                                                     ChunkV4Directory.TYPE_TEXT, 4, 6, null, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void aCorruptDictionaryLengthCannotReserveMemory()
    {
        // 65,535 entries claimed inside a 40-byte section: bounded against the section before the
        // array exists, so this throws rather than reserving.
        byte[] bytes = hex(GOLDEN_TEXT_DICT_HEX).clone();
        ByteBuffer view = ByteBuffer.wrap(bytes);
        view.putShort(0, (short) 0xFFFF);
        view.putInt(4, 0x7FFFFFFF);
        assertThatThrownBy(() -> ChunkV4Section.read(ByteBuffer.wrap(bytes), 0, bytes.length,
                                                     ChunkV4Directory.TYPE_TEXT, 64, 6, null, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // robustness
    // -----------------------------------------------------------------------------------------

    @Test
    public void sectionOffsetAndLengthMustBeEightAligned()
    {
        byte[] bytes = hex(GOLDEN_INT64_HEX);
        for (int bad : new int[]{ 1, 4, 12 })
        {
            assertThatThrownBy(() -> ChunkV4Section.read(ByteBuffer.wrap(bytes), bad, 8,
                                                         ChunkV4Directory.TYPE_INT64, 4, 6, null, null))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ChunkV4Section.read(ByteBuffer.wrap(bytes), 0, bad,
                                                         ChunkV4Directory.TYPE_INT64, 4, 6, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        }
        // A section that runs off the end of the payload.
        assertThatThrownBy(() -> ChunkV4Section.read(ByteBuffer.wrap(bytes), 8, 40,
                                                     ChunkV4Directory.TYPE_INT64, 4, 6, null, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void truncateAtEveryPrefixLength()
    {
        // From 8, because a zero-length section is a legitimate state -- §2's O(1) column -- and its
        // inconsistency with a non-zero rowCount is the directory's invariant, not this layer's.
        byte[] full = hex(GOLDEN_TEXT_RAW_HEX);
        for (int length = 8; length < full.length; length += 8)
        {
            byte[] truncated = Arrays.copyOf(full, length);
            try
            {
                ChunkV4Section section = ChunkV4Section.read(ByteBuffer.wrap(truncated), 0, length,
                                                             ChunkV4Directory.TYPE_TEXT, 4, 6, null, null);
                decodeVariable(section, 4, 6);
                fail("a " + length + "-byte buffer parsed a " + full.length + "-byte section");
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
        }
        decodeVariable(ChunkV4Section.read(ByteBuffer.wrap(full), 0, full.length, ChunkV4Directory.TYPE_TEXT, 4, 6,
                                           null, null), 4, 6);
    }

    @Test
    public void singleBitFlipsNeverEscapeAsUncheckedOrOom()
    {
        byte[] full = hex(GOLDEN_INT64_HEX);
        int accepted = 0;
        for (int bit = 0; bit < full.length * 8; bit++)
        {
            byte[] flipped = full.clone();
            flipped[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try
            {
                ChunkV4Section section = ChunkV4Section.read(ByteBuffer.wrap(flipped), 0, flipped.length,
                                                             ChunkV4Directory.TYPE_INT64, 4, 6, null, null);
                decodeFixed(section, 4, 6);
                accepted++;
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
            catch (RuntimeException unexpected)
            {
                fail("section bit " + bit + " escaped as " + unexpected);
            }
        }
        // The extrema and the lane carry no redundancy, so some flips are undetectable by design.
        if (accepted == 0)
            fail("every bit flip was rejected, which means the section is not being parsed");
    }

    @Test
    public void aSectionOfTheWrongShapeIsRejected()
    {
        long[] values = { 1, 2, 3, 4 };
        assertThatThrownBy(() -> ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_TEXT, values, null, 4, 6, null,
                                                            null))
            .isInstanceOf(IllegalArgumentException.class);
        byte[][] text = { bytes("a") };
        assertThatThrownBy(() -> ChunkV4Section.encodeVariable(ChunkV4Directory.TYPE_INT64, text, 1, 6, null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Section.encodeFixed(ChunkV4Directory.TYPE_INVALID, values, null, 4, 6, null,
                                                            null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------------

    private static final class Sample
    {
        int typeCode;
        int rowCount;
        int blockSizeLog2;
        long[] values;
        boolean[] present;
        byte[] columnConstant;

        int presentCount()
        {
            if (present == null)
                return rowCount;
            int count = 0;
            for (int i = 0; i < rowCount; i++)
                if (present[i])
                    count++;
            return count;
        }
    }

    private static final class Decoded
    {
        final long[] values;
        final byte[][] variable;
        final boolean[] present;

        Decoded(long[] values, byte[][] variable, boolean[] present)
        {
            this.values = values;
            this.variable = variable;
            this.present = present;
        }
    }

    private static ChunkV4Section.Encoded encode(Sample sample)
    {
        return ChunkV4Section.encodeFixed(sample.typeCode, sample.values, sample.present, sample.rowCount,
                                          sample.blockSizeLog2, sample.columnConstant, null);
    }

    private static ChunkV4Section read(ChunkV4Section.Encoded encoded, int typeCode, int rowCount, int blockSizeLog2,
                                       byte[] columnConstant)
    {
        return ChunkV4Section.read(ByteBuffer.wrap(encoded.bytes), 0, encoded.sectionLen(), typeCode, rowCount,
                                   blockSizeLog2, columnConstant, null);
    }

    /**
     * Walks every block in order, scattering lane-order values back to row indices with a running
     * value index -- never a per-row {@code rank()}, which is what {@link BlockPresence#rank}'s
     * normative note forbids of a sequential scan.
     */
    private static Decoded decodeFixed(ChunkV4Section section, int rowCount, int blockSizeLog2)
    {
        int blockSize = 1 << blockSizeLog2;
        long[] values = new long[rowCount];
        boolean[] present = new boolean[rowCount];
        long[] words = new long[BlockPresence.wordCount(blockSize)];
        long[] block = new long[blockSize];
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(blockSize);
        for (int k = 0; k < section.blockCount(); k++)
        {
            int blockRows = section.blockRows(k);
            section.decodeFixedBlock(k, words, block, scratch);
            int valueIndex = 0;
            int start = k << blockSizeLog2;
            for (int i = 0; i < blockRows; i++)
            {
                boolean bit = ((words[i >>> 6] >>> (i & 63)) & 1L) != 0;
                present[start + i] = bit;
                if (bit)
                    values[start + i] = block[valueIndex++];
            }
        }
        return new Decoded(values, null, present);
    }

    private static Decoded decodeVariable(ChunkV4Section section, int rowCount, int blockSizeLog2)
    {
        int blockSize = 1 << blockSizeLog2;
        byte[][] values = new byte[rowCount][];
        boolean[] present = new boolean[rowCount];
        long[] words = new long[BlockPresence.wordCount(blockSize)];
        byte[][] block = new byte[blockSize][];
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(blockSize);
        for (int k = 0; k < section.blockCount(); k++)
        {
            int blockRows = section.blockRows(k);
            section.decodeVariableBlock(k, words, block, scratch);
            int valueIndex = 0;
            int start = k << blockSizeLog2;
            for (int i = 0; i < blockRows; i++)
            {
                boolean bit = ((words[i >>> 6] >>> (i & 63)) & 1L) != 0;
                present[start + i] = bit;
                if (bit)
                    values[start + i] = block[valueIndex++];
            }
        }
        return new Decoded(null, values, present);
    }

    private static Sample randomFixedSample(Random random, int maxBlockSizeLog2)
    {
        Sample sample = new Sample();
        sample.typeCode = new int[]{ ChunkV4Directory.TYPE_DOUBLE, ChunkV4Directory.TYPE_BOOLEAN,
                                     ChunkV4Directory.TYPE_INT32, ChunkV4Directory.TYPE_INT64,
                                     ChunkV4Directory.TYPE_DATE32 }[random.nextInt(5)];
        sample.blockSizeLog2 = 6 + random.nextInt(maxBlockSizeLog2 - 5);
        sample.rowCount = 1 + random.nextInt(2100);
        sample.values = new long[sample.rowCount];
        for (int i = 0; i < sample.rowCount; i++)
        {
            switch (sample.typeCode)
            {
                case ChunkV4Directory.TYPE_DOUBLE:
                    sample.values[i] = Double.doubleToRawLongBits(random.nextGaussian());
                    break;
                case ChunkV4Directory.TYPE_BOOLEAN:
                    sample.values[i] = random.nextBoolean() ? 1 : 0;
                    break;
                case ChunkV4Directory.TYPE_INT32:
                    sample.values[i] = random.nextInt();
                    break;
                case ChunkV4Directory.TYPE_DATE32:
                    sample.values[i] = random.nextInt() & 0xFFFFFFFFL;
                    break;
                default:
                    sample.values[i] = 1_700_000_000_000L + 1000L * i;
            }
        }
        int shape = random.nextInt(4);
        if (shape == 0)
        {
            sample.present = null;
        }
        else
        {
            sample.present = new boolean[sample.rowCount];
            for (int i = 0; i < sample.rowCount; i++)
                sample.present[i] = shape == 1 ? random.nextBoolean()
                                               : shape == 2 ? (i / 37) % 2 == 0
                                                            : false;
        }
        if (random.nextInt(6) == 0)
        {
            long constant = sample.values[0];
            Arrays.fill(sample.values, constant);
            sample.columnConstant = BlockEncodings.toFixedBytes(constant, sample.typeCode);
        }
        return sample;
    }

    private static void assertPresenceEquals(Sample sample, boolean[] actual)
    {
        for (int i = 0; i < sample.rowCount; i++)
            assertEquals("row " + i, sample.present == null || sample.present[i], actual[i]);
    }

    private static void assertVariableEquals(byte[][] expected, byte[][] actual)
    {
        for (int i = 0; i < expected.length; i++)
            assertArrayEquals("row " + i, expected[i], actual[i]);
    }

    private static byte[] bytes(String ascii)
    {
        return ascii.getBytes(StandardCharsets.US_ASCII);
    }
}
