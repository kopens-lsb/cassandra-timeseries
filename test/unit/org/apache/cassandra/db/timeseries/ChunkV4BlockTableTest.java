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
import java.util.List;
import java.util.Random;

import org.junit.Test;

import static org.apache.cassandra.db.timeseries.ChunkV4HeaderTest.hex;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the block table of chunk-format-v4 §5: three fixed entry widths, and the layout invariants
 * that make a derived body length safe.
 *
 * <p>The golden vectors are hand-computed for all three widths, because the entry is the one
 * structure in v4 whose fields are all fixed-width and adjacent -- exactly the shape where swapping
 * {@code bodyOffset} with a stat, or {@code enc} with {@code flags}, round-trips perfectly and
 * makes every existing chunk unreadable.
 */
public class ChunkV4BlockTableTest
{
    /**
     * Three 24-byte entries, {@code statWidth 8}: a bit-packed block with extrema -5..5, a
     * delta-packed block carrying exceptions over a bitmap presence, and an empty all-null tail
     * block whose unused extrema are zero (§5 rule 6). Table at section offset 0, section 1000
     * bytes, so the bodies run 72..200, 200..1000 and 1000..1000.
     */
    private static final String GOLDEN_STAT8_HEX =
        "FFFFFFFFFFFFFFFB" + "0000000000000005" + "00000048" + "10" + "00" + "0000" +
        "0000000000000064" + "00000000000000C8" + "000000C8" + "11" + "12" + "0000" +
        "0000000000000000" + "0000000000000000" + "000003E8" + "01" + "01" + "0000";

    /** Two 16-byte entries, {@code statWidth 4}. The second extremum is {@code 0xFFFFFFFF} raw. */
    private static final String GOLDEN_STAT4_HEX =
        "00000001" + "FFFFFFFF" + "00000020" + "30" + "00" + "0000" +
        "00000000" + "00000000" + "00000030" + "01" + "01" + "0000";

    /** Two 8-byte entries, {@code statWidth 0}: no extrema at all, only offset, encoding and flags. */
    private static final String GOLDEN_STAT0_HEX =
        "00000010" + "30" + "0C" + "0000" +
        "00000018" + "40" + "02" + "0000";

    private static List<ChunkV4BlockTable.Entry> goldenStat8()
    {
        List<ChunkV4BlockTable.Entry> entries = new ArrayList<>();
        entries.add(new ChunkV4BlockTable.Entry(8, -5L, 5L, 72, ChunkV4BlockTable.ENC_FOR_BITPACK,
                                                BlockPresence.MODE_ALL_PRESENT));
        entries.add(new ChunkV4BlockTable.Entry(8, 100L, 200L, 200, ChunkV4BlockTable.ENC_DELTA_FOR_BITPACK,
                                                BlockPresence.MODE_BITMAP | ChunkV4BlockTable.FLAG_HAS_EXCEPTIONS));
        entries.add(new ChunkV4BlockTable.Entry(8, 0L, 0L, 1000, ChunkV4BlockTable.ENC_EMPTY,
                                                BlockPresence.MODE_ALL_NULL));
        return entries;
    }

    private static List<ChunkV4BlockTable.Entry> goldenStat4()
    {
        List<ChunkV4BlockTable.Entry> entries = new ArrayList<>();
        entries.add(new ChunkV4BlockTable.Entry(4, 1L, 0xFFFFFFFFL, 32, ChunkV4BlockTable.ENC_BITPACK1,
                                                BlockPresence.MODE_ALL_PRESENT));
        entries.add(new ChunkV4BlockTable.Entry(4, 0L, 0L, 48, ChunkV4BlockTable.ENC_EMPTY,
                                                BlockPresence.MODE_ALL_NULL));
        return entries;
    }

    private static List<ChunkV4BlockTable.Entry> goldenStat0()
    {
        List<ChunkV4BlockTable.Entry> entries = new ArrayList<>();
        entries.add(new ChunkV4BlockTable.Entry(0, 0L, 0L, 16, ChunkV4BlockTable.ENC_BITPACK1,
                                                BlockPresence.MODE_ALL_PRESENT
                                                | ChunkV4BlockTable.FLAG_HAS_FALSE
                                                | ChunkV4BlockTable.FLAG_HAS_TRUE));
        entries.add(new ChunkV4BlockTable.Entry(0, 0L, 0L, 24, ChunkV4BlockTable.ENC_DICT,
                                                BlockPresence.MODE_BITMAP));
        return entries;
    }

    // -----------------------------------------------------------------------------------------
    // layout
    // -----------------------------------------------------------------------------------------

    @Test
    public void goldenVectorsForEachStatWidth()
    {
        assertArrayEquals(hex(GOLDEN_STAT8_HEX), write(goldenStat8(), 8, 0, 1000));
        assertArrayEquals(hex(GOLDEN_STAT4_HEX), write(goldenStat4(), 4, 0, 64));
        assertArrayEquals(hex(GOLDEN_STAT0_HEX), write(goldenStat0(), 0, 0, 32));
        assertEquals(72, GOLDEN_STAT8_HEX.length() / 2);
        assertEquals(32, GOLDEN_STAT4_HEX.length() / 2);
        assertEquals(16, GOLDEN_STAT0_HEX.length() / 2);

        ChunkV4BlockTable stat8 = read(hex(GOLDEN_STAT8_HEX), 3, 8, 0, 1000);
        assertEquals(3, stat8.blockCount());
        assertEquals(-5L, stat8.entry(0).min);
        assertEquals(5L, stat8.entry(0).max);
        assertEquals(72, stat8.bodyOffset(0));
        assertEquals(ChunkV4BlockTable.ENC_FOR_BITPACK, stat8.entry(0).blockEncoding);
        assertEquals(BlockPresence.MODE_ALL_PRESENT, stat8.entry(0).presenceMode());
        assertFalse(stat8.entry(0).hasExceptions());
        assertEquals(BlockPresence.MODE_BITMAP, stat8.entry(1).presenceMode());
        assertTrue(stat8.entry(1).hasExceptions());
        assertEquals(BlockPresence.MODE_ALL_NULL, stat8.entry(2).presenceMode());

        ChunkV4BlockTable stat4 = read(hex(GOLDEN_STAT4_HEX), 2, 4, 0, 64);
        // Zero-extended, not sign-extended: the four bytes 0xFFFFFFFF are 4294967295 here, and what
        // they mean is the column's StatOrder -- signed for int, unsigned for date.
        assertEquals(0xFFFFFFFFL, stat4.entry(0).max);
        assertEquals(1L, stat4.entry(0).min);

        ChunkV4BlockTable stat0 = read(hex(GOLDEN_STAT0_HEX), 2, 0, 0, 32);
        assertTrue(stat0.entry(0).hasFalse());
        assertTrue(stat0.entry(0).hasTrue());
        assertEquals(0L, stat0.entry(0).min);
        assertEquals(ChunkV4BlockTable.ENC_DICT, stat0.entry(1).blockEncoding);
    }

    @Test
    public void entrySizesAreWhereTheSpecSays()
    {
        assertEquals(24, ChunkV4BlockTable.entrySize(8));
        assertEquals(16, ChunkV4BlockTable.entrySize(4));
        assertEquals(8, ChunkV4BlockTable.entrySize(0));
        assertEquals(24, ChunkV4BlockTable.ENTRY_SIZE_STAT8);
        assertEquals(16, ChunkV4BlockTable.ENTRY_SIZE_STAT4);
        assertEquals(8, ChunkV4BlockTable.ENTRY_SIZE_STAT0);
        for (int bad : new int[]{ -1, 1, 2, 3, 5, 6, 7, 9, 16 })
            assertThatThrownBy(() -> ChunkV4BlockTable.entrySize(bad)).isInstanceOf(IllegalArgumentException.class);

        // All three widths are multiples of 8, so §6's "the block table's end is 8-padded" costs no
        // padding bytes and every body offset stays 8-aligned by construction.
        assertEquals(0, ChunkV4BlockTable.tableLength(17, 8) % 8);
        assertEquals(0, ChunkV4BlockTable.tableLength(17, 4) % 8);
        assertEquals(0, ChunkV4BlockTable.tableLength(17, 0) % 8);
        assertEquals(72, ChunkV4BlockTable.tableLength(3, 8));
        assertEquals(0, ChunkV4BlockTable.tableLength(0, 8));
    }

    @Test
    public void presenceModeBitsComeFromBlockPresence()
    {
        // Two definitions of a mode code is how a format acquires two presence layouts.
        assertEquals(BlockPresence.MODE_MASK, ChunkV4BlockTable.FLAGS_PRESENCE_MASK);
        assertEquals(0x03, ChunkV4BlockTable.FLAGS_PRESENCE_MASK);
        assertEquals(0x04, ChunkV4BlockTable.FLAG_HAS_FALSE);
        assertEquals(0x08, ChunkV4BlockTable.FLAG_HAS_TRUE);
        assertEquals(0x10, ChunkV4BlockTable.FLAG_HAS_EXCEPTIONS);
        assertEquals(0xE0, ChunkV4BlockTable.FLAGS_RESERVED_MASK);
        for (int mode : new int[]{ BlockPresence.MODE_ALL_PRESENT, BlockPresence.MODE_ALL_NULL,
                                   BlockPresence.MODE_BITMAP, BlockPresence.MODE_RLE })
            assertEquals(mode, mode & ChunkV4BlockTable.FLAGS_PRESENCE_MASK);
    }

    /**
     * §5: the length of a body is the distance to the next one, and the last body runs to the end
     * of the section. Nothing stores it, so nothing can contradict it -- which is the whole reason
     * the format can bounds-check a body it has never decoded.
     */
    @Test
    public void bodyLengthIsDerivedFromTheNextOffset()
    {
        ChunkV4BlockTable table = read(hex(GOLDEN_STAT8_HEX), 3, 8, 0, 1000);
        assertEquals(128, table.bodyLength(0));     // 200 - 72
        assertEquals(800, table.bodyLength(1));     // 1000 - 200
        assertEquals(0, table.bodyLength(2));       // 1000 - 1000: an EMPTY block has no body
        int total = 0;
        for (int k = 0; k < table.blockCount(); k++)
            total += table.bodyLength(k);
        assertEquals(1000 - ChunkV4BlockTable.tableLength(3, 8), total);
    }

    // -----------------------------------------------------------------------------------------
    // round trip and determinism
    // -----------------------------------------------------------------------------------------

    @Test
    public void roundTripOverRandomisedTables()
    {
        Random random = new Random(5150L);
        for (int trial = 0; trial < 500; trial++)
        {
            Table table = randomTable(random);
            byte[] bytes = write(table.entries, table.statWidth, table.tableOffset, table.sectionLen);
            ChunkV4BlockTable read = read(bytes, table.entries.size(), table.statWidth,
                                          table.tableOffset, table.sectionLen);
            assertEquals(table.entries.size(), read.blockCount());
            for (int k = 0; k < table.entries.size(); k++)
            {
                ChunkV4BlockTable.Entry expected = table.entries.get(k);
                ChunkV4BlockTable.Entry actual = read.entry(k);
                assertEquals(expected.min, actual.min);
                assertEquals(expected.max, actual.max);
                assertEquals(expected.bodyOffset, actual.bodyOffset);
                assertEquals(expected.blockEncoding, actual.blockEncoding);
                assertEquals(expected.blockFlags, actual.blockFlags);
            }
            assertArrayEquals(bytes, write(read.entries(), table.statWidth, table.tableOffset, table.sectionLen));
        }
    }

    /**
     * Written twice into buffers pre-filled with different garbage. The two pad bytes per entry are
     * the target: §5 rule 5 exists because an encoder reusing a scratch buffer leaks stale bytes
     * into exactly these fields, and no round-trip test in the suite would notice.
     */
    @Test
    public void encodeTwiceIsByteIdenticalIncludingThePadBytes()
    {
        Random random = new Random(606L);
        for (int trial = 0; trial < 300; trial++)
        {
            Table table = randomTable(random);
            byte[] first = writeInto(table, (byte) 0xFF);
            byte[] second = writeInto(table, (byte) 0x5A);
            assertArrayEquals(first, second);
            for (int k = 0; k < table.entries.size(); k++)
            {
                int padAt = k * ChunkV4BlockTable.entrySize(table.statWidth)
                            + ChunkV4BlockTable.entrySize(table.statWidth) - 2;
                assertEquals("pad byte 0 of block " + k, 0, first[padAt]);
                assertEquals("pad byte 1 of block " + k, 0, first[padAt + 1]);
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // layout invariants
    // -----------------------------------------------------------------------------------------

    @Test
    public void bodyOffsetsMustNotDecrease()
    {
        // Non-strict on purpose: an EMPTY block has no body, so its offset equals the next block's,
        // and requiring a strict increase would forbid the cheapest block in the format.
        List<ChunkV4BlockTable.Entry> flat = new ArrayList<>();
        flat.add(new ChunkV4BlockTable.Entry(8, 0, 0, 48, ChunkV4BlockTable.ENC_EMPTY,
                                             BlockPresence.MODE_ALL_NULL));
        flat.add(new ChunkV4BlockTable.Entry(8, 0, 0, 48, ChunkV4BlockTable.ENC_EMPTY,
                                             BlockPresence.MODE_ALL_NULL));
        write(flat, 8, 0, 48);

        // Block 1's body offset sent back below block 0's (200 -> 64). Without this check the
        // derived length of block 0 would be negative and its body would overlap block 1's.
        byte[] bytes = hex(GOLDEN_STAT8_HEX);
        ByteBuffer.wrap(bytes).putInt(24 + 16, 64);
        assertThatThrownBy(() -> read(bytes, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void bodyOffsetsMustLieInsideTheSectionAndAfterTheTable()
    {
        byte[] past = hex(GOLDEN_STAT8_HEX).clone();
        ByteBuffer.wrap(past).putInt(2 * 24 + 16, 1008);
        assertThatThrownBy(() -> read(past, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);

        // A body starting inside the table that describes it. Only checked because the caller says
        // where the table sits; sectionLen alone cannot see this.
        byte[] inside = hex(GOLDEN_STAT8_HEX).clone();
        ByteBuffer.wrap(inside).putInt(16, 64);
        assertThatThrownBy(() -> read(inside, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
        // ... and with a preamble, the same table is rejected because the bodies now start at 80.
        assertThatThrownBy(() -> read(hex(GOLDEN_STAT8_HEX), 3, 8, 8, 1000))
            .isInstanceOf(IllegalArgumentException.class);

        // Offsets, tableOffset and sectionLen are all 8-aligned (§6).
        byte[] unaligned = hex(GOLDEN_STAT8_HEX).clone();
        ByteBuffer.wrap(unaligned).putInt(16, 76);
        assertThatThrownBy(() -> read(unaligned, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> read(hex(GOLDEN_STAT8_HEX), 3, 8, 0, 1004))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> read(hex(GOLDEN_STAT8_HEX), 3, 8, 4, 1000))
            .isInstanceOf(IllegalArgumentException.class);
        // A table that does not fit the section it claims to describe.
        assertThatThrownBy(() -> read(hex(GOLDEN_STAT8_HEX), 3, 8, 0, 64))
            .isInstanceOf(IllegalArgumentException.class);
        // A negative offset, which is what bit 31 of a u32 becomes.
        byte[] negative = hex(GOLDEN_STAT8_HEX).clone();
        ByteBuffer.wrap(negative).putInt(16, 0x80000048);
        assertThatThrownBy(() -> read(negative, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void blockEncodingZeroAndUnknownCodesAreCorruption()
    {
        // §9 lets a future build add encoding codes without a version bump; §11 still requires this
        // build to read a code it does not know, inside a v4 payload, as a flipped bit.
        for (int code : new int[]{ 0x00, 0x04, 0x0F, 0x12, 0x22, 0x31, 0x42, 0xFF })
        {
            byte[] bytes = hex(GOLDEN_STAT8_HEX).clone();
            bytes[20] = (byte) code;
            assertThatThrownBy(() -> read(bytes, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
        }
        // An entirely zeroed table cannot parse, because encoding 0x00 is unassigned -- the cheapest
        // detector there is for a half-written or truncated section.
        assertThatThrownBy(() -> read(new byte[72], 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
        // Every assigned code does parse, so the loop above is testing the code and not the offset.
        for (int code : new int[]{ 0x01, 0x02, 0x03, 0x10, 0x11, 0x20, 0x21, 0x30, 0x40, 0x41 })
        {
            byte[] bytes = hex(GOLDEN_STAT8_HEX).clone();
            bytes[20] = (byte) code;
            read(bytes, 3, 8, 0, 1000);
        }
    }

    @Test
    public void reservedBlockFlagBitsAreRejected()
    {
        for (int bit : new int[]{ 0x20, 0x40, 0x80 })
        {
            byte[] bytes = hex(GOLDEN_STAT8_HEX).clone();
            bytes[21] = (byte) bit;
            assertThatThrownBy(() -> read(bytes, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new ChunkV4BlockTable.Entry(8, 0, 0, 24, ChunkV4BlockTable.ENC_EMPTY, bit))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void padBytesMustBeZeroAndAreVerified()
    {
        for (int entry = 0; entry < 3; entry++)
        {
            for (int padByte = 0; padByte < 2; padByte++)
            {
                byte[] bytes = hex(GOLDEN_STAT8_HEX).clone();
                bytes[entry * 24 + 22 + padByte] = 1;
                assertThatThrownBy(() -> read(bytes, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    /**
     * §5 rule 6, in the three places a block entry can hold a field it does not use. Each is a
     * determinism trap as much as a correctness one: an encoder that leaves the previous block's
     * extremum in an all-null entry produces different bytes for identical input.
     */
    @Test
    public void unusedStatFieldsMustBeZero()
    {
        // statWidth 0 has no room for extrema at all
        assertThatThrownBy(() -> new ChunkV4BlockTable.Entry(0, 1, 0, 8, ChunkV4BlockTable.ENC_RAW,
                                                             BlockPresence.MODE_ALL_PRESENT))
            .isInstanceOf(IllegalArgumentException.class);
        // an all-null block has no values, therefore no extrema
        assertThatThrownBy(() -> new ChunkV4BlockTable.Entry(8, 5, 7, 24, ChunkV4BlockTable.ENC_EMPTY,
                                                             BlockPresence.MODE_ALL_NULL))
            .isInstanceOf(IllegalArgumentException.class);
        byte[] bytes = hex(GOLDEN_STAT8_HEX).clone();
        ByteBuffer.wrap(bytes).putLong(2 * 24, 7L);
        assertThatThrownBy(() -> read(bytes, 3, 8, 0, 1000)).isInstanceOf(IllegalArgumentException.class);
        // HAS_FALSE/HAS_TRUE describe a boolean column, which is a statWidth 0 type
        assertThatThrownBy(() -> new ChunkV4BlockTable.Entry(8, 0, 0, 24, ChunkV4BlockTable.ENC_EMPTY,
                                                             ChunkV4BlockTable.FLAG_HAS_TRUE))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4BlockTable.Entry(4, 0, 0, 16, ChunkV4BlockTable.ENC_EMPTY,
                                                             ChunkV4BlockTable.FLAG_HAS_FALSE))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void widthFourExtremaMustBeZeroExtended()
    {
        // Sign-extending before storing would write the same value as different bytes, and for a
        // DATE32 column -- unsigned -- it would also be a different value.
        assertThatThrownBy(() -> new ChunkV4BlockTable.Entry(4, -1L, 0L, 16, ChunkV4BlockTable.ENC_FOR_BITPACK,
                                                             BlockPresence.MODE_ALL_PRESENT))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4BlockTable.Entry(4, 0L, 0x1_0000_0000L, 16,
                                                             ChunkV4BlockTable.ENC_FOR_BITPACK,
                                                             BlockPresence.MODE_ALL_PRESENT))
            .isInstanceOf(IllegalArgumentException.class);
        // 0xFFFFFFFF zero-extended is the legal way to say "all four bytes set".
        new ChunkV4BlockTable.Entry(4, 0L, 0xFFFFFFFFL, 16, ChunkV4BlockTable.ENC_FOR_BITPACK,
                                    BlockPresence.MODE_ALL_PRESENT);
    }

    @Test
    public void anEmptySectionHasNoTable()
    {
        // §2: a column whose section is 0 bytes has blockCount 0 and no block table -- the point
        // where v3's O(1) columns stay O(1).
        ChunkV4BlockTable empty = read(new byte[0], 0, 8, 0, 0);
        assertEquals(0, empty.blockCount());
        assertEquals(0, ChunkV4BlockTable.tableLength(0, 8));
    }

    @Test
    public void truncateAtEveryPrefixLength()
    {
        byte[] full = hex(GOLDEN_STAT8_HEX);
        for (int length = 0; length < full.length; length++)
        {
            byte[] truncated = Arrays.copyOf(full, length);
            try
            {
                read(truncated, 3, 8, 0, 1000);
                fail("a " + length + "-byte buffer parsed a 72-byte block table");
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
        }
        read(full, 3, 8, 0, 1000);
    }

    @Test
    public void singleBitFlipsNeverEscapeAsUncheckedOrOom()
    {
        byte[] full = hex(GOLDEN_STAT8_HEX);
        int accepted = 0;
        for (int bit = 0; bit < full.length * 8; bit++)
        {
            byte[] flipped = full.clone();
            flipped[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try
            {
                read(flipped, 3, 8, 0, 1000);
                accepted++;
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
            catch (RuntimeException unexpected)
            {
                fail("block-table bit " + bit + " escaped as " + unexpected);
            }
        }
        // The extrema of the two non-null blocks carry no redundancy, so flips there are
        // undetectable by construction; zero acceptances would mean the table is not parsed at all.
        if (accepted == 0)
            fail("every bit flip was rejected, which means the block table is not being parsed");
    }

    // -----------------------------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------------------------

    private static final class Table
    {
        final List<ChunkV4BlockTable.Entry> entries;
        final int statWidth;
        final int tableOffset;
        final int sectionLen;

        Table(List<ChunkV4BlockTable.Entry> entries, int statWidth, int tableOffset, int sectionLen)
        {
            this.entries = entries;
            this.statWidth = statWidth;
            this.tableOffset = tableOffset;
            this.sectionLen = sectionLen;
        }
    }

    private static Table randomTable(Random random)
    {
        int statWidth = new int[]{ 8, 4, 0 }[random.nextInt(3)];
        int blockCount = 1 + random.nextInt(40);
        int tableOffset = 8 * random.nextInt(4);
        int cursor = tableOffset + ChunkV4BlockTable.tableLength(blockCount, statWidth);
        int[] encodings = { ChunkV4BlockTable.ENC_EMPTY, ChunkV4BlockTable.ENC_CONSTANT,
                            ChunkV4BlockTable.ENC_PRESENCE_ONLY, ChunkV4BlockTable.ENC_FOR_BITPACK,
                            ChunkV4BlockTable.ENC_DELTA_FOR_BITPACK, ChunkV4BlockTable.ENC_ALP,
                            ChunkV4BlockTable.ENC_ALP_RD, ChunkV4BlockTable.ENC_BITPACK1,
                            ChunkV4BlockTable.ENC_DICT, ChunkV4BlockTable.ENC_RAW };

        List<ChunkV4BlockTable.Entry> entries = new ArrayList<>();
        for (int k = 0; k < blockCount; k++)
        {
            int mode = random.nextInt(4);
            int flags = mode;
            if (random.nextInt(4) == 0)
                flags |= ChunkV4BlockTable.FLAG_HAS_EXCEPTIONS;
            if (statWidth == 0 && random.nextInt(3) == 0)
                flags |= ChunkV4BlockTable.FLAG_HAS_FALSE;
            if (statWidth == 0 && random.nextInt(3) == 0)
                flags |= ChunkV4BlockTable.FLAG_HAS_TRUE;

            long min = 0;
            long max = 0;
            if (statWidth != 0 && mode != BlockPresence.MODE_ALL_NULL)
            {
                min = statWidth == 8 ? random.nextLong() : random.nextInt() & 0xFFFFFFFFL;
                max = statWidth == 8 ? random.nextLong() : random.nextInt() & 0xFFFFFFFFL;
            }
            entries.add(new ChunkV4BlockTable.Entry(statWidth, min, max, cursor,
                                                    encodings[random.nextInt(encodings.length)], flags));
            cursor += 8 * random.nextInt(4);        // 0 is legal: an EMPTY block has no body
        }
        return new Table(entries, statWidth, tableOffset, cursor + 8 * random.nextInt(3));
    }

    private static byte[] write(List<ChunkV4BlockTable.Entry> entries, int statWidth, int tableOffset, int sectionLen)
    {
        byte[] out = new byte[ChunkV4BlockTable.tableLength(entries.size(), statWidth)];
        ByteBuffer dst = ByteBuffer.wrap(out);
        assertEquals(out.length, ChunkV4BlockTable.write(entries, statWidth, tableOffset, sectionLen, dst));
        assertEquals(out.length, dst.position());
        return out;
    }

    private static byte[] writeInto(Table table, byte fill)
    {
        byte[] out = new byte[ChunkV4BlockTable.tableLength(table.entries.size(), table.statWidth)];
        Arrays.fill(out, fill);
        ChunkV4BlockTable.write(table.entries, table.statWidth, table.tableOffset, table.sectionLen,
                                ByteBuffer.wrap(out));
        return out;
    }

    private static ChunkV4BlockTable read(byte[] bytes, int blockCount, int statWidth, int tableOffset, int sectionLen)
    {
        return ChunkV4BlockTable.read(ByteBuffer.wrap(bytes), blockCount, statWidth, tableOffset, sectionLen);
    }
}
