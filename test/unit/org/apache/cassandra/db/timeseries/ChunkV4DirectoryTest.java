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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;

import static org.apache.cassandra.db.timeseries.ChunkV4HeaderTest.hex;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the column directory of chunk-format-v4 §4 and the {@link StatOrder} contract it carries.
 *
 * <p>{@link StatOrder} is tested here rather than in a class of its own because §4 is where the
 * field lives and where its rules bite: statistics exist only under a declared order, a
 * {@code CONSTANT} or {@code ALL_NULL} column has none, and an {@code OPAQUE} column is forced to
 * {@code NONE}. The comparator tests are the important half -- §11's {@code pruningIsSound} cannot
 * be written until each order provably matches the Cassandra comparator it claims to describe, and
 * the two orders most likely to be wrong ({@code date} as signed, {@code double} as raw bytes) are
 * exactly the ones that look right in a round-trip test.
 */
public class ChunkV4DirectoryTest
{
    /**
     * §4's entry layout, by hand, over three columns chosen to cover the three shapes an entry can
     * take: an {@code ALL_NULL} column with no section at all, an all-present {@code CONSTANT}
     * column carrying its value inline, and an ordinary column with a section, a block count and a
     * pair of extrema. 74 bytes of entries, padded to 80.
     */
    private static final String GOLDEN_HEX =
        // entry "a": INT32, ALL_NULL -- no section, no blocks, no stats (17 bytes)
        "03" + "02" + "00" + "01" + "61" + "00000000" + "00000000" + "00000000" +
        // entry "b": TEXT, ALL_PRESENT|CONSTANT, value "hi" -- O(1) column, no section (20 bytes)
        "06" + "05" + "00" + "01" + "62" + "00000000" + "00000000" + "00000000" + "02" + "6869" +
        // entry "cpu": INT64, ALL_PRESENT|HAS_STATS, statOrder SIGNED_INT, section 208+4096,
        // 3 blocks, min -5, max 5 (37 bytes)
        "04" + "09" + "01" + "03" + "637075" + "000000D0" + "00001000" + "00000003" +
        "08" + "FFFFFFFFFFFFFFFB" + "08" + "0000000000000005" +
        // §6 pad to 8: 74 -> 80
        "000000000000";

    private static final int PAYLOAD_BYTES = 208 + 4096;

    private static List<ChunkV4Directory.Entry> goldenEntries()
    {
        List<ChunkV4Directory.Entry> entries = new ArrayList<>();
        entries.add(new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT32,
                                               ChunkV4Directory.FLAG_ALL_NULL,
                                               StatOrder.NONE, "a", 0, 0, 0, null, null, null));
        entries.add(new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_TEXT,
                                               ChunkV4Directory.FLAG_ALL_PRESENT | ChunkV4Directory.FLAG_CONSTANT,
                                               StatOrder.NONE, "b", 0, 0, 0,
                                               "hi".getBytes(StandardCharsets.UTF_8), null, null));
        entries.add(new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                               ChunkV4Directory.FLAG_ALL_PRESENT | ChunkV4Directory.FLAG_HAS_STATS,
                                               StatOrder.SIGNED_INT, "cpu", 208, 4096, 3,
                                               null, int64(-5), int64(5)));
        return entries;
    }

    private static ChunkV4Header goldenHeader()
    {
        // directoryLen 80, so the sections may start at 120; the timestamp axis takes 120..208 and
        // the "cpu" section runs 208..4304, which is the payload's length.
        return new ChunkV4Header(2050, -1L, 4294967296L, 10, 3, 80, 120, 88);
    }

    // -----------------------------------------------------------------------------------------
    // layout
    // -----------------------------------------------------------------------------------------

    @Test
    public void goldenVectorFromSpecSection4()
    {
        assertEquals(80, GOLDEN_HEX.length() / 2);
        List<ChunkV4Directory.Entry> entries = goldenEntries();
        assertEquals(80, ChunkV4Directory.encodedLength(entries));
        assertArrayEquals(hex(GOLDEN_HEX), writeDirectory(entries));

        ChunkV4Directory directory = ChunkV4Directory.read(goldenPayload(), goldenHeader());
        assertEquals(3, directory.size());

        ChunkV4Directory.Entry a = directory.column("a");
        assertEquals(ChunkV4Directory.TYPE_INT32, a.typeCode);
        assertTrue(a.allNull());
        assertEquals(0, a.sectionLen);
        assertEquals(0, a.blockCount);
        assertSame(StatOrder.NONE, a.statOrder);
        assertNull(a.constBytes);
        assertNull(a.statMin);

        ChunkV4Directory.Entry b = directory.column("b");
        assertTrue(b.constant() && b.allPresent());
        assertArrayEquals("hi".getBytes(StandardCharsets.UTF_8), b.constBytes);
        assertEquals(0, b.sectionLen);

        ChunkV4Directory.Entry cpu = directory.column("cpu");
        assertEquals(ChunkV4Directory.TYPE_INT64, cpu.typeCode);
        assertSame(StatOrder.SIGNED_INT, cpu.statOrder);
        assertEquals(208, cpu.sectionOffset);
        assertEquals(4096, cpu.sectionLen);
        assertEquals(3, cpu.blockCount);
        assertArrayEquals(int64(-5), cpu.statMin);
        assertArrayEquals(int64(5), cpu.statMax);
        assertEquals(8, cpu.statWidth());

        assertNull(directory.column("nope"));
    }

    @Test
    public void statWidthFollowsTheTypeCode()
    {
        assertEquals(8, ChunkV4Directory.statWidth(ChunkV4Directory.TYPE_DOUBLE));
        assertEquals(8, ChunkV4Directory.statWidth(ChunkV4Directory.TYPE_INT64));
        assertEquals(4, ChunkV4Directory.statWidth(ChunkV4Directory.TYPE_INT32));
        assertEquals(4, ChunkV4Directory.statWidth(ChunkV4Directory.TYPE_DATE32));
        assertEquals(0, ChunkV4Directory.statWidth(ChunkV4Directory.TYPE_BOOLEAN));
        assertEquals(0, ChunkV4Directory.statWidth(ChunkV4Directory.TYPE_TEXT));
        assertEquals(0, ChunkV4Directory.statWidth(ChunkV4Directory.TYPE_OPAQUE));
        assertThatThrownBy(() -> ChunkV4Directory.statWidth(0x00)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkV4Directory.statWidth(0x08)).isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // round trip and determinism
    // -----------------------------------------------------------------------------------------

    @Test
    public void roundTripOverRandomisedDirectories()
    {
        Random random = new Random(2604L);
        for (int trial = 0; trial < 300; trial++)
        {
            Fixture fixture = randomFixture(random);
            ChunkV4Directory read = ChunkV4Directory.read(fixture.payload(), fixture.header);
            assertEquals(fixture.entries.size(), read.size());
            for (int i = 0; i < fixture.entries.size(); i++)
                assertEntryEquals(fixture.entries.get(i), read.entry(i));
            // Re-encoding what was read reproduces the bytes: the property the re-encoder's
            // "this chunk is unchanged" check depends on (§11 reencodeIsIdempotent, in miniature).
            assertArrayEquals(fixture.directoryBytes(), writeDirectoryFor(read.entries(), fixture.header));
        }
    }

    @Test
    public void encodeTwiceIsByteIdentical()
    {
        Random random = new Random(773L);
        for (int trial = 0; trial < 200; trial++)
        {
            Fixture fixture = randomFixture(random);
            byte[] first = writeDirectoryInto(fixture.entries, fixture.header, (byte) 0xFF);
            byte[] second = writeDirectoryInto(fixture.entries, fixture.header, (byte) 0x5A);
            assertArrayEquals(first, second);
            assertArrayEquals(fixture.directoryBytes(), first);
        }
    }

    /**
     * §5 rule 1 names the trap that produces the wrong order: {@code new TreeMap<>(map)} adopts the
     * source map's comparator instead of forcing natural order. The directory does not paper over
     * it -- a caller who hands entries in the wrong order gets an exception naming the pair --
     * because silently re-sorting would let the encoder ship a bug that only shows up as every
     * replica producing a different chunk.
     */
    @Test
    public void writeRejectsEntriesOutOfNaturalStringOrder()
    {
        List<ChunkV4Directory.Entry> entries = goldenEntries();
        Collections.reverse(entries);
        assertThatThrownBy(() -> writeDirectory(entries)).isInstanceOf(IllegalArgumentException.class);

        List<ChunkV4Directory.Entry> duplicated = goldenEntries();
        duplicated.set(1, duplicated.get(0));
        assertThatThrownBy(() -> writeDirectory(duplicated)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void sortingAShuffledInputReproducesTheSameBytes()
    {
        byte[] expected = writeDirectory(goldenEntries());
        Random random = new Random(11L);
        for (int trial = 0; trial < 20; trial++)
        {
            List<ChunkV4Directory.Entry> shuffled = goldenEntries();
            Collections.shuffle(shuffled, random);
            shuffled.sort((left, right) -> left.name.compareTo(right.name));
            assertArrayEquals(expected, writeDirectory(shuffled));
        }
    }

    // -----------------------------------------------------------------------------------------
    // §4 and §5 rules
    // -----------------------------------------------------------------------------------------

    @Test
    public void typeCodeZeroIsAPermanentZeroTrapAndUnknownCodesAreCorruption()
    {
        assertEquals(0x00, ChunkV4Directory.TYPE_INVALID);
        assertThatThrownBy(() -> readMutated(0, (byte) 0x00)).isInstanceOf(IllegalArgumentException.class);
        for (int code : new int[]{ 0x08, 0x09, 0x7F, 0xFF })
            assertThatThrownBy(() -> readMutated(0, (byte) code)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void reservedColFlagBitsAreRejected()
    {
        assertEquals(0xF0, ChunkV4Directory.FLAGS_RESERVED_MASK);
        for (int bit : new int[]{ 0x10, 0x20, 0x40, 0x80 })
            assertThatThrownBy(() -> readMutated(1, (byte) (0x02 | bit)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void unknownStatOrderCodesAreCorruption()
    {
        // §9 permits new statOrder codes without a version bump; §11 still requires a code this
        // build does not know, inside a v4 payload, to be read as a flipped bit rather than guessed.
        for (int code : new int[]{ 0x05, 0x10, 0xFF })
            assertThatThrownBy(() -> readMutated(2, (byte) code)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void constantAndAllNullColumnsCarryNoStats()
    {
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_NULL
                                                            | ChunkV4Directory.FLAG_HAS_STATS,
                                                            StatOrder.SIGNED_INT, "x", 0, 0, 0,
                                                            null, int64(1), int64(2)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT
                                                            | ChunkV4Directory.FLAG_CONSTANT
                                                            | ChunkV4Directory.FLAG_HAS_STATS,
                                                            StatOrder.SIGNED_INT, "x", 0, 0, 0,
                                                            int64(1), int64(1), int64(1)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * §4, and §12's fourth-ranked risk. A column downgraded to {@code OPAQUE} because one value was
     * the wrong width keeps extrema that were computed over the old fixed-width interpretation; if
     * {@code statOrder} does not follow the downgrade, those extrema are read under an order that no
     * longer applies and the reader skips blocks that do contain matching rows.
     */
    @Test
    public void opaqueColumnsCarryNoStatOrderAndThereforeNoStats()
    {
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_OPAQUE,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT
                                                            | ChunkV4Directory.FLAG_HAS_STATS,
                                                            StatOrder.BYTES_UNSIGNED, "x", 128, 8, 1,
                                                            null, new byte[]{ 1 }, new byte[]{ 2 }))
            .isInstanceOf(IllegalArgumentException.class);
        // ... and the same bytes read back off the wire are rejected too: entry "cpu" retyped as
        // OPAQUE while keeping its SIGNED_INT extrema is precisely the downgrade bug.
        assertThatThrownBy(() -> readMutated(37, (byte) ChunkV4Directory.TYPE_OPAQUE))
            .isInstanceOf(IllegalArgumentException.class);
        // A legal OPAQUE column: statOrder NONE, no stats.
        new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_OPAQUE, ChunkV4Directory.FLAG_ALL_PRESENT,
                                   StatOrder.NONE, "x", 128, 8, 1, null, null, null);
    }

    @Test
    public void statsExistExactlyWhenAnOrderIsDeclared()
    {
        // HAS_STATS without an order, and an order without HAS_STATS, are both unusable states, so
        // neither is representable: §5 rule 6, unused fields are zero.
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT
                                                            | ChunkV4Directory.FLAG_HAS_STATS,
                                                            StatOrder.NONE, "x", 128, 8, 1,
                                                            null, int64(1), int64(2)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT,
                                                            StatOrder.SIGNED_INT, "x", 128, 8, 1,
                                                            null, null, null))
            .isInstanceOf(IllegalArgumentException.class);
        // min above max is not a stat, it is a filter that excludes everything.
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT
                                                            | ChunkV4Directory.FLAG_HAS_STATS,
                                                            StatOrder.SIGNED_INT, "x", 128, 8, 1,
                                                            null, int64(5), int64(-5)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * §7. A truncated maximum compares <em>below</em> the value it was meant to bound, so it is not
     * an upper bound at all and a reader trusting it drops rows. Omission is always sound, so an
     * over-long extremum means no statistics -- and building one instead of omitting it fails here.
     */
    @Test
    public void statsAreOmittedNotTruncatedBeyond256Bytes()
    {
        assertEquals(256, ChunkV4Directory.MAX_STAT_BYTES);
        byte[] atLimit = new byte[256];
        byte[] overLimit = new byte[257];
        Arrays.fill(atLimit, (byte) 'a');
        Arrays.fill(overLimit, (byte) 'a');
        // 256 bytes is fine; 257 must have been dropped by the encoder rather than shortened.
        new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_TEXT, ChunkV4Directory.FLAG_ALL_PRESENT
                                                              | ChunkV4Directory.FLAG_HAS_STATS,
                                   StatOrder.BYTES_UNSIGNED, "x", 128, 8, 1, null, atLimit, atLimit);
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_TEXT,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT
                                                            | ChunkV4Directory.FLAG_HAS_STATS,
                                                            StatOrder.BYTES_UNSIGNED, "x", 128, 8, 1,
                                                            null, atLimit, overLimit))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void zeroByteSectionsAreExactlyTheOOneColumns()
    {
        // §2: only ALL_NULL and an all-present CONSTANT column have no section, and those are
        // exactly the columns that stay O(1) regardless of row count -- the largest single win in
        // the production shape, where four of eight columns are zero bytes.
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT,
                                                            StatOrder.NONE, "x", 0, 0, 0, null, null, null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_NULL,
                                                            StatOrder.NONE, "x", 128, 8, 1, null, null, null))
            .isInstanceOf(IllegalArgumentException.class);
        // blockCount and sectionLen are zero together or not at all.
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT,
                                                            StatOrder.NONE, "x", 128, 8, 0, null, null, null))
            .isInstanceOf(IllegalArgumentException.class);
        // sections are 8-aligned (§6)
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_PRESENT,
                                                            StatOrder.NONE, "x", 130, 8, 1, null, null, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void blockCountMustAgreeWithTheHeader()
    {
        // The field is redundant with the header, and that is the point: it is where §2's "every
        // column is cut on the same row boundaries" is stated in the bytes.
        assertEquals(3, ChunkV4Directory.expectedBlockCount(4096, 2050, 10));
        assertEquals(0, ChunkV4Directory.expectedBlockCount(0, 2050, 10));

        List<ChunkV4Directory.Entry> entries = goldenEntries();
        ChunkV4Directory.Entry cpu = entries.get(2);
        entries.set(2, new ChunkV4Directory.Entry(cpu.typeCode, cpu.colFlags, cpu.statOrder, cpu.name,
                                                  cpu.sectionOffset, cpu.sectionLen, 4,
                                                  null, cpu.statMin, cpu.statMax));
        assertThatThrownBy(() -> writeDirectory(entries)).isInstanceOf(IllegalArgumentException.class);
        // and on the way back in: "cpu"'s blockCount u32 sits at directory offset 52, set to 4
        // where the header's 2050 rows over 1024-row blocks imply 3.
        assertThatThrownBy(() -> readMutatedInt(52, 4)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void sectionsMustLieInThePayloadAndOutsideTheDirectoryAndTimestampAxis()
    {
        // The "cpu" section offset sits at directory byte 37 + 4 + 3 = 44; steer it into the
        // timestamp axis (120..208) and into the header/directory region, and past the payload.
        assertThatThrownBy(() -> readMutatedInt(44, 128)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> readMutatedInt(44, 64)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> readMutatedInt(44, PAYLOAD_BYTES)).isInstanceOf(IllegalArgumentException.class);
        // and a length that would run off the end
        assertThatThrownBy(() -> readMutatedInt(48, 0x7FFFFFF8)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void namesMustBeNonEmptyValidUtf8AndStrictlyAscending()
    {
        // nameLen 0
        assertThatThrownBy(() -> readMutated(3, (byte) 0x00)).isInstanceOf(IllegalArgumentException.class);
        // 0xFF is not a valid UTF-8 sequence; new String(bytes, UTF_8) would silently make it
        // U+FFFD and give the directory two byte forms for one column name.
        assertThatThrownBy(() -> readMutated(4, (byte) 0xFF)).isInstanceOf(IllegalArgumentException.class);
        // 'a' -> 'z' puts entry 0 after entry 1, so the strictly-ascending check fires
        assertThatThrownBy(() -> readMutated(4, (byte) 'z')).isInstanceOf(IllegalArgumentException.class);
        // 'a' -> 'b' duplicates entry 1's name
        assertThatThrownBy(() -> readMutated(4, (byte) 'b')).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_NULL, StatOrder.NONE,
                                                            "", 0, 0, 0, null, null, null))
            .isInstanceOf(IllegalArgumentException.class);
        StringBuilder tooLong = new StringBuilder();
        for (int i = 0; i < 256; i++)
            tooLong.append('x');
        assertThatThrownBy(() -> new ChunkV4Directory.Entry(ChunkV4Directory.TYPE_INT64,
                                                            ChunkV4Directory.FLAG_ALL_NULL, StatOrder.NONE,
                                                            tooLong.toString(), 0, 0, 0, null, null, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void paddingIsZeroAndVerified()
    {
        // 74 entry bytes, 6 bytes of pad: every one of them must be zero, and each is checked.
        for (int offset = 74; offset < 80; offset++)
        {
            int padByte = offset;
            assertThatThrownBy(() -> readMutated(padByte, (byte) 1)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void directoryLenMustAgreeWithWhatWasParsed()
    {
        // Trailing zeroes beyond the padded entry length would decode identically and re-encode
        // shorter, giving one directory two byte forms -- a permanent chunkUnchanged mismatch.
        ChunkV4Header longer = new ChunkV4Header(2050, -1L, 4294967296L, 10, 3, 88, 128, 80);
        assertThatThrownBy(() -> ChunkV4Directory.read(goldenPayload(), longer))
            .isInstanceOf(IllegalArgumentException.class);
        // Too short: the last entry runs out of region.
        ChunkV4Header shorter = new ChunkV4Header(2050, -1L, 4294967296L, 10, 3, 72, 120, 88);
        assertThatThrownBy(() -> ChunkV4Directory.read(goldenPayload(), shorter))
            .isInstanceOf(IllegalArgumentException.class);
        // A column count that disagrees with the bytes.
        ChunkV4Header twoColumns = new ChunkV4Header(2050, -1L, 4294967296L, 10, 2, 80, 120, 88);
        assertThatThrownBy(() -> ChunkV4Directory.read(goldenPayload(), twoColumns))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void varintsMustBeCanonical()
    {
        // constLen 2, written as the two-byte 0x82 0x00. It decodes to the same 2, which is exactly
        // why it must be rejected: a length with two byte forms gives the directory two byte forms.
        String nonCanonical = "06" + "05" + "00" + "01" + "78" + "00000000" + "00000000" + "00000000" +
                              "8200" + "6869" + "000000";
        assertEquals(24, nonCanonical.length() / 2);
        assertThatThrownBy(() -> readStandalone(nonCanonical, 24))
            .isInstanceOf(IllegalArgumentException.class);
        // The canonical form of the same entry parses, so the rejection above is about minimality
        // and not about the surrounding bytes.
        String canonical = "06" + "05" + "00" + "01" + "78" + "00000000" + "00000000" + "00000000" +
                           "02" + "6869" + "00000000";
        assertEquals(24, canonical.length() / 2);
        assertEquals(1, readStandalone(canonical, 24).size());
    }

    /**
     * A length field is the one place a corrupt payload can ask for memory. Every allocation in the
     * directory is checked against the bytes the region still holds before the array exists, so a
     * constant claiming 2 GiB throws in constant time instead of reserving anything.
     */
    @Test
    public void corruptLengthsThrowInsteadOfAllocating()
    {
        String hugeConstant = "06" + "05" + "00" + "01" + "78" + "00000000" + "00000000" + "00000000" +
                              "FFFFFFFF07" + "0000";
        assertEquals(24, hugeConstant.length() / 2);
        long before = System.nanoTime();
        assertThatThrownBy(() -> readStandalone(hugeConstant, 24)).isInstanceOf(IllegalArgumentException.class);
        // Not a timing assertion on the JVM's behalf -- a generous ceiling that a 2 GiB allocation
        // (or its OutOfMemoryError) could not slip under.
        if (System.nanoTime() - before > 2_000_000_000L)
            fail("rejecting a 2 GiB constant length took long enough to have allocated it");
    }

    @Test
    public void truncateAtEveryPrefixLength()
    {
        ByteBuffer full = goldenPayload();
        ChunkV4Header header = goldenHeader();
        byte[] payload = new byte[full.remaining()];
        full.duplicate().get(payload);
        for (int length = 0; length < 120; length++)
        {
            ByteBuffer truncated = ByteBuffer.wrap(Arrays.copyOf(payload, length));
            try
            {
                ChunkV4Directory.read(truncated, header);
                fail("a " + length + "-byte payload parsed a directory that needs 120");
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
        ByteBuffer full = goldenPayload();
        byte[] payload = new byte[full.remaining()];
        full.duplicate().get(payload);
        ChunkV4Header header = goldenHeader();
        int accepted = 0;
        for (int bit = 40 * 8; bit < 120 * 8; bit++)
        {
            byte[] flipped = payload.clone();
            flipped[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try
            {
                ChunkV4Directory.read(ByteBuffer.wrap(flipped), header);
                accepted++;
            }
            catch (IllegalArgumentException expected)
            {
                // the only permitted outcome
            }
            catch (RuntimeException unexpected)
            {
                fail("directory bit " + bit + " escaped as " + unexpected);
            }
        }
        // The bits inside a constant value and inside an extremum carry no redundancy, so some
        // flips are undetectable by construction; zero acceptances would mean the parse is not
        // reaching them at all.
        if (accepted == 0)
            fail("every bit flip was rejected, which means the directory is not being parsed");
    }

    // -----------------------------------------------------------------------------------------
    // StatOrder: the codes
    // -----------------------------------------------------------------------------------------

    @Test
    public void statOrderCodesAreTheOnesSpecSection4Assigns()
    {
        assertEquals(0x00, StatOrder.NONE.code);
        assertEquals(0x01, StatOrder.SIGNED_INT.code);
        assertEquals(0x02, StatOrder.UNSIGNED_INT.code);
        assertEquals(0x03, StatOrder.IEEE754_TOTAL.code);
        assertEquals(0x04, StatOrder.BYTES_UNSIGNED.code);
        for (StatOrder order : StatOrder.values())
            assertSame(order, StatOrder.forCode(order.code));
        for (int code : new int[]{ 5, 6, 0x80, 0xFF, -1 })
            assertThatThrownBy(() -> StatOrder.forCode(code)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void noneDeclaresNoComparator()
    {
        assertFalse(StatOrder.NONE.hasOrder());
        for (StatOrder order : StatOrder.values())
            if (order != StatOrder.NONE)
                assertTrue(order.hasOrder());
        // Not IllegalArgumentException: reaching this is a reader ignoring §4's matching rule, not
        // a property of any bytes.
        assertThatThrownBy(() -> StatOrder.NONE.compare(new byte[1], new byte[1]))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void typeMappingIsTheOneSpecSection4Describes()
    {
        assertSame(StatOrder.SIGNED_INT, StatOrder.forType(Int32Type.instance));
        assertSame(StatOrder.SIGNED_INT, StatOrder.forType(LongType.instance));
        assertSame(StatOrder.SIGNED_INT, StatOrder.forType(TimestampType.instance));
        assertSame(StatOrder.UNSIGNED_INT, StatOrder.forType(SimpleDateType.instance));
        assertSame(StatOrder.IEEE754_TOTAL, StatOrder.forType(DoubleType.instance));
        assertSame(StatOrder.BYTES_UNSIGNED, StatOrder.forType(UTF8Type.instance));
        assertSame(StatOrder.BYTES_UNSIGNED, StatOrder.forType(AsciiType.instance));
        assertSame(StatOrder.BYTES_UNSIGNED, StatOrder.forType(BytesType.instance));

        // float is not double: Float.compare over four bytes is not IEEE754_TOTAL, and v4 has no
        // type code for it, so a float column carries no stats rather than approximate ones.
        assertSame(StatOrder.NONE, StatOrder.forType(FloatType.instance));
        // boolean normalises any non-zero byte to true before comparing, so no byte order describes
        // it; §5 gives boolean blocks HAS_FALSE/HAS_TRUE instead of extrema.
        assertSame(StatOrder.NONE, StatOrder.forType(BooleanType.instance));
        assertThatThrownBy(() -> StatOrder.forType(null)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The mapping the design brief assumed was {@code time -> SIGNED_INT}. The type is
     * {@code ComparisonType.BYTE_ORDER}, i.e. unsigned, so the declaration follows the comparator
     * and not the expected domain. Over legal times (0 .. 86399999999999 ns) the two agree, which
     * is exactly why a signed declaration would survive every test built from legal values.
     */
    @Test
    public void timeIsComparedUnsignedNotSigned()
    {
        assertSame(StatOrder.UNSIGNED_INT, StatOrder.forType(TimeType.instance));
        byte[] high = int64(Long.MIN_VALUE);        // bit 63 set
        byte[] one = int64(1L);
        assertEquals(1, Integer.signum(TimeType.instance.compare(ByteBuffer.wrap(high), ByteBuffer.wrap(one))));
        assertEquals(1, Integer.signum(StatOrder.UNSIGNED_INT.compare(high, one)));
        // and the mapping the brief assumed would have said the opposite
        assertEquals(-1, Integer.signum(StatOrder.SIGNED_INT.compare(high, one)));
    }

    /**
     * §4's headline example. {@code date} stores an unsigned day count with the epoch shifted to
     * {@code 0x80000000}, so the identical four bytes order one way as a date and the other way as
     * an {@code int} -- and both are four-byte columns with {@code statWidth} 4, indistinguishable
     * in the block table.
     */
    @Test
    public void dateIsUnsignedWhereIntOfTheSameWidthIsSigned()
    {
        byte[] minDate = int32(0x00000000);         // the earliest representable date
        byte[] epoch = int32(0x80000000);           // 1970-01-01
        assertEquals(-1, Integer.signum(SimpleDateType.instance.compare(ByteBuffer.wrap(minDate),
                                                                       ByteBuffer.wrap(epoch))));
        assertEquals(-1, Integer.signum(StatOrder.UNSIGNED_INT.compare(minDate, epoch)));
        // The same bytes under int32's order: 0x80000000 is Integer.MIN_VALUE, so the order flips.
        assertEquals(1, Integer.signum(Int32Type.instance.compare(ByteBuffer.wrap(minDate),
                                                                  ByteBuffer.wrap(epoch))));
        assertEquals(1, Integer.signum(StatOrder.SIGNED_INT.compare(minDate, epoch)));
    }

    @Test
    public void nanAndNegativeZeroExtremaFollowDoubleCompare()
    {
        byte[] negativeZero = ieee(-0.0);
        byte[] positiveZero = ieee(0.0);
        byte[] nan = ieee(Double.NaN);
        byte[] otherNan = ieee(Double.longBitsToDouble(0x7FF8_0000_DEAD_BEEFL));
        byte[] infinity = ieee(Double.POSITIVE_INFINITY);
        byte[] minusOne = ieee(-1.0);
        byte[] one = ieee(1.0);

        assertEquals(-1, Integer.signum(StatOrder.IEEE754_TOTAL.compare(negativeZero, positiveZero)));
        assertEquals(1, Integer.signum(StatOrder.IEEE754_TOTAL.compare(nan, infinity)));
        assertEquals(0, StatOrder.IEEE754_TOTAL.compare(nan, otherNan));
        assertEquals(-1, Integer.signum(StatOrder.IEEE754_TOTAL.compare(minusOne, one)));
        // Each of those agrees with DoubleType, which is the claim that makes the stat usable.
        assertEquals(Integer.signum(DoubleType.instance.compare(ByteBuffer.wrap(negativeZero),
                                                               ByteBuffer.wrap(positiveZero))),
                     Integer.signum(StatOrder.IEEE754_TOTAL.compare(negativeZero, positiveZero)));
        assertEquals(Integer.signum(DoubleType.instance.compare(ByteBuffer.wrap(nan), ByteBuffer.wrap(infinity))),
                     Integer.signum(StatOrder.IEEE754_TOTAL.compare(nan, infinity)));

        // The mistake this order exists to prevent: raw bits ascend as a negative double descends,
        // so BYTES_UNSIGNED would report -1.0 as the larger value.
        assertEquals(1, Integer.signum(StatOrder.BYTES_UNSIGNED.compare(minusOne, one)));
        // A double stat that is not eight bytes is corruption, not a comparison.
        assertThatThrownBy(() -> StatOrder.IEEE754_TOTAL.compare(new byte[4], one))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------------------------
    // StatOrder: agreement with Cassandra's own comparators
    // -----------------------------------------------------------------------------------------

    /**
     * The load-bearing test of §4. Each order is compared against the {@code AbstractType} it
     * claims to describe over a corpus that includes the cases where a plausible-looking
     * implementation diverges: empty values (every one of these types sorts empty first), values of
     * the wrong width (§12's {@code OPAQUE}-downgrade trigger), signed/unsigned boundaries, and
     * {@code -0.0}/NaN.
     */
    @Test
    public void eachComparatorAgreesWithItsCassandraType()
    {
        Random random = new Random(1123L);
        assertAgrees(Int32Type.instance, StatOrder.SIGNED_INT, fixedWidthCorpus(random, 4));
        assertAgrees(LongType.instance, StatOrder.SIGNED_INT, fixedWidthCorpus(random, 8));
        assertAgrees(TimestampType.instance, StatOrder.SIGNED_INT, fixedWidthCorpus(random, 8));
        assertAgrees(TimeType.instance, StatOrder.UNSIGNED_INT, timeCorpus(random));
        assertAgrees(SimpleDateType.instance, StatOrder.UNSIGNED_INT, fixedWidthCorpus(random, 4));
        assertAgrees(DoubleType.instance, StatOrder.IEEE754_TOTAL, doubleCorpus(random));
        assertAgrees(UTF8Type.instance, StatOrder.BYTES_UNSIGNED, bytesCorpus(random));
        assertAgrees(AsciiType.instance, StatOrder.BYTES_UNSIGNED, bytesCorpus(random));
        assertAgrees(BytesType.instance, StatOrder.BYTES_UNSIGNED, bytesCorpus(random));
    }

    private static void assertAgrees(AbstractType<?> type, StatOrder order, List<byte[]> corpus)
    {
        for (byte[] left : corpus)
        {
            for (byte[] right : corpus)
            {
                int expected = Integer.signum(type.compare(ByteBuffer.wrap(left), ByteBuffer.wrap(right)));
                int actual = Integer.signum(order.compare(left, right));
                assertEquals(type.getClass().getSimpleName() + " vs " + order + " on " +
                             hexOf(left) + " / " + hexOf(right), expected, actual);
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // corpora
    // -----------------------------------------------------------------------------------------

    /**
     * Fixed-width values around the sign boundary, plus an empty value and two off-width ones. The
     * off-width values are not noise: a single zero-length value in a fixed-width column is what
     * downgrades a column to {@code OPAQUE} (§12's fourth risk), and a comparator that cannot be
     * asked about one is a comparator the soundness test cannot exercise there.
     */
    private static List<byte[]> fixedWidthCorpus(Random random, int width)
    {
        List<byte[]> corpus = new ArrayList<>();
        corpus.add(new byte[0]);
        long[] edges = width == 4
                       ? new long[]{ 0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 0x7FFFFFFFL, 0x80000000L }
                       : new long[]{ 0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE, 0x00FFFFFFFFFFFFFFL };
        for (long edge : edges)
            corpus.add(width == 4 ? int32((int) edge) : int64(edge));
        for (int i = 0; i < 12; i++)
            corpus.add(width == 4 ? int32(random.nextInt()) : int64(random.nextLong()));
        corpus.add(new byte[]{ (byte) 0x80 });
        corpus.add(new byte[]{ 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF });
        return corpus;
    }

    private static List<byte[]> timeCorpus(Random random)
    {
        List<byte[]> corpus = new ArrayList<>();
        corpus.add(new byte[0]);
        for (long edge : new long[]{ 0L, 1L, 86_399_999_999_999L, Long.MIN_VALUE, -1L })
            corpus.add(int64(edge));
        for (int i = 0; i < 12; i++)
            corpus.add(int64((long) (random.nextDouble() * 86_400_000_000_000L)));
        return corpus;
    }

    private static List<byte[]> doubleCorpus(Random random)
    {
        List<byte[]> corpus = new ArrayList<>();
        corpus.add(new byte[0]);
        double[] edges = { 0.0, -0.0, 1.0, -1.0, Double.MIN_VALUE, -Double.MIN_VALUE, Double.MAX_VALUE,
                           -Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN,
                           Double.MIN_NORMAL };
        for (double edge : edges)
            corpus.add(ieee(edge));
        // NaN payloads: Double.compare canonicalises them, so all of these must compare equal.
        corpus.add(ieee(Double.longBitsToDouble(0x7FF8_0000_0000_0001L)));
        corpus.add(ieee(Double.longBitsToDouble(0xFFF8_0000_DEAD_BEEFL)));
        for (int i = 0; i < 10; i++)
            corpus.add(ieee(random.nextGaussian() * Math.pow(10, random.nextInt(40) - 20)));
        return corpus;
    }

    private static List<byte[]> bytesCorpus(Random random)
    {
        List<byte[]> corpus = new ArrayList<>();
        corpus.add(new byte[0]);
        corpus.add(new byte[]{ 0 });
        corpus.add(new byte[]{ 0, 0 });
        corpus.add(new byte[]{ 0x7F });
        corpus.add(new byte[]{ (byte) 0x80 });          // must sort above 0x7F: unsigned, not signed
        corpus.add(new byte[]{ (byte) 0xFF });
        corpus.add("a".getBytes(StandardCharsets.UTF_8));
        corpus.add("ab".getBytes(StandardCharsets.UTF_8));
        corpus.add("b".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 14; i++)
        {
            byte[] value = new byte[random.nextInt(6)];
            random.nextBytes(value);
            corpus.add(value);
        }
        return corpus;
    }

    // -----------------------------------------------------------------------------------------
    // fixtures and helpers
    // -----------------------------------------------------------------------------------------

    /** A directory plus the header and payload it is consistent with. */
    private static final class Fixture
    {
        final ChunkV4Header header;
        final List<ChunkV4Directory.Entry> entries;
        final int payloadBytes;

        Fixture(ChunkV4Header header, List<ChunkV4Directory.Entry> entries, int payloadBytes)
        {
            this.header = header;
            this.entries = entries;
            this.payloadBytes = payloadBytes;
        }

        byte[] directoryBytes()
        {
            return writeDirectoryFor(entries, header);
        }

        ByteBuffer payload()
        {
            byte[] payload = new byte[payloadBytes];
            header.write(ByteBuffer.wrap(payload));
            System.arraycopy(directoryBytes(), 0, payload, ChunkV4Header.HEADER_SIZE, header.directoryLen);
            return ByteBuffer.wrap(payload);
        }
    }

    /**
     * Builds a consistent directory in two passes: entry lengths do not depend on the section
     * offsets (every one of them is a fixed-width u32), so the first pass sizes the region and the
     * second lays the sections out after it.
     */
    private static Fixture randomFixture(Random random)
    {
        int rowCount = 1 + random.nextInt(5000);
        int blockSizeLog2 = 6 + random.nextInt(10);
        int columns = 1 + random.nextInt(6);
        List<int[]> shapes = new ArrayList<>();
        for (int i = 0; i < columns; i++)
            shapes.add(new int[]{ random.nextInt(5), 8 * (1 + random.nextInt(8)), random.nextInt(200) });

        // One seed for both passes, so the two differ only in where the sections were placed.
        long seed = random.nextLong();
        List<ChunkV4Directory.Entry> sized = buildEntries(shapes, rowCount, blockSizeLog2, 0, seed);
        int directoryLen = ChunkV4Directory.encodedLength(sized);
        int tsSectionOffset = ChunkV4Header.HEADER_SIZE + directoryLen;
        int tsSectionLen = 8 * random.nextInt(4);
        int sectionsFrom = tsSectionOffset + tsSectionLen;

        List<ChunkV4Directory.Entry> placed = buildEntries(shapes, rowCount, blockSizeLog2, sectionsFrom, seed);
        assertEquals(directoryLen, ChunkV4Directory.encodedLength(placed));
        int end = sectionsFrom;
        for (ChunkV4Directory.Entry entry : placed)
            end = Math.max(end, entry.sectionOffset + entry.sectionLen);

        ChunkV4Header header = new ChunkV4Header(rowCount, 100L, rowCount == 1 ? 100L : 100L + rowCount,
                                                 blockSizeLog2, placed.size(), directoryLen,
                                                 tsSectionOffset, tsSectionLen);
        return new Fixture(header, placed, end);
    }

    private static List<ChunkV4Directory.Entry> buildEntries(List<int[]> shapes, int rowCount, int blockSizeLog2,
                                                             int sectionsFrom, long seed)
    {
        Random random = new Random(seed);
        List<ChunkV4Directory.Entry> entries = new ArrayList<>();
        int cursor = sectionsFrom;
        for (int i = 0; i < shapes.size(); i++)
        {
            int[] shape = shapes.get(i);
            String name = String.format("c%03d", i);
            byte[] constBytes = null;
            byte[] min = null;
            byte[] max = null;
            int typeCode;
            int flags;
            int sectionLen;
            StatOrder order = StatOrder.NONE;
            switch (shape[0])
            {
                case 0:     // ALL_NULL: O(1)
                    typeCode = ChunkV4Directory.TYPE_INT32;
                    flags = ChunkV4Directory.FLAG_ALL_NULL;
                    sectionLen = 0;
                    break;
                case 1:     // all-present CONSTANT: O(1)
                    typeCode = ChunkV4Directory.TYPE_TEXT;
                    flags = ChunkV4Directory.FLAG_ALL_PRESENT | ChunkV4Directory.FLAG_CONSTANT;
                    sectionLen = 0;
                    constBytes = new byte[shape[2] % 40];
                    random.nextBytes(constBytes);
                    break;
                case 2:     // sparse CONSTANT: presence only, but a section all the same
                    typeCode = ChunkV4Directory.TYPE_DOUBLE;
                    flags = ChunkV4Directory.FLAG_CONSTANT;
                    sectionLen = shape[1];
                    constBytes = int64(random.nextLong());
                    break;
                case 3:     // OPAQUE: no order, therefore no stats (§4)
                    typeCode = ChunkV4Directory.TYPE_OPAQUE;
                    flags = ChunkV4Directory.FLAG_ALL_PRESENT;
                    sectionLen = shape[1];
                    break;
                default:    // ordinary column with statistics
                    typeCode = ChunkV4Directory.TYPE_INT64;
                    flags = ChunkV4Directory.FLAG_ALL_PRESENT | ChunkV4Directory.FLAG_HAS_STATS;
                    sectionLen = shape[1];
                    order = StatOrder.SIGNED_INT;
                    long low = random.nextLong();
                    long high = random.nextLong();
                    min = int64(Math.min(low, high));
                    max = int64(Math.max(low, high));
                    break;
            }
            int sectionOffset = sectionLen == 0 ? 0 : cursor;
            cursor += sectionLen;
            entries.add(new ChunkV4Directory.Entry(typeCode, flags, order, name, sectionOffset, sectionLen,
                                                   ChunkV4Directory.expectedBlockCount(sectionLen, rowCount,
                                                                                       blockSizeLog2),
                                                   constBytes, min, max));
        }
        return entries;
    }

    private static void assertEntryEquals(ChunkV4Directory.Entry expected, ChunkV4Directory.Entry actual)
    {
        assertEquals(expected.name, actual.name);
        assertEquals(expected.typeCode, actual.typeCode);
        assertEquals(expected.colFlags, actual.colFlags);
        assertSame(expected.statOrder, actual.statOrder);
        assertEquals(expected.sectionOffset, actual.sectionOffset);
        assertEquals(expected.sectionLen, actual.sectionLen);
        assertEquals(expected.blockCount, actual.blockCount);
        assertArrayEquals(expected.constBytes, actual.constBytes);
        assertArrayEquals(expected.statMin, actual.statMin);
        assertArrayEquals(expected.statMax, actual.statMax);
    }

    private static byte[] writeDirectory(List<ChunkV4Directory.Entry> entries)
    {
        return writeDirectoryFor(entries, goldenHeader());
    }

    private static byte[] writeDirectoryFor(List<ChunkV4Directory.Entry> entries, ChunkV4Header header)
    {
        byte[] out = new byte[ChunkV4Directory.encodedLength(entries)];
        ChunkV4Directory.write(entries, header.rowCount, header.blockSizeLog2, ByteBuffer.wrap(out));
        return out;
    }

    private static byte[] writeDirectoryInto(List<ChunkV4Directory.Entry> entries, ChunkV4Header header, byte fill)
    {
        byte[] out = new byte[ChunkV4Directory.encodedLength(entries)];
        Arrays.fill(out, fill);
        ChunkV4Directory.write(entries, header.rowCount, header.blockSizeLog2, ByteBuffer.wrap(out));
        return out;
    }

    private static ByteBuffer goldenPayload()
    {
        byte[] payload = new byte[PAYLOAD_BYTES];
        goldenHeader().write(ByteBuffer.wrap(payload));
        System.arraycopy(hex(GOLDEN_HEX), 0, payload, ChunkV4Header.HEADER_SIZE, 80);
        return ByteBuffer.wrap(payload);
    }

    /** Reads the golden directory with one byte of the directory region replaced. */
    private static ChunkV4Directory readMutated(int directoryOffset, byte value)
    {
        ByteBuffer payload = goldenPayload();
        payload.put(ChunkV4Header.HEADER_SIZE + directoryOffset, value);
        return ChunkV4Directory.read(payload, goldenHeader());
    }

    /** Reads the golden directory with one big-endian u32 of the directory region replaced. */
    private static ChunkV4Directory readMutatedInt(int directoryOffset, int value)
    {
        ByteBuffer payload = goldenPayload();
        payload.putInt(ChunkV4Header.HEADER_SIZE + directoryOffset, value);
        return ChunkV4Directory.read(payload, goldenHeader());
    }

    /** Reads a hand-written single-entry directory of {@code directoryLen} bytes. */
    private static ChunkV4Directory readStandalone(String directoryHex, int directoryLen)
    {
        int tsSectionOffset = ChunkV4Header.HEADER_SIZE + directoryLen;
        ChunkV4Header header = new ChunkV4Header(100, 1L, 2L, 10, 1, directoryLen, tsSectionOffset, 8);
        byte[] payload = new byte[tsSectionOffset + 8];
        header.write(ByteBuffer.wrap(payload));
        System.arraycopy(hex(directoryHex), 0, payload, ChunkV4Header.HEADER_SIZE, directoryLen);
        return ChunkV4Directory.read(ByteBuffer.wrap(payload), header);
    }

    private static byte[] int32(int value)
    {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static byte[] int64(long value)
    {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    /** Raw bits, so a NaN payload survives into the comparator instead of being canonicalised. */
    private static byte[] ieee(double value)
    {
        return int64(Double.doubleToRawLongBits(value));
    }

    private static String hexOf(byte[] value)
    {
        StringBuilder out = new StringBuilder(value.length * 2);
        for (byte b : value)
            out.append(String.format("%02X", b & 0xFF));
        return out.length() == 0 ? "(empty)" : out.toString();
    }
}
