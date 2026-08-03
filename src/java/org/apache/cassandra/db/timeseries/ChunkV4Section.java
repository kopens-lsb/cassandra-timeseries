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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * One column's section of a chunk-format-v4 payload (§4.5): an optional preamble, the block table,
 * and the block bodies.
 *
 * <pre>
 *   sectionOffset (a multiple of 8, from the directory)
 *     [preamble]        TEXT/OPAQUE only: dictCount u16 | pad u16 | dictBytesLen u32 | entries, pad 8
 *     block table       blockCount fixed-width entries, {@link ChunkV4BlockTable}
 *     body 0            presence bytes then the block's payload
 *     body 1            ...
 * </pre>
 *
 * <p><b>Bodies are contiguous and in index order, and that is a load-bearing property rather than a
 * layout preference.</b> It is the whole reason §5 can derive a body's length as the next entry's
 * {@code bodyOffset} minus this one's and store no length field: a derived length cannot disagree
 * with anything, whereas a stored one can disagree with the next offset and send a decoder reading
 * past its block into the next, returning the wrong values without throwing. Every body is a
 * multiple of 8 bytes (§6) -- presence modes are, and every payload {@link BlockEncodings} emits is
 * -- so the next body starts 8-aligned with no padding byte written between them.
 *
 * <p><b>A body is presence then payload</b>, which is what makes §2's block-independence contract
 * true: decoding block {@code k} needs the header, the directory entry, this section's preamble,
 * table entries {@code k} and {@code k+1}, and body {@code k}. Nothing else. {@code PRESENCE_ONLY}
 * is named for the case where the payload half is empty and the body is exactly the presence.
 *
 * <p><b>Dictionary order is ascending UNSIGNED byte order</b> (§5 rule 2). v3 sorted with
 * {@code ByteBuffer.compareTo}, which is <em>signed</em>: it puts every value whose first byte is
 * {@code >= 0x80} before every value whose first byte is {@code < 0x80}, so for UTF-8 text it sorts
 * most non-ASCII before all ASCII. Unsigned costs exactly the same instruction and makes the
 * dictionary codes order-preserving, which is what a v5 wanting per-block dictionary-code extrema
 * (§9 lists it) would need and what makes the code assignment explainable rather than arbitrary.
 * The comparator here is {@link StatOrder#BYTES_UNSIGNED}'s rule, and codes are the entries' indices
 * in that order.
 *
 * <p><b>Whether to build a dictionary at all is a section-level exact-size comparison</b>, computed
 * both ways and never guessed: the total of preamble plus table plus bodies with a dictionary
 * offered to every block, against the same total without one. Ties keep the dictionary-free layout.
 * A section of entirely distinct values would otherwise pay for a dictionary that stores every value
 * once and then codes them all again.
 *
 * <p><b>The preamble is present for every {@code TEXT}/{@code OPAQUE} section, including those with
 * no dictionary</b>, where it is eight bytes with {@code dictCount == 0}. §4.5 calls the preamble
 * optional; the reader has to locate the block table before it has parsed anything, so "optional"
 * cannot mean "detectable from the bytes". Keying its presence on the type code -- which the reader
 * already holds from the directory -- costs eight bytes on a dictionary-free text section and
 * removes the only ambiguity in the section layout. {@code dictBytesLen} is the entries' length
 * before the pad to 8, since the padding is derivable and the unpadded number is the one a bounds
 * check wants.
 *
 * <p><b>Zero-byte sections.</b> §2 keeps v3's O(1) columns: a column with no present value, and a
 * {@code CONSTANT} column with every row present, encode to nothing at all -- no preamble, no table,
 * no bodies. Those are exactly the two cases {@link ChunkV4Directory.Entry} requires
 * {@code sectionLen == 0} for, so the two layers agree by construction rather than by convention.
 *
 * <p>Corruption is {@link IllegalArgumentException} (§11). Every allocation on the read path is
 * bounded by metadata the caller derived from the header -- {@code blockCount} from §2,
 * {@code blockRows} from §2, {@code dictCount} against the bytes remaining in the preamble -- and
 * never by a length taken on trust from the payload.
 */
public final class ChunkV4Section
{
    /** §7: 65,535 dictionary entries, because codes are packed at {@code ceil(log2(dictCount))} bits. */
    public static final int MAX_DICTIONARY_ENTRIES = 0xFFFF;

    /** {@code dictCount u16 | pad u16 | dictBytesLen u32} (§4.5). */
    public static final int PREAMBLE_HEADER_BYTES = 8;

    private final ByteBuffer section;
    private final int typeCode;
    private final int rowCount;
    private final int blockSizeLog2;
    private final byte[] columnConstant;
    private final BlockEncodings.DoubleBlockCodec alp;
    private final byte[][] dictionary;
    private final int preambleLength;
    private final ChunkV4BlockTable blockTable;

    private ChunkV4Section(ByteBuffer section,
                           int typeCode,
                           int rowCount,
                           int blockSizeLog2,
                           byte[] columnConstant,
                           BlockEncodings.DoubleBlockCodec alp,
                           byte[][] dictionary,
                           int preambleLength,
                           ChunkV4BlockTable blockTable)
    {
        this.section = section;
        this.typeCode = typeCode;
        this.rowCount = rowCount;
        this.blockSizeLog2 = blockSizeLog2;
        this.columnConstant = columnConstant;
        this.alp = alp;
        this.dictionary = dictionary;
        this.preambleLength = preambleLength;
        this.blockTable = blockTable;
    }

    // -----------------------------------------------------------------------------------------
    // the encoded result
    // -----------------------------------------------------------------------------------------

    /** A built section: its bytes, and the block table the directory entry has to agree with. */
    public static final class Encoded
    {
        /** Exactly {@code sectionLen} bytes, a multiple of 8 (§6). Empty for an O(1) column (§2). */
        public final byte[] bytes;
        public final List<ChunkV4BlockTable.Entry> blockTable;
        public final int preambleLength;
        public final int dictionaryCount;

        private Encoded(byte[] bytes, List<ChunkV4BlockTable.Entry> blockTable, int preambleLength, int dictionaryCount)
        {
            this.bytes = bytes;
            this.blockTable = blockTable;
            this.preambleLength = preambleLength;
            this.dictionaryCount = dictionaryCount;
        }

        public int sectionLen()
        {
            return bytes.length;
        }

        public int blockCount()
        {
            return blockTable.size();
        }

        private static Encoded empty()
        {
            return new Encoded(new byte[0], Collections.emptyList(), 0, 0);
        }
    }

    // -----------------------------------------------------------------------------------------
    // encode
    // -----------------------------------------------------------------------------------------

    /**
     * Builds the section of a fixed-width column.
     *
     * @param values         row-indexed; entries at absent rows are ignored, never read
     * @param present        row-indexed presence, or null for a fully present column
     * @param columnConstant the directory's constant for a {@code CONSTANT} column, or null
     */
    public static Encoded encodeFixed(int typeCode,
                                      long[] values,
                                      boolean[] present,
                                      int rowCount,
                                      int blockSizeLog2,
                                      byte[] columnConstant,
                                      BlockEncodings.DoubleBlockCodec alp)
    {
        checkShape(typeCode, rowCount, blockSizeLog2, true);
        if (values.length < rowCount)
            throw new IllegalArgumentException("values array holds " + values.length + " < rowCount " + rowCount);
        if (present != null && present.length < rowCount)
            throw new IllegalArgumentException("presence array holds " + present.length + " < rowCount " + rowCount);
        if (columnConstant != null)
            BlockEncodings.fromFixedBytes(columnConstant, typeCode);     // validates width and domain

        int presentRows = countPresent(present, rowCount);
        if (isZeroByteSection(presentRows, rowCount, columnConstant))
            return Encoded.empty();

        int blockCount = ChunkV4Header.blockCount(rowCount, blockSizeLog2);
        int blockSize = 1 << blockSizeLog2;
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(blockSize);
        long[] blockValues = new long[blockSize];
        BlockPlan[] plans = new BlockPlan[blockCount];

        for (int k = 0; k < blockCount; k++)
        {
            BlockPlan plan = new BlockPlan(rowCount, blockSizeLog2, k);
            plan.gather(present);
            int n = 0;
            int start = k << blockSizeLog2;
            for (int i = 0; i < plan.blockRows; i++)
                if (present == null || present[start + i])
                    blockValues[n++] = values[start + i];
            plan.presentCount = n;
            fixedStats(typeCode, blockValues, n, plan);
            BlockEncodings.chooseFixed(typeCode, blockValues, n, plan.min, columnConstant, alp, scratch, plan.choice);
            plans[k] = plan;
        }

        Layout layout = new Layout(typeCode, 0, plans);
        ByteBuffer dst = layout.allocate();
        ChunkV4BlockTable.write(layout.entries, layout.statWidth, 0, layout.sectionLen, dst);
        for (int k = 0; k < blockCount; k++)
        {
            BlockPlan plan = plans[k];
            BlockPresence.encode(plan.presenceMode, plan.words, plan.blockRows, dst);
            int n = 0;
            int start = k << blockSizeLog2;
            for (int i = 0; i < plan.blockRows; i++)
                if (present == null || present[start + i])
                    blockValues[n++] = values[start + i];
            BlockEncodings.encodeFixed(plan.choice, typeCode, blockValues, n, plan.min, alp, scratch, dst);
        }
        return layout.finish(dst, 0);
    }

    /**
     * Builds the section of a {@code TEXT}/{@code OPAQUE} column. A null entry in {@code values} is
     * an absent row; a zero-length {@code byte[]} is a present, empty value.
     */
    public static Encoded encodeVariable(int typeCode,
                                         byte[][] values,
                                         int rowCount,
                                         int blockSizeLog2,
                                         byte[] columnConstant)
    {
        checkShape(typeCode, rowCount, blockSizeLog2, false);
        if (values.length < rowCount)
            throw new IllegalArgumentException("values array holds " + values.length + " < rowCount " + rowCount);

        int presentRows = 0;
        for (int i = 0; i < rowCount; i++)
            if (values[i] != null)
                presentRows++;
        if (isZeroByteSection(presentRows, rowCount, columnConstant))
            return Encoded.empty();

        // Both layouts are costed exactly and the cheaper wins; a tie keeps the dictionary-free one.
        byte[][] dictionary = buildDictionary(values, rowCount, presentRows);
        Layout without = planVariable(typeCode, values, rowCount, blockSizeLog2, columnConstant, null);
        Layout with = dictionary == null
                      ? null
                      : planVariable(typeCode, values, rowCount, blockSizeLog2, columnConstant, dictionary);
        Layout layout = with != null && with.sectionLen < without.sectionLen ? with : without;
        byte[][] chosen = layout == with ? dictionary : null;

        ByteBuffer dst = layout.allocate();
        writePreamble(dst, chosen, layout.preambleLength);
        ChunkV4BlockTable.write(layout.entries, layout.statWidth, layout.preambleLength, layout.sectionLen, dst);

        int blockSize = 1 << blockSizeLog2;
        byte[][] blockValues = new byte[blockSize][];
        BlockEncodings.Scratch scratch = new BlockEncodings.Scratch(blockSize);
        for (int k = 0; k < layout.plans.length; k++)
        {
            BlockPlan plan = layout.plans[k];
            BlockPresence.encode(plan.presenceMode, plan.words, plan.blockRows, dst);
            int n = gatherVariable(values, k << blockSizeLog2, plan.blockRows, blockValues);
            BlockEncodings.encodeVariable(plan.choice, typeCode, blockValues, n, chosen, scratch, dst);
        }
        return layout.finish(dst, chosen == null ? 0 : chosen.length);
    }

    private static Layout planVariable(int typeCode,
                                       byte[][] values,
                                       int rowCount,
                                       int blockSizeLog2,
                                       byte[] columnConstant,
                                       byte[][] dictionary)
    {
        int blockCount = ChunkV4Header.blockCount(rowCount, blockSizeLog2);
        int blockSize = 1 << blockSizeLog2;
        byte[][] blockValues = new byte[blockSize][];
        BlockPlan[] plans = new BlockPlan[blockCount];
        for (int k = 0; k < blockCount; k++)
        {
            BlockPlan plan = new BlockPlan(rowCount, blockSizeLog2, k);
            plan.gatherVariablePresence(values);
            plan.presentCount = gatherVariable(values, k << blockSizeLog2, plan.blockRows, blockValues);
            BlockEncodings.chooseVariable(typeCode, blockValues, plan.presentCount, columnConstant, dictionary,
                                          plan.choice);
            plans[k] = plan;
        }
        return new Layout(typeCode, preambleLength(dictionary), plans);
    }

    private static int gatherVariable(byte[][] values, int start, int blockRows, byte[][] dst)
    {
        int n = 0;
        for (int i = 0; i < blockRows; i++)
            if (values[start + i] != null)
                dst[n++] = values[start + i];
        return n;
    }

    // -----------------------------------------------------------------------------------------
    // read
    // -----------------------------------------------------------------------------------------

    /**
     * Parses the section at {@code sectionOffset} of {@code payload}. Neither buffer is modified;
     * parsing runs inside a slice limited to the section, so "bytes remaining" always means "bytes
     * remaining in this section" and a corrupt length cannot walk into a neighbour.
     */
    public static ChunkV4Section read(ByteBuffer payload,
                                      int sectionOffset,
                                      int sectionLen,
                                      int typeCode,
                                      int rowCount,
                                      int blockSizeLog2,
                                      byte[] columnConstant,
                                      BlockEncodings.DoubleBlockCodec alp)
    {
        boolean fixed = BlockEncodings.isFixedWidth(typeCode);
        checkShape(typeCode, rowCount, blockSizeLog2, fixed);
        if (sectionOffset < 0 || (sectionOffset & 7) != 0 || sectionLen < 0 || (sectionLen & 7) != 0)
            throw new IllegalArgumentException("section " + sectionOffset + '+' + sectionLen +
                                               " is negative or not 8-aligned (v4 §6)");
        if (payload.remaining() < (long) sectionOffset + sectionLen)
            throw new IllegalArgumentException("Corrupt v4 chunk: section " + sectionOffset + '+' + sectionLen +
                                               " passes the payload's " + payload.remaining() + " bytes");

        ByteBuffer slice = payload.duplicate();
        slice.order(ByteOrder.BIG_ENDIAN);
        slice.position(payload.position() + sectionOffset);
        slice.limit(payload.position() + sectionOffset + sectionLen);
        slice = slice.slice();
        slice.order(ByteOrder.BIG_ENDIAN);

        if (sectionLen == 0)
            return new ChunkV4Section(slice, typeCode, rowCount, blockSizeLog2, columnConstant, alp, null, 0,
                                      ChunkV4BlockTable.read(slice.duplicate(), 0,
                                                             ChunkV4Directory.statWidth(typeCode), 0, 0));

        ByteBuffer cursor = slice.duplicate();
        cursor.order(ByteOrder.BIG_ENDIAN);
        byte[][] dictionary = null;
        int preambleLength = 0;
        if (!fixed)
        {
            dictionary = readPreamble(cursor, sectionLen);
            preambleLength = cursor.position();
        }
        int blockCount = ChunkV4Header.blockCount(rowCount, blockSizeLog2);
        ChunkV4BlockTable table = ChunkV4BlockTable.read(cursor, blockCount, ChunkV4Directory.statWidth(typeCode),
                                                         preambleLength, sectionLen);
        return new ChunkV4Section(slice, typeCode, rowCount, blockSizeLog2, columnConstant, alp, dictionary,
                                  preambleLength, table);
    }

    public ChunkV4BlockTable blockTable()
    {
        return blockTable;
    }

    public int blockCount()
    {
        return blockTable.blockCount();
    }

    public int preambleLength()
    {
        return preambleLength;
    }

    /** The section's dictionary in ascending unsigned byte order, or null. Never mutate it. */
    public byte[][] dictionary()
    {
        return dictionary;
    }

    /** §2: rows block {@code k} covers. */
    public int blockRows(int block)
    {
        return ChunkV4Header.blockRows(rowCount, blockSizeLog2, block);
    }

    /**
     * Decodes block {@code k} of a fixed-width column: its presence into {@code presenceWords} and
     * its present values, densely and in lane order, into {@code values}. Returns the present count.
     *
     * <p><b>Values come back in lane order, not row order, on purpose.</b> A caller walking rows must
     * carry a running value index and add the row's own presence bit to it, per
     * {@link BlockPresence#rank}'s normative note; scattering here into a row-indexed array would
     * either repeat that loop or invite the per-row {@code rank()} call that turns a block walk into
     * O(n * n/64).
     */
    public int decodeFixedBlock(int block, long[] presenceWords, long[] values, BlockEncodings.Scratch scratch)
    {
        ChunkV4BlockTable.Entry entry = entryOf(block);
        int blockRows = blockRows(block);
        ByteBuffer body = bodyOf(block);
        int consumed = BlockPresence.decode(entry.presenceMode(), body, blockRows, presenceWords);
        int present = BlockPresence.cardinality(presenceWords, blockRows);
        BlockEncodings.decodeFixed(entry.blockEncoding, entry.hasExceptions(), body,
                                   blockTable.bodyLength(block) - consumed, typeCode, present,
                                   BlockEncodings.fromStatBits(entry.min, typeCode), columnConstant, alp, scratch,
                                   values);
        return present;
    }

    /** {@link #decodeFixedBlock} for a {@code TEXT}/{@code OPAQUE} column. */
    public int decodeVariableBlock(int block, long[] presenceWords, byte[][] values, BlockEncodings.Scratch scratch)
    {
        ChunkV4BlockTable.Entry entry = entryOf(block);
        int blockRows = blockRows(block);
        ByteBuffer body = bodyOf(block);
        int consumed = BlockPresence.decode(entry.presenceMode(), body, blockRows, presenceWords);
        int present = BlockPresence.cardinality(presenceWords, blockRows);
        BlockEncodings.decodeVariable(entry.blockEncoding, body, blockTable.bodyLength(block) - consumed, typeCode,
                                      present, columnConstant, dictionary, scratch, values);
        return present;
    }

    private ChunkV4BlockTable.Entry entryOf(int block)
    {
        if (block < 0 || block >= blockTable.blockCount())
            throw new IllegalArgumentException("block " + block + " outside 0.." + (blockTable.blockCount() - 1));
        return blockTable.entry(block);
    }

    /**
     * The bytes of one block's body, and nothing else. The limit is the derived length, so a payload
     * that tried to read past its block hits the end of the buffer rather than the next block's
     * bytes -- which is what makes §2's independence contract enforceable rather than merely stated.
     */
    private ByteBuffer bodyOf(int block)
    {
        int offset = blockTable.bodyOffset(block);
        int length = blockTable.bodyLength(block);
        ByteBuffer body = section.duplicate();
        body.order(ByteOrder.BIG_ENDIAN);
        body.position(offset);
        body.limit(offset + length);
        return body.slice().order(ByteOrder.BIG_ENDIAN);
    }

    // -----------------------------------------------------------------------------------------
    // dictionary
    // -----------------------------------------------------------------------------------------

    /** §5 rule 2: ascending unsigned bytes, shorter-is-smaller on a prefix tie. */
    public static int compareUnsigned(byte[] left, byte[] right)
    {
        int shared = Math.min(left.length, right.length);
        for (int i = 0; i < shared; i++)
        {
            int diff = (left[i] & 0xFF) - (right[i] & 0xFF);
            if (diff != 0)
                return diff;
        }
        return left.length - right.length;
    }

    /**
     * The code for {@code value}, i.e. its index in the dictionary, or -1. A binary search over the
     * sorted entries rather than a hash lookup: §5 rule 7 keeps hash structures out of anything that
     * reaches the bytes, and a sorted array cannot grow a hash-ordered view of itself.
     */
    public static int dictionaryCodeOf(byte[][] dictionary, byte[] value)
    {
        int low = 0;
        int high = dictionary.length - 1;
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            int cmp = compareUnsigned(dictionary[mid], value);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -1;
    }

    private static byte[][] buildDictionary(byte[][] values, int rowCount, int presentRows)
    {
        byte[][] all = new byte[presentRows][];
        int n = 0;
        for (int i = 0; i < rowCount; i++)
            if (values[i] != null)
                all[n++] = values[i];
        Arrays.sort(all, ChunkV4Section::compareUnsigned);
        int distinct = 0;
        for (int i = 0; i < n; i++)
            if (i == 0 || compareUnsigned(all[i], all[i - 1]) != 0)
                all[distinct++] = all[i];
        // §7 caps the dictionary because a code is ceil(log2(dictCount)) bits; past the cap there is
        // no dictionary layout to fall back to, only the dictionary-free one.
        return distinct > MAX_DICTIONARY_ENTRIES ? null : Arrays.copyOf(all, distinct);
    }

    private static int preambleLength(byte[][] dictionary)
    {
        long bytes = PREAMBLE_HEADER_BYTES + dictionaryBytes(dictionary);
        return (int) BitPacking.roundUpToMultipleOf8(bytes);
    }

    private static long dictionaryBytes(byte[][] dictionary)
    {
        if (dictionary == null)
            return 0;
        long bytes = 0;
        for (byte[] entry : dictionary)
            bytes += BlockPresence.varIntLength(entry.length) + (long) entry.length;
        return bytes;
    }

    private static void writePreamble(ByteBuffer dst, byte[][] dictionary, int preambleLength)
    {
        int base = dst.position();
        long entryBytes = dictionaryBytes(dictionary);
        if (entryBytes > Integer.MAX_VALUE)
            throw new IllegalArgumentException("dictionary of " + entryBytes + " bytes overflows int");
        dst.putShort((short) (dictionary == null ? 0 : dictionary.length));
        dst.putShort((short) 0);
        dst.putInt((int) entryBytes);
        if (dictionary != null)
        {
            for (int i = 0; i < dictionary.length; i++)
            {
                if (i > 0 && compareUnsigned(dictionary[i - 1], dictionary[i]) >= 0)
                    throw new IllegalArgumentException("dictionary entries out of ascending unsigned byte order at " +
                                                       i + " (v4 §5 rule 2)");
                BlockPresence.writeVarInt(dst, dictionary[i].length);
                dst.put(dictionary[i]);
            }
        }
        // §5 rule 5: written, not skipped, because this is where a reused scratch buffer leaks.
        while (dst.position() - base < preambleLength)
            dst.put((byte) 0);
    }

    private static byte[][] readPreamble(ByteBuffer src, int sectionLen)
    {
        int base = src.position();
        if (src.remaining() < PREAMBLE_HEADER_BYTES)
            throw new IllegalArgumentException("Corrupt v4 chunk: truncated section preamble");
        int dictCount = src.getShort() & 0xFFFF;
        int pad = src.getShort() & 0xFFFF;
        long dictBytesLen = src.getInt() & 0xFFFFFFFFL;
        if (pad != 0)
            throw new IllegalArgumentException("Corrupt v4 chunk: non-zero preamble padding " + pad +
                                               " (v4 §5 rule 5)");
        // Bounded against the section before anything is sized from it: an entry costs at least one
        // varint byte, so dictCount can never exceed the bytes the preamble claims.
        if (dictBytesLen > src.remaining() || dictCount > dictBytesLen)
            throw new IllegalArgumentException("Corrupt v4 chunk: preamble claims " + dictCount + " entries in " +
                                               dictBytesLen + " bytes, section holds " + src.remaining());
        if (dictCount == 0)
        {
            if (dictBytesLen != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: empty dictionary with " + dictBytesLen +
                                                   " bytes of entries");
            checkPreamblePadding(src, base, PREAMBLE_HEADER_BYTES);
            return null;
        }

        byte[][] dictionary = new byte[dictCount][];
        int entriesFrom = src.position();
        for (int i = 0; i < dictCount; i++)
        {
            long length = BlockPresence.readVarInt(src);
            if (length > src.remaining())
                throw new IllegalArgumentException("Corrupt v4 chunk: dictionary entry " + i + " claims " + length +
                                                   " bytes, section holds " + src.remaining());
            byte[] entry = new byte[(int) length];
            src.get(entry);
            // Strictly ascending catches an out-of-order dictionary and a duplicate in one test, and
            // both matter: a duplicate would give one value two codes and therefore one section two
            // byte forms, and an unordered dictionary breaks the order-preserving code property.
            if (i > 0 && compareUnsigned(dictionary[i - 1], entry) >= 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: dictionary entries out of ascending unsigned " +
                                                   "byte order at " + i + " (v4 §5 rule 2)");
            dictionary[i] = entry;
        }
        if (src.position() - entriesFrom != dictBytesLen)
            throw new IllegalArgumentException("Corrupt v4 chunk: dictBytesLen " + dictBytesLen + " but " + dictCount +
                                               " entries occupy " + (src.position() - entriesFrom));
        int consumed = src.position() - base;
        int padded = (int) BitPacking.roundUpToMultipleOf8(consumed);
        if (padded > sectionLen)
            throw new IllegalArgumentException("Corrupt v4 chunk: preamble of " + padded + " bytes passes sectionLen " +
                                               sectionLen);
        checkPreamblePadding(src, base, padded);
        return dictionary;
    }

    private static void checkPreamblePadding(ByteBuffer src, int base, int padded)
    {
        for (int i = src.position() - base; i < padded; i++)
            if (src.get() != 0)
                throw new IllegalArgumentException("Corrupt v4 chunk: non-zero preamble padding at byte " + i +
                                                   " (v4 §5 rule 5)");
    }

    // -----------------------------------------------------------------------------------------
    // planning
    // -----------------------------------------------------------------------------------------

    /** One block's presence, statistics and encoding choice, before any byte exists. */
    private static final class BlockPlan
    {
        final int start;
        final int blockRows;
        long[] words;
        int presenceMode;
        int presenceLength;
        int presentCount;
        long min;
        long max;
        int extraFlags;
        final BlockEncodings.Choice choice = new BlockEncodings.Choice();
        int bodyOffset;

        BlockPlan(int rowCount, int blockSizeLog2, int block)
        {
            this.start = block << blockSizeLog2;
            this.blockRows = ChunkV4Header.blockRows(rowCount, blockSizeLog2, block);
        }

        void gather(boolean[] present)
        {
            words = new long[BlockPresence.wordCount(blockRows)];
            if (present == null)
                for (int i = 0; i < blockRows; i++)
                    words[i >>> 6] |= 1L << (i & 63);
            else
                for (int i = 0; i < blockRows; i++)
                    if (present[start + i])
                        words[i >>> 6] |= 1L << (i & 63);
            finishPresence();
        }

        void gatherVariablePresence(byte[][] values)
        {
            words = new long[BlockPresence.wordCount(blockRows)];
            for (int i = 0; i < blockRows; i++)
                if (values[start + i] != null)
                    words[i >>> 6] |= 1L << (i & 63);
            finishPresence();
        }

        private void finishPresence()
        {
            presenceMode = BlockPresence.chooseMode(words, blockRows);
            presenceLength = BlockPresence.encodedLength(presenceMode, words, blockRows);
        }

        int bodyLength()
        {
            return presenceLength + choice.payloadLength;
        }
    }

    /** Body offsets, the block table, and the section length they imply. */
    private static final class Layout
    {
        final int statWidth;
        final int preambleLength;
        final BlockPlan[] plans;
        final List<ChunkV4BlockTable.Entry> entries;
        final int sectionLen;

        Layout(int typeCode, int preambleLength, BlockPlan[] plans)
        {
            this.statWidth = ChunkV4Directory.statWidth(typeCode);
            this.preambleLength = preambleLength;
            this.plans = plans;
            this.entries = new ArrayList<>(plans.length);
            long offset = preambleLength + (long) ChunkV4BlockTable.tableLength(plans.length, statWidth);
            for (BlockPlan plan : plans)
            {
                if (offset > Integer.MAX_VALUE)
                    throw new IllegalArgumentException("section overflows int at block offset " + offset);
                plan.bodyOffset = (int) offset;
                int flags = plan.presenceMode | plan.extraFlags
                            | (plan.choice.hasExceptions() ? ChunkV4BlockTable.FLAG_HAS_EXCEPTIONS : 0);
                entries.add(new ChunkV4BlockTable.Entry(statWidth,
                                                        BlockEncodings.toStatBits(plan.min, typeCode),
                                                        BlockEncodings.toStatBits(plan.max, typeCode),
                                                        plan.bodyOffset, plan.choice.encoding, flags));
                offset += plan.bodyLength();
            }
            if (offset > Integer.MAX_VALUE)
                throw new IllegalArgumentException("section of " + offset + " bytes overflows int");
            this.sectionLen = (int) offset;
        }

        ByteBuffer allocate()
        {
            return ByteBuffer.wrap(new byte[sectionLen]).order(ByteOrder.BIG_ENDIAN);
        }

        Encoded finish(ByteBuffer dst, int dictionaryCount)
        {
            if (dst.position() != sectionLen)
                throw new IllegalArgumentException("wrote " + dst.position() + " bytes, planned " + sectionLen);
            return new Encoded(dst.array(), entries, preambleLength, dictionaryCount);
        }
    }

    private static void fixedStats(int typeCode, long[] values, int count, BlockPlan plan)
    {
        if (count == 0)
        {
            // §5 rule 6: a block with no values has no extrema, and leaving the previous block's is
            // both meaningless and byte-nondeterministic in a field no round-trip test reads.
            plan.min = 0;
            plan.max = 0;
            return;
        }
        if (typeCode == ChunkV4Directory.TYPE_BOOLEAN)
        {
            // §5 gives boolean blocks presence flags instead of extrema, because BooleanType
            // normalises any non-zero byte to true and so has no byte order to record.
            for (int i = 0; i < count; i++)
                plan.extraFlags |= values[i] == 0 ? ChunkV4BlockTable.FLAG_HAS_FALSE
                                                  : ChunkV4BlockTable.FLAG_HAS_TRUE;
            plan.min = 0;
            plan.max = 0;
            return;
        }
        long min = values[0];
        long max = values[0];
        for (int i = 1; i < count; i++)
        {
            if (BlockEncodings.compareValues(values[i], min, typeCode) < 0)
                min = values[i];
            if (BlockEncodings.compareValues(values[i], max, typeCode) > 0)
                max = values[i];
        }
        plan.min = min;
        plan.max = max;
    }

    // -----------------------------------------------------------------------------------------
    // argument checks
    // -----------------------------------------------------------------------------------------

    private static void checkShape(int typeCode, int rowCount, int blockSizeLog2, boolean fixed)
    {
        if (BlockEncodings.isFixedWidth(typeCode) != fixed)
            throw new IllegalArgumentException("type code " + typeCode + " is not " +
                                               (fixed ? "fixed" : "variable") + " width");
        if (rowCount < 1)
            throw new IllegalArgumentException("a section covers at least one row, got " + rowCount);
        ChunkV4Header.blockCount(rowCount, blockSizeLog2);   // validates rowCount and blockSizeLog2
    }

    private static int countPresent(boolean[] present, int rowCount)
    {
        if (present == null)
            return rowCount;
        int count = 0;
        for (int i = 0; i < rowCount; i++)
            if (present[i])
                count++;
        return count;
    }

    /** §2's two O(1) columns, and exactly the two {@link ChunkV4Directory.Entry} wants 0 bytes for. */
    private static boolean isZeroByteSection(int presentRows, int rowCount, byte[] columnConstant)
    {
        return presentRows == 0 || (columnConstant != null && presentRows == rowCount);
    }
}
