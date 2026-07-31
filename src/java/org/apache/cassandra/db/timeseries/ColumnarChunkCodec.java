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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Chunk codec version 3: a columnar format storing many named columns per chunk against one
 * shared timestamp axis, instead of the one-column-per-chunk layout of {@link Chimp128Codec}
 * (version 2). Column values are grouped by column (not interleaved by row) so a reader projecting
 * a subset of columns can skip the rest without decoding them -- every column's data section
 * records its own byte length in the directory for exactly this purpose.
 * <p>
 * Per-column encoding is picked to make constant and all-null columns cost O(1) bytes regardless
 * of row count: a column whose present values are all identical stores the value once in the
 * directory (0-byte data section); a column with no present values at all stores nothing. Double
 * columns use {@link Chimp128Codec}'s value-only stream; text/opaque columns dictionary-encode
 * when at most 256 distinct present values occur, else fall back to length-prefixed raw bytes.
 * <p>
 * Layout: {@code HEADER_SIZE}-byte header (version, row count, first/last timestamp, column
 * count, directory size) -- column directory (one entry per column, sorted by name) -- null
 * presence bitmaps (RLE, skipped for columns that are all-present or all-null) -- timestamps
 * (first value raw, then a {@link TimestampCodec#writeDod}/{@link TimestampCodec#readDod}
 * delta-of-delta bitstream) -- column data sections, one per column, each exactly the
 * {@code sectionLen} bytes recorded for it in the directory.
 * <p>
 * Corruption surfaces as {@link IllegalArgumentException} (stricter than version 2, which may
 * also throw {@link IndexOutOfBoundsException} or {@link java.nio.BufferUnderflowException}):
 * every parsing path here is wrapped so truncated/malformed payloads are reported uniformly.
 * Buffers are read big-endian and never mutated.
 */
public final class ColumnarChunkCodec
{
    public static final byte VERSION = 3;
    public static final int HEADER_SIZE = 25;

    /**
     * Type code {@code 0x00} was DOUBLE_GORILLA, dropped when chimp128 became the only double
     * codec. It is permanently reserved -- never reassigned to another type -- and rejected on
     * read by {@link #readColumnMeta} with a message naming it, so a v3 chunk written by the
     * pre-removal code fails loudly instead of being decoded as something it is not.
     */
    static final byte TYPE_DOUBLE_GORILLA_REMOVED = 0x00;

    public static final byte TYPE_DOUBLE_CHIMP = 0x01;
    public static final byte TYPE_BOOLEAN = 0x02;
    public static final byte TYPE_INT32 = 0x03;
    public static final byte TYPE_INT64 = 0x04;
    public static final byte TYPE_TEXT = 0x05;
    public static final byte TYPE_OPAQUE = 0x06;

    private static final byte FLAG_ALL_PRESENT = 0x01;
    private static final byte FLAG_ALL_NULL = 0x02;
    private static final byte FLAG_CONSTANT = 0x04;

    private static final int MODE_DICTIONARY = 0;
    private static final int MODE_RAW = 1;
    private static final int MAX_DICTIONARY_SIZE = 256;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private ColumnarChunkCodec()
    {
    }

    /**
     * One column's input to {@link #encode}: its type and its values, in row order (null = absent).
     * The type code is authoritative -- it is the code written to the directory verbatim, so it must
     * be one of the {@code TYPE_*} constants above.
     */
    public static final class ColumnInput
    {
        public final byte typeCode;
        public final ByteBuffer[] values;

        public ColumnInput(byte typeCode, ByteBuffer[] values)
        {
            this.typeCode = typeCode;
            this.values = values;
        }
    }

    public static ByteBuffer encode(long[] timestamps, int count, SortedMap<String, ColumnInput> columns)
    {
        if (count < 1)
            throw new IllegalArgumentException("count must be >= 1, got " + count);
        if (timestamps.length < count)
            throw new IllegalArgumentException("timestamps array shorter than count " + count);
        for (int i = 1; i < count; i++)
            if (timestamps[i] <= timestamps[i - 1])
                throw new IllegalArgumentException("timestamps must be strictly increasing: " +
                                                   timestamps[i] + " after " + timestamps[i - 1]);

        // Re-sort by natural String order regardless of the caller's SortedMap comparator, so the
        // directory's determinism does not depend on how the caller happened to build the map.
        // NOTE: this must be `new TreeMap<>()` + putAll(), not `new TreeMap<>(columns)` -- per
        // TreeMap(SortedMap), that constructor ADOPTS the source map's comparator (or its absence)
        // instead of forcing natural order, which would silently defeat this whole re-sort.
        SortedMap<String, ColumnInput> sorted = new TreeMap<>();
        sorted.putAll(columns);
        if (sorted.size() > 0xFFFF)
            throw new IllegalArgumentException("too many columns: " + sorted.size());

        ByteArrayOutputStream directory = new ByteArrayOutputStream();
        ByteArrayOutputStream nullBitmaps = new ByteArrayOutputStream();
        List<byte[]> dataSections = new ArrayList<>(sorted.size());

        for (Map.Entry<String, ColumnInput> column : sorted.entrySet())
            encodeColumn(column.getKey(), column.getValue(), count, directory, nullBitmaps, dataSections);

        BitWriter timestampBits = new BitWriter();
        long previousTimestamp = timestamps[0];
        long previousDelta = 0;
        for (int i = 1; i < count; i++)
        {
            long delta = timestamps[i] - previousTimestamp;
            TimestampCodec.writeDod(timestampBits, delta - previousDelta);
            previousTimestamp = timestamps[i];
            previousDelta = delta;
        }

        int dirSize = directory.size();
        if (dirSize > 0xFFFF)
            throw new IllegalArgumentException("column directory too large: " + dirSize + " bytes");

        int dataSize = 0;
        for (byte[] section : dataSections)
            dataSize += section.length;

        int totalSize = HEADER_SIZE + dirSize + nullBitmaps.size() +
                         Long.BYTES + timestampBits.sizeInBytes() + dataSize;

        ByteBuffer out = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
        out.put(VERSION);
        out.putInt(count);
        out.putLong(timestamps[0]);
        out.putLong(timestamps[count - 1]);
        out.putShort((short) sorted.size());
        out.putShort((short) dirSize);
        out.put(directory.toByteArray());
        out.put(nullBitmaps.toByteArray());
        out.putLong(timestamps[0]);
        timestampBits.writeTo(out);
        for (byte[] section : dataSections)
            out.put(section);
        out.flip();
        return out;
    }

    private static void encodeColumn(String name, ColumnInput input, int count, ByteArrayOutputStream directory,
                                      ByteArrayOutputStream nullBitmaps, List<byte[]> dataSections)
    {
        if (input.typeCode < TYPE_DOUBLE_CHIMP || input.typeCode > TYPE_OPAQUE)
            throw new IllegalArgumentException("column " + name + ": unknown type code " + input.typeCode);
        if (input.values.length < count)
            throw new IllegalArgumentException("column " + name + ": values array shorter than count " + count);

        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > 0xFF)
            throw new IllegalArgumentException("column name too long: " + name);

        boolean[] presence = new boolean[count];
        int presentCount = 0;
        for (int i = 0; i < count; i++)
        {
            if (input.values[i] != null)
            {
                presence[i] = true;
                presentCount++;
            }
        }

        boolean allPresent = presentCount == count;
        boolean allNull = presentCount == 0;
        byte flags = allPresent ? FLAG_ALL_PRESENT : 0;
        byte[] sectionBytes = EMPTY_BYTES;
        byte[] constBytes = null;

        if (allNull)
        {
            flags |= FLAG_ALL_NULL;
        }
        else
        {
            byte[][] presentBytes = new byte[presentCount][];
            int p = 0;
            for (int i = 0; i < count; i++)
            {
                if (presence[i])
                {
                    ByteBuffer value = input.values[i];
                    byte[] bytes = new byte[value.remaining()];
                    value.duplicate().get(bytes);
                    presentBytes[p++] = bytes;
                }
            }

            boolean constant = true;
            for (int i = 1; i < presentCount; i++)
            {
                if (!Arrays.equals(presentBytes[0], presentBytes[i]))
                {
                    constant = false;
                    break;
                }
            }

            if (constant)
            {
                flags |= FLAG_CONSTANT;
                constBytes = presentBytes[0];
            }
            else
            {
                sectionBytes = encodeSection(name, input.typeCode, presentBytes, presentCount);
            }
        }

        directory.write(input.typeCode);
        directory.write(flags);
        directory.write(nameBytes.length);
        directory.write(nameBytes, 0, nameBytes.length);
        writeVarLong(directory, sectionBytes.length);
        if ((flags & FLAG_CONSTANT) != 0)
        {
            writeVarLong(directory, constBytes.length);
            directory.write(constBytes, 0, constBytes.length);
        }

        if (!allPresent && !allNull)
            nullBitmaps.writeBytes(encodeNullBitmap(presence, count));

        dataSections.add(sectionBytes);
    }

    private static byte[] encodeSection(String name, byte typeCode, byte[][] presentBytes, int n)
    {
        switch (typeCode)
        {
            case TYPE_DOUBLE_CHIMP:
            {
                double[] values = new double[n];
                for (int i = 0; i < n; i++)
                    values[i] = ByteBuffer.wrap(presentBytes[i]).getDouble();
                return encodeDoubleSection(values, n);
            }
            case TYPE_BOOLEAN:
            {
                boolean[] values = new boolean[n];
                for (int i = 0; i < n; i++)
                    values[i] = presentBytes[i][0] != 0;
                return encodeBooleanSection(values, n);
            }
            case TYPE_INT32:
            {
                long[] values = new long[n];
                for (int i = 0; i < n; i++)
                    values[i] = ByteBuffer.wrap(presentBytes[i]).getInt();
                return encodeIntSection(values, n, 4);
            }
            case TYPE_INT64:
            {
                long[] values = new long[n];
                for (int i = 0; i < n; i++)
                    values[i] = ByteBuffer.wrap(presentBytes[i]).getLong();
                return encodeIntSection(values, n, 8);
            }
            case TYPE_TEXT:
            case TYPE_OPAQUE:
                return encodeDictOrRaw(presentBytes, n);
            default:
                throw new IllegalArgumentException("column " + name + ": unknown type code " + typeCode);
        }
    }

    private static byte[] encodeDoubleSection(double[] values, int n)
    {
        BitWriter chimp = new BitWriter();
        Chimp128Codec.encodeValues(chimp, values, n);
        ByteBuffer buffer = ByteBuffer.allocate(chimp.sizeInBytes());
        chimp.writeTo(buffer);
        return buffer.array();
    }

    private static byte[] encodeBooleanSection(boolean[] values, int n)
    {
        BitWriter bits = new BitWriter();
        for (int i = 0; i < n; i++)
            bits.writeBit(values[i]);
        ByteBuffer buffer = ByteBuffer.allocate(bits.sizeInBytes());
        bits.writeTo(buffer);
        return buffer.array();
    }

    private static byte[] encodeIntSection(long[] values, int n, int width)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeFixed(out, values[0], width);
        long previous = values[0];
        for (int i = 1; i < n; i++)
        {
            writeVarLong(out, zigzagEncode(values[i] - previous));
            previous = values[i];
        }
        return out.toByteArray();
    }

    private static byte[] encodeDictOrRaw(byte[][] presentBytes, int n)
    {
        TreeSet<ByteBuffer> distinct = new TreeSet<>();
        for (byte[] bytes : presentBytes)
            distinct.add(ByteBuffer.wrap(bytes));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (distinct.size() <= MAX_DICTIONARY_SIZE)
        {
            out.write(MODE_DICTIONARY);
            List<ByteBuffer> dictionary = new ArrayList<>(distinct);   // TreeSet iterates in sorted order
            Map<ByteBuffer, Integer> codeOf = new HashMap<>();
            writeVarLong(out, dictionary.size());
            for (int i = 0; i < dictionary.size(); i++)
            {
                ByteBuffer entry = dictionary.get(i);
                codeOf.put(entry, i);
                byte[] bytes = entry.array();
                writeVarLong(out, bytes.length);
                out.write(bytes, 0, bytes.length);
            }
            for (byte[] bytes : presentBytes)
                out.write(codeOf.get(ByteBuffer.wrap(bytes)));
        }
        else
        {
            out.write(MODE_RAW);
            for (byte[] bytes : presentBytes)
            {
                writeVarLong(out, bytes.length);
                out.write(bytes, 0, bytes.length);
            }
        }
        return out.toByteArray();
    }

    private static byte[] encodeNullBitmap(boolean[] presence, int count)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        boolean current = presence[0];
        out.write(current ? 1 : 0);
        int runLength = 0;
        for (int i = 0; i < count; i++)
        {
            if (presence[i] == current)
            {
                runLength++;
            }
            else
            {
                writeVarLong(out, runLength);
                current = !current;
                runLength = 1;
            }
        }
        writeVarLong(out, runLength);
        return out.toByteArray();
    }

    private static void writeFixed(ByteArrayOutputStream out, long value, int width)
    {
        for (int shift = (width - 1) * 8; shift >= 0; shift -= 8)
            out.write((int) (value >>> shift) & 0xFF);
    }

    private static void writeVarLong(ByteArrayOutputStream out, long value)
    {
        while (true)
        {
            int low7 = (int) (value & 0x7F);
            value >>>= 7;
            if (value == 0)
            {
                out.write(low7);
                return;
            }
            out.write(low7 | 0x80);
        }
    }

    private static long zigzagEncode(long value)
    {
        return (value << 1) ^ (value >> 63);
    }

    private static long zigzagDecode(long value)
    {
        return (value >>> 1) ^ -(value & 1);
    }

    public static ColumnarCursor cursor(ByteBuffer payload, Set<String> projection)
    {
        try
        {
            return buildCursor(payload, projection);
        }
        catch (IllegalArgumentException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException("Corrupt columnar chunk: " + e, e);
        }
    }

    private static ColumnarCursor buildCursor(ByteBuffer payload, Set<String> projection)
    {
        int payloadRemaining = payload.remaining();
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get();
        if (version != VERSION)
            throw new IllegalArgumentException("Unsupported columnar chunk version: " + version);
        int rowCount = buffer.getInt();
        // Even in the cheapest legitimate encoding (all-zero delta-of-delta, 1 bit/row), rowCount
        // rows need at least rowCount/8 bytes of timestamp bitstream alone -- a rowCount that could
        // not possibly fit in a payload this size is corrupt, and must be rejected before it is
        // used to size any allocation (new long[rowCount], new boolean[rowCount], ...): otherwise a
        // single corrupted header field turns into an OutOfMemoryError instead of a clean reject.
        if (rowCount < 1 || rowCount > 8L * payloadRemaining)
            throw new IllegalArgumentException("Corrupt columnar chunk: rowCount " + rowCount +
                                               " is not plausible for a " + payloadRemaining + "-byte payload");
        long firstTimestamp = buffer.getLong();
        buffer.getLong();   // last timestamp: header-only metadata, re-derived below from the DoD stream
        int columnCount = buffer.getShort() & 0xFFFF;
        int dirSize = buffer.getShort() & 0xFFFF;

        int dirStart = buffer.position();
        int dirEnd = dirStart + dirSize;
        List<ColumnMeta> metas = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++)
            metas.add(readColumnMeta(buffer, dirEnd));
        int dirConsumed = buffer.position() - dirStart;
        if (dirConsumed != dirSize)
            throw new IllegalArgumentException("Corrupt columnar chunk: directory size mismatch (header says " +
                                               dirSize + ", parsed " + dirConsumed + ")");

        for (ColumnMeta meta : metas)
            if ((meta.flags & (FLAG_ALL_PRESENT | FLAG_ALL_NULL)) == 0)
                meta.presence = decodeNullBitmap(buffer, rowCount);

        long timestampsFirst = buffer.getLong();
        if (timestampsFirst != firstTimestamp)
            throw new IllegalArgumentException("Corrupt columnar chunk: timestamp section first-value mismatch");
        long[] timestamps = new long[rowCount];
        timestamps[0] = firstTimestamp;
        BitReader timestampBits = new BitReader(buffer);
        long delta = 0;
        long previous = firstTimestamp;
        for (int i = 1; i < rowCount; i++)
        {
            delta += TimestampCodec.readDod(timestampBits);
            previous += delta;
            timestamps[i] = previous;
        }
        int timestampSectionBytes = (timestampBits.bitPosition() + 7) >>> 3;
        int dataStart = buffer.position() + timestampSectionBytes;

        int[] offsets = new int[columnCount];
        int running = dataStart;
        for (int i = 0; i < columnCount; i++)
        {
            offsets[i] = running;
            running += metas.get(i).sectionLen;
        }
        if (running > buffer.limit())
            throw new IllegalArgumentException("Corrupt columnar chunk: data sections overrun payload (need " +
                                               running + " bytes, have " + buffer.limit() + ")");

        Set<String> effective = new LinkedHashSet<>();
        for (ColumnMeta meta : metas)
            if (projection == null || projection.contains(meta.name))
                effective.add(meta.name);
        Set<String> columnsView = Collections.unmodifiableSet(effective);

        Map<String, ByteBuffer[]> decoded = new HashMap<>();
        for (int ci = 0; ci < columnCount; ci++)
        {
            ColumnMeta meta = metas.get(ci);
            if (!effective.contains(meta.name))
                continue;   // not projected: never touch this column's data section bytes
            decoded.put(meta.name, decodeColumn(buffer, offsets[ci], meta, rowCount));
        }

        return new ColumnarCursorImpl(rowCount, timestamps, columnsView, decoded);
    }

    private static ColumnMeta readColumnMeta(ByteBuffer buffer, int dirEnd)
    {
        byte typeCode = buffer.get();
        // Validated here rather than at the point of use, so it covers constant/all-null columns
        // too -- those never reach decodeColumn's type switch, and a chunk carrying a type code
        // this build cannot honour must be rejected whatever its flags say.
        if (typeCode == TYPE_DOUBLE_GORILLA_REMOVED)
            throw new IllegalArgumentException("Unsupported columnar chunk: double column type code " +
                                               TYPE_DOUBLE_GORILLA_REMOVED + " (gorilla) was removed -- chimp128 " +
                                               "(type code " + TYPE_DOUBLE_CHIMP + ") is the only double codec");
        if (typeCode < TYPE_DOUBLE_CHIMP || typeCode > TYPE_OPAQUE)
            throw new IllegalArgumentException("Corrupt columnar chunk: unknown column type code " + typeCode);
        byte flags = buffer.get();
        int nameLen = buffer.get() & 0xFF;
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        String name = new String(nameBytes, StandardCharsets.UTF_8);
        int sectionLen = (int) readVarLong(buffer);
        byte[] constBytes = null;
        if ((flags & FLAG_CONSTANT) != 0)
        {
            long constLenRaw = readVarLong(buffer);
            // bound against what is actually left in the DIRECTORY (not the whole remaining
            // payload): constBytes lives in the directory, so the directory's own end is the
            // correct, tighter limit -- bounding against the whole payload would still be memory
            // safe (the dirConsumed != dirSize check downstream catches an overrun either way) but
            // would let an implausible constLen slip past this check when the rest of the payload
            // (timestamps, column data) happens to be large.
            int constLen = checkedLength("constBytes", constLenRaw, dirEnd - buffer.position());
            constBytes = new byte[constLen];
            buffer.get(constBytes);
        }
        return new ColumnMeta(name, typeCode, flags, sectionLen, constBytes);
    }

    /**
     * Validates a length read out of a payload before it is used to size an allocation: a
     * negative length, or one exceeding what is actually left to read in the enclosing buffer or
     * section, is definitely corrupt and must fail fast here rather than let a bogus huge value
     * reach {@code new byte[len]} (or {@code new byte[len][]}) and OOM the node.
     */
    private static int checkedLength(String field, long len, int available)
    {
        if (len < 0 || len > available)
            throw new IllegalArgumentException("Corrupt columnar chunk: " + field + " length " + len +
                                               " invalid (available " + available + ")");
        return (int) len;
    }

    private static ByteBuffer[] decodeColumn(ByteBuffer buffer, int offset, ColumnMeta meta, int rowCount)
    {
        ByteBuffer[] rowValues = new ByteBuffer[rowCount];
        boolean allPresent = (meta.flags & FLAG_ALL_PRESENT) != 0;
        boolean allNull = (meta.flags & FLAG_ALL_NULL) != 0;
        boolean constant = (meta.flags & FLAG_CONSTANT) != 0;

        if (allNull)
            return rowValues;

        if (constant)
        {
            ByteBuffer constValue = ByteBuffer.wrap(meta.constBytes).asReadOnlyBuffer();
            for (int r = 0; r < rowCount; r++)
                if (allPresent || meta.presence[r])
                    rowValues[r] = constValue.duplicate();
            return rowValues;
        }

        ByteBuffer section = buffer.duplicate();
        section.position(offset);
        section.limit(offset + meta.sectionLen);
        int presentCount = allPresent ? rowCount : countPresent(meta.presence);

        switch (meta.typeCode)
        {
            case TYPE_DOUBLE_CHIMP:
            {
                double[] values = decodeDoubleSection(section, presentCount);
                int p = 0;
                for (int r = 0; r < rowCount; r++)
                    if (allPresent || meta.presence[r])
                    {
                        ByteBuffer value = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                        value.putDouble(values[p++]);
                        rowValues[r] = value.flip();
                    }
                break;
            }
            case TYPE_BOOLEAN:
            {
                boolean[] values = decodeBooleanSection(section, presentCount);
                int p = 0;
                for (int r = 0; r < rowCount; r++)
                    if (allPresent || meta.presence[r])
                    {
                        ByteBuffer value = ByteBuffer.allocate(1);
                        value.put((byte) (values[p++] ? 1 : 0));
                        rowValues[r] = value.flip();
                    }
                break;
            }
            case TYPE_INT32:
            {
                long[] values = decodeIntSection(section, presentCount, 4);
                int p = 0;
                for (int r = 0; r < rowCount; r++)
                    if (allPresent || meta.presence[r])
                    {
                        ByteBuffer value = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
                        value.putInt((int) values[p++]);
                        rowValues[r] = value.flip();
                    }
                break;
            }
            case TYPE_INT64:
            {
                long[] values = decodeIntSection(section, presentCount, 8);
                int p = 0;
                for (int r = 0; r < rowCount; r++)
                    if (allPresent || meta.presence[r])
                    {
                        ByteBuffer value = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                        value.putLong(values[p++]);
                        rowValues[r] = value.flip();
                    }
                break;
            }
            case TYPE_TEXT:
            case TYPE_OPAQUE:
            {
                byte[][] values = decodeDictOrRaw(section, presentCount);
                int p = 0;
                for (int r = 0; r < rowCount; r++)
                    if (allPresent || meta.presence[r])
                        rowValues[r] = ByteBuffer.wrap(values[p++]).asReadOnlyBuffer();
                break;
            }
            default:
                throw new IllegalArgumentException("Corrupt columnar chunk: unknown type code " +
                                                   meta.typeCode + " for column " + meta.name);
        }
        return rowValues;
    }

    private static int countPresent(boolean[] presence)
    {
        int n = 0;
        for (boolean b : presence)
            if (b)
                n++;
        return n;
    }

    private static double[] decodeDoubleSection(ByteBuffer section, int n)
    {
        BitReader bits = new BitReader(section);
        double[] out = new double[n];
        Chimp128Codec.ValueDecoder decoder = new Chimp128Codec.ValueDecoder();
        for (int i = 0; i < n; i++)
            out[i] = Double.longBitsToDouble(decoder.readBits(bits));
        return out;
    }

    private static boolean[] decodeBooleanSection(ByteBuffer section, int n)
    {
        BitReader bits = new BitReader(section);
        boolean[] out = new boolean[n];
        for (int i = 0; i < n; i++)
            out[i] = bits.readBit();
        return out;
    }

    private static long[] decodeIntSection(ByteBuffer section, int n, int width)
    {
        long[] out = new long[n];
        out[0] = readFixed(section, width);
        long previous = out[0];
        for (int i = 1; i < n; i++)
        {
            previous += zigzagDecode(readVarLong(section));
            out[i] = previous;
        }
        return out;
    }

    private static long readFixed(ByteBuffer buffer, int width)
    {
        long value = 0;
        for (int i = 0; i < width; i++)
            value = (value << 8) | (buffer.get() & 0xFFL);
        if (width == 4)
            value = (int) value;   // sign-extend the 32-bit pattern back to a signed long
        return value;
    }

    private static byte[][] decodeDictOrRaw(ByteBuffer section, int n)
    {
        byte[][] out = new byte[n][];
        int mode = section.get() & 0xFF;
        if (mode == MODE_DICTIONARY)
        {
            long dictCountLong = readVarLong(section);
            // the write side never emits more than MAX_DICTIONARY_SIZE entries (see
            // encodeDictOrRaw); a larger value here is definitionally corrupt, and rejecting it
            // up front also keeps the `new byte[dictCount][]` below cheap regardless of payload size
            if (dictCountLong < 0 || dictCountLong > MAX_DICTIONARY_SIZE)
                throw new IllegalArgumentException("Corrupt columnar chunk: dictionary size " + dictCountLong +
                                                   " outside [0, " + MAX_DICTIONARY_SIZE + "]");
            int dictCount = (int) dictCountLong;
            byte[][] dictionary = new byte[dictCount][];
            for (int i = 0; i < dictCount; i++)
            {
                int len = checkedLength("dictionary entry", readVarLong(section), section.remaining());
                byte[] bytes = new byte[len];
                section.get(bytes);
                dictionary[i] = bytes;
            }
            for (int i = 0; i < n; i++)
            {
                int code = section.get() & 0xFF;
                if (code >= dictCount)
                    throw new IllegalArgumentException("Corrupt columnar chunk: dictionary code " + code +
                                                       " >= dictionary size " + dictCount);
                out[i] = dictionary[code];
            }
        }
        else if (mode == MODE_RAW)
        {
            for (int i = 0; i < n; i++)
            {
                int len = checkedLength("text/opaque value", readVarLong(section), section.remaining());
                byte[] bytes = new byte[len];
                section.get(bytes);
                out[i] = bytes;
            }
        }
        else
        {
            throw new IllegalArgumentException("Corrupt columnar chunk: unknown text/opaque section mode " + mode);
        }
        return out;
    }

    private static boolean[] decodeNullBitmap(ByteBuffer buffer, int count)
    {
        boolean current = buffer.get() != 0;
        boolean[] presence = new boolean[count];
        int filled = 0;
        while (filled < count)
        {
            long run = readVarLong(buffer);
            if (run < 0 || filled + run > count)
                throw new IllegalArgumentException("Corrupt columnar chunk: null bitmap run " + run +
                                                   " overflows row count " + count);
            Arrays.fill(presence, filled, filled + (int) run, current);
            filled += (int) run;
            current = !current;
        }
        return presence;
    }

    private static long readVarLong(ByteBuffer buffer)
    {
        long result = 0;
        int shift = 0;
        while (true)
        {
            byte b = buffer.get();
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0)
                return result;
            shift += 7;
            if (shift > 63)
                throw new IllegalArgumentException("Corrupt columnar chunk: varint exceeds 64 bits");
        }
    }

    public static int rowCount(ByteBuffer payload)
    {
        try
        {
            ByteBuffer buffer = checkedHeader(payload);
            return buffer.getInt(buffer.position() + 1);
        }
        catch (IllegalArgumentException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException("Corrupt columnar chunk: truncated header", e);
        }
    }

    public static long firstTimestamp(ByteBuffer payload)
    {
        try
        {
            ByteBuffer buffer = checkedHeader(payload);
            return buffer.getLong(buffer.position() + 5);
        }
        catch (IllegalArgumentException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException("Corrupt columnar chunk: truncated header", e);
        }
    }

    public static long lastTimestamp(ByteBuffer payload)
    {
        try
        {
            ByteBuffer buffer = checkedHeader(payload);
            return buffer.getLong(buffer.position() + 13);
        }
        catch (IllegalArgumentException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException("Corrupt columnar chunk: truncated header", e);
        }
    }

    /**
     * Duplicates {@code payload}, pins the duplicate's byte order to big-endian, and verifies the
     * version byte. Bounds/format problems surfacing from here (an {@link IndexOutOfBoundsException}
     * from a too-short buffer, for instance) are caught and rewrapped by each of this method's three
     * callers above, not here -- {@link #rowCount}/{@link #firstTimestamp}/{@link #lastTimestamp}
     * each read further into the header after this call returns, so the header peeks need one
     * wrapping layer around their whole body, not just around this version check.
     */
    private static ByteBuffer checkedHeader(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        byte version = buffer.get(buffer.position());
        if (version != VERSION)
            throw new IllegalArgumentException("Unsupported columnar chunk version: " + version);
        return buffer;
    }

    /** Parsed column directory entry, plus its decoded null bitmap (filled in after all entries are read). */
    private static final class ColumnMeta
    {
        final String name;
        final byte typeCode;
        final byte flags;
        final int sectionLen;
        final byte[] constBytes;
        boolean[] presence;

        ColumnMeta(String name, byte typeCode, byte flags, int sectionLen, byte[] constBytes)
        {
            this.name = name;
            this.typeCode = typeCode;
            this.flags = flags;
            this.sectionLen = sectionLen;
            this.constBytes = constBytes;
        }
    }

    private static final class ColumnarCursorImpl implements ColumnarCursor
    {
        private final int rowCount;
        private final long[] timestamps;
        private final Set<String> columns;
        private final Map<String, ByteBuffer[]> decoded;
        private int row = -1;

        ColumnarCursorImpl(int rowCount, long[] timestamps, Set<String> columns, Map<String, ByteBuffer[]> decoded)
        {
            this.rowCount = rowCount;
            this.timestamps = timestamps;
            this.columns = columns;
            this.decoded = decoded;
        }

        @Override
        public boolean advance()
        {
            if (row + 1 >= rowCount)
                return false;
            row++;
            return true;
        }

        @Override
        public long timestamp()
        {
            if (row < 0)
                throw new IllegalStateException("advance() not called");
            return timestamps[row];
        }

        @Override
        public boolean hasColumn(String name)
        {
            return columns.contains(name);
        }

        @Override
        public boolean isNull(String name)
        {
            if (row < 0)
                throw new IllegalStateException("advance() not called");
            ByteBuffer[] values = decoded.get(name);
            return values == null || values[row] == null;
        }

        @Override
        public ByteBuffer getBytes(String name)
        {
            if (row < 0)
                throw new IllegalStateException("advance() not called");
            ByteBuffer[] values = decoded.get(name);
            if (values == null || values[row] == null)
                return null;
            return values[row].duplicate();
        }

        @Override
        public Set<String> columns()
        {
            return columns;
        }
    }
}
