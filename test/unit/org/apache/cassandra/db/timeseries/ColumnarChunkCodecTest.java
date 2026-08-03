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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.FastByteOperations;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the version-3 columnar chunk format: many named columns sharing one timestamp axis,
 * with per-column O(1) encoding for constant/all-null columns and byte-exact projection skip.
 * See {@link ColumnarChunkCodec} for the format and {@link ChunkCodecs} for how it is (and is
 * not) reachable from the single-column dispatcher.
 */
public class ColumnarChunkCodecTest
{
    @Test
    public void roundtripAllTypes()
    {
        int n = 200;
        long[] ts = sequentialTimestamps(n);

        double[] walk = new double[n];
        double w = 50.0;
        Random random = new Random(11);
        for (int i = 0; i < n; i++)
        {
            w += (random.nextInt(3) - 1) * 0.1;
            walk[i] = Math.round(w * 10.0) / 10.0;
        }

        String[] words = { "alpha", "beta", "gamma", "delta" };
        ByteBuffer[] doubleValues = new ByteBuffer[n];
        ByteBuffer[] boolValues = new ByteBuffer[n];
        ByteBuffer[] int32Values = new ByteBuffer[n];
        ByteBuffer[] int64Values = new ByteBuffer[n];
        ByteBuffer[] textValues = new ByteBuffer[n];
        ByteBuffer[] opaqueValues = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            doubleValues[i] = bytesOf(walk[i]);
            boolValues[i] = bytesOf(i % 3 == 0);
            int32Values[i] = bytesOf(i * 7 - 500);
            int64Values[i] = bytesOf((long) i * 1_000_000_007L - 12345L);
            textValues[i] = bytesOf(words[i % words.length]);
            opaqueValues[i] = bytesOf(new byte[]{ (byte) i, (byte) (i >> 8), 0x42 });
        }

        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("c_double", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP, doubleValues));
        columns.put("c_bool", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_BOOLEAN, boolValues));
        columns.put("c_int32", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, int32Values));
        columns.put("c_int64", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT64, int64Values));
        columns.put("c_text", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, textValues));
        columns.put("c_opaque", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_OPAQUE, opaqueValues));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        assertEquals(n, ColumnarChunkCodec.rowCount(payload));
        assertEquals(ts[0], ColumnarChunkCodec.firstTimestamp(payload));
        assertEquals(ts[n - 1], ColumnarChunkCodec.lastTimestamp(payload));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        assertEquals(columns.keySet(), cursor.columns());
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(ts[i], cursor.timestamp());
            assertEquals("row " + i, walk[i], asDouble(cursor.getBytes("c_double")), 0.0);
            assertEquals("row " + i, i % 3 == 0, asBool(cursor.getBytes("c_bool")));
            assertEquals("row " + i, i * 7 - 500, asInt(cursor.getBytes("c_int32")));
            assertEquals("row " + i, (long) i * 1_000_000_007L - 12345L, asLong(cursor.getBytes("c_int64")));
            assertEquals("row " + i, words[i % words.length], asText(cursor.getBytes("c_text")));
            assertArrayEquals("row " + i, new byte[]{ (byte) i, (byte) (i >> 8), 0x42 }, asBytes(cursor.getBytes("c_opaque")));
        }
        assertFalse(cursor.advance());
    }

    @Test
    public void constantColumnCostsConstantBytes()
    {
        int constantSmall = encodeIntColumnSize(100, true);
        int constantLarge = encodeIntColumnSize(10_000, true);
        int variableSmall = encodeIntColumnSize(100, false);
        int variableLarge = encodeIntColumnSize(10_000, false);

        int constantGrowth = constantLarge - constantSmall;
        int variableGrowth = variableLarge - variableSmall;

        // both payloads pay the same (DoD-compressed) timestamp-axis cost for the extra rows; the
        // constant column itself contributes ~0 additional bytes, while the variable column's
        // zigzag-varint deltas cost real bytes per row, so growth must differ by a wide margin
        assertTrue("constant grew " + constantGrowth + ", variable grew " + variableGrowth,
                   constantGrowth < variableGrowth / 4);
        assertTrue("constant payload size grew too much going 100 -> 10000 rows: " +
                   constantSmall + " -> " + constantLarge, constantLarge < constantSmall + 3000);
    }

    private static int encodeIntColumnSize(int n, boolean constant)
    {
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        Random random = new Random(3);
        for (int i = 0; i < n; i++)
            values[i] = bytesOf(constant ? 192 : random.nextInt(1_000_000));
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("quality", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, values));
        return ColumnarChunkCodec.encode(ts, n, columns).remaining();
    }

    @Test
    public void allNullColumn()
    {
        int n = 50;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];   // every entry null
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("maybe_col", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertTrue(cursor.hasColumn("maybe_col"));
            assertTrue(cursor.isNull("maybe_col"));
            assertNull(cursor.getBytes("maybe_col"));
        }
    }

    @Test
    public void mixedNullsRoundtrip()
    {
        int n = 40;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
            values[i] = (i % 4 == 0) ? null : bytesOf(1000 + i);
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("latency", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            if (i % 4 == 0)
            {
                assertTrue("row " + i, cursor.isNull("latency"));
                assertNull("row " + i, cursor.getBytes("latency"));
            }
            else
            {
                assertFalse("row " + i, cursor.isNull("latency"));
                assertEquals("row " + i, 1000 + i, asInt(cursor.getBytes("latency")));
            }
        }
    }

    @Test
    public void textDictionaryVsRawFallbackAcrossThreshold()
    {
        assertTextRoundtrip(200);   // dictionary mode
        assertTextRoundtrip(256);   // dictionary mode, exactly at the threshold
        assertTextRoundtrip(257);   // crosses the threshold -> raw fallback
        assertTextRoundtrip(500);   // comfortably over -> raw fallback
    }

    private static void assertTextRoundtrip(int distinctCount)
    {
        int n = distinctCount;   // one row per distinct value keeps this simple and deterministic
        long[] ts = sequentialTimestamps(n);
        String[] expected = new String[n];
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            expected[i] = "value-" + i;
            values[i] = bytesOf(expected[i]);
        }
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("tag", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertEquals("distinctCount=" + distinctCount + " row=" + i, expected[i], asText(cursor.getBytes("tag")));
        }
    }

    @Test
    public void dictionaryModeIsMuchSmallerThanRawForRepeatedValues()
    {
        int n = 5000;
        long[] ts = sequentialTimestamps(n);

        ByteBuffer[] fewDistinct = new ByteBuffer[n];    // 10 distinct values repeated -> dictionary mode
        ByteBuffer[] manyDistinct = new ByteBuffer[n];   // all unique, well over 256 -> raw mode
        for (int i = 0; i < n; i++)
        {
            fewDistinct[i] = bytesOf("tag-" + (i % 10));
            manyDistinct[i] = bytesOf("unique-value-number-" + i);
        }

        SortedMap<String, ColumnarChunkCodec.ColumnInput> few = new TreeMap<>();
        few.put("tag", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, fewDistinct));
        SortedMap<String, ColumnarChunkCodec.ColumnInput> many = new TreeMap<>();
        many.put("tag", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, manyDistinct));

        int dictSize = ColumnarChunkCodec.encode(ts, n, few).remaining();
        int rawSize = ColumnarChunkCodec.encode(ts, n, many).remaining();

        assertTrue("dictionary encoding should be far smaller: dict=" + dictSize + " raw=" + rawSize,
                   dictSize < rawSize / 5);
    }

    @Test
    public void opaqueRoundtripFrozenMapLikePayload()
    {
        int n = 30;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        byte[][] expected = new byte[n][];
        Random random = new Random(5);
        for (int i = 0; i < n; i++)
        {
            // stand-in for a serialized frozen<map<text,text>> cell: an opaque, variable-length blob
            byte[] blob = new byte[8 + (i % 5) * 3];
            random.nextBytes(blob);
            blob[0] = (byte) i;
            expected[i] = blob;
            values[i] = bytesOf(blob);
        }
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("attribute", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_OPAQUE, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertArrayEquals("row " + i, expected[i], asBytes(cursor.getBytes("attribute")));
        }
    }

    @Test
    public void projectionSkipsUndecodedSectionBytes()
    {
        int n = 500;
        long[] ts = sequentialTimestamps(n);

        ByteBuffer[] keepValues = new ByteBuffer[n];
        ByteBuffer[] skipValues = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            keepValues[i] = bytesOf(i);
            skipValues[i] = bytesOf("skip-me-" + i);   // 500 distinct values -> raw mode, real per-row bytes
        }

        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("aaa_keep", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, keepValues));
        // "zzz_skip" sorts last among the two columns, so its data section is the payload's tail
        columns.put("zzz_skip", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, skipValues));

        ByteBuffer clean = ColumnarChunkCodec.encode(ts, n, columns);

        // sanity: uncorrupted, projecting both columns round-trips both
        Set<String> both = new HashSet<>(Arrays.asList("aaa_keep", "zzz_skip"));
        ColumnarCursor sanity = ColumnarChunkCodec.cursor(clean.duplicate(), both);
        for (int i = 0; i < n; i++)
        {
            assertTrue(sanity.advance());
            assertEquals(i, asInt(sanity.getBytes("aaa_keep")));
            assertEquals("skip-me-" + i, asText(sanity.getBytes("zzz_skip")));
        }

        // corrupt the last 16 bytes of the payload -- guaranteed to fall inside "zzz_skip"'s data
        // section since that column sorts last and its raw-mode section is far larger than 16 bytes
        byte[] rawCopy = new byte[clean.remaining()];
        clean.duplicate().get(rawCopy);
        for (int i = rawCopy.length - 16; i < rawCopy.length; i++)
            rawCopy[i] = (byte) ~rawCopy[i];

        Set<String> projected = Collections.singleton("aaa_keep");
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(ByteBuffer.wrap(rawCopy), projected);
        assertEquals(Collections.singleton("aaa_keep"), cursor.columns());
        assertFalse(cursor.hasColumn("zzz_skip"));
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(ts[i], cursor.timestamp());
            assertEquals(i, asInt(cursor.getBytes("aaa_keep")));
            assertNull(cursor.getBytes("zzz_skip"));   // excluded from the projection regardless of corruption
        }
        assertFalse(cursor.advance());
    }

    @Test
    public void encodeTwiceIsByteIdentical()
    {
        int n = 300;
        long[] ts = sequentialTimestamps(n);
        SortedMap<String, ColumnarChunkCodec.ColumnInput> natural = representativeColumns(n);

        ByteBuffer first = ColumnarChunkCodec.encode(ts, n, natural);
        ByteBuffer second = ColumnarChunkCodec.encode(ts, n, natural);
        assertEquals(first, second);

        // same entries, but built with a reversed comparator and a reversed insertion order, to
        // prove the directory's determinism comes from encode() re-sorting by name itself, not
        // from whatever order/comparator the caller's SortedMap happened to be built with
        SortedMap<String, ColumnarChunkCodec.ColumnInput> reversed = new TreeMap<>(Collections.<String>reverseOrder());
        List<String> keysInReverseInsertOrder = new ArrayList<>(natural.keySet());
        Collections.reverse(keysInReverseInsertOrder);
        for (String key : keysInReverseInsertOrder)
            reversed.put(key, natural.get(key));

        ByteBuffer third = ColumnarChunkCodec.encode(ts, n, reversed);
        assertEquals(first, third);
    }

    /**
     * Every value a cursor hands out must be usable by Cassandra's own comparison machinery. This is
     * not a style preference: {@link FastByteOperations}'s unsafe path branches on
     * {@code hasArray()}, and if that is false it reads the buffer's {@code address} field and
     * dereferences it. A read-only HEAP buffer -- which is what {@code asReadOnlyBuffer()} produces --
     * answers {@code false} to both {@code hasArray()} and {@code isDirect()}, so its address is 0
     * and the comparison segfaults the JVM. The decoder returned exactly such buffers from the
     * CONSTANT and TEXT/OPAQUE paths until it was fixed, and no existing assertion noticed, because
     * reading the bytes back out works fine; only comparing them crashes.
     * <p>
     * So this asserts the comparison RESULT (not merely that nothing throws -- a SIGSEGV would not
     * throw anyway) over one value from each decode path, plus the structural property that makes
     * the fast path safe.
     */
    @Test
    public void decodedValuesSurviveCassandrasComparisonPaths()
    {
        int n = 8;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] constantDouble = new ByteBuffer[n];      // CONSTANT path (directory, 0-byte section)
        ByteBuffer[] varyingDouble = new ByteBuffer[n];       // normal fixed-width data section
        ByteBuffer[] text = new ByteBuffer[n];                // TEXT dictionary path
        ByteBuffer[] opaque = new ByteBuffer[n];              // OPAQUE dictionary path
        ByteBuffer[] ints = new ByteBuffer[n];                // TYPE_INT32 fixed-width path
        ByteBuffer[] longs = new ByteBuffer[n];               // TYPE_INT64 fixed-width path
        ByteBuffer[] bools = new ByteBuffer[n];               // TYPE_BOOLEAN path
        for (int i = 0; i < n; i++)
        {
            constantDouble[i] = bytesOf(1.5);
            varyingDouble[i] = bytesOf(i * 1.25);
            text[i] = bytesOf("label-" + (i % 3));
            opaque[i] = ByteBuffer.wrap(new byte[]{ (byte) i, (byte) (i + 1) });
            ints[i] = bytesOf(100 + i);
            longs[i] = bytesOf(1_000_000_000_000L + i);
            bools[i] = bytesOf(i % 2 == 0);
        }
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("constant", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP, constantDouble));
        columns.put("varying", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP, varyingDouble));
        columns.put("label", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, text));
        columns.put("blobish", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_OPAQUE, opaque));
        columns.put("counter32", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, ints));
        columns.put("counter64", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT64, longs));
        columns.put("flag", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_BOOLEAN, bools));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(ColumnarChunkCodec.encode(ts, n, columns), null);
        assertTrue(cursor.advance());

        assertComparable("constant", cursor.getBytes("constant"), bytesOf(1.5), bytesOf(9.0));
        assertComparable("varying", cursor.getBytes("varying"), bytesOf(0.0), bytesOf(9.0));
        assertComparable("label", cursor.getBytes("label"), bytesOf("label-0"), bytesOf("label-9"));
        assertComparable("blobish", cursor.getBytes("blobish"),
                         ByteBuffer.wrap(new byte[]{ 0, 1 }), ByteBuffer.wrap(new byte[]{ 9, 9 }));
        assertComparable("counter32", cursor.getBytes("counter32"), bytesOf(100), bytesOf(999));
        assertComparable("counter64", cursor.getBytes("counter64"),
                         bytesOf(1_000_000_000_000L), bytesOf(9_000_000_000_000L));
        assertComparable("flag", cursor.getBytes("flag"), bytesOf(true), ByteBuffer.wrap(new byte[]{ 9 }));

        // A second row, so the per-row (non-constant) sections are exercised at an offset too.
        assertTrue(cursor.advance());
        assertComparable("constant", cursor.getBytes("constant"), bytesOf(1.5), bytesOf(9.0));
        assertComparable("varying", cursor.getBytes("varying"), bytesOf(1.25), bytesOf(9.0));
        assertComparable("counter32", cursor.getBytes("counter32"), bytesOf(101), bytesOf(999));
        assertComparable("counter64", cursor.getBytes("counter64"),
                         bytesOf(1_000_000_000_001L), bytesOf(9_000_000_000_000L));
        assertComparable("flag", cursor.getBytes("flag"), bytesOf(false), bytesOf(true));
    }

    /**
     * @param decoded a value straight out of the cursor
     * @param same    a separately-built buffer holding the identical bytes
     * @param greater a buffer that must sort strictly after {@code decoded}
     */
    private static void assertComparable(String column, ByteBuffer decoded, ByteBuffer same, ByteBuffer greater)
    {
        // The structural invariant the fast path depends on. Checked first so a regression names the
        // cause rather than crashing the JVM on the next line.
        assertTrue(column + ": a decoded value must be array-backed for FastByteOperations' fast path",
                   decoded.hasArray());

        // The exact route cell reconciliation takes (Cells.compareValues -> ValueAccessor.compare ->
        // FastByteOperations), asserting the ORDERING, not just the absence of an exception.
        assertEquals(column + ": must compare equal to its own bytes", 0,
                     FastByteOperations.compareUnsigned(decoded, same));
        assertEquals(column + ": must compare equal in the other direction", 0,
                     FastByteOperations.compareUnsigned(same, decoded));
        assertTrue(column + ": must sort before a larger value",
                   FastByteOperations.compareUnsigned(decoded, greater) < 0);
        assertTrue(column + ": a larger value must sort after it",
                   FastByteOperations.compareUnsigned(greater, decoded) > 0);
        // And through the accessor Cells actually uses, so the test tracks reconciliation rather than
        // one utility method.
        assertEquals(column + ": ValueAccessor must agree", 0,
                     ValueAccessor.compare(decoded, ByteBufferAccessor.instance, same, ByteBufferAccessor.instance));
    }

    private static SortedMap<String, ColumnarChunkCodec.ColumnInput> representativeColumns(int n)
    {
        ByteBuffer[] doubleValues = new ByteBuffer[n];
        ByteBuffer[] textValues = new ByteBuffer[n];
        ByteBuffer[] constantValues = new ByteBuffer[n];
        Random random = new Random(42);
        for (int i = 0; i < n; i++)
        {
            doubleValues[i] = (i % 7 == 0) ? null : bytesOf(random.nextDouble() * 100);
            textValues[i] = bytesOf("word-" + (i % 20));
            constantValues[i] = bytesOf(0);
        }
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("readings", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP, doubleValues));
        columns.put("labels", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, textValues));
        columns.put("error_code", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, constantValues));
        return columns;
    }

    @Test
    public void hasColumnFalseForUnknownName()
    {
        int n = 10;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
            values[i] = bytesOf(i);
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("existing_col", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        assertTrue(cursor.hasColumn("existing_col"));
        assertFalse(cursor.hasColumn("added_later_col"));   // simulates an ALTER TABLE ADD after this chunk was written
        assertTrue(cursor.advance());
        assertTrue(cursor.isNull("added_later_col"));
        assertNull(cursor.getBytes("added_later_col"));
    }

    @Test
    public void truncatedPayloadThrowsIllegalArgument()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        for (int cut = 1; cut <= 5; cut++)
        {
            ByteBuffer truncated = ByteBuffer.wrap(bytes, 0, bytes.length - cut);
            assertThatThrownBy(() -> ColumnarChunkCodec.cursor(truncated, null))
                .as("cut=" + cut).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void corruptDirectoryLengthThrowsIllegalArgument()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        // header is HEADER_SIZE bytes; the first directory entry starts there as
        // typeCode(1) + flags(1) + nameLen(1) + ... -- corrupt nameLen to an absurd value
        bytes[ColumnarChunkCodec.HEADER_SIZE + 2] = (byte) 0xFF;
        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(corrupted, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void corruptDictionaryCountThrowsWithoutAllocating()
    {
        int n = 5;
        // exactly 2 distinct values -> dictionary mode; "QQMARKERQQ" sorts first (starts with 'Q',
        // ASCII < 'z') among the two, so it is the dictionary's first entry, right after the mode
        // byte and the dictCount varint
        String marker = "QQMARKERQQ";
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
            values[i] = bytesOf(i % 2 == 0 ? marker : "zzz-other");
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("tag", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(sequentialTimestamps(n), n, columns);
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);

        byte[] markerBytes = marker.getBytes(StandardCharsets.UTF_8);
        int markerPos = indexOf(bytes, markerBytes);
        assertTrue("marker not found in encoded payload", markerPos >= 0);
        int dictCountPos = markerPos - 2;   // marker's own len-prefix byte, then dictCount, then mode
        assertEquals("mode byte", 0, bytes[dictCountPos - 1] & 0xFF);
        assertEquals("dictCount before corruption", 2, bytes[dictCountPos] & 0xFF);
        assertEquals("marker's own length prefix", markerBytes.length, bytes[markerPos - 1] & 0xFF);

        // Overwrite the (originally single-byte) dictCount varint with a multi-byte varint
        // encoding a large but int-POSITIVE value (~536M, same magnitude as
        // corruptRowCountThrowsWithoutAllocating). This matters: the earlier FF FF FF FF 7F
        // pattern here encoded 2^35-1, which truncates to -1 under the `(int)` cast in the
        // pre-fix code -- `new byte[-1]` throws NegativeArraySizeException, which cursor()'s
        // pre-existing generic RuntimeException wrapper already converted to
        // IllegalArgumentException *before this guard existed*, so that pattern did not actually
        // discriminate fixed from unfixed code (verified by mutation -- see class javadoc note /
        // fix-round-2 report). A value that survives the cast as a large positive int is required
        // so only the new dictCount <= MAX_DICTIONARY_SIZE check can stop it before the
        // `new byte[dictCount][]` allocation.
        byte[] corrupted = bytes.clone();
        System.arraycopy(varLongBytes(0x20000000L), 0, corrupted, dictCountPos, 5);

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(corrupted), null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void corruptRawModeEntryLengthThrowsIllegalArgument()
    {
        int n = 300;   // > 256 distinct values forces raw mode
        String firstValue = "ROWMARKERZERO";
        ByteBuffer[] values = new ByteBuffer[n];
        values[0] = bytesOf(firstValue);
        for (int i = 1; i < n; i++)
            values[i] = bytesOf("distinct-" + i);
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("tag", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(sequentialTimestamps(n), n, columns);
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);

        // raw mode lays entries out in row order (unsorted), so row 0's value is the very first
        // entry right after the mode byte: [mode][len][bytes]...
        byte[] markerBytes = firstValue.getBytes(StandardCharsets.UTF_8);
        int markerPos = indexOf(bytes, markerBytes);
        assertTrue("marker not found in encoded payload", markerPos >= 0);
        int lenPos = markerPos - 1;
        assertEquals("row 0's own length prefix", markerBytes.length, bytes[lenPos] & 0xFF);
        assertEquals("mode byte", 1, bytes[lenPos - 1] & 0xFF);

        // large but int-positive value (~536M), not the sign-flipping 2^35-1 pattern -- see the
        // comment in corruptDictionaryCountThrowsWithoutAllocating for why that distinction matters
        byte[] corrupted = bytes.clone();
        System.arraycopy(varLongBytes(0x20000000L), 0, corrupted, lenPos, 5);

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(corrupted), null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void corruptConstantValueLengthInDirectoryThrowsIllegalArgument()
    {
        int n = 10;
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
            values[i] = bytesOf(192);   // identical everywhere -> CONSTANT column
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("c", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, values));

        ByteBuffer payload = ColumnarChunkCodec.encode(sequentialTimestamps(n), n, columns);
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);

        // the single column "c"'s directory entry starts right after the header: typeCode(1) +
        // flags(1) + nameLen(1) + name(1, "c") + sectionLen varint(1, value 0 -- constant columns
        // have a 0-byte data section) + constLen varint -- this is reachable before any data
        // section is read, so a corrupt constLen fires earliest of all three sites
        int constLenPos = ColumnarChunkCodec.HEADER_SIZE + 5;
        assertEquals("int32's canonical form is 4 bytes", 4, bytes[constLenPos] & 0xFF);

        // large but int-positive value (~536M), not the sign-flipping 2^35-1 pattern -- see the
        // comment in corruptDictionaryCountThrowsWithoutAllocating for why that distinction matters
        byte[] corrupted = bytes.clone();
        System.arraycopy(varLongBytes(0x20000000L), 0, corrupted, constLenPos, 5);

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(corrupted), null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void corruptRowCountThrowsWithoutAllocating()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);

        // rowCount is the int32 right after the version byte
        byte[] corrupted = bytes.clone();
        corrupted[1] = (byte) 0x20;   // ~536 million rows, still positive after the int cast
        corrupted[2] = 0;
        corrupted[3] = 0;
        corrupted[4] = 0;

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(corrupted), null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void headerPeeksOnTruncatedBufferThrowIllegalArgument()
    {
        ByteBuffer truncated = ByteBuffer.wrap(new byte[]{ 3, 0 });   // valid version byte, nothing else
        assertThatThrownBy(() -> ColumnarChunkCodec.rowCount(truncated)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.firstTimestamp(truncated)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.lastTimestamp(truncated)).isInstanceOf(IllegalArgumentException.class);
    }

    /** LEB128 unsigned varint encoding, mirroring ColumnarChunkCodec's private writeVarLong. */
    private static byte[] varLongBytes(long value)
    {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        while (true)
        {
            int low7 = (int) (value & 0x7F);
            value >>>= 7;
            if (value == 0)
            {
                out.write(low7);
                return out.toByteArray();
            }
            out.write(low7 | 0x80);
        }
    }

    private static int indexOf(byte[] haystack, byte[] needle)
    {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++)
        {
            for (int j = 0; j < needle.length; j++)
                if (haystack[i + j] != needle[j])
                    continue outer;
            return i;
        }
        return -1;
    }

    @Test
    public void foreignVersionPayloadsAreRejectedByBothEntryPoints()
    {
        ByteBuffer chimpPayload = Chimp128Codec.encode(new long[]{ 1L, 2L, 3L }, new double[]{ 1.0, 2.0, 3.0 }, 3);
        ByteBuffer gorillaPayload = chimpPayload.duplicate();
        gorillaPayload.put(gorillaPayload.position(), (byte) 1);   // the removed v1 version byte

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(gorillaPayload, null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(chimpPayload, null))
            .isInstanceOf(IllegalArgumentException.class);

        ByteBuffer columnarPayload = simpleTwoColumnPayload();
        assertThatThrownBy(() -> ChunkCodecs.cursor(columnarPayload))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void removedGorillaColumnTypeCodeIsRejectedNotMisread()
    {
        // Type code 0 was DOUBLE_GORILLA. A v3 chunk written before that codec was dropped must be
        // rejected by name -- for a CONSTANT double column especially, whose raw constBytes would
        // otherwise decode perfectly well under any type code and hide the format change.
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("d", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP,
                                                             new ByteBuffer[]{ bytesOf(1.5), bytesOf(1.5),
                                                                               bytesOf(1.5) }));
        ByteBuffer payload = ColumnarChunkCodec.encode(new long[]{ 1L, 2L, 3L }, 3, columns);

        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        // the first directory entry starts right after the header; its first byte is the type code
        assertEquals(ColumnarChunkCodec.TYPE_DOUBLE_ALP, bytes[ColumnarChunkCodec.HEADER_SIZE]);
        bytes[ColumnarChunkCodec.HEADER_SIZE] = 0x00;

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(bytes), null))
            .isInstanceOf(UnsupportedChunkFormatException.class)
            .hasMessageContaining("gorilla");
    }

    @Test
    public void removedChimpColumnTypeCodeIsRejectedNotMisread()
    {
        // Type code 1 was DOUBLE_CHIMP until ALP replaced it as the only double encoding. Same
        // reasoning as the gorilla case above: a CONSTANT double column stores its value raw in the
        // directory, so it would decode perfectly under the new code and hide the fact that this
        // chunk's non-constant double sections are unreadable.
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("d", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP,
                                                             new ByteBuffer[]{ bytesOf(1.5), bytesOf(2.5),
                                                                               bytesOf(3.5) }));
        ByteBuffer payload = ColumnarChunkCodec.encode(new long[]{ 1L, 2L, 3L }, 3, columns);

        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        assertEquals(ColumnarChunkCodec.TYPE_DOUBLE_ALP, bytes[ColumnarChunkCodec.HEADER_SIZE]);
        bytes[ColumnarChunkCodec.HEADER_SIZE] = 0x01;

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(bytes), null))
            .isInstanceOf(UnsupportedChunkFormatException.class)
            .hasMessageContaining("chimp128");
    }

    /**
     * Doubles carry ALP's type code and nothing else. Which of ALP's two variants a column got is
     * recorded <em>inside</em> the section, not in the directory, so a distribution ALP-RD wins and
     * one decimal ALP wins are indistinguishable at this level -- there is exactly one double code.
     */
    @Test
    public void doubleColumnsAlwaysCarryTheAlpTypeCode()
    {
        int n = 400;
        Random random = new Random(17);
        ByteBuffer[] decimals = new ByteBuffer[n];      // decimal ALP's home ground
        ByteBuffer[] highEntropy = new ByteBuffer[n];   // nothing decimal here: ALP-RD must win
        double walk = 20.0;
        for (int i = 0; i < n; i++)
        {
            walk += (random.nextInt(3) - 1) * 0.01;
            decimals[i] = bytesOf(Math.round(walk * 100.0) / 100.0);
            highEntropy[i] = ByteBuffer.wrap(rawBytes(random.nextLong()));
        }

        // One column each, so the directory entry under test is always the first one and no offset
        // arithmetic over a variable-length name and varint is needed to find it.
        assertEquals("decimal-like doubles must use the ALP type code",
                     ColumnarChunkCodec.TYPE_DOUBLE_ALP, firstTypeCodeOf(n, decimals));
        assertEquals("high-entropy doubles must use the same type code",
                     ColumnarChunkCodec.TYPE_DOUBLE_ALP, firstTypeCodeOf(n, highEntropy));

        // And the sub-format byte really did differ, i.e. the fixtures exercise both variants.
        assertTrue("the two fixtures must pick different ALP variants",
                   alpSubFormatOf(n, decimals) != alpSubFormatOf(n, highEntropy));
    }

    private static byte firstTypeCodeOf(int n, ByteBuffer[] values)
    {
        return payloadBytes(n, values)[ColumnarChunkCodec.HEADER_SIZE];
    }

    /** The first byte of the sole column's data section, which for an ALP column is its sub-format. */
    private static byte alpSubFormatOf(int n, ByteBuffer[] values)
    {
        byte[] bytes = payloadBytes(n, values);
        // Data sections are the payload's tail and this payload has exactly one column, so the
        // section begins exactly sectionLen bytes before the end.
        ByteBuffer directory = ByteBuffer.wrap(bytes);
        directory.position(ColumnarChunkCodec.HEADER_SIZE + 3 + 1);   // typeCode, flags, nameLen, "v"
        long sectionLen = 0;
        for (int shift = 0; ; shift += 7)
        {
            byte b = directory.get();
            sectionLen |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0)
                break;
        }
        return bytes[(int) (bytes.length - sectionLen)];
    }

    private static byte[] payloadBytes(int n, ByteBuffer[] values)
    {
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("v", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP, values));
        ByteBuffer payload = ColumnarChunkCodec.encode(sequentialTimestamps(n), n, columns);
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        return bytes;
    }

    /**
     * The whole columnar path, not just the codec: a double cell's bytes must come back out of a
     * cursor bit-identical, including the patterns Java's own {@code Double} helpers rewrite.
     * {@code ByteArrayUtil.bytes(double)} goes through {@code Double.doubleToLongBits}, which
     * canonicalises every NaN payload -- so a decoder that materialised doubles rather than raw bit
     * patterns would pass every other test here and still lose data on this one.
     */
    @Test
    public void doubleEdgeCaseBitPatternsSurviveTheWholeColumnarPath()
    {
        long[] patterns =
        {
            0x0000000000000000L,   // +0.0
            0x8000000000000000L,   // -0.0
            0x7FF0000000000000L,   // +Infinity
            0xFFF0000000000000L,   // -Infinity
            0x7FF8000000000000L,   // canonical NaN
            0x7FF8000000000001L,   // quiet NaN with a payload
            0x7FF0000000000001L,   // signalling NaN
            0xFFFFFFFFFFFFFFFFL,   // negative NaN, all payload bits set
            0x0000000000000001L,   // Double.MIN_VALUE
            0x000FFFFFFFFFFFFFL,   // largest subnormal
            0x0010000000000000L,   // Double.MIN_NORMAL
            0x7FEFFFFFFFFFFFFFL,   // Double.MAX_VALUE
            0xFFEFFFFFFFFFFFFFL,   // -Double.MAX_VALUE
            Double.doubleToRawLongBits(20.76),
            Double.doubleToRawLongBits(157.0),
            Double.doubleToRawLongBits(-0.1),
        };

        // Every second row null, so the presence bitmap and the scatter back to row indexes are in
        // play too -- an off-by-one there would shift values onto neighbouring rows.
        int n = patterns.length * 2;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] values = new ByteBuffer[n];
        for (int i = 0; i < patterns.length; i++)
            values[i * 2] = ByteBuffer.wrap(rawBytes(patterns[i]));

        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("reading", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP, values));

        ColumnarCursor cursor = ColumnarChunkCodec.cursor(ColumnarChunkCodec.encode(ts, n, columns), null);
        for (int i = 0; i < n; i++)
        {
            assertTrue(cursor.advance());
            if (i % 2 == 1)
            {
                assertTrue("row " + i + " must stay null", cursor.isNull("reading"));
                continue;
            }
            long expected = patterns[i / 2];
            ByteBuffer decoded = cursor.getBytes("reading");
            assertTrue("row " + i + ": decoded doubles must be array-backed (see the buffer contract)",
                       decoded.hasArray());
            assertArrayEquals("row " + i + ": " + String.format("0x%016X", expected),
                              rawBytes(expected), asBytes(decoded));
        }
        assertFalse(cursor.advance());
    }

    /**
     * Truncating a payload whose tail is an ALP section must be reported as corruption, the same as
     * every other section type. The double column sorts last here so its data section really is the
     * payload's tail.
     */
    @Test
    public void truncatedAlpSectionThrowsIllegalArgument()
    {
        int n = 300;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] head = new ByteBuffer[n];
        ByteBuffer[] doubles = new ByteBuffer[n];
        Random random = new Random(23);
        double walk = 41.5;
        for (int i = 0; i < n; i++)
        {
            head[i] = bytesOf(i);
            walk += (random.nextInt(3) - 1) * 0.01;
            doubles[i] = bytesOf(Math.round(walk * 100.0) / 100.0);
        }
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("aaa_int", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, head));
        columns.put("zzz_double", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_ALP, doubles));

        ByteBuffer payload = ColumnarChunkCodec.encode(ts, n, columns);
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);

        for (int cut = 1; cut <= 24; cut++)
        {
            ByteBuffer truncated = ByteBuffer.wrap(bytes, 0, bytes.length - cut);
            assertThatThrownBy(() -> ColumnarChunkCodec.cursor(truncated, null))
                .as("cut=" + cut).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static byte[] rawBytes(long bits)
    {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++)
            out[i] = (byte) (bits >>> (56 - 8 * i));
        return out;
    }

    @Test
    public void unknownColumnTypeCodeIsRejected()
    {
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("d", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32,
                                                             new ByteBuffer[]{ bytesOf(1), bytesOf(2), bytesOf(3) }));
        ByteBuffer payload = ColumnarChunkCodec.encode(new long[]{ 1L, 2L, 3L }, 3, columns);

        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        bytes[ColumnarChunkCodec.HEADER_SIZE] = 0x7F;

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(ByteBuffer.wrap(bytes), null))
            .isInstanceOf(IllegalArgumentException.class)
            .isNotInstanceOf(UnsupportedChunkFormatException.class)   // corruption, not a removed format
            .hasMessageContaining("unknown column type code");
    }

    @Test
    public void rejectsNonIncreasingTimestamps()
    {
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("c", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32,
                                                             new ByteBuffer[]{ bytesOf(1), bytesOf(2), bytesOf(3) }));

        assertThatThrownBy(() -> ColumnarChunkCodec.encode(new long[]{ 1000L, 999L, 1001L }, 3, columns))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.encode(new long[]{ 1000L, 1000L, 1001L }, 3, columns))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsZeroCount()
    {
        ColumnarChunkCodec.encode(new long[0], 0, new TreeMap<>());
    }

    @Test
    public void headerPeeksDoNotConsumeBuffer()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        int position = payload.position();

        assertEquals(20, ColumnarChunkCodec.rowCount(payload));
        assertEquals(0L, ColumnarChunkCodec.firstTimestamp(payload));
        assertEquals(19_000L, ColumnarChunkCodec.lastTimestamp(payload));
        assertEquals(position, payload.position());
    }

    @Test
    public void cursorAccessBeforeAdvanceThrows()
    {
        ByteBuffer payload = simpleTwoColumnPayload();
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);

        assertThatThrownBy(cursor::timestamp).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cursor.getBytes("colA")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cursor.isNull("colA")).isInstanceOf(IllegalStateException.class);
    }

    private static ByteBuffer simpleTwoColumnPayload()
    {
        int n = 20;
        long[] ts = sequentialTimestamps(n);
        ByteBuffer[] a = new ByteBuffer[n];
        ByteBuffer[] b = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            a[i] = bytesOf(i);
            b[i] = bytesOf("v" + i);
        }
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("colA", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32, a));
        columns.put("colB", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT, b));
        return ColumnarChunkCodec.encode(ts, n, columns);
    }

    private static long[] sequentialTimestamps(int n)
    {
        long[] ts = new long[n];
        for (int i = 0; i < n; i++)
            ts[i] = i * 1000L;
        return ts;
    }

    private static ByteBuffer bytesOf(double v)
    {
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        b.putDouble(v);
        b.flip();
        return b;
    }

    private static ByteBuffer bytesOf(int v)
    {
        ByteBuffer b = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        b.putInt(v);
        b.flip();
        return b;
    }

    private static ByteBuffer bytesOf(long v)
    {
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        b.putLong(v);
        b.flip();
        return b;
    }

    private static ByteBuffer bytesOf(boolean v)
    {
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put((byte) (v ? 1 : 0));
        b.flip();
        return b;
    }

    private static ByteBuffer bytesOf(String v)
    {
        return ByteBuffer.wrap(v.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteBuffer bytesOf(byte[] v)
    {
        return ByteBuffer.wrap(v);
    }

    private static double asDouble(ByteBuffer b)
    {
        return b.duplicate().order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    private static int asInt(ByteBuffer b)
    {
        return b.duplicate().order(ByteOrder.BIG_ENDIAN).getInt();
    }

    private static long asLong(ByteBuffer b)
    {
        return b.duplicate().order(ByteOrder.BIG_ENDIAN).getLong();
    }

    private static boolean asBool(ByteBuffer b)
    {
        return b.duplicate().get() != 0;
    }

    private static String asText(ByteBuffer b)
    {
        return new String(asBytes(b), StandardCharsets.UTF_8);
    }

    private static byte[] asBytes(ByteBuffer b)
    {
        byte[] array = new byte[b.remaining()];
        b.duplicate().get(array);
        return array;
    }
}
