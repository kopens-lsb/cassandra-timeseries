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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

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
        columns.put("c_double", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_GORILLA, doubleValues));
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
        columns.put("maybe_col", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_GORILLA, values));

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
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = representativeColumns(n);

        ByteBuffer first = ColumnarChunkCodec.encode(ts, n, columns);
        ByteBuffer second = ColumnarChunkCodec.encode(ts, n, columns);
        assertEquals(first, second);
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
        columns.put("readings", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_GORILLA, doubleValues));
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
    public void foreignVersionPayloadsAreRejectedByBothEntryPoints()
    {
        ByteBuffer gorillaPayload = GorillaCodec.encode(new long[]{ 1L, 2L, 3L }, new double[]{ 1.0, 2.0, 3.0 }, 3);
        ByteBuffer chimpPayload = Chimp128Codec.encode(new long[]{ 1L, 2L, 3L }, new double[]{ 1.0, 2.0, 3.0 }, 3);

        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(gorillaPayload, null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(chimpPayload, null))
            .isInstanceOf(IllegalArgumentException.class);

        ByteBuffer columnarPayload = simpleTwoColumnPayload();
        assertThatThrownBy(() -> ChunkCodecs.cursor(columnarPayload))
            .isInstanceOf(IllegalArgumentException.class);
        assertEquals(ChunkCodecs.Codec.COLUMNAR, ChunkCodecs.codecOf(columnarPayload));
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
