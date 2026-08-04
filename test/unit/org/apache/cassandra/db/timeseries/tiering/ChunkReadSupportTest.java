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

package org.apache.cassandra.db.timeseries.tiering;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.ImmutableSet;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.ChunkV4Codec;
import org.apache.cassandra.db.timeseries.ChunkV4Directory;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SP3 Task 1 / SP4 Task 3: decoding a chunk payload into synthetic CQL rows for the
 * transparent-read merge. Contract: clustering = sample timestamp; one cell per non-null column of
 * the sample, each with writetime = the chunk's max_row_writetime; range filter
 * [startMs, endMsExcl); optional reverse.
 */
public class ChunkReadSupportTest
{
    private static final long BASE = 1_577_836_800_000L;             // 2020-01-01T00:00Z
    private static final long WRITETIME = 1_650_000_000_000_000L;    // micros (max_row_writetime)
    /** Reconstructed cells post-date the re-encoder's own tombstone by one micro -- see ChunkReadSupport. */
    private static final long CELL_WRITETIME = WRITETIME + 1;

    private static TableMetadata metadata;
    private static ColumnMetadata valueColumn;
    private static ColumnMetadata qualityColumn;
    private static ColumnMetadata labelColumn;

    @BeforeClass
    public static void setUpClazz()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("sp3ks", "sp3tbl")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                                .addClusteringColumn("timestamp", TimestampType.instance)
                                .addRegularColumn("value", DoubleType.instance)
                                .addRegularColumn("quality", Int32Type.instance)
                                .addRegularColumn("label", UTF8Type.instance)
                                .build();
        valueColumn = metadata.getColumn(ByteBufferUtil.bytes("value"));
        qualityColumn = metadata.getColumn(ByteBufferUtil.bytes("quality"));
        labelColumn = metadata.getColumn(ByteBufferUtil.bytes("label"));
    }

    /** A chunk carrying only the {@code value} double column, ramping 20.0 by 0.1 per sample. */
    private static ByteBuffer chunk(int count, long stepMs)
    {
        long[] ts = new long[count];
        ByteBuffer[] values = new ByteBuffer[count];
        for (int i = 0; i < count; i++)
        {
            ts[i] = BASE + i * stepMs;
            values[i] = DoubleType.instance.decompose(20.0 + i * 0.1);
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", input(ChunkV4Directory.TYPE_DOUBLE, values));
        return ColumnarChunkCodec.encode(ts, count, columns);
    }

    /** A {@link ChunkV4Codec.ColumnInput} declaring the type code's own canonical stat order. */
    private static ChunkV4Codec.ColumnInput input(int typeCode, ByteBuffer... values)
    {
        return new ChunkV4Codec.ColumnInput(typeCode, ChunkV4Codec.canonicalStatOrder(typeCode), values);
    }

    private static void assertRow(Row row, long expectedTsMs, double expectedValue)
    {
        assertEquals(TimestampType.instance.decompose(new Date(expectedTsMs)), row.clustering().bufferAt(0));
        Cell<?> cell = row.getCell(valueColumn);
        assertEquals(expectedValue, DoubleType.instance.compose(cell.buffer()), 0.0);
        assertEquals(CELL_WRITETIME, cell.timestamp());
    }

    private static List<Row> decode(ByteBuffer payload)
    {
        return list(ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, Long.MIN_VALUE, Long.MAX_VALUE,
                                                   false, null));
    }

    /**
     * Drains a decode. {@code rowsFromChunk} hands back an iterator so a read that stops early stops
     * building rows (see its class javadoc); these tests want the whole window either way.
     */
    private static List<Row> list(Iterator<Row> rows)
    {
        List<Row> all = new ArrayList<>();
        while (rows.hasNext())
            all.add(rows.next());
        return all;
    }

    @Test
    public void roundTripFullRange()
    {
        List<Row> rows = decode(chunk(100, 1000));
        assertEquals(100, rows.size());
        assertRow(rows.get(0), BASE, 20.0);
        assertRow(rows.get(99), BASE + 99_000, 20.0 + 99 * 0.1);
    }

    @Test
    public void roundTripHonoursTheChunkStep()
    {
        List<Row> rows = decode(chunk(50, 2000));
        assertEquals(50, rows.size());
        assertRow(rows.get(49), BASE + 49 * 2000, 20.0 + 49 * 0.1);
    }

    @Test
    public void rangeFilterIsInclusiveExclusive()
    {
        ByteBuffer payload = chunk(10, 1000);
        // [BASE+2000, BASE+5000): expect samples at +2000, +3000, +4000
        List<Row> rows = list(ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, BASE + 2000, BASE + 5000,
                                                             false, null));
        assertEquals(3, rows.size());
        assertRow(rows.get(0), BASE + 2000, 20.2);
        assertRow(rows.get(2), BASE + 4000, 20.4);
    }

    @Test
    public void emptyRangeYieldsNoRows()
    {
        ByteBuffer payload = chunk(10, 1000);
        assertTrue(list(ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, BASE + 100_000, BASE + 200_000,
                                                       false, null)).isEmpty());
    }

    @Test
    public void reversedEmitsDescending()
    {
        List<Row> rows = list(ChunkReadSupport.rowsFromChunk(metadata, chunk(5, 1000), WRITETIME,
                                                             Long.MIN_VALUE, Long.MAX_VALUE, true, null));
        assertEquals(5, rows.size());
        assertRow(rows.get(0), BASE + 4000, 20.4);
        assertRow(rows.get(4), BASE, 20.0);
        // Every row, not just the ends: newest-first is assembled back-to-front from a captured
        // window rather than built forwards and reversed, so an off-by-one in the walk would show up
        // in the middle while both ends still looked right.
        for (int i = 0; i < 5; i++)
            assertRow(rows.get(i), BASE + (4 - i) * 1000L, 20.0 + (4 - i) * 0.1);
    }

    @Test
    public void reversedOverAnEmptyRangeIsExhaustedImmediately()
    {
        Iterator<Row> rows = ChunkReadSupport.rowsFromChunk(metadata, chunk(10, 1000), WRITETIME,
                                                            BASE + 100_000, BASE + 200_000, true, null);
        assertFalse(rows.hasNext());
        try
        {
            rows.next();
            fail("expected NoSuchElementException from an exhausted descending iterator");
        }
        catch (NoSuchElementException expected)
        {
            // the hand-written reverse iterator must end like the list iterator it replaced
        }
    }

    @Test
    public void everyColumnBecomesItsOwnCellAndNullsStayNull()
    {
        // 3 samples x 3 columns with holes: value null on sample 1, quality null on sample 2,
        // label present throughout. Each cell must land on its own column, and a null must produce
        // NO cell rather than a zero/empty one.
        long[] ts = { BASE, BASE + 1000, BASE + 2000 };
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", input(ChunkV4Directory.TYPE_DOUBLE,
                                   DoubleType.instance.decompose(1.5),
                                   null,
                                   DoubleType.instance.decompose(2.5)));
        columns.put("quality", input(ChunkV4Directory.TYPE_INT32,
                                     Int32Type.instance.decompose(192),
                                     Int32Type.instance.decompose(0),
                                     null));
        columns.put("label", input(ChunkV4Directory.TYPE_TEXT,
                                   UTF8Type.instance.decompose("a"),
                                   UTF8Type.instance.decompose("b"),
                                   UTF8Type.instance.decompose("")));

        List<Row> rows = decode(ColumnarChunkCodec.encode(ts, 3, columns));
        assertEquals(3, rows.size());

        assertEquals(1.5, DoubleType.instance.compose(rows.get(0).getCell(valueColumn).buffer()), 0.0);
        assertEquals(192, (int) Int32Type.instance.compose(rows.get(0).getCell(qualityColumn).buffer()));
        assertEquals("a", UTF8Type.instance.compose(rows.get(0).getCell(labelColumn).buffer()));

        assertNull("a null value must not produce a cell", rows.get(1).getCell(valueColumn));
        assertEquals(0, (int) Int32Type.instance.compose(rows.get(1).getCell(qualityColumn).buffer()));
        assertEquals("b", UTF8Type.instance.compose(rows.get(1).getCell(labelColumn).buffer()));

        assertEquals(2.5, DoubleType.instance.compose(rows.get(2).getCell(valueColumn).buffer()), 0.0);
        assertNull("a null quality must not produce a cell", rows.get(2).getCell(qualityColumn));
        assertEquals("", UTF8Type.instance.compose(rows.get(2).getCell(labelColumn).buffer()));

        // Every cell carries max_row_writetime + 1 (see ChunkReadSupport's class javadoc).
        assertEquals(CELL_WRITETIME, rows.get(0).getCell(valueColumn).timestamp());
    }

    @Test
    public void anAllNullSampleBecomesALiveRowWithNoCells()
    {
        // The row existed in the base table (a bare primary-key insert) and the re-encoder chunked
        // it so the range delete would not destroy it. Rebuilt it must still be a live row, or it
        // would be indistinguishable from a row that never existed.
        long[] ts = { BASE };
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", input(ChunkV4Directory.TYPE_DOUBLE, new ByteBuffer[]{ null }));

        List<Row> rows = decode(ColumnarChunkCodec.encode(ts, 1, columns));
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertNull(row.getCell(valueColumn));
        assertFalse("an all-null sample must not decode to a phantom (dead) row", row.isEmpty());
        assertEquals(CELL_WRITETIME, row.primaryKeyLivenessInfo().timestamp());
    }

    @Test
    public void aColumnTheTableNoLongerHasIsIgnored()
    {
        // ALTER TABLE ... DROP after the chunk was written: the chunk still carries the column, but
        // there is nowhere to put its cells. It must be dropped silently, not fail the read.
        long[] ts = { BASE };
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", input(ChunkV4Directory.TYPE_DOUBLE, DoubleType.instance.decompose(7.5)));
        columns.put("gone", input(ChunkV4Directory.TYPE_TEXT, UTF8Type.instance.decompose("x")));

        List<Row> rows = decode(ColumnarChunkCodec.encode(ts, 1, columns));
        assertEquals(1, rows.size());
        assertNotNull(rows.get(0).getCell(valueColumn));
        assertEquals(1, rows.get(0).columnCount());
    }

    /**
     * Decoding must fail loudly -- the CALLER decides to skip-and-warn (plan R4) -- and it must fail
     * <b>out of the call itself</b>, not out of the returned iterator. That is what keeps a corrupt
     * window all-or-nothing now that rows are built lazily: if the failure could surface mid-iteration
     * the caller's "skip this window" would have already published a truncated prefix of it.
     * <p>
     * Both directions and both shapes of corruption, because laziness moved for both: a header that
     * names no known format is caught by the first bytes {@code ColumnarChunkCodec.cursor} looks at,
     * while a payload whose data sections do not fit is only caught once the cursor has walked the
     * directory and the timestamp stream -- and that whole-payload decode is precisely what makes it
     * safe to defer building the rows.
     */
    @Test
    public void corruptPayloadThrowsBeforeTheFirstRowIsEmitted()
    {
        ByteBuffer unknownVersion = chunk(10, 1000).duplicate();
        unknownVersion.put(0, (byte) 99);                            // names no known chunk format

        ByteBuffer truncated = chunk(10, 1000).duplicate();
        truncated.limit(truncated.limit() - 8);                      // data sections now overrun it

        for (boolean descending : new boolean[]{ false, true })
        {
            assertFailsFromTheCall(unknownVersion, descending, IllegalArgumentException.class);
            assertFailsFromTheCall(truncated, descending, IllegalArgumentException.class);
        }
    }

    /**
     * The one failure a caller must never swallow (see {@code ChunkRowSource}): it is systematic, so
     * skipping it would silently truncate history on every read. Deferring row assembly must not let
     * it turn into anything else, in either direction.
     */
    @Test
    public void anUnsupportedFormatPropagatesFromTheCall()
    {
        ByteBuffer payload = chunk(10, 1000).duplicate();
        payload.put(0, (byte) 2);        // chimp128's version byte: a known-but-removed chunk format

        for (boolean descending : new boolean[]{ false, true })
            assertFailsFromTheCall(payload, descending, UnsupportedChunkFormatException.class);
    }

    private static void assertFailsFromTheCall(ByteBuffer payload, boolean descending,
                                               Class<? extends RuntimeException> expected)
    {
        String what = expected.getSimpleName() + (descending ? " (descending)" : " (ascending)");
        try
        {
            Iterator<Row> rows = ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME,
                                                                Long.MIN_VALUE, Long.MAX_VALUE, descending, null);
            fail("expected " + what + " from the call, got an iterator: " + rows);
        }
        catch (RuntimeException e)
        {
            // the point: thrown by rowsFromChunk, not by a later hasNext()/next()
            if (!expected.isInstance(e))
                throw new AssertionError("expected " + what + ", got " + e, e);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Laziness. rowsFromChunk builds a Row only for the samples the consumer actually takes, in both
    // directions -- which is invisible in the rows themselves, so these assert on the assembler's own
    // count (ChunkReadSupport.RowCounting).
    // ---------------------------------------------------------------------------------------------

    /**
     * The fix this suite exists for. A newest-first {@code LIMIT 1} over a window used to decode,
     * build and reverse every row in it; against the eager implementation the first assertion below
     * reports 1,000 instead of 1.
     * <p>
     * The reverse walk is not free -- {@link org.apache.cassandra.db.timeseries.ColumnarCursor} is
     * forward-only, so the samples still have to be walked past to reach the newest one -- but
     * walking is array reads, and what is asserted here is that no {@code Row}, clustering, cell or
     * BTree is built for a sample that is never emitted.
     */
    @Test
    public void descendingBuildsOnlyTheRowsItEmits()
    {
        ChunkReadSupport.RowCounting rows = counting(chunk(1000, 1000), Long.MIN_VALUE, Long.MAX_VALUE, true);

        assertEquals("no row may be built before the first pull", 0, rows.rowsAssembled());
        assertTrue(rows.hasNext());
        assertEquals("hasNext() must not build a row either", 0, rows.rowsAssembled());

        assertRow(rows.next(), BASE + 999_000, 20.0 + 999 * 0.1);
        assertEquals("a newest-first LIMIT 1 must cost exactly one row", 1, rows.rowsAssembled());

        assertRow(rows.next(), BASE + 998_000, 20.0 + 998 * 0.1);
        assertEquals(2, rows.rowsAssembled());

        // ...and the window is still whole: taking the rest builds the rest, exactly once each.
        int taken = 2;
        while (rows.hasNext())
        {
            rows.next();
            taken++;
        }
        assertEquals(1000, taken);
        assertEquals(1000, rows.rowsAssembled());
    }

    /** The same guarantee for the streaming direction, which has always had it. */
    @Test
    public void ascendingBuildsOnlyTheRowsItEmits()
    {
        ChunkReadSupport.RowCounting rows = counting(chunk(1000, 1000), Long.MIN_VALUE, Long.MAX_VALUE, false);

        assertEquals("no row may be built before the first pull", 0, rows.rowsAssembled());
        assertRow(rows.next(), BASE, 20.0);
        assertEquals("an oldest-first LIMIT 1 must cost exactly one row", 1, rows.rowsAssembled());
    }

    /**
     * Samples outside {@code [startMs, endMs)} must not be built either, in either direction: the
     * range filter runs on the walk, ahead of assembly, so a narrow range over a wide window costs
     * the rows it selects and no others.
     */
    @Test
    public void outOfRangeSamplesAreNeverBuilt()
    {
        for (boolean descending : new boolean[]{ false, true })
        {
            ChunkReadSupport.RowCounting rows = counting(chunk(1000, 1000), BASE + 10_000, BASE + 13_000, descending);
            assertEquals(3, list(rows).size());
            assertEquals("direction " + descending, 3, rows.rowsAssembled());
        }
    }

    private static ChunkReadSupport.RowCounting counting(ByteBuffer payload, long startMs, long endMsExcl,
                                                         boolean descending)
    {
        return (ChunkReadSupport.RowCounting) ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME,
                                                                            startMs, endMsExcl, descending, null);
    }

    // ---------------------------------------------------------------------------------------------
    // Round-trip equality: whatever the two directions do internally, one must be the other reversed.
    // ---------------------------------------------------------------------------------------------

    /**
     * A window with holes in two columns, samples with no value at all, a column the table has since
     * dropped, and a range that selects only part of it -- decoded both ways. Descending must be the
     * exact reverse of ascending, row for row and cell for cell.
     * <p>
     * The sizes straddle the growth points of the capture buffer the reverse walk fills (it starts at
     * 16 slots and doubles), and 1 sample with the range below selecting none of it covers the empty
     * case.
     */
    @Test
    public void descendingIsTheExactReverseOfAscending()
    {
        for (int count : new int[]{ 1, 15, 16, 17, 33, 1000 })
            assertMirrored(mixedChunk(count), BASE + 3_000, BASE + 40_000, null, "count " + count);
    }

    /** The same, decoding only some of the columns: projection must not skew either direction. */
    @Test
    public void descendingIsTheExactReverseOfAscendingUnderProjection()
    {
        ByteBuffer payload = mixedChunk(200);
        assertMirrored(payload, BASE + 3_000, BASE + 40_000, ImmutableSet.of("value"), "one column");
        assertMirrored(payload, BASE + 3_000, BASE + 40_000, ImmutableSet.of("value", "label"), "two columns");
        // Nothing the table still has: every row falls back to primary-key liveness, both ways.
        assertMirrored(payload, BASE + 3_000, BASE + 40_000, ImmutableSet.of("gone"), "only a dropped column");
    }

    /**
     * Every column the chunk carries has been dropped from the table, so no row has a cell to build
     * and the reverse walk captures timestamps only. The rows must still come back -- newest first,
     * live, and one per sample -- because their existence is exactly what tiering chunked them for.
     */
    @Test
    public void descendingSurvivesAChunkWhoseColumnsWereAllDropped()
    {
        long[] ts = new long[20];                                    // > 16, so the capture buffer grows
        ByteBuffer[] gone = new ByteBuffer[20];
        for (int i = 0; i < 20; i++)
        {
            ts[i] = BASE + i * 1000L;
            gone[i] = UTF8Type.instance.decompose("g" + i);
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("gone", input(ChunkV4Directory.TYPE_TEXT, gone));
        ByteBuffer payload = ColumnarChunkCodec.encode(ts, 20, columns);

        List<Row> rows = list(ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME,
                                                             Long.MIN_VALUE, Long.MAX_VALUE, true, null));
        assertEquals(20, rows.size());
        for (int i = 0; i < 20; i++)
        {
            Row row = rows.get(i);
            assertEquals(TimestampType.instance.decompose(new Date(BASE + (19 - i) * 1000L)),
                         row.clustering().bufferAt(0));
            assertEquals(0, row.columnCount());
            assertFalse("a row with no buildable cells must still be live", row.isEmpty());
            assertEquals(CELL_WRITETIME, row.primaryKeyLivenessInfo().timestamp());
        }
        assertMirrored(payload, Long.MIN_VALUE, Long.MAX_VALUE, null, "all columns dropped");
    }

    private static void assertMirrored(ByteBuffer payload, long startMs, long endMsExcl,
                                       Set<String> projection, String what)
    {
        List<Row> ascending = list(ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME,
                                                                  startMs, endMsExcl, false, projection));
        List<Row> descending = list(ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME,
                                                                   startMs, endMsExcl, true, projection));
        assertEquals(what + ": row count", ascending.size(), descending.size());
        for (int i = 0; i < ascending.size(); i++)
        {
            Row expected = ascending.get(ascending.size() - 1 - i);
            Row actual = descending.get(i);
            String where = what + ", descending row " + i;
            // Spelled out before the whole-row comparison so a failure names what differs.
            assertEquals(where + ": clustering", expected.clustering().bufferAt(0), actual.clustering().bufferAt(0));
            assertEquals(where + ": cell count", expected.columnCount(), actual.columnCount());
            assertEquals(where + ": primary key liveness",
                         expected.primaryKeyLivenessInfo(), actual.primaryKeyLivenessInfo());
            for (ColumnMetadata column : new ColumnMetadata[]{ valueColumn, qualityColumn, labelColumn })
            {
                Cell<?> expectedCell = expected.getCell(column);
                Cell<?> actualCell = actual.getCell(column);
                if (expectedCell == null)
                {
                    assertNull(where + ": " + column.name + " must have no cell", actualCell);
                    continue;
                }
                assertNotNull(where + ": " + column.name + " lost its cell", actualCell);
                assertEquals(where + ": " + column.name, expectedCell.buffer(), actualCell.buffer());
                assertEquals(where + ": " + column.name + " writetime",
                             expectedCell.timestamp(), actualCell.timestamp());
            }
            assertEquals(where, expected, actual);
        }
    }

    /**
     * A window that exercises what the two directions have to agree on: holes in a fixed-width and a
     * variable-width column, samples with nothing on them at all (which decode to a live row with no
     * cells), a low-cardinality text column (dictionary-encoded, so its values are arrays the decoder
     * shares between rows), and a column the table has dropped since the chunk was written.
     */
    private static ByteBuffer mixedChunk(int count)
    {
        long[] ts = new long[count];
        ByteBuffer[] values = new ByteBuffer[count];
        ByteBuffer[] qualities = new ByteBuffer[count];
        ByteBuffer[] labels = new ByteBuffer[count];
        ByteBuffer[] gone = new ByteBuffer[count];
        for (int i = 0; i < count; i++)
        {
            ts[i] = BASE + i * 1000L;
            boolean bare = i % 7 == 3;                     // no value on any column the table has
            values[i] = bare || i % 3 == 0 ? null : DoubleType.instance.decompose(20.0 + i * 0.1);
            qualities[i] = bare || i % 4 == 1 ? null : Int32Type.instance.decompose(i);
            labels[i] = bare ? null : UTF8Type.instance.decompose(i % 5 == 0 ? "" : "l" + (i % 3));
            gone[i] = UTF8Type.instance.decompose("dropped");
        }
        SortedMap<String, ChunkV4Codec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", input(ChunkV4Directory.TYPE_DOUBLE, values));
        columns.put("quality", input(ChunkV4Directory.TYPE_INT32, qualities));
        columns.put("label", input(ChunkV4Directory.TYPE_TEXT, labels));
        columns.put("gone", input(ChunkV4Directory.TYPE_TEXT, gone));
        return ColumnarChunkCodec.encode(ts, count, columns);
    }
}
