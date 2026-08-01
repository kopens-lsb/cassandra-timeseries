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
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
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
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_CHIMP, values));
        return ColumnarChunkCodec.encode(ts, count, columns);
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
        return ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, Long.MIN_VALUE, Long.MAX_VALUE, false);
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
        List<Row> rows = ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, BASE + 2000, BASE + 5000, false);
        assertEquals(3, rows.size());
        assertRow(rows.get(0), BASE + 2000, 20.2);
        assertRow(rows.get(2), BASE + 4000, 20.4);
    }

    @Test
    public void emptyRangeYieldsNoRows()
    {
        ByteBuffer payload = chunk(10, 1000);
        assertTrue(ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, BASE + 100_000, BASE + 200_000, false).isEmpty());
    }

    @Test
    public void reversedEmitsDescending()
    {
        List<Row> rows = ChunkReadSupport.rowsFromChunk(metadata, chunk(5, 1000), WRITETIME,
                                                        Long.MIN_VALUE, Long.MAX_VALUE, true);
        assertEquals(5, rows.size());
        assertRow(rows.get(0), BASE + 4000, 20.4);
        assertRow(rows.get(4), BASE, 20.0);
    }

    @Test
    public void everyColumnBecomesItsOwnCellAndNullsStayNull()
    {
        // 3 samples x 3 columns with holes: value null on sample 1, quality null on sample 2,
        // label present throughout. Each cell must land on its own column, and a null must produce
        // NO cell rather than a zero/empty one.
        long[] ts = { BASE, BASE + 1000, BASE + 2000 };
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_CHIMP,
                                                                new ByteBuffer[]{ DoubleType.instance.decompose(1.5),
                                                                                  null,
                                                                                  DoubleType.instance.decompose(2.5) }));
        columns.put("quality", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32,
                                                                  new ByteBuffer[]{ Int32Type.instance.decompose(192),
                                                                                    Int32Type.instance.decompose(0),
                                                                                    null }));
        columns.put("label", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT,
                                                                new ByteBuffer[]{ UTF8Type.instance.decompose("a"),
                                                                                  UTF8Type.instance.decompose("b"),
                                                                                  UTF8Type.instance.decompose("") }));

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
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_CHIMP,
                                                                new ByteBuffer[]{ null }));

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
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("value", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_DOUBLE_CHIMP,
                                                                new ByteBuffer[]{ DoubleType.instance.decompose(7.5) }));
        columns.put("gone", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_TEXT,
                                                               new ByteBuffer[]{ UTF8Type.instance.decompose("x") }));

        List<Row> rows = decode(ColumnarChunkCodec.encode(ts, 1, columns));
        assertEquals(1, rows.size());
        assertNotNull(rows.get(0).getCell(valueColumn));
        assertEquals(1, rows.get(0).columnCount());
    }

    @Test
    public void corruptPayloadThrows()
    {
        ByteBuffer payload = chunk(10, 1000);
        ByteBuffer corrupt = payload.duplicate();
        corrupt.put(0, (byte) 99);                                   // unknown version byte
        try
        {
            ChunkReadSupport.rowsFromChunk(metadata, corrupt, WRITETIME, Long.MIN_VALUE, Long.MAX_VALUE, false);
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected)
        {
            // decoding must fail loudly - the CALLER decides to skip-and-warn (plan R4)
        }
    }
}
