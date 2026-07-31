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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.ChunkCodecs;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SP3 Task 1: decoding a chunk payload into synthetic CQL rows for the transparent-read merge.
 * Contract (plan R3): clustering = sample timestamp, single value cell with
 * writetime = the chunk's max_row_writetime, range filter [startMs, endMsExcl), optional reverse.
 */
public class ChunkReadSupportTest
{
    private static final long BASE = 1_577_836_800_000L;             // 2020-01-01T00:00Z
    private static final long WRITETIME = 1_650_000_000_000_000L;    // micros

    private static TableMetadata metadata;
    private static ColumnMetadata valueColumn;

    @BeforeClass
    public static void setUpClazz()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("sp3ks", "sp3tbl")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                                .addClusteringColumn("timestamp", TimestampType.instance)
                                .addRegularColumn("value", DoubleType.instance)
                                .build();
        valueColumn = metadata.getColumn(ByteBufferUtil.bytes("value"));
    }

    private static ByteBuffer chunk(int count, long stepMs)
    {
        long[] ts = new long[count];
        double[] vs = new double[count];
        for (int i = 0; i < count; i++)
        {
            ts[i] = BASE + i * stepMs;
            vs[i] = 20.0 + i * 0.1;
        }
        return ChunkCodecs.encode(ts, vs, count);
    }

    private static void assertRow(Row row, long expectedTsMs, double expectedValue)
    {
        assertEquals(TimestampType.instance.decompose(new Date(expectedTsMs)), row.clustering().bufferAt(0));
        Cell<?> cell = row.getCell(valueColumn);
        assertEquals(expectedValue, DoubleType.instance.compose(cell.buffer()), 0.0);
        assertEquals(WRITETIME, cell.timestamp());
    }

    @Test
    public void roundTripFullRange()
    {
        ByteBuffer payload = chunk(100, 1000);
        List<Row> rows = ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, Long.MIN_VALUE, Long.MAX_VALUE, false);
        assertEquals(100, rows.size());
        assertRow(rows.get(0), BASE, 20.0);
        assertRow(rows.get(99), BASE + 99_000, 20.0 + 99 * 0.1);
    }

    @Test
    public void roundTripHonoursTheChunkStep()
    {
        ByteBuffer payload = chunk(50, 2000);
        List<Row> rows = ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, Long.MIN_VALUE, Long.MAX_VALUE, false);
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
        ByteBuffer payload = chunk(5, 1000);
        List<Row> rows = ChunkReadSupport.rowsFromChunk(metadata, payload, WRITETIME, Long.MIN_VALUE, Long.MAX_VALUE, true);
        assertEquals(5, rows.size());
        assertRow(rows.get(0), BASE + 4000, 20.4);
        assertRow(rows.get(4), BASE, 20.0);
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
