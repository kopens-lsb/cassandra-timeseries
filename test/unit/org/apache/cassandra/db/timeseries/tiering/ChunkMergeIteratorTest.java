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

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * SP3 Task 2: coordinator merge of hot partitions with synthetic chunk rows — clustering-ordered
 * two-way merge, hot wins equal-timestamp conflicts (via writetime reconciliation), fully-chunked
 * partitions synthesized in expected-key order.
 */
public class ChunkMergeIteratorTest
{
    private static final long BASE = 1_577_836_800_000L;
    private static final long HOT_WT = 2_000_000L;                   // hot cell writetime (micros)
    private static final long CHUNK_WT = 1_000_000L;                 // chunk max_row_writetime < hot

    private static TableMetadata metadata;
    private static ColumnMetadata valueColumn;

    @BeforeClass
    public static void setUpClazz()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("sp3ks", "sp3merge")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                                .addClusteringColumn("timestamp", TimestampType.instance)
                                .addRegularColumn("value", DoubleType.instance)
                                .build();
        valueColumn = metadata.getColumn(ByteBufferUtil.bytes("value"));
    }

    private static DecoratedKey key(String tag)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(tag));
    }

    private static Row row(long tsMs, double value, long writetime)
    {
        return BTreeRow.singleCellRow(Clustering.make(TimestampType.instance.decompose(new Date(tsMs))),
                                      BufferCell.live(valueColumn, writetime, DoubleType.instance.decompose(value)));
    }

    private static RowIterator partition(DecoratedKey key, boolean reversed, List<Row> rows)
    {
        Iterator<Row> it = rows.iterator();
        return new RowIterator()
        {
            public boolean hasNext() { return it.hasNext(); }
            public Row next() { return it.next(); }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return reversed; }
            public RegularAndStaticColumns columns() { return metadata.regularAndStaticColumns(); }
            public DecoratedKey partitionKey() { return key; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public void close() {}
        };
    }

    private static PartitionIterator partitions(List<RowIterator> parts)
    {
        Iterator<RowIterator> it = parts.iterator();
        return new PartitionIterator()
        {
            public boolean hasNext() { return it.hasNext(); }
            public RowIterator next() { return it.next(); }
            public void close() {}
        };
    }

    /** Drains the merged iterator into tag → list of (tsMs, value) for easy assertions. */
    private static List<String> drain(PartitionIterator merged)
    {
        List<String> result = new ArrayList<>();
        while (merged.hasNext())
        {
            try (RowIterator p = merged.next())
            {
                StringBuilder sb = new StringBuilder(UTF8Type.instance.compose(p.partitionKey().getKey()));
                while (p.hasNext())
                {
                    Row r = p.next();
                    long ts = TimestampType.instance.compose(r.clustering().bufferAt(0)).getTime() - BASE;
                    double v = DoubleType.instance.compose(r.getCell(valueColumn).buffer());
                    sb.append(' ').append(ts).append(':').append(v);
                }
                result.add(sb.toString());
            }
        }
        return result;
    }

    @Test
    public void hotOnlyPassthrough()
    {
        DecoratedKey k = key("t1");
        PartitionIterator merged = ChunkMergePartitionIterator.wrap(
            partitions(List.of(partition(k, false, List.of(row(BASE + 1, 1.0, HOT_WT))))),
            List.of(k), dk -> List.of(), metadata, false);
        assertEquals(List.of("t1 1:1.0"), drain(merged));
    }

    @Test
    public void interleavedMergeKeepsClusteringOrder()
    {
        DecoratedKey k = key("t1");
        List<Row> chunk = List.of(row(BASE + 1, 10.0, CHUNK_WT), row(BASE + 3, 30.0, CHUNK_WT));
        PartitionIterator merged = ChunkMergePartitionIterator.wrap(
            partitions(List.of(partition(k, false, List.of(row(BASE + 2, 20.0, HOT_WT), row(BASE + 4, 40.0, HOT_WT))))),
            List.of(k), dk -> chunk, metadata, false);
        assertEquals(List.of("t1 1:10.0 2:20.0 3:30.0 4:40.0"), drain(merged));
    }

    @Test
    public void equalTimestampHotWins()
    {
        DecoratedKey k = key("t1");
        List<Row> chunk = List.of(row(BASE + 1, 999.0, CHUNK_WT));
        PartitionIterator merged = ChunkMergePartitionIterator.wrap(
            partitions(List.of(partition(k, false, List.of(row(BASE + 1, 1.0, HOT_WT))))),
            List.of(k), dk -> chunk, metadata, false);
        assertEquals(List.of("t1 1:1.0"), drain(merged));
    }

    @Test
    public void fullyChunkedPartitionIsSynthesized()
    {
        DecoratedKey k = key("t1");
        List<Row> chunk = List.of(row(BASE + 1, 10.0, CHUNK_WT), row(BASE + 2, 20.0, CHUNK_WT));
        PartitionIterator merged = ChunkMergePartitionIterator.wrap(
            partitions(List.of()),                                   // hot has nothing at all
            List.of(k), dk -> chunk, metadata, false);
        assertEquals(List.of("t1 1:10.0 2:20.0"), drain(merged));
    }

    @Test
    public void missingPartitionSynthesizedAmongHotOnes()
    {
        DecoratedKey k1 = key("t1");
        DecoratedKey k2 = key("t2");
        DecoratedKey k3 = key("t3");
        Map<DecoratedKey, List<Row>> chunks = Map.of(k1, List.of(),
                                                     k2, List.of(row(BASE + 5, 50.0, CHUNK_WT)),
                                                     k3, List.of());
        PartitionIterator merged = ChunkMergePartitionIterator.wrap(
            partitions(List.of(partition(k1, false, List.of(row(BASE + 1, 1.0, HOT_WT))),
                               partition(k3, false, List.of(row(BASE + 3, 3.0, HOT_WT))))),
            List.of(k1, k2, k3), chunks::get, metadata, false);
        assertEquals(List.of("t1 1:1.0", "t2 5:50.0", "t3 3:3.0"), drain(merged));
    }

    @Test
    public void reversedMerge()
    {
        DecoratedKey k = key("t1");
        List<Row> chunk = new ArrayList<>(List.of(row(BASE + 3, 30.0, CHUNK_WT), row(BASE + 1, 10.0, CHUNK_WT)));
        PartitionIterator merged = ChunkMergePartitionIterator.wrap(
            partitions(List.of(partition(k, true, List.of(row(BASE + 4, 40.0, HOT_WT), row(BASE + 2, 20.0, HOT_WT))))),
            List.of(k), dk -> chunk, metadata, true);
        assertEquals(List.of("t1 4:40.0 3:30.0 2:20.0 1:10.0"), drain(merged));
    }

    @Test
    public void keyWithNoDataAnywhereIsSkipped()
    {
        DecoratedKey k1 = key("t1");
        DecoratedKey k2 = key("t2");
        PartitionIterator merged = ChunkMergePartitionIterator.wrap(
            partitions(List.of(partition(k2, false, List.of(row(BASE + 1, 1.0, HOT_WT))))),
            List.of(k1, k2), dk -> List.of(), metadata, false);
        assertEquals(List.of("t2 1:1.0"), drain(merged));
        assertFalse(merged.hasNext());
    }
}
