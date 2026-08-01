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
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SP3 Task 2 / SP4: merge of hot partitions with synthetic chunk rows on the UNFILTERED stream --
 * clustering-ordered two-way merge, hot wins equal-timestamp conflicts (via writetime
 * reconciliation), fully-chunked partitions synthesized in expected-key order.
 */
public class ChunkMergeIteratorTest
{
    private static final long BASE = 1_577_836_800_000L;
    private static final long HOT_WT = 2_000_000L;                   // hot cell writetime (micros)
    private static final long CHUNK_WT = 1_000_000L;                 // chunk max_row_writetime < hot

    private static TableMetadata metadata;
    /** The same table declared {@code WITH CLUSTERING ORDER BY (timestamp DESC)} -- the comparator is reversed. */
    private static TableMetadata descMetadata;
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
        descMetadata = TableMetadata.builder("sp3ks", "sp3merge_desc")
                                    .partitioner(Murmur3Partitioner.instance)
                                    .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                                    .addClusteringColumn("timestamp", ReversedType.getInstance(TimestampType.instance))
                                    .addRegularColumn("value", DoubleType.instance)
                                    .build();
    }

    private static DecoratedKey key(String tag)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(tag));
    }

    private static Row row(long tsMs, double value, long writetime)
    {
        return row(metadata, tsMs, value, writetime);
    }

    private static Row row(TableMetadata md, long tsMs, double value, long writetime)
    {
        return BTreeRow.singleCellRow(Clustering.make(TimestampType.instance.decompose(new Date(tsMs))),
                                      BufferCell.live(md.getColumn(ByteBufferUtil.bytes("value")),
                                                      writetime, DoubleType.instance.decompose(value)));
    }

    private static UnfilteredRowIterator partition(DecoratedKey key, boolean reversed, List<Row> rows)
    {
        return partition(metadata, key, reversed, rows);
    }

    private static UnfilteredRowIterator partition(TableMetadata md, DecoratedKey key, boolean reversed, List<Row> rows)
    {
        Iterator<Row> it = rows.iterator();
        return new org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator(md, key, DeletionTime.LIVE,
                                                                             md.regularAndStaticColumns(),
                                                                             Rows.EMPTY_STATIC_ROW, reversed,
                                                                             EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext() { return it.hasNext() ? it.next() : endOfData(); }
        };
    }

    /** Wraps a partition so a test can assert it was closed. */
    private static final class CloseTracking implements org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;
        boolean closed;

        CloseTracking(UnfilteredRowIterator wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        @Override
        public void close()
        {
            closed = true;
            wrapped.close();
        }
    }

    private static UnfilteredPartitionIterator partitions(List<UnfilteredRowIterator> parts)
    {
        return partitions(metadata, parts);
    }

    private static UnfilteredPartitionIterator partitions(TableMetadata md, List<UnfilteredRowIterator> parts)
    {
        Iterator<UnfilteredRowIterator> it = parts.iterator();
        return new UnfilteredPartitionIterator()
        {
            public TableMetadata metadata() { return md; }
            public boolean hasNext() { return it.hasNext(); }
            public UnfilteredRowIterator next() { return it.next(); }
            public void close() {}
        };
    }

    /** Drains the merged iterator into tag → list of (tsMs, value) for easy assertions. */
    private static List<String> drain(UnfilteredPartitionIterator merged)
    {
        return drain(metadata, merged);
    }

    private static List<String> drain(TableMetadata md, UnfilteredPartitionIterator merged)
    {
        ColumnMetadata column = md.getColumn(ByteBufferUtil.bytes("value"));
        List<String> result = new ArrayList<>();
        while (merged.hasNext())
        {
            try (UnfilteredRowIterator p = merged.next())
            {
                StringBuilder sb = new StringBuilder(UTF8Type.instance.compose(p.partitionKey().getKey()));
                while (p.hasNext())
                {
                    Unfiltered u = p.next();
                    if (!u.isRow())
                    {
                        // Range tombstone markers are Unfiltered too; rendering them keeps drain()
                        // total (it used to ClassCastException on one) and lets a test assert on them.
                        sb.append(" |marker|");
                        continue;
                    }
                    Row r = (Row) u;
                    long ts = TimestampType.instance.compose(r.clustering().bufferAt(0)).getTime() - BASE;
                    Cell<?> cell = r.getCell(column);
                    sb.append(' ').append(ts).append(':').append(cell == null ? "-" : DoubleType.instance.compose(cell.buffer()));
                }
                result.add(sb.toString());
            }
        }
        return result;
    }

    @Test
    public void hotOnlyPassthrough()
    {
        // With no chunk rows for the key there is nothing to merge, and the requirement is stronger
        // than "the right rows come out": the hot partition must be handed straight back, unwrapped.
        // Asserting only on the drained rows passes even if the merge machinery runs over an empty
        // second source, which is what this test used to do -- it exercised nothing.
        DecoratedKey k = key("t1");
        UnfilteredRowIterator hotPartition = partition(k, false, List.of(row(BASE + 1, 1.0, HOT_WT)));
        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(List.of(hotPartition)), List.of(k), dk -> List.of(), metadata, false);

        assertTrue(merged.hasNext());
        assertSame("a partition with no chunk rows must not be wrapped in a merge", hotPartition, merged.next());
        assertFalse(merged.hasNext());
    }

    /**
     * The ASC/DESC x reversed matrix. {@code CLUSTERING ORDER BY (ts DESC)} wraps the clustering type
     * in {@link ReversedType}, so comparator order is already newest-first and the read's
     * {@code reversed} flag flips it again -- four combinations, and the synthetic chunk iterator has
     * to report the same {@code isReverseOrder()} as the hot one in every single one of them or the
     * merge compares clusterings the wrong way round. The previous coverage was one quadrant
     * (ASC x reversed).
     */
    @Test
    public void ascComparatorForwardRead()
    {
        assertQuadrant(metadata, false, List.of(2L, 4L), List.of(1L, 3L), "t1 1:10.0 2:20.0 3:30.0 4:40.0");
    }

    @Test
    public void ascComparatorReversedRead()
    {
        assertQuadrant(metadata, true, List.of(4L, 2L), List.of(3L, 1L), "t1 4:40.0 3:30.0 2:20.0 1:10.0");
    }

    @Test
    public void descComparatorForwardRead()
    {
        // Comparator order on a DESC table is already newest-first, so an unreversed read emits 4,3,2,1.
        assertQuadrant(descMetadata, false, List.of(4L, 2L), List.of(3L, 1L), "t1 4:40.0 3:30.0 2:20.0 1:10.0");
    }

    @Test
    public void descComparatorReversedRead()
    {
        assertQuadrant(descMetadata, true, List.of(2L, 4L), List.of(1L, 3L), "t1 1:10.0 2:20.0 3:30.0 4:40.0");
    }

    /**
     * Builds a hot partition from {@code hotOffsets} and chunk rows from {@code chunkOffsets} (each
     * already in the iteration order that quadrant implies), merges them and asserts the emitted
     * order. Values are the offset x 10 so every row is identifiable.
     */
    private static void assertQuadrant(TableMetadata md, boolean reversed,
                                       List<Long> hotOffsets, List<Long> chunkOffsets, String expected)
    {
        DecoratedKey k = key("t1");
        List<Row> hotRows = new ArrayList<>();
        for (long offset : hotOffsets)
            hotRows.add(row(md, BASE + offset, offset * 10.0, HOT_WT));
        List<Row> chunkRows = new ArrayList<>();
        for (long offset : chunkOffsets)
            chunkRows.add(row(md, BASE + offset, offset * 10.0, CHUNK_WT));

        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(md, List.of(partition(md, k, reversed, hotRows))),
            List.of(k), dk -> chunkRows, md, reversed);
        assertEquals(List.of(expected), drain(md, merged));
    }

    /**
     * The chunk-key list and the hot iterator must agree on partition order. They do for every path
     * wired today, but {@code SinglePartitionReadQuery.Group#executeLocally} sorts by token while
     * {@code queries} is sorted by partition-key value, so this is one wiring change away -- and the
     * old "synthesize and carry on" handling answered a divergence by emitting a partition TWICE
     * (once chunk-only, once merged). Duplicated partitions are a wrong answer nothing downstream
     * rejects, so the invariant fails loudly instead.
     */
    @Test
    public void hotPartitionOutOfExpectedKeyOrderFailsLoudly()
    {
        DecoratedKey k1 = key("t1");
        DecoratedKey k2 = key("t2");
        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(List.of(partition(k2, false, List.of(row(BASE + 2, 2.0, HOT_WT))),
                               partition(k1, false, List.of(row(BASE + 1, 1.0, HOT_WT))))),
            List.of(k1, k2),                                 // ...but the expected order is t1, t2
            dk -> List.of(row(BASE, 0.0, CHUNK_WT)), metadata, false);

        try
        {
            drain(merged);
            fail("expected the key-order violation to fail the read rather than duplicate a partition");
        }
        catch (IllegalStateException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("out of the order"));
        }
    }

    @Test
    public void interleavedMergeKeepsClusteringOrder()
    {
        DecoratedKey k = key("t1");
        List<Row> chunk = List.of(row(BASE + 1, 10.0, CHUNK_WT), row(BASE + 3, 30.0, CHUNK_WT));
        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(List.of(partition(k, false, List.of(row(BASE + 2, 20.0, HOT_WT), row(BASE + 4, 40.0, HOT_WT))))),
            List.of(k), dk -> chunk, metadata, false);
        assertEquals(List.of("t1 1:10.0 2:20.0 3:30.0 4:40.0"), drain(merged));
    }

    @Test
    public void equalTimestampHotWins()
    {
        DecoratedKey k = key("t1");
        List<Row> chunk = List.of(row(BASE + 1, 999.0, CHUNK_WT));
        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(List.of(partition(k, false, List.of(row(BASE + 1, 1.0, HOT_WT))))),
            List.of(k), dk -> chunk, metadata, false);
        assertEquals(List.of("t1 1:1.0"), drain(merged));
    }

    @Test
    public void fullyChunkedPartitionIsSynthesized()
    {
        DecoratedKey k = key("t1");
        List<Row> chunk = List.of(row(BASE + 1, 10.0, CHUNK_WT), row(BASE + 2, 20.0, CHUNK_WT));
        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
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
        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(List.of(partition(k1, false, List.of(row(BASE + 1, 1.0, HOT_WT))),
                               partition(k3, false, List.of(row(BASE + 3, 3.0, HOT_WT))))),
            List.of(k1, k2, k3), chunks::get, metadata, false);
        assertEquals(List.of("t1 1:1.0", "t2 5:50.0", "t3 3:3.0"), drain(merged));
    }

    // (reversedMerge used to live here; it is exactly the ASC x reversed quadrant of the matrix above,
    // which now also covers the two DESC-comparator quadrants it never reached.)

    @Test
    public void rangeTombstoneMarkersFromTheHotSideSurviveTheMerge()
    {
        // The whole reason the merge moved onto the unfiltered stream: deletion information must
        // still be in the output for Filter/reconciliation downstream to act on. A marker from the
        // hot side must pass through the merge, not be dropped or crash it.
        DecoratedKey k = key("t1");
        DeletionTime deletion = DeletionTime.build(HOT_WT, 1);
        Clustering<?> from = Clustering.make(TimestampType.instance.decompose(new Date(BASE + 2)));
        Clustering<?> to = Clustering.make(TimestampType.instance.decompose(new Date(BASE + 3)));
        RangeTombstoneBoundMarker open = RangeTombstoneBoundMarker.inclusiveOpen(false, from, deletion);
        RangeTombstoneBoundMarker close = RangeTombstoneBoundMarker.inclusiveClose(false, to, deletion);

        List<Unfiltered> hotUnfiltered = new ArrayList<>();
        hotUnfiltered.add(row(BASE + 1, 1.0, HOT_WT));
        hotUnfiltered.add(open);
        hotUnfiltered.add(close);
        Iterator<Unfiltered> hotContents = hotUnfiltered.iterator();
        UnfilteredRowIterator hotPartition =
            new org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator(metadata, k, DeletionTime.LIVE,
                                                                           metadata.regularAndStaticColumns(),
                                                                           Rows.EMPTY_STATIC_ROW, false,
                                                                           EncodingStats.NO_STATS)
            {
                protected Unfiltered computeNext() { return hotContents.hasNext() ? hotContents.next() : endOfData(); }
            };

        // Two chunk rows: one at +2, inside the tombstoned range and older than it, and one at +4,
        // outside it.
        List<Row> chunk = List.of(row(BASE + 2, 99.0, CHUNK_WT), row(BASE + 4, 44.0, CHUNK_WT));
        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(List.of(hotPartition)), List.of(k), dk -> chunk, metadata, false);

        String out = drain(merged).get(0);
        // The markers pass through, so the deletion information reaches Filter/reconciliation...
        assertTrue(out, out.contains("|marker|"));
        // ...and the reconciliation the merge performs has already eliminated the chunk row the
        // range tombstone covers -- which is the entire reason the merge moved onto the unfiltered
        // stream. Post-filter, the tombstone would have been purged and 99.0 would have come back.
        assertFalse(out, out.contains("2:99.0"));
        // A chunk row outside the tombstoned range is untouched, as is the hot row.
        assertTrue(out, out.contains("4:44.0"));
        assertTrue(out, out.contains("1:1.0"));
    }

    @Test
    public void closeReleasesEveryPartitionItStillHolds()
    {
        // A limit can stop the read after one partition while the next hot partition is already
        // peeked. Dropping it leaks the SSTable/memtable iterator's ref-count and file descriptor.
        DecoratedKey k1 = key("t1");                     // fully chunked: no hot partition
        DecoratedKey k2 = key("t2");                     // hot partition, peeked but never consumed
        CloseTracking k2Hot = new CloseTracking(partition(k2, false, List.of(row(BASE + 5, 5.0, HOT_WT))));

        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(List.of(k2Hot)), List.of(k1, k2),
            dk -> dk.equals(k1) ? List.of(row(BASE + 1, 1.0, CHUNK_WT)) : List.of(), metadata, false);

        assertTrue(merged.hasNext());
        merged.next().close();                           // take k1's synthetic partition and stop
        assertFalse("k2's hot partition must still be held at this point", k2Hot.closed);

        merged.close();
        assertTrue("close() must release the partition it was still holding", k2Hot.closed);
    }

    @Test
    public void keyWithNoDataAnywhereIsSkipped()
    {
        DecoratedKey k1 = key("t1");
        DecoratedKey k2 = key("t2");
        UnfilteredPartitionIterator merged = ChunkMergeUnfilteredIterator.wrap(
            partitions(List.of(partition(k2, false, List.of(row(BASE + 1, 1.0, HOT_WT))))),
            List.of(k1, k2), dk -> List.of(), metadata, false);
        assertEquals(List.of("t2 1:1.0"), drain(merged));
        assertFalse(merged.hasNext());
    }
}
