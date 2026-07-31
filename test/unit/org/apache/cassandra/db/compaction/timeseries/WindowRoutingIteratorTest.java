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

package org.apache.cassandra.db.compaction.timeseries;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the D2 routing rules of {@link WindowRoutingIterator}: every Unfiltered of one
 * partition is routed to the time window of its max cell write-timestamp (rows) or range-tombstone
 * deletion timestamp (markers), preserving clustering order within each output window.
 */
public class WindowRoutingIteratorTest
{
    private static final long HOUR_MS = 3_600_000L;
    /** An arbitrary hour-aligned base well in the past (2020-01-01T00:00Z). */
    private static final long BASE = 1_577_836_800_000L;
    private static final LongUnaryOperator WINDOW = ms -> ms - Math.floorMod(ms, HOUR_MS);

    private static TableMetadata metadata;
    private static ColumnMetadata value;
    private static ColumnMetadata value2;
    private static DecoratedKey key;

    @BeforeClass
    public static void setUpClazz()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("t3ks", "t3tbl")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("tag_id", UTF8Type.instance)
                                .addClusteringColumn("timestamp", TimestampType.instance)
                                .addRegularColumn("value", DoubleType.instance)
                                .addRegularColumn("value2", DoubleType.instance)
                                .build();
        value = metadata.getColumn(ByteBufferUtil.bytes("value"));
        value2 = metadata.getColumn(ByteBufferUtil.bytes("value2"));
        key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes("tag-1"));
    }

    private static Clustering<?> ck(long eventMs)
    {
        return Clustering.make(TimestampType.instance.decompose(new Date(eventMs)));
    }

    private static long micros(long ms)
    {
        return ms * 1000;
    }

    /** Single-cell row whose routing timestamp is the value cell's write timestamp. */
    private static Row row(long eventMs, long writeTsMs)
    {
        return BTreeRow.singleCellRow(ck(eventMs), BufferCell.live(value, micros(writeTsMs), DoubleType.instance.decompose(1.0)));
    }

    private static Row twoCellRow(long eventMs, long writeTsMsA, long writeTsMsB)
    {
        BufferCell a = BufferCell.live(value, micros(writeTsMsA), DoubleType.instance.decompose(1.0));
        BufferCell b = BufferCell.live(value2, micros(writeTsMsB), DoubleType.instance.decompose(2.0));
        return BTreeRow.create(ck(eventMs), org.apache.cassandra.db.LivenessInfo.EMPTY, Row.Deletion.LIVE,
                               BTree.build(List.of(a, b)));
    }

    private static UnfilteredRowIterator partition(List<Unfiltered> content)
    {
        return new Util.UnfilteredSource(metadata, key, null, content.iterator());
    }

    private static NavigableMap<Long, List<Unfiltered>> route(List<Unfiltered> content)
    {
        return WindowRoutingIterator.route(partition(content), WINDOW, TimeUnit.MICROSECONDS);
    }

    @Test
    public void singleWindowRowsStayTogether()
    {
        List<Unfiltered> rows = List.of(row(BASE + 1, BASE + 1), row(BASE + 2, BASE + 2), row(BASE + 3, BASE + 3));
        NavigableMap<Long, List<Unfiltered>> routed = route(rows);
        assertEquals(1, routed.size());
        List<Unfiltered> only = routed.get(BASE);
        assertEquals(3, only.size());
        for (int i = 0; i < 3; i++)
            assertSame(rows.get(i), only.get(i));
    }

    @Test
    public void rowsSplitAcrossTwoWindows()
    {
        Row w1a = row(BASE + 1, BASE + 1);
        Row w2 = row(BASE + 2, BASE + HOUR_MS + 5);   // late event written in the next window
        Row w1b = row(BASE + 3, BASE + 10);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(w1a, w2, w1b));
        assertEquals(2, routed.size());
        assertEquals(List.of(w1a, w1b), routed.get(BASE));
        assertEquals(List.of(w2), routed.get(BASE + HOUR_MS));
    }

    @Test
    public void multiCellRowRoutesByMaxCellTimestamp()
    {
        Row r = twoCellRow(BASE + 1, BASE + 1, BASE + HOUR_MS + 1);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(1, routed.size());
        assertEquals(List.of(r), routed.get(BASE + HOUR_MS));
    }

    @Test
    public void rowDeletionOnlyRoutesByDeletionTime()
    {
        long delMs = BASE + HOUR_MS + 42;
        Row tombstone = BTreeRow.emptyDeletedRow(ck(BASE + 1), Row.Deletion.regular(DeletionTime.build(micros(delMs), 1000)));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(tombstone));
        assertEquals(1, routed.size());
        assertEquals(List.of(tombstone), routed.get(BASE + HOUR_MS));
    }

    @Test
    public void openClosePairTravelTogether()
    {
        long delMs = BASE + 7;
        DeletionTime del = DeletionTime.build(micros(delMs), 1000);
        Unfiltered open = RangeTombstoneBoundMarker.inclusiveOpen(false, ck(BASE + 1), del);
        Row mid = row(BASE + 2, BASE + 2);
        Unfiltered close = RangeTombstoneBoundMarker.inclusiveClose(false, ck(BASE + 3), del);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(open, mid, close));
        assertEquals(1, routed.size());
        assertEquals(List.of(open, mid, close), routed.get(BASE));
    }

    @Test
    public void boundaryMarkerSplitsWhenDeletionsDiverge()
    {
        long closeDelMs = BASE + 5;                    // window 1
        long openDelMs = BASE + HOUR_MS + 5;           // window 2
        RangeTombstoneBoundaryMarker boundary =
            RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false,
                                                                     ck(BASE + 30),
                                                                     DeletionTime.build(micros(closeDelMs), 1000),
                                                                     DeletionTime.build(micros(openDelMs), 1000));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(boundary));
        assertEquals(2, routed.size());

        List<Unfiltered> w1 = routed.get(BASE);
        assertEquals(1, w1.size());
        RangeTombstoneBoundMarker closeMarker = (RangeTombstoneBoundMarker) w1.get(0);
        assertTrue(closeMarker.isClose(false));
        assertEquals(micros(closeDelMs), closeMarker.deletionTime().markedForDeleteAt());

        List<Unfiltered> w2 = routed.get(BASE + HOUR_MS);
        assertEquals(1, w2.size());
        RangeTombstoneBoundMarker openMarker = (RangeTombstoneBoundMarker) w2.get(0);
        assertTrue(openMarker.isOpen(false));
        assertEquals(micros(openDelMs), openMarker.deletionTime().markedForDeleteAt());
    }

    @Test
    public void boundaryMarkerStaysWholeWhenDeletionsShareWindow()
    {
        RangeTombstoneBoundaryMarker boundary =
            RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false,
                                                                     ck(BASE + 30),
                                                                     DeletionTime.build(micros(BASE + 5), 1000),
                                                                     DeletionTime.build(micros(BASE + 9), 1000));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(boundary));
        assertEquals(1, routed.size());
        assertEquals(List.of((Unfiltered) boundary), routed.get(BASE));
    }

    @Test
    public void farFutureRowRoutesToItsOwnWindow()
    {
        long future = BASE + 365L * 24 * HOUR_MS;
        Row r = row(BASE + 1, future + 3);
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(1, routed.size());
        assertEquals(List.of(r), routed.get(WINDOW.applyAsLong(future + 3)));
    }

    @Test
    public void exactBoundaryTimestampRoutesToItsWindow()
    {
        Row r = row(BASE + 1, BASE + HOUR_MS);         // written exactly at the boundary
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(1, routed.size());
        assertEquals(List.of(r), routed.get(BASE + HOUR_MS));
    }

    @Test
    public void livenessInfoBeatsOlderCells()
    {
        BufferCell old = BufferCell.live(value, micros(BASE + 1), DoubleType.instance.decompose(1.0));
        Row r = BTreeRow.create(ck(BASE + 1),
                                org.apache.cassandra.db.LivenessInfo.create(micros(BASE + HOUR_MS + 8)),
                                Row.Deletion.LIVE,
                                BTree.build(List.of(old)));
        NavigableMap<Long, List<Unfiltered>> routed = route(List.of(r));
        assertEquals(1, routed.size());
        assertEquals(List.of(r), routed.get(BASE + HOUR_MS));
    }

    @Test
    public void clusteringOrderPreservedWithinEachWindow()
    {
        List<Unfiltered> content = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            content.add(row(BASE + i, (i % 2 == 0 ? BASE : BASE + HOUR_MS) + i));
        NavigableMap<Long, List<Unfiltered>> routed = route(content);
        assertEquals(2, routed.size());
        for (List<Unfiltered> bucket : routed.values())
        {
            ByteBuffer prev = null;
            for (Unfiltered u : bucket)
            {
                ByteBuffer cur = ((Row) u).clustering().bufferAt(0);
                if (prev != null)
                    assertTrue(TimestampType.instance.compare(prev, cur) < 0);
                prev = cur;
            }
        }
    }
}
