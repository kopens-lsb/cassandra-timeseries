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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Merges rows decoded from the {@code __chunks} shadow table into a read's <b>unfiltered</b>
 * partition iterator, i.e. while range tombstones, row deletions and cell tombstones are still in
 * the stream.
 * <p>
 * The timing is the whole point. The synthetic chunk rows go to
 * {@link UnfilteredRowIterators#merge(List)} as just another source, so Cassandra's ordinary
 * reconciliation decides what survives -- a partition deletion, a range tombstone covering the
 * sample's clustering, a row deletion or a newer cell tombstone all shadow the reconstructed data
 * exactly as they would shadow a real base row. Merging after {@code Filter} has run (what SP3
 * originally did) cannot honour a delete at all: {@code Filter} purges every tombstone and drops
 * every row left empty, so the merge would be handed a stream with no deletion information in it.
 * <p>
 * This works only because the reconstruction post-dates the re-encoder's own delete: see
 * {@link ChunkReadSupport}, which stamps every synthetic row at {@code max_row_writetime + 1}. At
 * {@code max_row_writetime} the tiering tombstone -- issued at exactly that timestamp -- would
 * shadow everything this class produces.
 * <p>
 * Partitions the hot iterator does not contain at all (fully chunked tags) are synthesized in the
 * position given by {@code expectedKeys}, which must list the query's partition keys in the same
 * order the hot iterator yields them (the read command/group order).
 */
public final class ChunkMergeUnfilteredIterator implements UnfilteredPartitionIterator
{
    private final UnfilteredPartitionIterator hot;
    private final Iterator<DecoratedKey> expectedKeys;
    private final Function<DecoratedKey, List<Row>> chunkRows;
    private final TableMetadata metadata;
    private final boolean reversed;

    private UnfilteredRowIterator peekedHot;
    private UnfilteredRowIterator next;

    private ChunkMergeUnfilteredIterator(UnfilteredPartitionIterator hot,
                                         List<DecoratedKey> expectedKeys,
                                         Function<DecoratedKey, List<Row>> chunkRows,
                                         TableMetadata metadata,
                                         boolean reversed)
    {
        this.hot = hot;
        this.expectedKeys = expectedKeys.iterator();
        this.chunkRows = chunkRows;
        this.metadata = metadata;
        this.reversed = reversed;
    }

    /**
     * @param expectedKeys the query's partition keys, in the order the hot iterator yields them
     * @param chunkRows    per-key synthetic rows (already range-filtered and ordered to match
     *                     {@code reversed}); an empty list means "no chunks for this key"
     * @param reversed     whether the read runs in reverse clustering-comparator order, i.e. the hot
     *                     iterators' {@code isReverseOrder()}; the synthetic iterator must report the
     *                     same or {@code merge} compares clusterings the wrong way round
     */
    public static UnfilteredPartitionIterator wrap(UnfilteredPartitionIterator hot,
                                                   List<DecoratedKey> expectedKeys,
                                                   Function<DecoratedKey, List<Row>> chunkRows,
                                                   TableMetadata metadata,
                                                   boolean reversed)
    {
        return new ChunkMergeUnfilteredIterator(hot, expectedKeys, chunkRows, metadata, reversed);
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public boolean hasNext()
    {
        while (next == null)
        {
            if (peekedHot == null && hot.hasNext())
                peekedHot = hot.next();

            if (!expectedKeys.hasNext())
            {
                // Defensive: hot partitions beyond the expected keys still merge (should not happen --
                // hot results are a subset of the requested keys).
                if (peekedHot == null)
                    return false;
                next = merge(peekedHot, chunkRows.apply(peekedHot.partitionKey()));
                peekedHot = null;
                break;
            }

            DecoratedKey expected = expectedKeys.next();
            if (peekedHot != null && peekedHot.partitionKey().equals(expected))
            {
                next = merge(peekedHot, chunkRows.apply(expected));
                peekedHot = null;
            }
            else
            {
                List<Row> synthetic = chunkRows.apply(expected);
                if (!synthetic.isEmpty())
                    next = synthetic(metadata, expected, reversed, synthetic);
                // else: no hot rows and no chunks for this key - nothing to emit, try the next key
            }
        }
        return true;
    }

    @Override
    public UnfilteredRowIterator next()
    {
        if (next == null && !hasNext())
            throw new NoSuchElementException();
        UnfilteredRowIterator result = next;
        next = null;
        return result;
    }

    @Override
    public void close()
    {
        hot.close();
    }

    private UnfilteredRowIterator merge(UnfilteredRowIterator hotPartition, List<Row> synthetic)
    {
        if (synthetic.isEmpty())
            return hotPartition;

        List<UnfilteredRowIterator> sources = new ArrayList<>(2);
        sources.add(hotPartition);
        // isReverseOrder must match the hot iterator's own, not the wrapper's idea of it, or the
        // merge compares clusterings in one order while a source produced them in the other.
        sources.add(synthetic(metadata, hotPartition.partitionKey(), hotPartition.isReverseOrder(), synthetic));
        return UnfilteredRowIterators.merge(sources);
    }

    /** A partition made only of chunk-decoded rows: no partition deletion, no static row, no tombstones. */
    private static UnfilteredRowIterator synthetic(TableMetadata metadata,
                                                   DecoratedKey key,
                                                   boolean reversed,
                                                   List<Row> rows)
    {
        Iterator<Row> iterator = rows.iterator();
        return new AbstractUnfilteredRowIterator(metadata,
                                                 key,
                                                 DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(),
                                                 Rows.EMPTY_STATIC_ROW,
                                                 reversed,
                                                 EncodingStats.NO_STATS)
        {
            @Override
            protected Unfiltered computeNext()
            {
                return iterator.hasNext() ? iterator.next() : endOfData();
            }
        };
    }
}
