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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
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
import org.apache.cassandra.utils.Throwables;

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
 * <p>
 * <b>That ordering is an enforced invariant, not an assumption.</b> If the hot iterator ever yields a
 * key {@code expectedKeys} has already passed -- which is one wiring change away, since
 * {@code SinglePartitionReadQuery.Group#executeLocally} sorts by <em>token</em> while {@code queries}
 * is sorted by partition-key <em>value</em> -- the old "synthesize and carry on" handling emitted that
 * partition <b>twice</b>: once chunk-only, once merged. Two partitions with the same key in one result
 * is silent duplication that no downstream stage rejects, so a divergence fails the query instead (see
 * {@link #outOfOrder}).
 */
public final class ChunkMergeUnfilteredIterator implements UnfilteredPartitionIterator
{
    private final UnfilteredPartitionIterator hot;
    private final Iterator<DecoratedKey> expectedKeys;
    /** The expected keys not yet consumed, for the order check -- see {@link #outOfOrder}. */
    private final Set<DecoratedKey> pendingKeys;
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
        this.pendingKeys = new HashSet<>(expectedKeys);
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
                if (peekedHot == null)
                    return false;
                // A hot partition left over after every expected key has been consumed: either its key
                // was never requested, or it arrived after the position expectedKeys gave it. Merging
                // it here would emit a partition this iterator has already emitted chunk-only.
                throw outOfOrder(peekedHot.partitionKey());
            }

            DecoratedKey expected = expectedKeys.next();
            pendingKeys.remove(expected);
            if (peekedHot != null && peekedHot.partitionKey().equals(expected))
            {
                next = merge(peekedHot, chunkRows.apply(expected));
                peekedHot = null;
            }
            else
            {
                // Skipping `expected` is only legitimate if the peeked hot partition is still ahead of
                // us in expectedKeys, i.e. the hot side simply has nothing for `expected` (a fully
                // chunked tag). A peeked key that is NOT still pending means the two orders diverged.
                if (peekedHot != null && !pendingKeys.contains(peekedHot.partitionKey()))
                    throw outOfOrder(peekedHot.partitionKey());

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
        // Everything held must be closed, not just `hot`. A limit (or an exception out of chunkRows,
        // including the deliberate UnsupportedChunkFormatException rethrow) can stop the read while a
        // peeked hot partition or an already-built merged partition is still open, and those wrap
        // live SSTable/memtable iterators -- dropping them leaks the ref-count and the file
        // descriptor ("LEAK DETECTED"). Cassandra's own BaseIterator.close() closes its held `next`
        // for exactly this reason. `hot` is closed last and unconditionally.
        Throwable failure = closeQuietly(peekedHot, null);
        failure = closeQuietly(next, failure);
        peekedHot = null;
        next = null;
        failure = closeQuietly(hot, failure);
        Throwables.maybeFail(failure);
    }

    /**
     * The hot iterator and {@code expectedKeys} disagree about partition order. Reported rather than
     * worked around: the only ways to "continue" are to emit the partition twice or to drop one of the
     * two copies, and a duplicated or dropped partition is a wrong answer that nothing downstream can
     * detect. Every path wired today (the local group read and the coordinator's per-command resolve)
     * satisfies the invariant, so this firing means a caller changed.
     */
    private IllegalStateException outOfOrder(DecoratedKey key)
    {
        return new IllegalStateException(
            String.format("Tiered read of %s.%s: the hot partition iterator yielded %s out of the order the read's " +
                          "partition keys were given in. The chunk merge cannot place it without emitting some " +
                          "partition twice, so the query is failed instead of returning duplicated rows.",
                          metadata.keyspace, metadata.name, key));
    }

    private static Throwable closeQuietly(AutoCloseable closeable, Throwable failure)
    {
        if (closeable == null)
            return failure;
        try
        {
            closeable.close();
        }
        catch (Throwable t)
        {
            return Throwables.merge(failure, t);
        }
        return failure;
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
