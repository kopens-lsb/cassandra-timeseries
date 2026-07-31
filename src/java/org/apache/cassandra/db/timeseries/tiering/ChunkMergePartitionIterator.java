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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.schema.TableMetadata;

/**
 * SP3 transparent reads: decorates a coordinator-side {@link PartitionIterator} (hot rows, already
 * filtered) by merging in synthetic rows decoded from the {@code __chunks} shadow table (design
 * spec section 3.3.1). Within a partition, rows merge in clustering order; equal-clustering
 * conflicts reconcile through {@link Rows#merge}, whose timestamp rules pick the hot row (its
 * writetime is always greater than the chunk's max_row_writetime - see {@link ChunkReadSupport}).
 *
 * Partitions the hot iterator does not contain at all (fully chunked tags) are synthesized in the
 * position given by {@code expectedKeys}, which must list the query's partition keys in the same
 * order the hot iterator yields them (the read group's command order).
 */
public final class ChunkMergePartitionIterator implements PartitionIterator
{
    private final PartitionIterator hot;
    private final Iterator<DecoratedKey> expectedKeys;
    private final Function<DecoratedKey, List<Row>> chunkRows;
    private final TableMetadata metadata;
    private final boolean reversed;

    private RowIterator peekedHot;
    private RowIterator next;

    private ChunkMergePartitionIterator(PartitionIterator hot,
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
     * @param chunkRows    per-key synthetic rows (already range-filtered and ordered per
     *                     {@code reversed}); an empty list means "no chunks for this key"
     */
    public static PartitionIterator wrap(PartitionIterator hot,
                                         List<DecoratedKey> expectedKeys,
                                         Function<DecoratedKey, List<Row>> chunkRows,
                                         TableMetadata metadata,
                                         boolean reversed)
    {
        return new ChunkMergePartitionIterator(hot, expectedKeys, chunkRows, metadata, reversed);
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
                // Defensive: hot partitions beyond the expected keys still merge (should not happen -
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
                    next = new SyntheticRowIterator(metadata, expected, reversed, synthetic.iterator());
                // else: no hot rows and no chunks for this key - nothing to emit, try the next key
            }
        }
        return true;
    }

    @Override
    public RowIterator next()
    {
        if (next == null && !hasNext())
            throw new NoSuchElementException();
        RowIterator result = next;
        next = null;
        return result;
    }

    @Override
    public void close()
    {
        hot.close();
    }

    private RowIterator merge(RowIterator hotPartition, List<Row> synthetic)
    {
        if (synthetic.isEmpty())
            return hotPartition;
        return new MergedRowIterator(hotPartition, synthetic.iterator());
    }

    /** Two-way sorted merge of a hot partition with synthetic chunk rows; ties reconcile via {@link Rows#merge}. */
    private final class MergedRowIterator implements RowIterator
    {
        private final RowIterator hotPartition;
        private final Iterator<Row> synthetic;
        private Row pendingHot;
        private Row pendingSynthetic;
        private Row nextRow;

        MergedRowIterator(RowIterator hotPartition, Iterator<Row> synthetic)
        {
            this.hotPartition = hotPartition;
            this.synthetic = synthetic;
        }

        @Override
        public boolean hasNext()
        {
            if (nextRow != null)
                return true;
            if (pendingHot == null && hotPartition.hasNext())
                pendingHot = hotPartition.next();
            if (pendingSynthetic == null && synthetic.hasNext())
                pendingSynthetic = synthetic.next();

            if (pendingHot == null && pendingSynthetic == null)
                return false;
            if (pendingHot == null)
            {
                nextRow = pendingSynthetic;
                pendingSynthetic = null;
            }
            else if (pendingSynthetic == null)
            {
                nextRow = pendingHot;
                pendingHot = null;
            }
            else
            {
                int cmp = metadata.comparator.compare(pendingHot.clustering(), pendingSynthetic.clustering());
                if (reversed)
                    cmp = -cmp;
                if (cmp < 0)
                {
                    nextRow = pendingHot;
                    pendingHot = null;
                }
                else if (cmp > 0)
                {
                    nextRow = pendingSynthetic;
                    pendingSynthetic = null;
                }
                else
                {
                    nextRow = Rows.merge(pendingSynthetic, pendingHot);
                    pendingHot = null;
                    pendingSynthetic = null;
                }
            }
            return true;
        }

        @Override
        public Row next()
        {
            if (nextRow == null && !hasNext())
                throw new NoSuchElementException();
            Row result = nextRow;
            nextRow = null;
            return result;
        }

        @Override
        public TableMetadata metadata()
        {
            return hotPartition.metadata();
        }

        @Override
        public boolean isReverseOrder()
        {
            return hotPartition.isReverseOrder();
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return hotPartition.columns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return hotPartition.partitionKey();
        }

        @Override
        public Row staticRow()
        {
            return hotPartition.staticRow();
        }

        @Override
        public void close()
        {
            hotPartition.close();
        }
    }

    /** A partition that exists only in chunk form (all its base rows were re-encoded and deleted). */
    private final class SyntheticRowIterator implements RowIterator
    {
        private final TableMetadata tableMetadata;
        private final DecoratedKey key;
        private final boolean reverse;
        private final Iterator<Row> rows;

        SyntheticRowIterator(TableMetadata tableMetadata, DecoratedKey key, boolean reverse, Iterator<Row> rows)
        {
            this.tableMetadata = tableMetadata;
            this.key = key;
            this.reverse = reverse;
            this.rows = rows;
        }

        @Override
        public boolean hasNext()
        {
            return rows.hasNext();
        }

        @Override
        public Row next()
        {
            return rows.next();
        }

        @Override
        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return reverse;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return tableMetadata.regularAndStaticColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public Row staticRow()
        {
            return Rows.EMPTY_STATIC_ROW;
        }

        @Override
        public void close()
        {
        }
    }
}
