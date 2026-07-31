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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;

/**
 * SP3 transparent reads (design spec section 3.3.1): decides whether a SELECT on a tiering-enabled
 * table must merge in decoded chunk rows, fetches and decodes the relevant chunks at the user's
 * consistency level, and wraps the hot {@link PartitionIterator} with the merge decorator.
 *
 * Activation requires ALL of: a valid tiering policy on the table; a single-partition (or IN) read
 * group; a slice or names clustering filter; and a requested time range reaching below the hot
 * boundary. Everything else returns the hot iterator untouched - the non-tiered fast path pays
 * nothing beyond one schema-extension lookup.
 */
public final class TransparentReads
{
    private static final Logger logger = LoggerFactory.getLogger(TransparentReads.class);

    /**
     * The re-encoder's own base-table reads run through the same SELECT machinery; merging chunks
     * into THEM would make already-encoded windows look like live base rows again (re-encode loops,
     * broken idempotency). The tiering machinery brackets its work with this per-thread bypass.
     */
    private static final ThreadLocal<Boolean> INTERNAL_BYPASS = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private TransparentReads()
    {
    }

    public static void enterInternalBypass()
    {
        INTERNAL_BYPASS.set(Boolean.TRUE);
    }

    public static void exitInternalBypass()
    {
        INTERNAL_BYPASS.set(Boolean.FALSE);
    }

    /**
     * @param cl the user query's consistency level, or null for the internal/local execution path
     * @return the merged iterator, or {@code hot} itself when transparent reading does not apply
     */
    public static PartitionIterator maybeWrap(TableMetadata metadata, ReadQuery query, PartitionIterator hot, ConsistencyLevel cl)
    {
        if (INTERNAL_BYPASS.get())
            return hot;

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            return hot;                                   // invalid policy must never break reads
        }
        if (policy == null)
            return hot;
        if (!(query instanceof SinglePartitionReadCommand.Group))
        {
            // Range scans cannot merge chunks (no partition context) and would otherwise silently
            // see hot rows only - warn instead of returning a quietly incomplete answer (v1 scope;
            // spec section 3.3).
            ClientWarn.instance.warn("tiered table " + metadata.keyspace + '.' + metadata.name +
                                     ": range scans read hot rows only; query by partition key for merged hot+cold reads");
            return hot;
        }

        SinglePartitionReadCommand.Group group = (SinglePartitionReadCommand.Group) query;
        if (group.queries.isEmpty())
            return hot;

        // Every timestamp read below assumes exactly one timestamp clustering column -- what
        // TieringPolicy.unsupportedSchemaError enforces before anything is ever chunked. A table
        // carrying the extension but not that shape has no chunks to merge, and must not have its
        // ordinary SELECTs broken by this code path.
        if (metadata.clusteringColumns().size() != 1)
            return hot;

        SinglePartitionReadCommand first = group.queries.get(0);
        ClusteringIndexFilter filter = first.clusteringIndexFilter();
        // WITH CLUSTERING ORDER BY (ts DESC) wraps the clustering type in ReversedType, so both
        // Slices and the names filter's NavigableSet are ordered by DESCENDING timestamp: the
        // comparator-order start bound is then the range's UPPER time bound, and the comparator-order
        // end bound its LOWER one. Reading them as if ascending produces a nonsensical (usually
        // empty) time range and silently serves no cold rows at all -- on exactly the DESC shape that
        // is the dominant time-series idiom.
        boolean clusteringDescending = metadata.clusteringColumns().get(0).isReversedType();
        // The order rows must be EMITTED in, expressed as timestamps: the hot iterator runs in
        // clustering-comparator order unless the filter reverses it, and comparator order is already
        // descending-by-timestamp on a DESC table.
        boolean emitDescending = clusteringDescending != filter.isReversed();

        long startMs;
        long endMsExcl;
        NavigableSet<Clustering<?>> exactClusterings = null;
        if (filter instanceof ClusteringIndexSliceFilter)
        {
            Slices slices = ((ClusteringIndexSliceFilter) filter).requestedSlices();
            if (slices.isEmpty())
                return hot;
            ClusteringBound<?> comparatorStart = slices.get(0).start();
            ClusteringBound<?> comparatorEnd = slices.get(slices.size() - 1).end();
            startMs = lowerBoundMs(clusteringDescending ? comparatorEnd : comparatorStart);
            endMsExcl = upperBoundMsExclusive(clusteringDescending ? comparatorStart : comparatorEnd);
        }
        else if (filter instanceof ClusteringIndexNamesFilter)
        {
            exactClusterings = ((ClusteringIndexNamesFilter) filter).requestedRows();
            if (exactClusterings.isEmpty())
                return hot;
            Clustering<?> comparatorFirst = exactClusterings.first();
            Clustering<?> comparatorLast = exactClusterings.last();
            startMs = clusteringMs(clusteringDescending ? comparatorLast : comparatorFirst);
            endMsExcl = clusteringMs(clusteringDescending ? comparatorFirst : comparatorLast) + 1;
        }
        else
        {
            return hot;
        }

        long hotBoundary = Clock.Global.currentTimeMillis() - policy.hotWindowMillis;
        if (startMs >= hotBoundary)
            return hot;                                   // entirely within the hot window: fast path

        List<DecoratedKey> keys = new ArrayList<>(group.queries.size());
        for (SinglePartitionReadCommand command : group.queries)
            keys.add(command.partitionKey());

        long queryStartMs = startMs;
        long queryEndMsExcl = endMsExcl;
        NavigableSet<Clustering<?>> exact = exactClusterings;
        // chunkRows is told the TIMESTAMP order to emit in; the merge decorator is told whether the
        // iterator runs against clustering-comparator order, which is filter.isReversed() -- the
        // comparator itself already encodes the DESC-ness.
        return ChunkMergePartitionIterator.wrap(hot, keys,
                                                key -> chunkRows(metadata, policy, key, queryStartMs, queryEndMsExcl,
                                                                 exact, emitDescending, cl),
                                                metadata, filter.isReversed());
    }

    /** True when {@link #maybeWrap} returned a merge wrapper rather than the hot iterator itself. */
    public static boolean isMerged(PartitionIterator iterator)
    {
        return iterator instanceof ChunkMergePartitionIterator;
    }

    /** @param descending emit rows newest-timestamp-first (see {@code emitDescending} in {@link #maybeWrap}) */
    private static List<Row> chunkRows(TableMetadata metadata,
                                       TieringPolicy policy,
                                       DecoratedKey key,
                                       long startMs,
                                       long endMsExcl,
                                       NavigableSet<Clustering<?>> exactClusterings,
                                       boolean descending,
                                       ConsistencyLevel cl)
    {
        // Windows are floor-aligned: a chunk whose window_start is up to one chunk_window before the
        // range start can still contain samples inside the range. Unbounded slice ends arrive as
        // Long.MIN/MAX_VALUE - clamp to +/-2^62 BEFORE the window arithmetic so the floor-and-subtract
        // cannot wrap (the clamped bounds are still ~146M years away from any real timestamp).
        long safeStart = Math.max(startMs, -(1L << 62));
        long windowLow = policy.windowStartFor(safeStart) - policy.chunkWindowMillis;
        long windowHigh = Math.min(endMsExcl, 1L << 62);
        // The chunk table mirrors the base table's whole partition key (same names, same order), so the
        // restriction names every key column and binds the key's components in that order.
        List<ColumnMetadata> tagColumns = metadata.partitionKeyColumns();
        StringBuilder tagPredicate = new StringBuilder();
        for (ColumnMetadata column : tagColumns)
        {
            if (tagPredicate.length() > 0)
                tagPredicate.append(" AND ");
            tagPredicate.append(column.name.toCQLString()).append(" = ?");
        }
        String select = String.format("SELECT window_start, max_row_writetime, payload FROM %s.\"%s\" " +
                                      "WHERE %s AND window_start >= ? AND window_start < ?",
                                      metadata.keyspace, ChunkTables.chunkTableName(metadata.name), tagPredicate);
        List<ByteBuffer> values = new ArrayList<>(tagColumns.size() + 2);
        // A composite partition key arrives as one CompositeType-encoded buffer; split it back into the
        // per-column values the chunk table's own key columns expect.
        Collections.addAll(values, tagColumns.size() == 1
                                   ? new ByteBuffer[]{ key.getKey() }
                                   : ((CompositeType) metadata.partitionKeyType).split(key.getKey()));
        values.add(TimestampType.instance.decompose(new Date(windowLow)));
        values.add(TimestampType.instance.decompose(new Date(windowHigh)));
        UntypedResultSet chunks;
        try
        {
            chunks = cl == null
                     ? QueryProcessor.executeInternal(select, values.toArray())
                     : QueryProcessor.process(select, cl, values);
        }
        catch (Throwable t)
        {
            // Chunk-table read failure degrades to hot-only data with a client-visible warning
            // rather than failing the whole SELECT (availability-first, mirrors the re-encoder's
            // per-tag isolation philosophy). Escalating instead would make cold data a hard
            // dependency of every historical read.
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                             metadata.keyspace + '.' + metadata.name + ":chunk-read", 1, TimeUnit.MINUTES,
                             "Chunk read for transparent tiered SELECT on {}.{} failed; returning hot data only",
                             metadata.keyspace, metadata.name, t);
            ClientWarn.instance.warn("tiered read: chunk fetch failed, cold data omitted (" + t.getClass().getSimpleName() + ')');
            return List.of();
        }

        List<Row> result = new ArrayList<>();
        List<UntypedResultSet.Row> ordered = new ArrayList<>();
        for (UntypedResultSet.Row chunk : chunks)
            ordered.add(chunk);
        if (descending)
            java.util.Collections.reverse(ordered);       // windows arrive in ascending window_start order

        for (UntypedResultSet.Row chunk : ordered)
        {
            try
            {
                List<Row> rows = ChunkReadSupport.rowsFromChunk(metadata,
                                                                chunk.getBytes("payload"),
                                                                chunk.getLong("max_row_writetime"),
                                                                startMs, endMsExcl, descending);
                if (exactClusterings != null)
                {
                    for (Row row : rows)
                        if (exactClusterings.contains(row.clustering()))
                            result.add(row);
                }
                else
                {
                    result.addAll(rows);
                }
            }
            catch (UnsupportedChunkFormatException e)
            {
                // NOT skippable, unlike corruption below. A removed codec version is systematic --
                // every chunk this table wrote under the old build carries it -- so skipping would
                // let this SELECT succeed while silently returning truncated history, and keep doing
                // so on every future read. Fail the query instead, loudly and with the window that
                // proved it, so the condition is impossible to miss.
                String detail = format("%s.%s: the chunk for window %s was written by an older build and cannot " +
                                        "be read (%s). Drop %s.\"%s\" and let tiering re-run -- that data is not " +
                                        "recoverable, its base rows were deleted when it was encoded.",
                                        metadata.keyspace, metadata.name, chunk.getTimestamp("window_start"),
                                        e.getMessage(), metadata.keyspace,
                                        ChunkTables.chunkTableName(metadata.name));
                NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR,
                                 metadata.keyspace + '.' + metadata.name + ":chunk-unsupported", 1, TimeUnit.MINUTES,
                                 "{}", detail);
                throw new UnsupportedChunkFormatException(detail);
            }
            catch (IllegalArgumentException e)
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                 metadata.keyspace + '.' + metadata.name + ":chunk-corrupt", 1, TimeUnit.MINUTES,
                                 "Corrupt chunk skipped during transparent read of {}.{} window {}: {}",
                                 metadata.keyspace, metadata.name, chunk.getTimestamp("window_start"), e.getMessage());
                ClientWarn.instance.warn("tiered read: corrupt chunk skipped for window " + chunk.getTimestamp("window_start"));
            }
        }
        return result;
    }

    /**
     * The inclusive lower TIME bound expressed by a slice bound that restricts timestamps from below
     * -- the comparator-order start on an ASC clustering, the comparator-order end on a DESC one. An
     * unbounded bound is either BOTTOM (ASC) or TOP (DESC), hence both are tested.
     */
    private static long lowerBoundMs(ClusteringBound<?> bound)
    {
        if (bound.isBottom() || bound.isTop())
            return Long.MIN_VALUE;
        long ms = boundMs(bound);
        return bound.isInclusive() ? ms : ms + 1;
    }

    /** The exclusive upper TIME bound of a slice bound that restricts timestamps from above (see above). */
    private static long upperBoundMsExclusive(ClusteringBound<?> bound)
    {
        if (bound.isTop() || bound.isBottom())
            return Long.MAX_VALUE;
        long ms = boundMs(bound);
        return bound.isInclusive() ? ms + 1 : ms;
    }

    private static long boundMs(ClusteringBound<?> bound)
    {
        return TimestampType.instance.compose(bound.bufferAt(0)).getTime();
    }

    private static long clusteringMs(Clustering<?> clustering)
    {
        return TimestampType.instance.compose(clustering.bufferAt(0)).getTime();
    }
}
