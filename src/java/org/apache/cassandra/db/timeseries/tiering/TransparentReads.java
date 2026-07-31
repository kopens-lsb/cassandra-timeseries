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
import java.util.Arrays;
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
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

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
        if (policy == null || !(query instanceof SinglePartitionReadCommand.Group))
            return hot;

        SinglePartitionReadCommand.Group group = (SinglePartitionReadCommand.Group) query;
        if (group.queries.isEmpty())
            return hot;

        SinglePartitionReadCommand first = group.queries.get(0);
        ClusteringIndexFilter filter = first.clusteringIndexFilter();
        boolean reversed = filter.isReversed();

        long startMs;
        long endMsExcl;
        NavigableSet<Clustering<?>> exactClusterings = null;
        if (filter instanceof ClusteringIndexSliceFilter)
        {
            Slices slices = ((ClusteringIndexSliceFilter) filter).requestedSlices();
            if (slices.isEmpty())
                return hot;
            startMs = lowerBoundMs(slices.get(0).start());
            endMsExcl = upperBoundMsExclusive(slices.get(slices.size() - 1).end());
        }
        else if (filter instanceof ClusteringIndexNamesFilter)
        {
            exactClusterings = ((ClusteringIndexNamesFilter) filter).requestedRows();
            if (exactClusterings.isEmpty())
                return hot;
            startMs = clusteringMs(exactClusterings.first());
            endMsExcl = clusteringMs(exactClusterings.last()) + 1;
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
        return ChunkMergePartitionIterator.wrap(hot, keys,
                                                key -> chunkRows(metadata, policy, key, queryStartMs, queryEndMsExcl, exact, reversed, cl),
                                                metadata, reversed);
    }

    /** True when {@link #maybeWrap} returned a merge wrapper rather than the hot iterator itself. */
    public static boolean isMerged(PartitionIterator iterator)
    {
        return iterator instanceof ChunkMergePartitionIterator;
    }

    private static List<Row> chunkRows(TableMetadata metadata,
                                       TieringPolicy policy,
                                       DecoratedKey key,
                                       long startMs,
                                       long endMsExcl,
                                       NavigableSet<Clustering<?>> exactClusterings,
                                       boolean reversed,
                                       ConsistencyLevel cl)
    {
        // Windows are floor-aligned: a chunk whose window_start is up to one chunk_window before the
        // range start can still contain samples inside the range. Unbounded slice ends arrive as
        // Long.MIN/MAX_VALUE - clamp to +/-2^62 BEFORE the window arithmetic so the floor-and-subtract
        // cannot wrap (the clamped bounds are still ~146M years away from any real timestamp).
        long safeStart = Math.max(startMs, -(1L << 62));
        long windowLow = policy.windowStartFor(safeStart) - policy.chunkWindowMillis;
        long windowHigh = Math.min(endMsExcl, 1L << 62);
        String select = String.format("SELECT window_start, max_row_writetime, payload FROM %s.\"%s\" " +
                                      "WHERE tag = ? AND window_start >= ? AND window_start < ?",
                                      metadata.keyspace, ChunkTables.chunkTableName(metadata.name));
        List<ByteBuffer> values = Arrays.asList(key.getKey(),
                                                TimestampType.instance.decompose(new Date(windowLow)),
                                                TimestampType.instance.decompose(new Date(windowHigh)));
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
        if (reversed)
            java.util.Collections.reverse(ordered);       // windows arrive in ascending clustering order

        for (UntypedResultSet.Row chunk : ordered)
        {
            try
            {
                List<Row> rows = ChunkReadSupport.rowsFromChunk(metadata,
                                                                chunk.getBytes("payload"),
                                                                chunk.getLong("max_row_writetime"),
                                                                startMs, endMsExcl, reversed);
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

    private static long lowerBoundMs(ClusteringBound<?> start)
    {
        if (start.isBottom())
            return Long.MIN_VALUE;
        long ms = boundMs(start);
        return start.isInclusive() ? ms : ms + 1;
    }

    private static long upperBoundMsExclusive(ClusteringBound<?> end)
    {
        if (end.isTop())
            return Long.MAX_VALUE;
        long ms = boundMs(end);
        return end.isInclusive() ? ms + 1 : ms;
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
