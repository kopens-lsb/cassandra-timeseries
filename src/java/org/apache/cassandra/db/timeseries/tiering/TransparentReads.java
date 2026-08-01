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

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;

/**
 * SP3 transparent reads (design spec section 3.3.1): decides whether a SELECT on a tiering-enabled
 * table must merge in decoded chunk rows, fetches and decodes the relevant chunks at the user's
 * consistency level, and wraps the hot {@link UnfilteredPartitionIterator} with the merge decorator.
 *
 * Activation requires ALL of: a single-partition (or IN) read group; a slice or names clustering
 * filter; a chunk table that exists; and a requested time range reaching below the point where cold
 * data can start. Everything else returns the hot iterator untouched - the non-tiered fast path pays
 * nothing beyond one schema-extension lookup and one cached {@link ChunkCoverage} lookup.
 *
 * <b>The merge gate is real coverage, not the current policy.</b> The only sound question is "do
 * chunks exist at or below this query's timestamps?", answered by {@link ChunkCoverage}; asking the
 * live {@link TieringPolicy} instead lets three ordinary tuning actions -- raising {@code hot_window},
 * dropping the extension, shrinking {@code chunk_window} -- silently hide history whose base rows
 * were deleted when it was encoded. The policy still contributes a floor
 * ({@code max(coverageTop, hotBoundary)}): anything the re-encoder has just written is by definition
 * below the current hot boundary, so a node whose cached coverage is a minute stale still merges it.
 *
 * <b>The wrap happens on the UNFILTERED stream</b>, before {@code Filter} purges tombstones, so
 * deletes are reconciled by the ordinary read machinery instead of being invisible to the merge --
 * see {@link ChunkMergeUnfilteredIterator}. Every unfiltered-to-filtered conversion point on a
 * single-partition read path therefore calls {@link #maybeWrap}: the local ones
 * ({@code SinglePartitionReadQuery.Group#executeInternal}, {@code AbstractReadQuery#executeInternal})
 * and the coordinator ones ({@code DigestResolver#getData}, {@code DataResolver#getData},
 * {@code DataResolver#resolveInternal}).
 */
public final class TransparentReads
{
    private static final Logger logger = LoggerFactory.getLogger(TransparentReads.class);

    /**
     * The re-encoder's own base-table reads run through the same SELECT machinery; merging chunks
     * into THEM would make already-encoded windows look like live base rows again (re-encode loops,
     * broken idempotency). The tiering machinery brackets its work with this per-thread bypass.
     */
    private static final ThreadLocal<Integer> INTERNAL_BYPASS = ThreadLocal.withInitial(() -> 0);

    private TransparentReads()
    {
    }

    /**
     * Nesting-safe on purpose: several unrelated things bracket reads with this now (the re-encoder's
     * own cycle, and each CAS precondition read), and a plain boolean would let an inner
     * {@code exit} re-enable merging while an outer bracket was still meant to hold -- silently, and
     * on a path where the consequence is a linearizability violation or a JVM crash.
     */
    public static void enterInternalBypass()
    {
        INTERNAL_BYPASS.set(INTERNAL_BYPASS.get() + 1);
    }

    public static void exitInternalBypass()
    {
        int depth = INTERNAL_BYPASS.get() - 1;
        INTERNAL_BYPASS.set(Math.max(0, depth));
    }

    /** True while this thread is inside tiering's own machinery (see {@link #enterInternalBypass}). */
    public static boolean inInternalBypass()
    {
        return INTERNAL_BYPASS.get() > 0;
    }

    /**
     * @param cl the user query's consistency level, or null for the internal/local execution path
     * @return the merged iterator, or {@code hot} itself when transparent reading does not apply
     */
    public static UnfilteredPartitionIterator maybeWrap(TableMetadata metadata, ReadQuery query,
                                                        UnfilteredPartitionIterator hot, ConsistencyLevel cl)
    {
        if (inInternalBypass())
            return hot;

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            // An invalid policy must never break reads -- and must not hide chunks either. A typo in
            // the extension JSON is a configuration error, not an instruction to make every encoded
            // window disappear, so fall through to coverage exactly as an absent policy does.
            policy = null;
        }

        // What the chunk table ACTUALLY holds, cached per table. A table with no chunk table has
        // provably never encoded anything (the re-encoder creates it, and waits for it to be visible
        // cluster-wide, before writing the first chunk), so this is where every non-tiered read --
        // and every tiered read before the first cycle -- leaves, at the cost of one cached lookup.
        ChunkCoverage.Coverage coverage = ChunkCoverage.forTable(metadata, cl);
        if (policy == null && !coverage.chunkTableExists())
            return hot;
        // The hook fires both per read GROUP (the local path executes a whole group at once) and per
        // read COMMAND (the coordinator resolves one partition at a time), so accept either shape and
        // normalise to the list of single-partition queries this iterator will carry.
        List<? extends SinglePartitionReadQuery> commands;
        if (query instanceof SinglePartitionReadQuery.Group)
            commands = ((SinglePartitionReadQuery.Group<?>) query).queries;
        else if (query instanceof SinglePartitionReadQuery)
            commands = Collections.singletonList((SinglePartitionReadQuery) query);
        else
        {
            // Range scans cannot merge chunks (no partition context) and would otherwise silently
            // see hot rows only - warn instead of returning a quietly incomplete answer (v1 scope;
            // spec section 3.3). The hook now fires from several places per query, so de-duplicate
            // against the warnings already accumulated for THIS request rather than sending the
            // client the same sentence three times.
            String warning = "tiered table " + metadata.keyspace + '.' + metadata.name +
                             ": range scans read hot rows only; query by partition key for merged hot+cold reads";
            List<String> existing = ClientWarn.instance.getWarnings();
            if (existing == null || !existing.contains(warning))
                ClientWarn.instance.warn(warning);
            return hot;
        }

        if (commands.isEmpty())
            return hot;

        // Every timestamp read below assumes exactly one timestamp clustering column -- what
        // TieringPolicy.unsupportedSchemaError enforces before anything is ever chunked. A table
        // carrying the extension but not that shape has no chunks to merge, and must not have its
        // ordinary SELECTs broken by this code path.
        if (metadata.clusteringColumns().size() != 1)
            return hot;

        // Policy present, but nothing encoded yet: the shadow table has never been created, so there
        // is no cold data and (unlike before) no chunk read to fail and warn about on every
        // historical read of a freshly-configured table.
        if (!coverage.chunkTableExists())
            return hot;

        SinglePartitionReadQuery first = commands.get(0);
        ClusteringIndexFilter filter = first.clusteringIndexFilter();
        // WITH CLUSTERING ORDER BY (ts DESC) wraps the clustering type in ReversedType, so both
        // Slices and the names filter's NavigableSet are ordered by DESCENDING timestamp: the
        // comparator-order start bound is then the range's UPPER time bound, and the comparator-order
        // end bound its LOWER one. Reading them as if ascending produces a nonsensical (usually
        // empty) time range and silently serves no cold rows at all -- on exactly the DESC shape that
        // is the dominant time-series idiom.
        boolean clusteringDescending = ColdBoundary.isDescending(metadata);
        // The order rows must be EMITTED in, expressed as timestamps: the hot iterator runs in
        // clustering-comparator order unless the filter reverses it, and comparator order is already
        // descending-by-timestamp on a DESC table.
        boolean emitDescending = clusteringDescending != filter.isReversed();

        long startMs;
        long endMsExcl;
        NavigableSet<Clustering<?>> exactClusterings = null;
        // Only set when the filter carries MORE THAN ONE slice: the [start, end) range derived below
        // is then the hull of all of them, and a chunk row falling in a gap BETWEEN slices would be
        // injected into a result the hot side never selected. Unreachable with a single timestamp
        // clustering column (CQL cannot express a disjunction over it), but the hull is what the code
        // computes, so the gap is closed explicitly rather than left to the schema to prevent.
        Slices gappedSlices = null;
        if (filter instanceof ClusteringIndexSliceFilter)
        {
            Slices slices = ((ClusteringIndexSliceFilter) filter).requestedSlices();
            if (slices.isEmpty())
                return hot;
            startMs = ColdBoundary.lowestMs(slices.get(0), clusteringDescending);
            endMsExcl = ColdBoundary.highestMsExclusive(slices.get(slices.size() - 1), clusteringDescending);
            if (slices.size() > 1)
                gappedSlices = slices;
        }
        else if (filter instanceof ClusteringIndexNamesFilter)
        {
            exactClusterings = ((ClusteringIndexNamesFilter) filter).requestedRows();
            if (exactClusterings.isEmpty())
                return hot;
            Clustering<?> comparatorFirst = exactClusterings.first();
            Clustering<?> comparatorLast = exactClusterings.last();
            startMs = ColdBoundary.clusteringMs(clusteringDescending ? comparatorLast : comparatorFirst);
            endMsExcl = ColdBoundary.clusteringMs(clusteringDescending ? comparatorFirst : comparatorLast) + 1;
        }
        else
        {
            return hot;
        }

        // The one correct question: can any chunk hold data at or above this query's start? Coverage
        // answers it from what the chunk table really contains; the hot boundary is a floor under that
        // answer, covering the window in which a just-written chunk may not be in this node's cached
        // coverage yet (it is necessarily below the boundary, so the merge still runs).
        long mergeBelowMs = coverage.topExclusiveMs();
        if (policy != null)
            mergeBelowMs = Math.max(mergeBelowMs, ColdBoundary.hotBoundaryMs(policy));
        if (startMs >= mergeBelowMs)
            return hot;                                   // no chunk reaches up here: fast path

        List<DecoratedKey> keys = new ArrayList<>(commands.size());
        for (SinglePartitionReadQuery command : commands)
            keys.add(command.partitionKey());

        long queryStartMs = startMs;
        long queryEndMsExcl = endMsExcl;
        NavigableSet<Clustering<?>> exact = exactClusterings;
        Slices slicesToHonour = gappedSlices;
        // Built once per query, not once per partition key per query: it depends only on the table.
        String select = chunkSelect(metadata);
        // The WIDEST chunk_window any existing chunk was written with, not the configured one: after
        // an operator shrinks chunk_window, every wider legacy chunk sits further before the range
        // start than one current window, and a look-back of one current window would never find it.
        long lookbackMs = coverage.lookbackMillis();
        // chunkRows is told the TIMESTAMP order to emit in; the merge decorator is told whether the
        // iterators run against clustering-comparator order, which is filter.isReversed() -- the
        // comparator itself already encodes the DESC-ness.
        return ChunkMergeUnfilteredIterator.wrap(hot, keys,
                                                 key -> chunkRows(metadata, select, lookbackMs, key,
                                                                  queryStartMs, queryEndMsExcl,
                                                                  exact, slicesToHonour, emitDescending, cl),
                                                 metadata, filter.isReversed());
    }

    /**
     * The per-table chunk {@code SELECT}. The chunk table mirrors the base table's whole partition key
     * (same names, same order), so the restriction names every key column.
     * <p>
     * Both sides of the table reference are quoted: a mixed-case or reserved-word keyspace name is as
     * real as a mixed-case table name, and an unquoted keyspace here would send this SELECT to the
     * wrong (lowercased) keyspace -- or to none at all.
     */
    private static String chunkSelect(TableMetadata metadata)
    {
        StringBuilder tagPredicate = new StringBuilder();
        for (ColumnMetadata column : metadata.partitionKeyColumns())
        {
            if (tagPredicate.length() > 0)
                tagPredicate.append(" AND ");
            tagPredicate.append(column.name.toCQLString()).append(" = ?");
        }
        return format("SELECT window_start, max_row_writetime, payload FROM %s " +
                      "WHERE %s AND window_start >= ? AND window_start < ?",
                      chunkRef(metadata), tagPredicate);
    }

    /**
     * @param lookbackMs how far before {@code startMs} a chunk's {@code window_start} may sit and
     *                   still reach into the range -- {@link ChunkCoverage.Coverage#lookbackMillis()}
     * @param slicesToHonour non-null only for a multi-slice filter, whose {@code [startMs, endMsExcl)}
     *                   hull spans gaps the query does not select
     * @param descending emit rows newest-timestamp-first (see {@code emitDescending} in {@link #maybeWrap})
     */
    private static List<Row> chunkRows(TableMetadata metadata,
                                       String select,
                                       long lookbackMs,
                                       DecoratedKey key,
                                       long startMs,
                                       long endMsExcl,
                                       NavigableSet<Clustering<?>> exactClusterings,
                                       Slices slicesToHonour,
                                       boolean descending,
                                       ConsistencyLevel cl)
    {
        // A chunk of width W starting at S holds timestamps in [S, S+W), so it can reach into the
        // range iff S > startMs - W. Unbounded slice ends arrive as Long.MIN/MAX_VALUE - clamp to
        // +/-2^62 BEFORE subtracting so the arithmetic cannot wrap (the clamped bounds are still
        // ~146M years away from any real timestamp, and W is capped at 31 days).
        long safeStart = Math.max(startMs, -(1L << 62));
        long windowLow = safeStart - lookbackMs;
        long windowHigh = Math.min(endMsExcl, 1L << 62);
        List<ColumnMetadata> tagColumns = metadata.partitionKeyColumns();
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
        catch (RuntimeException e)
        {
            // FAIL the query. A ReadTimeoutException, an UnavailableException or a
            // TombstoneOverwhelmingException here is routine at QUORUM, and the base rows for this
            // range were deleted when they were encoded -- so degrading to hot-only data would turn
            // an availability fault into a SUCCESSFUL SELECT that is missing all of its cold history,
            // behind a client warning most drivers never surface. "Slow" and "wrong" are not
            // interchangeable; a read that cannot see the cold tier must say so.
            //
            // The legitimately-silent case -- nothing has ever been encoded, so there is no chunk
            // table -- never reaches here: maybeWrap leaves on ChunkCoverage.chunkTableExists().
            // Error is deliberately not caught.
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR,
                             metadata.keyspace + '.' + metadata.name + ":chunk-read", 1, TimeUnit.MINUTES,
                             "Chunk read for transparent tiered SELECT on {}.{} failed; failing the query rather " +
                             "than returning hot rows only, whose cold half was deleted when it was encoded",
                             metadata.keyspace, metadata.name, e);
            throw e;
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
                else if (slicesToHonour != null)
                {
                    // The decoded rows were range-filtered against the hull of every slice; keep only
                    // those a slice actually selects, so nothing from a gap between them is injected.
                    for (Row row : rows)
                        if (slicesToHonour.selects(row.clustering()))
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
                                        "be read (%s). Drop %s and let tiering re-run -- that data is not " +
                                        "recoverable, its base rows were deleted when it was encoded.",
                                        metadata.keyspace, metadata.name, chunk.getTimestamp("window_start"),
                                        e.getMessage(), chunkRef(metadata));
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
     * @return {@code base}'s chunk table as a CQL-safe qualified name, both sides quoted if they need
     * it. Mirrors {@code TieredStorageService.quotedRef}.
     */
    private static String chunkRef(TableMetadata base)
    {
        return format("%s.%s",
                      ColumnIdentifier.maybeQuote(base.keyspace),
                      ColumnIdentifier.maybeQuote(ChunkTables.chunkTableName(base.name)));
    }
}
