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
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;

/**
 * The cold half of a transparent tiered read: turns one partition key into the chunk-decoded rows
 * that {@link ChunkMergeUnfilteredIterator} merges with the hot ones. Built once per query (its
 * state depends only on the query), used once per partition key.
 *
 * <h2>Why this is lazy, and what it is worth</h2>
 * A tiered read has no way to narrow the chunk table by anything but time, so a query with <b>no
 * clustering restriction</b> -- {@code SELECT * ... LIMIT 1000}, {@code SELECT static_col ... LIMIT 1},
 * any "newest N rows" query -- names every chunk in the partition. Fetching and decoding them all is
 * work proportional to the retention period for a result of a handful of rows, and it happens
 * <em>before</em> {@code LIMIT} gets to run: limits apply to the merged stream, which cannot start
 * until the merge's inputs exist. That is the whole cost of tiering on unbounded queries.
 * <p>
 * So nothing here is eager past the point of need. Two layers:
 * <ol>
 *   <li><b>The window list carries no payloads.</b> {@code SELECT window_start, max_row_writetime}
 *       reads tens of bytes per window instead of the compressed blob; the payload of a given window
 *       is fetched by its own {@code SELECT payload ... WHERE window_start = ?}, and only when the
 *       merge asks for that window's rows.</li>
 *   <li><b>Windows are decoded one at a time, in emission order.</b> When the limit is satisfied the
 *       consumer stops pulling, and every window after the current one is neither fetched nor
 *       decoded.</li>
 * </ol>
 * The window list itself is materialised -- it has to be, since {@code descending} emits it
 * back-to-front -- but it is read through {@link TieredStorageService#pagedSelect} (or its internal
 * equivalent) so that a partition with years of hourly windows is paged rather than assembled in one
 * page.
 *
 * <h2>Order is inherited, never re-derived</h2>
 * {@code descending} is the caller's {@code emitDescending} -- the order rows must come out in
 * expressed as timestamps, which is <em>not</em> the read's {@code reversed} flag (see
 * {@link TransparentReads#maybeWrap}). It reverses the window list and is handed straight to
 * {@link ChunkReadSupport#rowsFromChunk} for the order within a window. Both were already correct
 * when this was eager; laziness changes when a window is decoded, never which one comes first.
 *
 * <h2>Failure handling is unchanged</h2>
 * A failed chunk query fails the SELECT (the base rows were deleted when they were encoded, so
 * hot-only would be a silently incomplete success); a corrupt chunk is skipped with a warning; a
 * chunk from a removed codec fails the SELECT, because that is systematic rather than isolated.
 * Splitting the lookup in two only moves where those are raised: the corrupt/unsupported cases still
 * surface from {@link ChunkReadSupport#rowsFromChunk}, which decodes the whole payload before
 * returning its first row.
 */
final class ChunkRowSource
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkRowSource.class);

    /**
     * Payload {@code SELECT}s issued since the process started. Instrumentation, not state: early
     * termination is invisible in a query's results -- the same rows come back either way -- so this
     * is the only thing a test can assert on to prove that an unbounded {@code LIMIT n} stopped
     * after the windows it needed. See {@code TransparentReadTest}.
     */
    @VisibleForTesting
    static final AtomicLong payloadReads = new AtomicLong();

    /**
     * Window rows pulled from the chunk table's window listing. The companion of
     * {@link #payloadReads} for the step before it: a read can decode one payload and still have
     * enumerated every window in the partition to find it, which is invisible in the results and
     * was the real cost of an unbounded newest-first {@code LIMIT}.
     */
    @VisibleForTesting
    static final AtomicLong windowRowsListed = new AtomicLong();

    private final TableMetadata metadata;
    /** {@code SELECT window_start, max_row_writetime ...} -- deliberately no payload column. */
    private final String windowSelect;
    /** {@code SELECT payload ... WHERE window_start = ?} -- one window's blob, on demand. */
    private final String payloadSelect;
    private final long lookbackMs;
    private final long startMs;
    private final long endMsExcl;
    private final NavigableSet<Clustering<?>> exactClusterings;
    private final Slices slicesToHonour;
    private final boolean descending;
    private final Set<String> projection;
    private final ConsistencyLevel cl;

    /**
     * @param lookbackMs how far before {@code startMs} a chunk's {@code window_start} may sit and
     *                   still reach into the range -- {@link ChunkCoverage.Coverage#lookbackMillis()}
     * @param exactClusterings non-null only for a names filter, whose exact clusterings a decoded row
     *                   must match
     * @param slicesToHonour non-null only for a multi-slice filter, whose {@code [startMs, endMsExcl)}
     *                   hull spans gaps the query does not select
     * @param descending emit rows newest-timestamp-first (see {@code emitDescending} in
     *                   {@link TransparentReads#maybeWrap})
     * @param projection the columns to decode, or {@code null} for all
     *                   (see {@link TransparentReads#chunkProjection})
     * @param cl the user query's consistency level, or null for the internal/local execution path
     */
    ChunkRowSource(TableMetadata metadata,
                   long lookbackMs,
                   long startMs,
                   long endMsExcl,
                   NavigableSet<Clustering<?>> exactClusterings,
                   Slices slicesToHonour,
                   boolean descending,
                   Set<String> projection,
                   ConsistencyLevel cl)
    {
        this.metadata = metadata;
        // The chunk table mirrors the base table's whole partition key (same names, same order), so
        // the restriction names every key column.
        StringBuilder tagPredicate = new StringBuilder();
        for (ColumnMetadata column : metadata.partitionKeyColumns())
        {
            if (tagPredicate.length() > 0)
                tagPredicate.append(" AND ");
            tagPredicate.append(column.name.toCQLString()).append(" = ?");
        }
        String ref = chunkRef(metadata);
        // Newest-first queries push the ordering into the chunk table instead of listing every
        // window and reversing in memory: window_start is the clustering column and the whole
        // partition key is restricted, so ORDER BY ... DESC is a plain reversed single-partition
        // read. Combined with the lazy window iterator below, an unbounded `LIMIT n` now touches
        // one page of window rows rather than every window the tag has ever had.
        String select = format("SELECT window_start, max_row_writetime FROM %s " +
                               "WHERE %s AND window_start >= ? AND window_start < ?", ref, tagPredicate);
        this.windowSelect = descending ? select + " ORDER BY window_start DESC" : select;
        this.payloadSelect = format("SELECT payload FROM %s WHERE %s AND window_start = ?", ref, tagPredicate);
        this.lookbackMs = lookbackMs;
        this.startMs = startMs;
        this.endMsExcl = endMsExcl;
        this.exactClusterings = exactClusterings;
        this.slicesToHonour = slicesToHonour;
        this.descending = descending;
        this.projection = projection;
        this.cl = cl;
    }

    /**
     * @return this partition's chunk rows, in emission order, decoded a window at a time. The caller
     *         owns the iterator and must {@link CloseableIterator#close() close} it -- closing is
     *         what stops the remaining windows from ever being fetched.
     */
    CloseableIterator<Row> rowsFor(DecoratedKey key)
    {
        List<ByteBuffer> tagValues = tagValues(key);
        return new ChunkRows(tagValues, windows(tagValues));
    }

    /**
     * The windows whose data can reach into this query's range, in emission order — ascending
     * {@code window_start}, or descending when the query emits newest-first.
     *
     * <p>Lazy: the rows are pulled a page at a time as the consumer walks them, so a query that
     * stops early never pays for the windows past the one that satisfied it. Listing them all was
     * the dominant cost of an unbounded {@code LIMIT 1} on a tag with years of history.
     */
    private Iterator<Window> windows(List<ByteBuffer> tagValues)
    {
        // A chunk of width W starting at S holds timestamps in [S, S+W), so it can reach into the
        // range iff S > startMs - W. Unbounded slice ends arrive as Long.MIN/MAX_VALUE - clamp to
        // +/-2^62 BEFORE subtracting so the arithmetic cannot wrap (the clamped bounds are still
        // ~146M years away from any real timestamp, and W is capped at 31 days).
        long safeStart = Math.max(startMs, -(1L << 62));
        long windowLow = safeStart - lookbackMs;
        long windowHigh = Math.min(endMsExcl, 1L << 62);

        List<ByteBuffer> values = new ArrayList<>(tagValues.size() + 2);
        values.addAll(tagValues);
        values.add(TimestampType.instance.decompose(new Date(windowLow)));
        values.add(TimestampType.instance.decompose(new Date(windowHigh)));

        Iterator<UntypedResultSet.Row> rows;
        try
        {
            // Paged AND lazily consumed on both branches: a partition holding years of hourly
            // windows has tens of thousands of them, and while each row is tiny, materializing all
            // of them before the first payload is read is what made `LIMIT 1` cost a full scan.
            rows = cl == null
                   ? QueryProcessor.executeInternalWithPaging(windowSelect, TieredStorageService.PAGE_SIZE,
                                                              values.toArray()).iterator()
                   : TieredStorageService.pagedSelectLazy(windowSelect, cl, values);
        }
        catch (RuntimeException e)
        {
            throw chunkReadFailed(e);
        }

        // The failure contract is per-page, not per-call: paging means a later page can throw long
        // after this method returned, and that must surface as the same chunk-read failure rather
        // than as a raw driver exception from inside the row iterator.
        return new AbstractIterator<Window>()
        {
            @Override
            protected Window computeNext()
            {
                try
                {
                    if (!rows.hasNext())
                        return endOfData();
                    UntypedResultSet.Row row = rows.next();
                    windowRowsListed.incrementAndGet();
                    return new Window(row.getBytes("window_start"), row.getLong("max_row_writetime"));
                }
                catch (RuntimeException e)
                {
                    throw chunkReadFailed(e);
                }
            }
        };
    }

    /**
     * FAIL the query. A ReadTimeoutException, an UnavailableException or a
     * TombstoneOverwhelmingException here is routine at QUORUM, and the base rows for this range were
     * deleted when they were encoded -- so degrading to hot-only data would turn an availability
     * fault into a SUCCESSFUL SELECT that is missing all of its cold history, behind a client warning
     * most drivers never surface. "Slow" and "wrong" are not interchangeable; a read that cannot see
     * the cold tier must say so.
     * <p>
     * The legitimately-silent case -- nothing has ever been encoded, so there is no chunk table --
     * never reaches here: {@link TransparentReads#maybeWrap} leaves on
     * {@link ChunkCoverage.Coverage#chunkTableExists()}. Error is deliberately not caught.
     */
    private RuntimeException chunkReadFailed(RuntimeException e)
    {
        NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR,
                         metadata.keyspace + '.' + metadata.name + ":chunk-read", 1, TimeUnit.MINUTES,
                         "Chunk read for transparent tiered SELECT on {}.{} failed; failing the query rather " +
                         "than returning hot rows only, whose cold half was deleted when it was encoded",
                         metadata.keyspace, metadata.name, e);
        return e;
    }

    /**
     * A composite partition key arrives as one CompositeType-encoded buffer; split it back into the
     * per-column values the chunk table's own key columns expect.
     */
    private List<ByteBuffer> tagValues(DecoratedKey key)
    {
        List<ColumnMetadata> tagColumns = metadata.partitionKeyColumns();
        List<ByteBuffer> values = new ArrayList<>(tagColumns.size());
        Collections.addAll(values, tagColumns.size() == 1
                                   ? new ByteBuffer[]{ key.getKey() }
                                   : ((CompositeType) metadata.partitionKeyType).split(key.getKey()));
        return values;
    }

    /** True if the query's clustering filter actually selects {@code row}. */
    private boolean selects(Row row)
    {
        if (exactClusterings != null)
            return exactClusterings.contains(row.clustering());
        // The decoded rows were range-filtered against the hull of every slice; keep only those a
        // slice actually selects, so nothing from a gap between them is injected.
        return slicesToHonour == null || slicesToHonour.selects(row.clustering());
    }

    /**
     * @return {@code base}'s chunk table as a CQL-safe qualified name, both sides quoted if they need
     * it. Mirrors {@code TieredStorageService.quotedRef}.
     * <p>
     * Both sides of the table reference are quoted: a mixed-case or reserved-word keyspace name is as
     * real as a mixed-case table name, and an unquoted keyspace here would send these SELECTs to the
     * wrong (lowercased) keyspace -- or to none at all.
     */
    private static String chunkRef(TableMetadata base)
    {
        return format("%s.%s",
                      ColumnIdentifier.maybeQuote(base.keyspace),
                      ColumnIdentifier.maybeQuote(ChunkTables.chunkTableName(base.name)));
    }

    /** One chunk-table row minus its payload: enough to fetch and interpret the blob later. */
    private static final class Window
    {
        private final ByteBuffer start;
        private final long maxRowWritetime;

        Window(ByteBuffer start, long maxRowWritetime)
        {
            this.start = start;
            this.maxRowWritetime = maxRowWritetime;
        }

        Date startDate()
        {
            return TimestampType.instance.compose(start);
        }
    }

    /**
     * The lazy half: walks {@code windows} in emission order, fetching and decoding one payload at a
     * time and never reaching for the next until the current one is exhausted.
     */
    private final class ChunkRows extends AbstractIterator<Row>
    {
        private final List<ByteBuffer> tagValues;
        private final Iterator<Window> windows;
        private Iterator<Row> current = Collections.emptyIterator();
        private boolean closed;

        ChunkRows(List<ByteBuffer> tagValues, Iterator<Window> windows)
        {
            this.tagValues = tagValues;
            this.windows = windows;
        }

        @Override
        protected Row computeNext()
        {
            while (true)
            {
                while (current.hasNext())
                {
                    Row row = current.next();
                    if (selects(row))
                        return row;
                }
                // hasNext() on the window iterator is what fetches the next page of the window
                // listing, so a consumer that stopped early never triggers it.
                if (closed || !windows.hasNext())
                    return endOfData();
                current = decode(windows.next());
            }
        }

        /**
         * Nothing here owns a file descriptor -- the chunk table is read through ordinary CQL, which
         * releases its own resources per statement -- but closing is still load-bearing: it is what
         * guarantees a consumer that stopped early cannot cause another payload to be fetched, even
         * if something downstream keeps a reference and pulls again.
         */
        @Override
        public void close()
        {
            closed = true;
            current = Collections.emptyIterator();
        }

        /** @return the rows of {@code window}, or nothing if its chunk is unreadable or gone. */
        private Iterator<Row> decode(Window window)
        {
            ByteBuffer payload = payload(window);
            if (payload == null)
                return Collections.emptyIterator();
            try
            {
                return ChunkReadSupport.rowsFromChunk(metadata, payload, window.maxRowWritetime,
                                                      startMs, endMsExcl, descending, projection);
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
                                       metadata.keyspace, metadata.name, window.startDate(),
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
                                 metadata.keyspace, metadata.name, window.startDate(), e.getMessage());
                ClientWarn.instance.warn("tiered read: corrupt chunk skipped for window " + window.startDate());
                return Collections.emptyIterator();
            }
        }

        /**
         * @return {@code window}'s blob, or {@code null} if the chunk row is not there. Splitting the
         *         lookup in two opens a gap the single query did not have -- the window is listed by
         *         one statement and its payload read by another -- so this case is now reachable two
         *         ways, and neither is worth failing a query over:
         *         <ul>
         *           <li>retention dropped the chunk in between, which deletes the whole row: the data
         *               is expired, and the query was never entitled to it;</li>
         *           <li>at {@code CL=ONE} the two statements were served by different replicas, one
         *               of which has the chunk and one of which does not yet. That is the same
         *               incompleteness a single query already had at {@code CL=ONE} -- a replica
         *               missing the chunk row returns nothing for it either -- not a new one.</li>
         *         </ul>
         *         Both are rare and both are silent in the result, so they are logged: "a window
         *         existed a moment ago and its payload does not" is the shape of a real problem
         *         (a half-deleted chunk, a replica losing data) and must be diagnosable after the
         *         fact. Not a {@code ClientWarn}: the expiry race is legitimate and would be noise.
         */
        private ByteBuffer payload(Window window)
        {
            List<ByteBuffer> values = new ArrayList<>(tagValues.size() + 1);
            values.addAll(tagValues);
            values.add(window.start);
            payloadReads.incrementAndGet();
            UntypedResultSet result;
            try
            {
                result = cl == null
                         ? QueryProcessor.executeInternal(payloadSelect, values.toArray())
                         : QueryProcessor.process(payloadSelect, cl, values);
            }
            catch (RuntimeException e)
            {
                throw chunkReadFailed(e);
            }
            ByteBuffer payload = result == null || result.isEmpty() ? null : result.one().getBytes("payload");
            if (payload == null)
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                 metadata.keyspace + '.' + metadata.name + ":chunk-vanished", 1, TimeUnit.MINUTES,
                                 "Chunk for {}.{} window {} was listed but its payload could not be read; it was " +
                                 "expired by retention between the two reads, or this replica does not have it. " +
                                 "That window contributes no rows to this SELECT.",
                                 metadata.keyspace, metadata.name, window.startDate());
            return payload;
        }
    }
}
