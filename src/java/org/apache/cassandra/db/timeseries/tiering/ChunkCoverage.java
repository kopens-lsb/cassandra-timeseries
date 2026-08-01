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
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;

/**
 * What the {@code __chunks} shadow table <b>actually contains</b>, per base table: how far up the
 * time axis encoded data reaches, how far down it starts, and the widest {@code chunk_window} any
 * existing chunk was written with.
 *
 * <p><b>Why this exists.</b> Tiering deletes the base rows of every window it encodes, so a chunk is
 * the only copy of that data. The read path therefore has exactly one correct question to ask before
 * it decides to skip the chunk merge: <em>do chunks exist at or below this query's timestamps?</em>
 * Asking the <em>current</em> {@link TieringPolicy} instead -- "does the policy call this cold?" --
 * makes three ordinary tuning actions silently hide already-encoded history:
 * <ul>
 *     <li>raising {@code hot_window} moves the hot boundary back past data that was encoded under the
 *     old, shorter window, so queries over that span take the hot-only fast path and see nothing;</li>
 *     <li>dropping the {@code timeseries_tiering} extension makes <em>all</em> cold history vanish
 *     from every {@code SELECT};</li>
 *     <li>shrinking {@code chunk_window} makes the merge's look-back too short to find wider legacy
 *     chunks, whose {@code window_start} sits further before the query range than one current window.</li>
 * </ul>
 * All three are answered by consulting real coverage rather than the live policy.
 *
 * <p><b>Where the numbers come from.</b> They cannot be derived from the chunk table cheaply -- there
 * is no global index on {@code window_start}, so "the highest window ever encoded" would be a full
 * scan of a table designed to hold a decade of data. They are instead maintained as a tiny ledger
 * table beside the chunk table ({@link ChunkTables#coverageTableName}), written by the re-encoder
 * <b>before</b> it inserts a chunk (see {@link #claim}), so a crash between the two can only leave
 * coverage <em>wider</em> than reality -- which costs an unnecessary chunk read, never a wrong answer.
 * One row per node, aggregated by the reader (min/max/max), so a node with a stale view cannot
 * regress another node's claim the way a single last-write-wins row would.
 *
 * <p><b>Cost.</b> Consulted on every read of every table, so it is a cache lookup: one
 * {@link ConcurrentHashMap} hit keyed by {@link TableId}, refreshed at most every
 * {@value #REFRESH_SECONDS}s (a single-partition read of at most one row per node, or -- for the
 * overwhelmingly common case of a table that has no chunk table at all -- a schema lookup).
 *
 * <p><b>Staleness is safe by construction.</b> A stale-low coverage top can only under-state where
 * chunks reach, and {@link TransparentReads} takes {@code max(coverageTop, hotBoundary)}: anything
 * the re-encoder has just written is by definition below the current hot boundary, so it is merged
 * regardless of when this node last refreshed. The ledger is what keeps the answer right after the
 * hot boundary itself moves.
 */
public final class ChunkCoverage
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkCoverage.class);

    /**
     * The ledger's single partition key value. The ledger describes one thing (this base table's
     * chunk table), so it is one partition; the per-node rows are its clustering.
     */
    static final String SCOPE = "chunks";

    static final String MIN_WINDOW_START = "min_window_start";
    static final String MAX_WINDOW_START = "max_window_start";
    static final String MAX_CHUNK_WINDOW = "max_chunk_window";
    static final String SCOPE_COLUMN = "scope";
    static final String NODE_COLUMN = "node";

    private static final long REFRESH_SECONDS = 60;
    private static final long REFRESH_MILLIS = TimeUnit.SECONDS.toMillis(REFRESH_SECONDS);

    /** Bounded like {@code TieringPolicy.PARSED}: cleared wholesale rather than grown without limit. */
    private static final int MAX_CACHED_TABLES = 4096;

    private static final ConcurrentHashMap<TableId, Entry> CACHE = new ConcurrentHashMap<>();

    private ChunkCoverage()
    {
    }

    /**
     * A snapshot of what the chunk table holds. Four distinguishable states, because the read path
     * has to treat them differently:
     * <ul>
     *     <li><b>no chunk table</b> -- nothing has ever been encoded for this table, provably: skip
     *     the merge entirely, at zero cost. This is every non-tiered table, and every tiered table
     *     before its first cycle.</li>
     *     <li><b>empty ledger</b> -- the chunk table exists but no cycle has written a chunk yet.
     *     Also "no cold data", but reached through a real read, so it refreshes.</li>
     *     <li><b>known</b> -- chunks exist and reach up to {@link #topExclusiveMs()}.</li>
     *     <li><b>unknown</b> -- the ledger could not be consulted (unreadable, or absent beside an
     *     existing chunk table). Chunks may reach anywhere, so the merge must always run; the
     *     look-back falls back to {@code chunk_window}'s validated maximum, which bounds every chunk
     *     any policy could ever have written.</li>
     * </ul>
     */
    public static final class Coverage
    {
        static final Coverage NO_CHUNK_TABLE = new Coverage(false, true, false, 0, 0, 0);
        static final Coverage EMPTY = new Coverage(true, true, false, 0, 0, 0);
        static final Coverage UNKNOWN = new Coverage(true, false, false, 0, 0, 0);

        private final boolean chunkTableExists;
        private final boolean known;
        private final boolean anyChunks;
        private final long minWindowStartMs;
        private final long maxWindowStartMs;
        private final long widestChunkWindowMs;

        private Coverage(boolean chunkTableExists, boolean known, boolean anyChunks,
                         long minWindowStartMs, long maxWindowStartMs, long widestChunkWindowMs)
        {
            this.chunkTableExists = chunkTableExists;
            this.known = known;
            this.anyChunks = anyChunks;
            this.minWindowStartMs = minWindowStartMs;
            this.maxWindowStartMs = maxWindowStartMs;
            this.widestChunkWindowMs = widestChunkWindowMs;
        }

        /**
         * @return {@code false} only when the shadow table does not exist, which proves nothing has
         * ever been encoded for this base table -- the one condition under which the read path may
         * skip the merge without consulting anything else.
         */
        public boolean chunkTableExists()
        {
            return chunkTableExists;
        }

        /** @return {@code true} if at least one chunk is known to exist. */
        public boolean anyChunks()
        {
            return known && anyChunks;
        }

        /**
         * @return one past the newest timestamp any chunk can hold: a query starting at or after this
         * needs no merge. {@link Long#MIN_VALUE} when nothing is encoded (nothing to merge, ever),
         * {@link Long#MAX_VALUE} when coverage is unknown (merge everything).
         */
        public long topExclusiveMs()
        {
            if (!known)
                return Long.MAX_VALUE;
            if (!anyChunks)
                return Long.MIN_VALUE;
            // Saturating: a corrupt/absurd ledger value must widen coverage, never wrap it to a
            // negative number that would make the fast path swallow every query.
            long top = maxWindowStartMs + widestChunkWindowMs;
            return top < maxWindowStartMs ? Long.MAX_VALUE : top;
        }

        /**
         * @return the oldest {@code window_start} any chunk carries: a query ending at or before this
         * needs no merge. {@link Long#MAX_VALUE} when nothing is encoded, {@link Long#MIN_VALUE} when
         * coverage is unknown.
         */
        public long bottomInclusiveMs()
        {
            if (!known)
                return Long.MIN_VALUE;
            return anyChunks ? minWindowStartMs : Long.MAX_VALUE;
        }

        /**
         * @return how far before a query's start the chunk {@code SELECT} must look for a chunk that
         * could still reach into the range -- the <b>widest</b> {@code chunk_window} any existing
         * chunk was written with, not the currently configured one. Falls back to the validated
         * maximum {@code chunk_window} when coverage is unknown, which is a true upper bound on any
         * chunk any accepted policy could have produced.
         */
        public long lookbackMillis()
        {
            return anyChunks() ? widestChunkWindowMs : TieringPolicy.MAX_CHUNK_WINDOW_MILLIS;
        }

        /**
         * @return this coverage widened to also span {@code [minWindowStart, maxWindowStart]} with a
         * chunk width of at least {@code widthMs}.
         */
        Coverage widenedBy(long minWindowStart, long maxWindowStart, long widthMs)
        {
            if (!anyChunks())
                return new Coverage(true, true, true, minWindowStart, maxWindowStart, widthMs);

            return new Coverage(true, true, true,
                                Math.min(minWindowStartMs, minWindowStart),
                                Math.max(maxWindowStartMs, maxWindowStart),
                                Math.max(widestChunkWindowMs, widthMs));
        }

        boolean sameAs(Coverage other)
        {
            return known == other.known
                   && anyChunks == other.anyChunks
                   && minWindowStartMs == other.minWindowStartMs
                   && maxWindowStartMs == other.maxWindowStartMs
                   && widestChunkWindowMs == other.widestChunkWindowMs;
        }

        @Override
        public String toString()
        {
            if (!known)
                return "Coverage{unknown}";
            if (!anyChunks)
                return format("Coverage{empty, chunkTable=%s}", chunkTableExists);
            return format("Coverage{[%d, %d], widest=%dms, topExcl=%d}",
                          minWindowStartMs, maxWindowStartMs, widestChunkWindowMs, topExclusiveMs());
        }
    }

    private static final class Entry
    {
        final Coverage coverage;
        final long loadedAtMillis;

        Entry(Coverage coverage, long loadedAtMillis)
        {
            this.coverage = coverage;
            this.loadedAtMillis = loadedAtMillis;
        }
    }

    /**
     * @param cl the consistency level of the query asking, or {@code null} on the internal/local
     *           execution path -- mirrored from {@link TransparentReads}'s chunk read so the ledger
     *           and the chunks it describes are always read with the same reach.
     * @return {@code base}'s chunk coverage, from cache when fresh.
     */
    public static Coverage forTable(TableMetadata base, ConsistencyLevel cl)
    {
        Entry entry = CACHE.get(base.id);
        long now = Clock.Global.currentTimeMillis();
        if (entry != null && now - entry.loadedAtMillis < REFRESH_MILLIS)
            return entry.coverage;

        Coverage loaded = load(base, cl);
        // An unreadable ledger is not cached: it must be retried on the next read rather than pinning
        // "merge everything" (or, worse, a wrong answer) for a whole refresh interval.
        if (loaded.known || !loaded.chunkTableExists)
            put(base.id, loaded, now);
        return loaded;
    }

    /**
     * Re-reads {@code base}'s ledger now, discarding any cached value. Called once per re-encode
     * cycle, which is also what warms the cache after a restart (the global sweep runs a cycle for
     * every policy-bearing table).
     */
    public static void refresh(TableMetadata base, ConsistencyLevel cl)
    {
        CACHE.remove(base.id);
        forTable(base, cl);
    }

    @VisibleForTesting
    public static void invalidateAll()
    {
        CACHE.clear();
    }

    /**
     * Records, <b>before</b> the chunk is written, that {@code base}'s chunk table is about to hold a
     * chunk of {@code chunkWindowMillis} covering {@code windowStartMs}, and that nothing this cycle
     * writes can start later than {@code cutoffMs}.
     * <p>
     * Ordering is the point: the ledger has to be at least as wide as the chunk table at every
     * instant, so it is widened first and the chunk written second. The reverse order would leave a
     * crash window in which a chunk exists that the read path's fast path does not know about -- data
     * whose base rows are already deleted, invisible until the next cycle.
     * <p>
     * {@code cutoffMs} rather than {@code windowStartMs} for the top: it is the ceiling of everything
     * this cycle can encode, so one write per cycle claims the whole cycle instead of one write per
     * window. Over-claiming the top is free (it only costs an unnecessary chunk read), under-claiming
     * it is exactly the bug this class exists to prevent.
     * <p>
     * A no-op when the claim adds nothing to what is already recorded, which is the steady state.
     */
    public static void claim(TableMetadata base, ConsistencyLevel cl,
                             long windowStartMs, long cutoffMs, long chunkWindowMillis)
    {
        Coverage current = forTable(base, cl);
        Coverage widened = current.widenedBy(windowStartMs, cutoffMs, chunkWindowMillis);
        if (current.sameAs(widened))
            return;

        String insert = format("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?)",
                               coverageRef(base), SCOPE_COLUMN, NODE_COLUMN,
                               MIN_WINDOW_START, MAX_WINDOW_START, MAX_CHUNK_WINDOW);
        List<ByteBuffer> values = new ArrayList<>(5);
        values.add(UTF8Type.instance.decompose(SCOPE));
        values.add(UTF8Type.instance.decompose(nodeId()));
        values.add(TimestampType.instance.decompose(new Date(widened.minWindowStartMs)));
        values.add(TimestampType.instance.decompose(new Date(widened.maxWindowStartMs)));
        values.add(LongType.instance.decompose(widened.widestChunkWindowMs));
        // Deliberately unguarded: a failure here must abort the window rather than let the chunk be
        // written behind a ledger that does not cover it. The re-encoder's per-tag handler catches it
        // and counts the tag as skipped.
        QueryProcessor.process(insert, cl == null ? ConsistencyLevel.LOCAL_QUORUM : cl, values);
        put(base.id, widened, Clock.Global.currentTimeMillis());
    }

    private static void put(TableId id, Coverage coverage, long atMillis)
    {
        if (CACHE.size() >= MAX_CACHED_TABLES)
            CACHE.clear();
        CACHE.put(id, new Entry(coverage, atMillis));
    }

    private static Coverage load(TableMetadata base, ConsistencyLevel cl)
    {
        if (Schema.instance.getTableMetadata(base.keyspace, ChunkTables.chunkTableName(base.name)) == null)
            return Coverage.NO_CHUNK_TABLE;

        if (Schema.instance.getTableMetadata(base.keyspace, ChunkTables.coverageTableName(base.name)) == null)
        {
            // A chunk table with no ledger beside it. Nothing this build writes produces that shape
            // (both tables are created together, and the ledger is written before the first chunk),
            // so it means someone dropped the ledger -- in which case the only safe reading of the
            // chunk table is "it may hold anything".
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                             base.keyspace + '.' + base.name + ":coverage-missing", 1, TimeUnit.MINUTES,
                             "Tiered storage: {}.{} has a chunk table but no {} ledger, so how far its cold data " +
                             "reaches cannot be established; every read will merge chunks until the ledger is " +
                             "rebuilt by the next re-encode cycle",
                             base.keyspace, base.name, ChunkTables.coverageTableName(base.name));
            return Coverage.UNKNOWN;
        }

        String select = format("SELECT %s, %s, %s FROM %s WHERE %s = ?",
                               MIN_WINDOW_START, MAX_WINDOW_START, MAX_CHUNK_WINDOW,
                               coverageRef(base), SCOPE_COLUMN);
        UntypedResultSet rows;
        // The ledger is ordinary user-keyspace data, so reading it re-enters the read path. Bracket
        // it: nothing about the ledger is itself tiered, and the bypass keeps this from recursing.
        TransparentReads.enterInternalBypass();
        try
        {
            rows = cl == null
                   ? QueryProcessor.executeInternal(select, SCOPE)
                   : QueryProcessor.process(select, cl, List.of(UTF8Type.instance.decompose(SCOPE)));
        }
        catch (RuntimeException e)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                             base.keyspace + '.' + base.name + ":coverage-read", 1, TimeUnit.MINUTES,
                             "Tiered storage: could not read {}.{}'s chunk coverage ledger; merging chunks into " +
                             "every read of this table until it can be read again", base.keyspace, base.name, e);
            return Coverage.UNKNOWN;
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }

        if (rows == null || rows.isEmpty())
            return Coverage.EMPTY;

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long widest = 0;
        // One row per node that has ever encoded: aggregate rather than trust any single one, so a
        // node whose own claim is behind cannot narrow the cluster's coverage.
        for (UntypedResultSet.Row row : rows)
        {
            if (!row.has(MIN_WINDOW_START) || !row.has(MAX_WINDOW_START) || !row.has(MAX_CHUNK_WINDOW))
                continue;
            min = Math.min(min, row.getTimestamp(MIN_WINDOW_START).getTime());
            max = Math.max(max, row.getTimestamp(MAX_WINDOW_START).getTime());
            widest = Math.max(widest, row.getLong(MAX_CHUNK_WINDOW));
        }
        if (widest <= 0)
            return Coverage.EMPTY;

        return new Coverage(true, true, true, min, max, widest);
    }

    /** @return this node's ledger row key; stable across restarts, distinct per node. */
    private static String nodeId()
    {
        return FBUtilities.getBroadcastAddressAndPort().toString();
    }

    private static String coverageRef(TableMetadata base)
    {
        return format("%s.%s",
                      ColumnIdentifier.maybeQuote(base.keyspace),
                      ColumnIdentifier.maybeQuote(ChunkTables.coverageTableName(base.name)));
    }
}
