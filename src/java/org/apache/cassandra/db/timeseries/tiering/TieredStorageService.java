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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.timeseries.ChunkCodecs;
import org.apache.cassandra.db.timeseries.SampleCursor;
import org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;

/**
 * Background re-encoder that turns closed, hot-window-expired time-series rows into Chimp128
 * chunks in a shadow {@code "<table>__chunks"} table (see {@link ChunkTables}), then tombstones the
 * source rows it just encoded.
 * <p>
 * {@link #runOnce} is the whole cycle for one (keyspace, table) pair, run synchronously and
 * idempotently -- see docs/superpowers/plans/2026-07-31-chunk-store-sp2.md ("재인코딩 사이클") for the
 * normative algorithm this implements. {@link #instance} is the process-wide singleton wired up by
 * {@link org.apache.cassandra.service.CassandraDaemon#setup()} via {@link #setup()}, which registers
 * the {@link TieredStorageServiceMBean} and schedules a single, global sweep (see {@link #sweep}) that
 * drives {@link #runOnce} for every policy-bearing table on its own {@code interval}; callers that just
 * want the core cycle (tests, {@code nodetool retier}) can still call {@link #runOnce} directly, or go
 * through the per-table overlap guard via {@link #retier}.
 * <p>
 * <b>Invariants (violating either fails review):</b>
 * <ul>
 *     <li>Every data read/write/delete goes through {@link QueryProcessor#process} at the policy's
 *     configured {@link ConsistencyLevel} -- never {@code executeInternal}, which would only see
 *     locally-held data and could tombstone rows a distributed delete should have fanned out to.</li>
 *     <li>The range delete of source rows for a re-encoded window always uses
 *     {@code USING TIMESTAMP <maxWritetimeOfRowsEncodedIntoTheChunk>}, so a row written after this
 *     cycle read the window (a "late" row) has a newer write timestamp than the tombstone and
 *     survives it -- to be merged into the chunk on a subsequent cycle.</li>
 * </ul>
 * <p>
 * Per-tag work is memory-bounded: rather than reading every closed row for a tag into one list,
 * {@link #runOnce} walks the tag's closed windows one at a time (see {@code firstClosedWindowStart}/
 * {@code nextClosedWindowStart}), so peak memory is one window's worth of rows, not the whole
 * backlog. A failure re-encoding one tag (a decode error on a corrupt existing chunk, a read/write
 * timeout, ...) is logged and skipped rather than aborting every other tag on the table.
 * <p>
 * The window walk's loop guards {@code windowStart >= cutoff} on every path that can advance it --
 * initial discovery, the empty-window jump, and the dense (window-after-window) continuation alike --
 * so no window that reaches into the hot region is ever fetched, encoded, or deleted, no matter how
 * densely a tag is being ingested.
 */
public class TieredStorageService implements TieredStorageServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(TieredStorageService.class);

    /** Network page size for the paged scans this cycle issues (tag enumeration, per-window row reads, cold-expiry candidates). */
    private static final int PAGE_SIZE = 5000;

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=TieredStorage";

    /** Fixed delay between global sweep ticks; each policy-bearing table is still gated by its own {@code interval}. */
    private static final long SWEEP_DELAY_SECONDS = 60;

    /** Process-wide singleton wired up by {@link org.apache.cassandra.service.CassandraDaemon#setup()} via {@link #setup}. */
    public static final TieredStorageService instance = new TieredStorageService();

    /** Per-{@link #runOnce} call counters, also surfaced via the virtual table / {@link #statusRows}. */
    public static class TierRunStats
    {
        public long windowsEncoded;
        public long rowsEncoded;
        public long lateMerges;
        public long chunksExpired;
        public long bytesWritten;
    }

    private final AtomicBoolean setupDone = new AtomicBoolean(false);

    /** One entry per table that has ever been run (sweep- or {@link #retier}-triggered); {@code true} while a run is in flight. */
    private final ConcurrentHashMap<String, AtomicBoolean> runningGates = new ConcurrentHashMap<>();
    /** {@code "keyspace.table"} -> epoch millis of that table's last *completed* run. */
    private final ConcurrentHashMap<String, Long> lastRunAtMillisByTable = new ConcurrentHashMap<>();
    /** {@code "keyspace.table"} -> stats from that table's last *completed* run. */
    private final ConcurrentHashMap<String, TierRunStats> lastStatsByTable = new ConcurrentHashMap<>();

    /**
     * Test-only seam: invoked with {@code (keyspace, table)} at the top of every guarded run, i.e.
     * inside the scope whose failures {@link #sweep} must isolate per table (and {@link #retier} must
     * propagate). Lets a test deterministically fail one table's run -- standing in for, say, an
     * {@code UnavailableException} out of {@link #runOnce}'s tag enumeration -- which cannot be
     * provoked naturally on a healthy single-node cluster.
     */
    @VisibleForTesting
    volatile BiConsumer<String, String> preRunHookForTesting;

    /**
     * Hard cap on the samples a single (tag, window) may accumulate in one cycle -- the codec's
     * per-chunk limit ({@link ChunkCodecs#MAX_SAMPLES}), pre-checked here so an over-dense window is
     * aborted while still paging (memory stays bounded) with an actionable error, instead of being
     * fully materialized and then blowing up in {@code ChunkCodecs.encode} every cycle forever.
     * Non-final only as a test seam: shrinking it lets a test trigger the abort with a handful of
     * rows; production code must never write it.
     */
    @VisibleForTesting
    volatile int maxSamplesPerWindow = ChunkCodecs.MAX_SAMPLES;

    /**
     * Registers the {@value #MBEAN_NAME} MBean and schedules the single, process-wide sweep
     * ({@link #sweep}) that drives every policy-bearing table's re-encode cycle, on
     * {@link ScheduledExecutors#optionalTasks} at a fixed {@value #SWEEP_DELAY_SECONDS}s delay.
     * <p>
     * Idempotent: guarded by {@link #setupDone}, so a second (or later) call is a silent no-op rather
     * than double-registering the MBean or scheduling a second sweep loop -- callers (namely
     * {@link org.apache.cassandra.service.CassandraDaemon#setup()}) do not need to track whether this
     * has already run.
     */
    public void setup()
    {
        if (!setupDone.compareAndSet(false, true))
            return;

        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(this::sweep, SWEEP_DELAY_SECONDS, SWEEP_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * One global tick: walks every table of every non-system keyspace ({@link Schema#getUserKeyspaces}),
     * and for each with a {@link TieringPolicy} whose {@code interval} has elapsed since its last
     * completed run, invokes the guarded runner ({@link #runGuarded}). A table whose policy fails to
     * parse is retried every tick too (cheaply -- {@link #runOnce} fails fast and logs the specifics)
     * rather than being permanently ignored until an operator notices and fixes it.
     * <p>
     * Each table's run is individually isolated: one table failing (say, an
     * {@link org.apache.cassandra.exceptions.UnavailableException} because its keyspace cannot
     * currently meet the policy's consistency level) must neither abort the tick for every table
     * iterated after it -- the failing table's {@code lastRunAt} never advances, so it would
     * deterministically fail first and starve the rest every tick -- nor escape into the scheduled
     * executor, whose failure wrapper swallows request-failure exceptions without logging.
     */
    @VisibleForTesting
    void sweep()
    {
        long now = Clock.Global.currentTimeMillis();
        for (KeyspaceMetadata keyspace : Schema.instance.getUserKeyspaces())
        {
            for (TableMetadata table : keyspace.tables)
            {
                try
                {
                    if (dueForSweep(keyspace.name, table, now))
                        runGuarded(keyspace.name, table.name, false);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn("Tiered storage sweep failed for {}.{}; skipping it this tick and continuing " +
                                "with the remaining tables", keyspace.name, table.name, t);
                }
            }
        }
    }

    private boolean dueForSweep(String keyspace, TableMetadata table, long now)
    {
        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(table);
        }
        catch (ConfigurationException e)
        {
            return true;
        }
        if (policy == null)
            return false;

        Long lastRun = lastRunAtMillisByTable.get(key(keyspace, table.name));
        return lastRun == null || now - lastRun >= policy.intervalMillis;
    }

    /**
     * Runs one re-encode cycle for {@code keyspace.table} under that table's per-table overlap gate,
     * recording the result for {@link #statusRows} / the virtual table. If a run for this table is
     * already in flight: silently skipped when {@code throwIfBusy} is {@code false} (the sweep's own
     * calls -- the next tick will simply try again), or fails with {@link IllegalStateException} when
     * {@code true} ({@link #retier}, which is a direct operator request and should say so rather than
     * silently doing nothing).
     */
    private void runGuarded(String keyspace, String table, boolean throwIfBusy)
    {
        String key = key(keyspace, table);
        AtomicBoolean gate = runningGates.computeIfAbsent(key, ignored -> new AtomicBoolean(false));
        if (!gate.compareAndSet(false, true))
        {
            if (throwIfBusy)
                throw new IllegalStateException(format("Tiered storage run already in flight for %s.%s", keyspace, table));

            logger.debug("Tiered storage sweep: {}.{} is already running -- skipping this tick", keyspace, table);
            return;
        }

        try
        {
            BiConsumer<String, String> hook = preRunHookForTesting;
            if (hook != null)
                hook.accept(keyspace, table);

            TierRunStats stats = runOnce(keyspace, table, Clock.Global.currentTimeMillis());
            lastRunAtMillisByTable.put(key, Clock.Global.currentTimeMillis());
            lastStatsByTable.put(key, stats);
        }
        finally
        {
            gate.set(false);
        }
    }

    @Override
    public void retier(String keyspace, String table)
    {
        runGuarded(keyspace, table, true);
    }

    @Override
    public List<String> statusRows()
    {
        List<String> rows = new ArrayList<>();
        for (KeyspaceMetadata keyspace : Schema.instance.getUserKeyspaces())
        {
            for (TableMetadata table : keyspace.tables)
            {
                TieringPolicy policy;
                try
                {
                    policy = TieringPolicy.fromTable(table);
                }
                catch (ConfigurationException e)
                {
                    continue;
                }
                if (policy == null)
                    continue;

                String key = key(keyspace.name, table.name);
                Long lastRun = lastRunAtMillisByTable.get(key);
                TierRunStats stats = lastStatsByTable.get(key);
                rows.add(format("%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d",
                                keyspace.name, table.name, policy.intervalMillis,
                                lastRun == null ? -1L : lastRun,
                                stats == null ? 0L : stats.windowsEncoded,
                                stats == null ? 0L : stats.rowsEncoded,
                                stats == null ? 0L : stats.lateMerges,
                                stats == null ? 0L : stats.chunksExpired));
            }
        }
        return rows;
    }

    /** @return {@code keyspace.table}'s last *completed* run's stats, or {@code null} if it has never run. */
    public TierRunStats lastStats(String keyspace, String table)
    {
        return lastStatsByTable.get(key(keyspace, table));
    }

    /** @return epoch millis of {@code keyspace.table}'s last *completed* run, or {@code null} if it has never run. */
    public Long lastRunAtMillis(String keyspace, String table)
    {
        return lastRunAtMillisByTable.get(key(keyspace, table));
    }

    /**
     * Test-only hook onto the same per-table overlap gate {@link #runGuarded} uses, so a test can hold
     * it open and assert {@link #retier} throws {@link IllegalStateException} -- without needing a real
     * concurrent re-encode cycle in flight.
     *
     * @return {@code true} if the gate was free and is now held; {@code false} if it was already held
     */
    @VisibleForTesting
    boolean acquireGateForTesting(String keyspace, String table)
    {
        AtomicBoolean gate = runningGates.computeIfAbsent(key(keyspace, table), ignored -> new AtomicBoolean(false));
        return gate.compareAndSet(false, true);
    }

    @VisibleForTesting
    void releaseGateForTesting(String keyspace, String table)
    {
        AtomicBoolean gate = runningGates.get(key(keyspace, table));
        if (gate != null)
            gate.set(false);
    }

    private static String key(String keyspace, String table)
    {
        return keyspace + '.' + table;
    }

    /**
     * Runs one re-encode cycle for {@code keyspace.table}. No-ops (returning all-zero stats) if the
     * table has no {@code timeseries_tiering} policy, the policy is invalid, or the table's schema is
     * not the canonical single-partition-key/timestamp-clustering/double-value shape the re-encoder
     * requires -- in the latter two cases an error is logged rather than failing silently.
     */
    public TierRunStats runOnce(String keyspace, String table, long nowMillis)
    {
        // The re-encoder's own reads must see raw base rows, never the transparent hot+chunk merge -
        // merged reads would make already-encoded windows look live again (idempotency loop).
        TransparentReads.enterInternalBypass();
        try
        {
            return runOnceInner(keyspace, table, nowMillis);
        }
        finally
        {
            TransparentReads.exitInternalBypass();
        }
    }

    private TierRunStats runOnceInner(String keyspace, String table, long nowMillis)
    {
        TierRunStats stats = new TierRunStats();

        TableMetadata base = Schema.instance.getTableMetadata(keyspace, table);
        if (base == null)
        {
            logger.warn("Tiered storage runOnce skipped: {}.{} does not exist", keyspace, table);
            return stats;
        }

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(base);
        }
        catch (ConfigurationException e)
        {
            logger.error("Tiered storage runOnce skipped: {}.{} has an invalid timeseries_tiering policy: {}",
                         keyspace, table, e.getMessage());
            return stats;
        }
        if (policy == null)
            return stats;

        String schemaError = TieringPolicy.canonicalSchemaError(base);
        if (schemaError != null)
        {
            logger.error("Tiered storage runOnce skipped: {}.{} has a timeseries_tiering policy but is not a " +
                         "canonical time-series table: {}", keyspace, table, schemaError);
            return stats;
        }

        ChunkTables.ensureChunkTable(base);
        String chunkRef = quotedRef(keyspace, ChunkTables.chunkTableName(table));

        ColumnMetadata tagColumn = base.partitionKeyColumns().get(0);
        ColumnMetadata tsColumn = base.clusteringColumns().get(0);
        ColumnMetadata valueColumn = base.regularColumns().iterator().next();

        String tagCql = tagColumn.name.toCQLString();
        String tsCql = tsColumn.name.toCQLString();
        String valueCql = valueColumn.name.toCQLString();
        String tagRaw = tagColumn.name.toString();
        String tsRaw = tsColumn.name.toString();
        String valueRaw = valueColumn.name.toString();
        String baseRef = base.toString();

        ConsistencyLevel cl = policy.consistency;
        long cutoff = policy.windowStartFor(nowMillis - policy.hotWindowMillis);

        // Reused/grown across every window of every tag this call -- not reallocated per window.
        long[] tsBuf = new long[1024];
        double[] valBuf = new double[1024];
        // Rows with a null value/writetime cell are common (e.g. a bare tag+ts insert, or a
        // TTL-expired value) and expected, not exceptional -- summarized once at the end of the run
        // rather than logged per row.
        long[] nullCellsSkipped = { 0 };

        // Bounded, per-window queries -- see the class javadoc. "oldest"/"next" only ever return the
        // single row needed to locate the next window with work; the row scan itself is restricted to
        // exactly one [windowStart, windowEnd) range at a time, so memory is bounded by one window's
        // row count, not a tag's entire closed backlog.
        String oldestRowQuery = format("SELECT %s FROM %s WHERE %s = ? AND %s < ? ORDER BY %s ASC LIMIT 1",
                                       tsCql, baseRef, tagCql, tsCql, tsCql);
        // Both LIMIT 1 probes say ORDER BY ... ASC explicitly: on a table declared WITH CLUSTERING
        // ORDER BY (ts DESC) -- the dominant time-series idiom -- the default order is newest-first,
        // so an order-less probe would return the NEWEST row in range and the empty-window jump would
        // leap over every window between here and the cutoff instead of landing on the next one.
        String nextRowQuery = format("SELECT %s FROM %s WHERE %s = ? AND %s >= ? AND %s < ? ORDER BY %s ASC LIMIT 1",
                                     tsCql, baseRef, tagCql, tsCql, tsCql, tsCql, tsCql);
        String windowRowsQuery = format("SELECT %s, %s, WRITETIME(%s) AS wt FROM %s WHERE %s = ? AND %s >= ? " +
                                        "AND %s < ? ORDER BY %s ASC",
                                        tsCql, valueCql, valueCql, baseRef, tagCql, tsCql, tsCql, tsCql);
        String existingChunkQuery = format("SELECT payload, max_row_writetime, WRITETIME(payload) AS chunk_wt " +
                                           "FROM %s WHERE %s = ? AND window_start = ?", chunkRef, tagCql);
        String insertChunkQuery = format("INSERT INTO %s (%s, window_start, codec, samples, max_row_writetime, payload) " +
                                         "VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?", chunkRef, tagCql);
        String deleteRowsQuery = format("DELETE FROM %s USING TIMESTAMP ? WHERE %s = ? AND %s >= ? AND %s < ?",
                                        baseRef, tagCql, tsCql, tsCql);
        String selectExpiredQuery = format("SELECT window_start FROM %s WHERE %s = ? AND window_start < ?", chunkRef, tagCql);
        String deleteExpiredQuery = format("DELETE FROM %s WHERE %s = ? AND window_start < ?", chunkRef, tagCql);

        for (ByteBuffer tag : enumerateTags(base, tagCql, tagRaw, baseRef, cl))
        {
            // One tag's worth of trouble -- a corrupt existing chunk, a read/write timeout, anything --
            // must not wedge every other tag on this table out of being re-encoded for good.
            try
            {
                long windowStart = firstClosedWindowStart(oldestRowQuery, tsRaw, tag, cutoff, policy, cl);
                while (windowStart >= 0)
                {
                    // Belt: no window that reaches into (or past) the hot region may ever be fetched,
                    // encoded, or deleted, no matter which path set windowStart -- discovery, the
                    // empty-window jump below, and the dense continuation at the bottom of this loop
                    // all funnel through this one check before doing anything.
                    if (windowStart >= cutoff)
                        break;

                    long windowEnd = windowStart + policy.chunkWindowMillis;
                    // Suspenders: windowStart is always chunk-window-aligned (windowStartFor's output,
                    // or windowStart+chunkWindowMillis below, which preserves alignment) and so is
                    // cutoff, so windowEnd <= cutoff is already implied by the guard just above -- this
                    // clamp is redundant given that invariant, but applied anyway to the two operations
                    // that actually touch hot data (this read, and the delete below), as a second,
                    // independent guard against the exact class of bug the belt above exists to fix.
                    long readEnd = Math.min(windowEnd, cutoff);
                    int maxSamples = maxSamplesPerWindow;
                    List<UntypedResultSet.Row> windowRows = pagedSelect(windowRowsQuery, cl, Arrays.asList(
                            tag,
                            TimestampType.instance.fromTimeInMillis(windowStart),
                            TimestampType.instance.fromTimeInMillis(readEnd)),
                            maxSamples);
                    if (windowRows.size() > maxSamples)
                    {
                        // Paging stopped as soon as the count crossed the cap (see pagedSelect), so
                        // memory stayed bounded -- but this window can never be encoded as configured.
                        // Encoding a partial window would delete rows the chunk doesn't contain, and
                        // retrying the full read every cycle would wedge this tag forever, so abort the
                        // tag's walk until an operator shrinks chunk_window.
                        logger.error("Tiered storage runOnce: {}.{} tag {} window [{}, {}) holds more than {} rows, " +
                                     "over the {}-sample per-chunk codec limit -- it cannot be encoded. Lower the " +
                                     "table's timeseries_tiering chunk_window so one window holds at most {} samples; " +
                                     "skipping this tag until then", keyspace, table, tagColumn.type.getString(tag),
                                     windowStart, readEnd, maxSamples, maxSamples, maxSamples);
                        break;
                    }

                    if (windowRows.isEmpty())
                    {
                        // Nothing in this window (raced with a concurrent process, or the oldest-row probe
                        // landed on a sparse tag) -- jump straight to the next window that has data instead
                        // of stepping through potentially many empty windows one at a time.
                        windowStart = nextClosedWindowStart(nextRowQuery, tsRaw, tag, windowEnd, cutoff, policy, cl);
                        continue;
                    }

                    UntypedResultSet existingRs = QueryProcessor.process(existingChunkQuery, cl,
                            Arrays.asList(tag, TimestampType.instance.fromTimeInMillis(windowStart)));
                    UntypedResultSet.Row existingRow = (existingRs == null || existingRs.isEmpty()) ? null : existingRs.one();

                    TreeMap<Long, Double> merged = new TreeMap<>();
                    long maxWt = Long.MIN_VALUE;
                    long existingChunkWt = Long.MIN_VALUE;
                    if (existingRow != null)
                    {
                        SampleCursor cursor = ChunkCodecs.cursor(existingRow.getBytes("payload"));
                        while (cursor.advance())
                            merged.put(cursor.timestamp(), cursor.value());
                        maxWt = existingRow.getLong("max_row_writetime");
                        existingChunkWt = existingRow.getLong("chunk_wt");
                    }

                    int rowsThisWindow = 0;
                    for (UntypedResultSet.Row row : windowRows)
                    {
                        // A row with no live value cell (bare tag+ts insert, an explicit `DELETE value`,
                        // an expired TTL, ...) has nothing to encode and no writetime to trust for the
                        // delete below -- skip it rather than NPE-ing on getDouble/getLong(null).
                        if (!row.has(valueRaw) || !row.has("wt"))
                        {
                            nullCellsSkipped[0]++;
                            continue;
                        }
                        // Rows always win over the previously-encoded chunk on a ts collision -- they are
                        // by definition a rewrite of that timestamp with a newer write time.
                        merged.put(row.getTimestamp(tsRaw).getTime(), row.getDouble(valueRaw));
                        maxWt = Math.max(maxWt, row.getLong("wt"));
                        rowsThisWindow++;
                    }

                    if (merged.isEmpty())
                    {
                        // Every row in the window was null-valued and there was no prior chunk to carry
                        // forward -- nothing to encode, and no trustworthy writetime to tombstone with, so
                        // leave the window untouched; it is retried (cheaply) on every future cycle.
                        windowStart = windowEnd;
                        continue;
                    }

                    int count = merged.size();
                    if (count > maxSamples)
                    {
                        // The window's own rows fit the cap, but merged with the existing chunk's
                        // samples (disjoint late-row timestamps) the total doesn't -- encode would
                        // throw, be caught by the per-tag handler, and retry identically forever.
                        logger.error("Tiered storage runOnce: {}.{} tag {} window [{}, {}) merges to {} samples, " +
                                     "over the {}-sample per-chunk codec limit -- it cannot be encoded. Lower the " +
                                     "table's timeseries_tiering chunk_window so one window holds at most {} samples; " +
                                     "skipping this tag until then", keyspace, table, tagColumn.type.getString(tag),
                                     windowStart, readEnd, count, maxSamples, maxSamples);
                        break;
                    }
                    if (tsBuf.length < count)
                    {
                        int newLength = tsBuf.length;
                        while (newLength < count)
                            newLength *= 2;
                        tsBuf = Arrays.copyOf(tsBuf, newLength);
                        valBuf = Arrays.copyOf(valBuf, newLength);
                    }
                    int idx = 0;
                    for (Map.Entry<Long, Double> sample : merged.entrySet())
                    {
                        tsBuf[idx] = sample.getKey();
                        valBuf[idx] = sample.getValue();
                        idx++;
                    }

                    ByteBuffer payload = ChunkCodecs.encode(tsBuf, valBuf, count);
                    // Read the version byte back out of the payload rather than naming a codec
                    // constant here: the `codec` column must describe what was actually written, so
                    // it stays honest if the encode path ever changes underneath this call.
                    byte codecByte = payload.get(payload.position());

                    // Always write strictly after both the rows just encoded AND the chunk row being
                    // replaced -- guards a crash-then-backfill corner where maxWt+1 could otherwise land
                    // exactly on the existing chunk's own write timestamp and tie (same-timestamp writes
                    // to different columns of the same row can resolve per-column, tearing the chunk).
                    long insertTs = Math.max(maxWt + 1, existingChunkWt + 1);

                    QueryProcessor.process(insertChunkQuery, cl, Arrays.asList(
                            tag,
                            TimestampType.instance.fromTimeInMillis(windowStart),
                            ByteType.instance.decompose(codecByte),
                            Int32Type.instance.decompose(count),
                            LongType.instance.decompose(maxWt),
                            payload,
                            LongType.instance.decompose(insertTs)));

                    QueryProcessor.process(deleteRowsQuery, cl, Arrays.asList(
                            LongType.instance.decompose(maxWt),
                            tag,
                            TimestampType.instance.fromTimeInMillis(windowStart),
                            TimestampType.instance.fromTimeInMillis(readEnd)));

                    stats.windowsEncoded++;
                    stats.rowsEncoded += rowsThisWindow;
                    if (existingRow != null)
                        stats.lateMerges++;
                    stats.bytesWritten += payload.remaining();

                    windowStart = windowEnd;
                }

            }
            catch (UnsupportedChunkFormatException e)
            {
                // The tag's existing chunk was written by a build whose chunk format this one cannot
                // read, so the late-merge read of it failed. Retrying achieves nothing -- unlike the
                // generic failure below, this never fixes itself -- and the tag is now permanently
                // stuck, so say what has to happen instead of implying a retry will help. Skipping is
                // still the safe response: no rows are deleted, so nothing further is lost.
                logger.error("Tiered storage runOnce: {}.{} cannot re-encode tag {}: its existing chunk was written " +
                             "by an older build and is unreadable ({}). Retrying will not fix this -- drop {} and " +
                             "let tiering re-run; that data is not recoverable. Source rows are left untouched.",
                             keyspace, table, tagColumn.type.getString(tag), e.getMessage(),
                             ChunkTables.chunkTableName(table));
            }
            catch (RuntimeException e)
            {
                logger.error("Tiered storage runOnce: {}.{} failed while re-encoding tag {} -- skipping to the " +
                             "next tag; this tag will be retried next cycle", keyspace, table,
                             tagColumn.type.getString(tag), e);
            }
        }

        if (nullCellsSkipped[0] > 0)
        {
            logger.warn("Tiered storage runOnce: {}.{} skipped {} row(s) with no live value cell to encode " +
                        "(bare key insert, deleted value, or expired TTL)", keyspace, table, nullCellsSkipped[0]);
        }

        // Cold expiry enumerates tags from the CHUNK table, not the base table (SP3 R6): a tag whose
        // base rows were all re-encoded (or TTL'd) away no longer appears in the base DISTINCT scan,
        // but its cold chunks must still expire.
        if (policy.coldWindowMillis >= 0)
        {
            TableMetadata chunkMeta = Schema.instance.getTableMetadata(keyspace, ChunkTables.chunkTableName(table));
            if (chunkMeta != null)
            {
                for (ByteBuffer tag : enumerateTags(chunkMeta, tagCql, tagRaw, chunkRef, cl))
                {
                    try
                    {
                        List<ByteBuffer> coldValues = Arrays.asList(
                                tag, TimestampType.instance.fromTimeInMillis(nowMillis - policy.coldWindowMillis));
                        List<UntypedResultSet.Row> expired = pagedSelect(selectExpiredQuery, cl, coldValues);
                        if (!expired.isEmpty())
                        {
                            stats.chunksExpired += expired.size();
                            QueryProcessor.process(deleteExpiredQuery, cl, coldValues);
                        }
                    }
                    catch (RuntimeException e)
                    {
                        logger.error("Tiered storage runOnce: {}.{} failed while expiring cold chunks of tag {} -- " +
                                     "skipping to the next tag; retried next cycle", keyspace, table,
                                     tagColumn.type.getString(tag), e);
                    }
                }
            }
        }

        return stats;
    }

    /** @return the {@code window_start} of the oldest closed (ts &lt; cutoff) row for {@code tag}, or -1 if none. */
    private static long firstClosedWindowStart(String oldestRowQuery, String tsRaw, ByteBuffer tag, long cutoff,
                                                TieringPolicy policy, ConsistencyLevel cl)
    {
        UntypedResultSet rs = QueryProcessor.process(oldestRowQuery, cl,
                Arrays.asList(tag, TimestampType.instance.fromTimeInMillis(cutoff)));
        if (rs == null || rs.isEmpty())
            return -1;
        return policy.windowStartFor(rs.one().getTimestamp(tsRaw).getTime());
    }

    /**
     * @return the {@code window_start} of the next closed row for {@code tag} with {@code ts} in
     * {@code [fromTsMillis, cutoff)}, or -1 if there is none -- used to skip past a run of empty
     * windows in one query instead of probing them one at a time.
     */
    private static long nextClosedWindowStart(String nextRowQuery, String tsRaw, ByteBuffer tag, long fromTsMillis,
                                               long cutoff, TieringPolicy policy, ConsistencyLevel cl)
    {
        UntypedResultSet rs = QueryProcessor.process(nextRowQuery, cl, Arrays.asList(
                tag, TimestampType.instance.fromTimeInMillis(fromTsMillis), TimestampType.instance.fromTimeInMillis(cutoff)));
        if (rs == null || rs.isEmpty())
            return -1;
        return policy.windowStartFor(rs.one().getTimestamp(tsRaw).getTime());
    }

    /**
     * Enumerates the distinct partition keys ("tags") of {@code base} restricted to this node's local
     * primary token ranges ({@link StorageService#getPrimaryRanges}), so a multi-node cluster's
     * re-encoders partition the tag space instead of every node redundantly re-encoding every tag.
     * <p>
     * A primary range can wrap the ring's zero point (e.g. a single-node cluster's sole range, which
     * is degenerate: {@code (t, t]} covering the whole ring); {@link Range#unwrap()} normalizes that
     * into 1-2 non-wrapping sub-ranges, each turned into its own bound {@code token(tag) > ? AND <= ?}
     * restriction (a bound equal to the partitioner's minimum token is omitted -- it is a sentinel,
     * not a real, ownable token). Bind values are encoded via the partitioner's own
     * {@link org.apache.cassandra.dht.IPartitioner#getTokenValidator()} type, so this works for any
     * partitioner's token representation (a raw {@code long} for Murmur3, a {@code BigInteger} for
     * RandomPartitioner, etc.), not just one whose {@code Token.toString()} happens to be a bare CQL
     * numeric literal. If {@code getPrimaryRanges} itself reports no ranges at all -- which should not
     * happen once a node has joined the ring, but is treated defensively rather than silently scanning
     * nothing -- this falls back to an unrestricted scan of every tag.
     */
    private static List<ByteBuffer> enumerateTags(TableMetadata base, String tagCql, String tagRaw, String baseRef,
                                                   ConsistencyLevel cl)
    {
        Set<ByteBuffer> tags = new LinkedHashSet<>();
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRanges(base.keyspace);

        if (primaryRanges.isEmpty())
        {
            logger.warn("Tiered storage: {}.{} has no local primary ranges reported for this node; falling back " +
                        "to an unrestricted tag scan rather than silently skipping data", base.keyspace, base.name);
            collectTags(tags, format("SELECT DISTINCT %s FROM %s", tagCql, baseRef), tagRaw, cl, Collections.emptyList());
            return new ArrayList<>(tags);
        }

        AbstractType<?> tokenType = base.partitioner.getTokenValidator();
        for (Range<Token> range : primaryRanges)
        {
            for (Range<Token> sub : range.unwrap())
            {
                List<ByteBuffer> values = new ArrayList<>(2);
                String query = tagRangeQuery(tagCql, baseRef, sub, tokenType, values);
                collectTags(tags, query, tagRaw, cl, values);
            }
        }

        return new ArrayList<>(tags);
    }

    /** Builds the token-range restricted tag query, appending its bind values (0-2) to {@code valuesOut} in order. */
    private static String tagRangeQuery(String tagCql, String baseRef, Range<Token> sub, AbstractType<?> tokenType,
                                        List<ByteBuffer> valuesOut)
    {
        boolean hasLower = !sub.left.isMinimum();
        boolean hasUpper = !sub.right.isMinimum();

        StringBuilder query = new StringBuilder("SELECT DISTINCT ").append(tagCql).append(" FROM ").append(baseRef);
        if (hasLower || hasUpper)
        {
            query.append(" WHERE ");
            if (hasLower)
            {
                query.append("token(").append(tagCql).append(") > ?");
                valuesOut.add(tokenType.decomposeUntyped(sub.left.getTokenValue()));
            }
            if (hasLower && hasUpper)
                query.append(" AND ");
            if (hasUpper)
            {
                query.append("token(").append(tagCql).append(") <= ?");
                valuesOut.add(tokenType.decomposeUntyped(sub.right.getTokenValue()));
            }
        }
        return query.toString();
    }

    private static void collectTags(Set<ByteBuffer> out, String query, String tagRaw, ConsistencyLevel cl,
                                     List<ByteBuffer> values)
    {
        for (UntypedResultSet.Row row : pagedSelect(query, cl, values))
            out.add(row.getBytes(tagRaw));
    }

    private static String quotedRef(String keyspace, String table)
    {
        return format("%s.%s", ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(table));
    }

    /**
     * Runs {@code query} to completion across as many pages as needed (network page size
     * {@link #PAGE_SIZE}), via {@link QueryProcessor#instance} directly rather than the static
     * {@link QueryProcessor#process(String, ConsistencyLevel, List)} convenience method, since that
     * overload has no way to carry a {@link PagingState} between calls. Only ever used for queries this
     * class has already bounded to at most one tag/one window/one candidate-list -- never for an
     * unbounded per-tag row scan (see the class javadoc).
     */
    private static List<UntypedResultSet.Row> pagedSelect(String query, ConsistencyLevel cl, List<ByteBuffer> values)
    {
        return pagedSelect(query, cl, values, Integer.MAX_VALUE);
    }

    /**
     * As {@link #pagedSelect(String, ConsistencyLevel, List)}, but stops fetching further pages as soon
     * as more than {@code maxRows} rows have accumulated, returning what it has (at most one page over
     * the cap). Callers that pass a real cap must treat {@code result.size() > maxRows} as "the scan
     * overflowed, and the result is truncated" -- see the {@code maxSamplesPerWindow} guard in
     * {@link #runOnce} -- so an over-dense window is detected mid-paging with bounded memory, never
     * fully materialized.
     */
    private static List<UntypedResultSet.Row> pagedSelect(String query, ConsistencyLevel cl, List<ByteBuffer> values,
                                                          int maxRows)
    {
        List<UntypedResultSet.Row> rows = new ArrayList<>();
        QueryState queryState = QueryState.forInternalCalls();
        PagingState pagingState = null;
        while (true)
        {
            QueryOptions options = QueryOptions.create(cl, values, false, PAGE_SIZE, pagingState, null,
                                                        ProtocolVersion.CURRENT, null);
            CQLStatement statement = QueryProcessor.instance.parse(query, queryState, options);
            ResultMessage result = QueryProcessor.instance.process(statement, queryState, options,
                                                                    Dispatcher.RequestTime.forImmediateExecution());
            if (!(result instanceof ResultMessage.Rows))
                break;

            ResultSet resultSet = ((ResultMessage.Rows) result).result;
            for (UntypedResultSet.Row row : UntypedResultSet.create(resultSet))
                rows.add(row);

            if (rows.size() > maxRows)
                break;

            pagingState = resultSet.metadata.getPagingState();
            if (pagingState == null)
                break;
        }
        return rows;
    }
}
