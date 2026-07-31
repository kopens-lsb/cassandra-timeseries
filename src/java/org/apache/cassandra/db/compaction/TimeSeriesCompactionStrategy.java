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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Time-series compaction: SSTables are classified into fixed time windows by max timestamp.
 * Active windows (current, or closed less than freeze_after ago) delegate their compaction to an
 * internal {@link UnifiedCompactionStrategy}; windows past the configured retention are dropped
 * whole via {@link TimeSeriesCompactionTask}/{@link TimeSeriesCompactionController}
 * (see {@link TimeSeriesCompactionStrategyOptions#isExpiredWindow}). Closed windows are frozen to a
 * single sstable per window (per strategy-instance slice) by
 * {@link FreezeCompactionTask}, which fires {@link org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener}
 * post-commit and, by running a real compaction controller, also reclaims TTL/tombstone data in closed
 * windows without requiring {@code retention}. Late-data isolation (flush/streaming window splitting)
 * arrives in T3 (design spec section 10).
 * <p>
 * Instances only ever see the sstable slice the CompactionStrategyManager assigns them (per
 * repair status and per disk), so all window state is derived per call and never assumed complete.
 */
public class TimeSeriesCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCompactionStrategy.class);

    private final TimeSeriesCompactionStrategyOptions tsOptions;
    private final UnifiedCompactionStrategy delegate;
    private final Set<SSTableReader> sstables = new HashSet<>();
    // the set of expired-window sstables selected on the most recent background round, so
    // getEstimatedRemainingTasks() can report the pending whole-window drop without recomputing it.
    private volatile Set<SSTableReader> lastExpiredSelection = Set.of();
    // number of FREEZING windows (>= 2 sstables) seen on the most recent background round, for
    // getEstimatedRemainingTasks() - freezes starve behind other tables' work if the CSM cannot see them.
    private volatile int freezeBacklog;
    // the freeze candidate that failed tryModify on the previous round: cross-round version of TWCS's
    // previousCandidate guard (:100-106) - warn when the same candidate is stuck two rounds running,
    // but keep retrying (unlike TWCS's intra-call loop, skipping here would never retry at all).
    private volatile Set<SSTableReader> previousFreezeCandidate = Set.of();

    public TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        this(cfs, options, new TimeSeriesCompactionStrategyOptions(options));
    }

    // Builds the delegate from tsOptions built exactly once by the caller (either public ctor above, or the
    // @VisibleForTesting ctor below), rather than each ctor building its own TimeSeriesCompactionStrategyOptions.
    private TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options, TimeSeriesCompactionStrategyOptions tsOptions)
    {
        this(cfs, options, tsOptions, new UnifiedCompactionStrategy(cfs, tsOptions.delegateOptions(options)));
    }

    @VisibleForTesting
    TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options, UnifiedCompactionStrategy delegate)
    {
        this(cfs, options, new TimeSeriesCompactionStrategyOptions(options), delegate);
    }

    private TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options,
                                         TimeSeriesCompactionStrategyOptions tsOptions, UnifiedCompactionStrategy delegate)
    {
        super(cfs, options);
        this.tsOptions = tsOptions;
        this.delegate = delegate;
    }

    @Override
    public Collection<AbstractCompactionTask> getNextBackgroundTasks(long gcBefore)
    {
        return getNextBackgroundTasksAt(Clock.Global.currentTimeMillis(), gcBefore);
    }

    @VisibleForTesting
    Collection<AbstractCompactionTask> getNextBackgroundTasksAt(long nowMillis, long gcBefore)
    {
        syncDelegate(nowMillis);

        Set<SSTableReader> expired = expiredSSTables(nowMillis);
        lastExpiredSelection = expired;
        if (!expired.isEmpty())
        {
            // Mirror UCS's zombie-sstable filter (UnifiedCompactionStrategy#getSSTables, CASSANDRA-18342):
            // only ever try to mark sstables the tracker still considers live, and never a suspect one, for
            // compaction. expiredSSTables() classifies purely off this instance's own bookkeeping, which can
            // be briefly stale relative to the tracker's live set.
            Set<SSTableReader> toDrop = new HashSet<>(AbstractCompactionStrategy.filterSuspectSSTables(expired));
            toDrop.retainAll(cfs.getLiveSSTables());
            if (!toDrop.isEmpty())
            {
                LifecycleTransaction txn = cfs.getTracker().tryModify(toDrop, OperationType.COMPACTION);
                if (txn != null)
                {
                    long cutoff = nowMillis - tsOptions.retentionMillis;
                    logger.info("Dropping {} expired-window sstables from {} (retention cutoff {})",
                                toDrop.size(), cfs.getTableName(), cutoff);
                    return List.of(new TimeSeriesCompactionTask(cfs, txn, gcBefore, cutoff, tsOptions.timestampResolution));
                }
                logger.debug("Unable to mark {} expired-window sstables for compaction in {}; probably a background " +
                             "compaction or the retention drop from a previous round got to them first",
                             toDrop.size(), cfs.getTableName());
            }
        }

        Map.Entry<Long, Set<SSTableReader>> freeze = nextFreezeCandidate(nowMillis);
        if (freeze != null)
        {
            // Same zombie filter as the expired branch: only live, non-suspect sstables.
            Set<SSTableReader> toFreeze = new HashSet<>(AbstractCompactionStrategy.filterSuspectSSTables(freeze.getValue()));
            toFreeze.retainAll(cfs.getLiveSSTables());
            if (toFreeze.size() > 1)                      // a single survivor is already "frozen enough"; self-heals next round
            {
                LifecycleTransaction txn = cfs.getTracker().tryModify(toFreeze, OperationType.COMPACTION);
                if (txn != null)
                {
                    previousFreezeCandidate = Set.of();
                    logger.debug("Freezing window {} of {}: {} sstables -> 1", freeze.getKey(), cfs.getTableName(), toFreeze.size());
                    return List.of(new FreezeCompactionTask(cfs, txn, gcBefore, freeze.getKey()));
                }
                // Refused (e.g. a delegate compaction started while the window was CLOSING is still running):
                // skip this round, fall through to the delegate, retry next round (spec section 8).
                if (toFreeze.equals(previousFreezeCandidate))
                    logger.warn("Could not acquire references for freezing sstables {} which is not a problem per se," +
                                " unless it happens frequently, in which case it must be reported. Will retry later.",
                                toFreeze);
                else
                    logger.debug("Unable to mark window {} of {} for freezing; will retry next round",
                                 freeze.getKey(), cfs.getTableName());
                previousFreezeCandidate = toFreeze;
            }
        }

        return delegate.getNextBackgroundTasks(gcBefore);
    }

    /** Sstables whose whole window has closed before the configured retention cutoff, if any. */
    @VisibleForTesting
    synchronized Set<SSTableReader> expiredSSTables(long nowMillis)
    {
        Set<SSTableReader> expired = new HashSet<>();
        if (tsOptions.retentionMillis < 0)
            return expired;
        for (SSTableReader sstable : sstables)
            if (tsOptions.isExpiredWindow(tsOptions.windowStartFor(maxTimestampMillis(sstable)), nowMillis))
                expired.add(sstable);
        return expired;
    }

    private synchronized void syncDelegate(long nowMillis)
    {
        Set<SSTableReader> active = new HashSet<>();
        for (SSTableReader sstable : sstables)
        {
            long windowStart = tsOptions.windowStartFor(maxTimestampMillis(sstable));
            if (tsOptions.isFarFutureWindow(windowStart, nowMillis))
            {
                // Garbage/misconfigured-writer timestamps: keep them out of both the UCS delegate and the
                // freeze machinery, and complain (throttled, keyed per table so one table's warn does not
                // suppress another's) so the operator investigates (design spec section 8).
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                 cfs.getKeyspaceName() + '.' + cfs.getTableName() + ":far-future-window",
                                 1, TimeUnit.MINUTES,
                                 "{}.{} has sstable(s) with max timestamp beyond now + {}ms (e.g. {} in window {}); " +
                                 "excluded from background/delegate compaction and freeze selection (maximal and " +
                                 "user-defined compactions still include them) - check writer clocks or " +
                                 "USING TIMESTAMP inputs",
                                 cfs.getKeyspaceName(), cfs.getTableName(), tsOptions.maxFutureWindowMillis,
                                 sstable, windowStart);
                continue;
            }
            if (tsOptions.isActiveWindow(windowStart, nowMillis))
                active.add(sstable);
        }

        Set<SSTableReader> inDelegate = new HashSet<>(delegate.getSSTables());
        Set<SSTableReader> toRemove = new HashSet<>(inDelegate);
        toRemove.removeAll(active);
        Set<SSTableReader> toAdd = new HashSet<>(active);
        toAdd.removeAll(inDelegate);
        if (!toRemove.isEmpty())
            delegate.removeSSTables(toRemove);
        if (!toAdd.isEmpty())
            delegate.addSSTables(toAdd);
    }

    long maxTimestampMillis(SSTableReader sstable)
    {
        return TimeUnit.MILLISECONDS.convert(sstable.getMaxTimestamp(), tsOptions.timestampResolution);
    }

    long minTimestampMillis(SSTableReader sstable)
    {
        return TimeUnit.MILLISECONDS.convert(sstable.getMinTimestamp(), tsOptions.timestampResolution);
    }

    /** Window states, derived statelessly from sstable min/max timestamps every round (design spec section 3). */
    public enum WindowState { CURRENT, CLOSING, FREEZING, FROZEN, EXPIRED }

    /**
     * Classifies one window of this instance's sstable slice. Stateless: nothing is persisted, the state is a
     * pure function of (window key, sstables, now) - restart-safe, and a FROZEN window that gains a late
     * sstable reverts to FREEZING simply because the derivation changes (design spec sections 3-4).
     * <p>
     * FROZEN demands a single sstable <b>fully contained</b> in the window ({@code windowStartFor(min) ==
     * windowStartFor(max)}). A single sstable spanning window boundaries classifies FREEZING - it is not
     * frozen - but freeze selection ({@link #nextFreezeCandidate}) will not pick a single-sstable window:
     * rewriting one spanning sstable cannot fix containment before T3's flush-split lands, and trying would
     * recompact it forever.
     * <p>
     * Scope: the CompactionStrategyManager splits strategy instances per repair status and per disk, so this
     * judgment (and the frozen event) is per instance slice, never per table.
     * <p>
     * Precondition: callers filter far-future windows first via
     * {@link TimeSeriesCompactionStrategyOptions#isFarFutureWindow} (design spec section 8).
     */
    @VisibleForTesting
    WindowState classify(long windowStartMillis, Set<SSTableReader> windowSSTables, long nowMillis)
    {
        if (tsOptions.isExpiredWindow(windowStartMillis, nowMillis))
            return WindowState.EXPIRED;
        if (tsOptions.isCurrentWindow(windowStartMillis, nowMillis))
            return WindowState.CURRENT;
        if (tsOptions.isActiveWindow(windowStartMillis, nowMillis))
            return WindowState.CLOSING;
        if (windowSSTables.size() == 1)
        {
            SSTableReader only = windowSSTables.iterator().next();
            if (tsOptions.windowStartFor(minTimestampMillis(only)) == tsOptions.windowStartFor(maxTimestampMillis(only)))
                return WindowState.FROZEN;
        }
        return WindowState.FREEZING;
    }

    @VisibleForTesting
    synchronized Map<Long, Set<SSTableReader>> windows()
    {
        Map<Long, Set<SSTableReader>> result = new TreeMap<>();
        for (SSTableReader sstable : sstables)
            result.computeIfAbsent(tsOptions.windowStartFor(maxTimestampMillis(sstable)), start -> new HashSet<>()).add(sstable);
        return result;
    }

    /**
     * The oldest FREEZING window with at least two sstables, or null. Single-sstable FREEZING windows
     * (a spanning sstable failing containment) are not candidates: rewriting one sstable cannot fix
     * containment before T3's flush-split, and selecting it would recompact it every round forever.
     * Side effect: refreshes {@link #freezeBacklog} with the number of eligible windows.
     */
    @VisibleForTesting
    synchronized Map.Entry<Long, Set<SSTableReader>> nextFreezeCandidate(long nowMillis)
    {
        Map.Entry<Long, Set<SSTableReader>> oldest = null;
        int backlog = 0;
        for (Map.Entry<Long, Set<SSTableReader>> window : windows().entrySet())
        {
            if (tsOptions.isFarFutureWindow(window.getKey(), nowMillis))
                continue;                                 // far-future guard: never judged for freezing (spec section 8)
            if (window.getValue().size() < 2)
                continue;
            if (classify(window.getKey(), window.getValue(), nowMillis) != WindowState.FREEZING)
                continue;
            backlog++;
            if (oldest == null)
                oldest = window;
        }
        freezeBacklog = backlog;
        return oldest;
    }

    @VisibleForTesting
    UnifiedCompactionStrategy delegate()
    {
        return delegate;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader removed)
    {
        sstables.remove(removed);
        delegate.removeSSTable(removed);
    }

    @Override
    protected synchronized Set<SSTableReader> getSSTables()
    {
        return new HashSet<>(sstables);
    }

    @Override
    public List<AbstractCompactionTask> getMaximalTasks(long gcBefore, boolean splitOutput)
    {
        return getMaximalTasksAt(Clock.Global.currentTimeMillis(), gcBefore, splitOutput);
    }

    @VisibleForTesting
    List<AbstractCompactionTask> getMaximalTasksAt(long nowMillis, long gcBefore, boolean splitOutput)
    {
        // Preserve the window invariant for maximal compaction too: never cross windows, one task per window.
        // A window that is itself expired is routed through TimeSeriesCompactionTask so its retention cutoff
        // is honoured here too, rather than silently rewriting it via a plain CompactionTask.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Map.Entry<Long, Set<SSTableReader>> entry : windows().entrySet())
        {
            Collection<SSTableReader> window = AbstractCompactionStrategy.filterSuspectSSTables(entry.getValue());
            if (window.isEmpty())
                continue;
            LifecycleTransaction txn = cfs.getTracker().tryModify(window, OperationType.COMPACTION);
            if (txn == null)
                continue;
            tasks.add(tsOptions.isExpiredWindow(entry.getKey(), nowMillis)
                      ? new TimeSeriesCompactionTask(cfs, txn, gcBefore, nowMillis - tsOptions.retentionMillis, tsOptions.timestampResolution)
                      : new CompactionTask(cfs, txn, gcBefore));
        }
        return tasks;                                     // never null, unlike TWCS
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> toCompact, long gcBefore)
    {
        assert !toCompact.isEmpty();
        LifecycleTransaction txn = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION);
        if (txn == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first", toCompact);
            return null;
        }
        return new CompactionTask(cfs, txn, gcBefore).setUserDefined(true);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return delegate.getEstimatedRemainingTasks()
               + (lastExpiredSelection.isEmpty() ? 0 : 1)
               + freezeBacklog;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public void startup()
    {
        super.startup();
        delegate.startup();
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        delegate.shutdown();
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> unchecked = AbstractCompactionStrategy.validateOptions(options);
        unchecked = TimeSeriesCompactionStrategyOptions.validateOptions(options, unchecked);
        unchecked = Controller.validateOptions(unchecked);
        unchecked.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        unchecked.remove(CompactionParams.Option.MAX_THRESHOLD.toString());
        return unchecked;
    }

    @Override
    public String toString()
    {
        return String.format("TimeSeriesCompactionStrategy[window=%dms/freeze=%dms/retention=%dms]",
                             tsOptions.windowSizeMillis, tsOptions.freezeAfterMillis, tsOptions.retentionMillis);
    }
}
