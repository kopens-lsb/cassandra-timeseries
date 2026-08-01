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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.timeseries.TimeWindowSplittingMultiWriter;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Time-series compaction: SSTables are classified into fixed time windows by max timestamp.
 * Active windows (current, or closed less than freeze_after ago) delegate their compaction to an
 * internal {@link UnifiedCompactionStrategy}; windows past the configured retention are dropped
 * whole via {@link TimeSeriesCompactionTask}/{@link TimeSeriesCompactionController}
 * (see {@link TimeSeriesCompactionStrategyOptions#isExpiredWindow}). Closed windows are frozen to a
 * single sstable per window (per strategy-instance slice) by
 * {@link FreezeCompactionTask}, which fires {@link org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener}
 * post-commit. Late-data isolation (flush/streaming window splitting) landed in T3: see
 * {@link org.apache.cassandra.db.compaction.timeseries.TimeWindowSplittingMultiWriter} and
 * {@link SplitRefreezeCompactionTask} (design spec sections 4 and 10).
 * <p>
 * <b>TTL reclaim, precisely.</b> Because the freeze runs a real {@link CompactionController} with the
 * caller's gcBefore, data that is already expired <em>at the moment the window freezes</em> is purged
 * there, with no {@code retention} configured. That is the whole of the guarantee: once a window is
 * down to one sstable it is never a freeze candidate again ({@link #nextFreezeCandidate} skips windows
 * with fewer than two sstables), so data that expires <em>after</em> the freeze is not reclaimed by
 * this strategy. Configure {@code retention} to bound how long such data survives.
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
    private volatile int splitBacklog;
    // the freeze candidate that failed tryModify on the previous round: cross-round version of TWCS's
    // previousCandidate guard (:100-106) - warn when the same candidate is stuck two rounds running,
    // but keep retrying (unlike TWCS's intra-call loop, skipping here would never retry at all).
    private volatile Set<SSTableReader> previousFreezeCandidate = Set.of();
    // sstables excluded from every automatic path because their window is far in the future; refreshed
    // each round by syncDelegate so operators can see the accumulation, not just a throttled WARN.
    private volatile Set<SSTableReader> farFutureSSTables = Set.of();
    // Per-window no-progress bookkeeping for the guard below; guarded by this.
    private final Map<Long, WindowProgress> windowProgress = new HashMap<>();
    // windows the guard has parked, with the sstables they are parked at; refreshed each round so
    // operators can see them - a parked window is deliberately absent from the backlogs, so it would
    // otherwise be invisible except for one WARN at the transition. See getParkedWindows().
    private volatile NavigableMap<Long, Set<SSTableReader>> parkedWindows = Collections.emptyNavigableMap();

    /**
     * How many <em>completed</em> rewrites of one window may leave that window's shape unchanged before
     * it is parked; a genuinely stuck window is therefore rewritten exactly this many times and then
     * never again. Two allows one futile rewrite - enough to absorb a rewrite that lost a race with
     * concurrent work - while keeping a stuck window's cost bounded instead of unbounded.
     */
    @VisibleForTesting
    static final int NO_PROGRESS_STRIKES = 2;

    /** What the guard remembers about a window whose rewrite completed without changing it. */
    private static final class WindowProgress
    {
        /** The window's shape after that rewrite; see {@link #windowSignature}. */
        private final long signature;
        private int strikes;
        private boolean parked;

        private WindowProgress(long signature)
        {
            this.signature = signature;
        }
    }

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

    // synchronized: freezeBacklog/splitBacklog/previousFreezeCandidate and the no-progress bookkeeping
    // are all read-modify-written across this method, and several background threads can select work for
    // the same strategy instance concurrently (T2-M10). The body only picks work - the compaction itself
    // runs outside this lock - so serialising selection costs nothing measurable.
    @VisibleForTesting
    synchronized Collection<AbstractCompactionTask> getNextBackgroundTasksAt(long nowMillis, long gcBefore)
    {
        syncDelegate(nowMillis);

        Set<SSTableReader> expired = expiredSSTables(nowMillis);
        lastExpiredSelection = expired;

        // Both candidates are computed every round, before any early return: their side effect is to
        // refresh freezeBacklog/splitBacklog, which feed getEstimatedRemainingTasks() and hence how the
        // CompactionStrategyManager ranks this table against every other one. Computing them only on
        // rounds that reach them left the backlogs stale on every retention-drop round (T2-M8).
        Map.Entry<Long, Set<SSTableReader>> freeze = nextFreezeCandidate(nowMillis);
        Map.Entry<Long, Set<SSTableReader>> split = nextSplitRefreezeCandidate(nowMillis);
        pruneWindowProgress();

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
                    // A drop round attempts no freeze, so it breaks the consecutive-failure chain the
                    // repeat-WARN below relies on (T2-M8).
                    previousFreezeCandidate = Set.of();
                    return List.of(new TimeSeriesCompactionTask(cfs, txn, gcBefore, cutoff, tsOptions.timestampResolution));
                }
                logger.debug("Unable to mark {} expired-window sstables for compaction in {}; probably a background " +
                             "compaction or the retention drop from a previous round got to them first",
                             toDrop.size(), cfs.getTableName());
            }
        }

        boolean freezeAttempted = false;
        if (freeze != null)
        {
            // Same zombie filter as the expired branch: only live, non-suspect sstables.
            Set<SSTableReader> toFreeze = new HashSet<>(AbstractCompactionStrategy.filterSuspectSSTables(freeze.getValue()));
            toFreeze.retainAll(cfs.getLiveSSTables());
            if (toFreeze.size() > 1)                      // a single survivor is already "frozen enough"; self-heals next round
            {
                freezeAttempted = true;
                LifecycleTransaction txn = cfs.getTracker().tryModify(toFreeze, OperationType.COMPACTION);
                if (txn != null)
                {
                    previousFreezeCandidate = Set.of();
                    logger.debug("Freezing window {} of {}: {} sstables -> 1", freeze.getKey(), cfs.getTableName(), toFreeze.size());
                    long window = freeze.getKey();
                    long before = windowSignature(freeze.getValue());
                    return List.of(new FreezeCompactionTask(cfs, txn, gcBefore, window,
                                                            () -> recordCompletedRewrite(window, before, "freeze")));
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
        if (!freezeAttempted)
        {
            // Legacy SPANNING sstables (single sstable failing containment in a closed window) are
            // split-rewritten into per-window sstables - lower priority than regular freezes (plan D7).
            if (split != null)
            {
                Set<SSTableReader> toSplit = new HashSet<>(AbstractCompactionStrategy.filterSuspectSSTables(split.getValue()));
                toSplit.retainAll(cfs.getLiveSSTables());
                if (toSplit.size() == 1)
                {
                    freezeAttempted = true;
                    LifecycleTransaction txn = cfs.getTracker().tryModify(toSplit, OperationType.COMPACTION);
                    if (txn != null)
                    {
                        previousFreezeCandidate = Set.of();
                        logger.debug("Split-refreezing spanning sstable in window {} of {}", split.getKey(), cfs.getTableName());
                        long window = split.getKey();
                        long before = windowSignature(split.getValue());
                        return List.of(new SplitRefreezeCompactionTask(cfs, txn, gcBefore,
                                                                       tsOptions::windowStartFor, tsOptions.timestampResolution,
                                                                       () -> recordCompletedRewrite(window, before, "split-refreeze")));
                    }
                    if (toSplit.equals(previousFreezeCandidate))
                        logger.warn("Could not acquire references for split-refreezing sstables {} which is not a problem" +
                                    " per se, unless it happens frequently, in which case it must be reported. Will retry later.",
                                    toSplit);
                    else
                        logger.debug("Unable to mark window {} of {} for split-refreeze; will retry next round",
                                     split.getKey(), cfs.getTableName());
                    previousFreezeCandidate = toSplit;
                }
            }
        }
        if (!freezeAttempted)
        {
            // Rounds with no freeze attempt (no candidate, or a lone survivor) break the "consecutive
            // failures" chain: the repeat-WARN above must mean two attempts in a row on the same set.
            previousFreezeCandidate = Set.of();
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
        Set<SSTableReader> farFuture = new HashSet<>();
        for (SSTableReader sstable : sstables)
        {
            long windowStart = tsOptions.windowStartFor(maxTimestampMillis(sstable));
            if (tsOptions.isFarFutureWindow(windowStart, nowMillis))
            {
                farFuture.add(sstable);
                // Garbage/misconfigured-writer timestamps: keep them out of both the UCS delegate and the
                // freeze machinery, and complain (throttled, keyed per table so one table's warn does not
                // suppress another's) so the operator investigates (design spec section 8).
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                 cfs.getKeyspaceName() + '.' + cfs.getTableName() + ":far-future-window",
                                 1, TimeUnit.MINUTES,
                                 "{}.{} now holds {} sstable(s) with max timestamp beyond now + {}ms (e.g. {} in " +
                                 "window {}). They are excluded from delegate compaction, from freeze, from split " +
                                 "and from retention, so they accumulate permanently - one per flush while the bad " +
                                 "clock persists. Remedy: fix the writer clock or USING TIMESTAMP input, then " +
                                 "rewrite them with a user-defined or maximal compaction (both still include them). " +
                                 "The current set is available programmatically via getFarFutureSSTables().",
                                 cfs.getKeyspaceName(), cfs.getTableName(), farFuture.size(),
                                 tsOptions.maxFutureWindowMillis, sstable, windowStart);
                continue;
            }
            if (tsOptions.isActiveWindow(windowStart, nowMillis))
                active.add(sstable);
        }
        farFutureSSTables = farFuture;

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
     * merging one sstable into one sstable cannot restore containment. Splitting it can, which is what
     * {@link #nextSplitRefreezeCandidate} selects it for.
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
     * The oldest FREEZING window with at least two sstables that the no-progress guard has not parked,
     * or null. Single-sstable FREEZING windows (a spanning sstable failing containment) are not
     * candidates: merging one sstable into one sstable cannot restore containment, and selecting it
     * would recompact it every round forever - {@link #nextSplitRefreezeCandidate} owns those.
     * Side effect: refreshes {@link #freezeBacklog} with the number of eligible windows.
     */
    @VisibleForTesting
    synchronized Map.Entry<Long, Set<SSTableReader>> nextFreezeCandidate(long nowMillis)
    {
        Map.Entry<Long, Set<SSTableReader>> oldest = null;
        int backlog = 0;
        for (Map.Entry<Long, Set<SSTableReader>> window : windows().entrySet())
        {
            // Precondition hygiene for classify's documented contract (callers filter far-future windows
            // first; spec section 8). Currently redundant defense-in-depth: isCurrentWindow also holds for
            // any future window, so classify would report CURRENT and the window would be skipped anyway -
            // but that is a property of today's predicates, not a guarantee. Keep the explicit filter so a
            // future predicate change cannot silently start freeze-judging garbage timestamps.
            if (tsOptions.isFarFutureWindow(window.getKey(), nowMillis))
                continue;
            if (window.getValue().size() < 2)
                continue;
            if (classify(window.getKey(), window.getValue(), nowMillis) != WindowState.FREEZING)
                continue;
            if (isParked(window.getKey(), window.getValue()))
                continue;                                 // freezing it again provably changes nothing; see recordCompletedRewrite
            backlog++;
            if (oldest == null)
                oldest = window;
        }
        freezeBacklog = backlog;
        return oldest;
    }

    /**
     * The oldest closed window whose single sstable fails containment and that the no-progress guard
     * has not parked. Such an sstable is either legacy (pre-T3 data or a strategy switch) or one TSCS
     * wrote unsplit on purpose - a partition too large to window-route, or a window folded onto another
     * writer at the writer cap. These classify FREEZING but cannot be fixed by freezing (merging one
     * sstable into one sstable never restores containment) - they need
     * {@link SplitRefreezeCompactionTask}. Side effect: refreshes {@link #splitBacklog}.
     */
    @VisibleForTesting
    synchronized Map.Entry<Long, Set<SSTableReader>> nextSplitRefreezeCandidate(long nowMillis)
    {
        Map.Entry<Long, Set<SSTableReader>> oldest = null;
        int backlog = 0;
        for (Map.Entry<Long, Set<SSTableReader>> window : windows().entrySet())
        {
            if (tsOptions.isFarFutureWindow(window.getKey(), nowMillis))
                continue;                                 // same precondition hygiene as nextFreezeCandidate
            if (window.getValue().size() != 1)
                continue;
            if (classify(window.getKey(), window.getValue(), nowMillis) != WindowState.FREEZING)
                continue;                                 // FROZEN (contained) or still active/expired
            if (isParked(window.getKey(), window.getValue()))
                continue;                                 // splitting it again provably changes nothing; see recordCompletedRewrite
            backlog++;
            if (oldest == null)
                oldest = window;
        }
        splitBacklog = backlog;
        return oldest;
    }

    /**
     * The no-progress guard, in one place for both rewrite paths.
     * <p>
     * Freeze and split-refreeze share a failure shape: the task's postcondition is not what
     * {@link #classify} tests, so the classifier re-selects the very same window on every background
     * round and the rewrite runs forever. Two real instances:
     * <ul>
     *   <li>a split whose output still spans window boundaries (a row whose timestamps straddle a
     *       boundary, before element-granularity routing; or a partition too large to route, which is
     *       written unsplit on purpose);</li>
     *   <li>a freeze that emits more than one sstable because {@code CompactionAwareWriter} switched
     *       location at a JBOD disk boundary, leaving the window at two sstables (T2-I3).</li>
     * </ul>
     * Candidate equality cannot detect either: every rewrite produces a fresh generation, so the
     * candidate set differs each round while the window's <em>shape</em> is unchanged. The guard
     * therefore compares the shape - {@link #windowSignature} - immediately before and immediately
     * after each rewrite, and parks a window that survives {@link #NO_PROGRESS_STRIKES} rewrites which
     * each left it exactly as they found it.
     * <p>
     * <b>Before-and-after, not round-to-round.</b> Comparing the shape a window has when a task is
     * handed out with the shape it had at the previous hand-out cannot tell a stuck window from a
     * healthy one under steady late data: a closed window that receives one late flush per round
     * oscillates 2 sstables -> 1 -> 2, and every round samples it at 2 with an identical signature, so
     * three <em>successful</em> freezes would park a window that is working perfectly. Attributing the
     * shape change to the rewrite that caused it fixes that - the freeze took the window from 2 to 1
     * and the guard sees it - while still catching the real loops, where the rewrite provably changed
     * nothing.
     * <p>
     * <b>Completed rewrites only.</b> This is called strictly post-commit by the task itself, so a
     * freeze aborted by the disk-space pre-flight, stopped by {@code nodetool stop COMPACTION} or
     * killed by an IO error never scores. Counting hand-outs instead meant three consecutive full-disk
     * rounds parked a window, and freeing the disk did not un-park it: nothing about the window had
     * changed, so nothing reset the guard, and only a restart or a shape change recovered it.
     * <p>
     * Parking is deliberately self-healing: the record is keyed on the signature, so any change to the
     * window (a late flush, an operator's {@code nodetool relocatesstables} or major compaction)
     * silently un-parks it. Parking is a safety net, not a fix - a parked window never freezes, so
     * {@link org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener} never fires for it and
     * downstream consumers never see it. That is why it warns, and why {@link #getParkedWindows()}
     * exposes the current set.
     *
     * @param signatureBefore the window's shape when this rewrite was handed out
     */
    @VisibleForTesting
    synchronized void recordCompletedRewrite(long windowStartMillis, long signatureBefore, String what)
    {
        Set<SSTableReader> after = windows().getOrDefault(windowStartMillis, Set.of());
        long signatureAfter = windowSignature(after);
        if (signatureAfter != signatureBefore)
        {
            // The rewrite changed the window: real progress, and any earlier strikes are stale.
            windowProgress.remove(windowStartMillis);
            return;
        }

        WindowProgress progress = windowProgress.get(windowStartMillis);
        if (progress == null || progress.signature != signatureAfter)
        {
            progress = new WindowProgress(signatureAfter);
            windowProgress.put(windowStartMillis, progress);
        }
        if (++progress.strikes < NO_PROGRESS_STRIKES)
            return;

        progress.parked = true;
        logger.warn("Parking window {} of {}.{}: {} completed {} time(s) without changing the window " +
                    "({} sstable(s), spanning windows {}). Not re-selecting it until the window changes - " +
                    "it will not freeze, so downstream consumers of the frozen-window event will not see it. " +
                    "Investigate: for a still-spanning single sstable check for partitions too large to " +
                    "window-route; for a window stuck at several sstables on JBOD, run nodetool relocatesstables.",
                    windowStartMillis, cfs.getKeyspaceName(), cfs.getTableName(), what, progress.strikes,
                    after.size(), describeSpans(after));
    }

    /** True if the guard has parked this window at its current shape. */
    @VisibleForTesting
    synchronized boolean isParked(long windowStartMillis, Set<SSTableReader> windowSSTables)
    {
        WindowProgress progress = windowProgress.get(windowStartMillis);
        return progress != null && progress.parked && progress.signature == windowSignature(windowSSTables);
    }

    /**
     * A window's "shape": how many sstables it holds, and how many windows those sstables span in
     * total. A freeze that works shrinks the count; a split that works shrinks the span (usually to the
     * point where the window holds one contained sstable and is not a candidate at all). Anything a
     * rewrite can legitimately achieve moves one of the two, so an unchanged pair means the rewrite
     * achieved nothing.
     */
    private long windowSignature(Set<SSTableReader> windowSSTables)
    {
        long spanTotal = 0;
        for (SSTableReader sstable : windowSSTables)
            spanTotal += windowsSpanned(sstable);
        return ((long) windowSSTables.size() << 32) | Math.min(spanTotal, 0xFFFFFFFFL);
    }

    /** How many windows this sstable's min..max write-timestamp range covers; 1 when contained. */
    private long windowsSpanned(SSTableReader sstable)
    {
        long first = tsOptions.windowStartFor(minTimestampMillis(sstable));
        long last = tsOptions.windowStartFor(maxTimestampMillis(sstable));
        long width = last - first;
        if (width <= 0)                                   // contained, inverted, or overflowed on garbage timestamps
            return 1;
        return Math.min(width / tsOptions.windowSizeMillis + 1, 1 << 20);
    }

    private String describeSpans(Set<SSTableReader> windowSSTables)
    {
        StringBuilder sb = new StringBuilder();
        for (SSTableReader sstable : windowSSTables)
            sb.append(sb.length() == 0 ? "" : ", ")
              .append(sstable)
              .append('[').append(tsOptions.windowStartFor(minTimestampMillis(sstable)))
              .append("..").append(tsOptions.windowStartFor(maxTimestampMillis(sstable))).append(']');
        return sb.toString();
    }

    /**
     * Drops guard bookkeeping for windows this instance no longer holds, so the map cannot grow
     * unbounded, and republishes {@link #parkedWindows} for {@link #getParkedWindows()}.
     */
    private synchronized void pruneWindowProgress()
    {
        Map<Long, Set<SSTableReader>> current = windows();
        if (!windowProgress.isEmpty())
            windowProgress.keySet().retainAll(current.keySet());

        NavigableMap<Long, Set<SSTableReader>> parked = new TreeMap<>();
        for (Map.Entry<Long, Set<SSTableReader>> window : current.entrySet())
            if (isParked(window.getKey(), window.getValue()))
                parked.put(window.getKey(), Set.copyOf(window.getValue()));
        parkedWindows = Collections.unmodifiableNavigableMap(parked);
    }

    /**
     * Windows the no-progress guard has parked, mapped to the sstables they are parked at. Exposed for
     * the same reason as {@link #getFarFutureSSTables()}: a parked window is deliberately skipped by
     * {@link #nextFreezeCandidate} and {@link #nextSplitRefreezeCandidate}, so it drops out of
     * {@code freezeBacklog}/{@code splitBacklog} and out of {@link #getEstimatedRemainingTasks()} - it
     * looks exactly like a table with nothing to do. It is not: a parked window never reaches FROZEN,
     * so {@link org.apache.cassandra.db.compaction.timeseries.WindowFrozenListener} never fires for it
     * and everything downstream of the freeze (tiered-storage compression included) stops for that
     * window. Without this, the only signal is a single WARN at the moment of parking.
     * <p>
     * Refreshed on every background round; empty is the healthy state. Un-parking is automatic on any
     * change to the window - see {@link #recordCompletedRewrite}.
     */
    public NavigableMap<Long, Set<SSTableReader>> getParkedWindows()
    {
        return parkedWindows;
    }

    /**
     * SSTables excluded from every automatic path (UCS delegate, freeze, split, and retention - an
     * expired window can never be a future one) because their window lies beyond {@code
     * max_future_window}. They accumulate permanently until an operator intervenes, so this is exposed
     * rather than only warned about; the remedy is a user-defined or maximal compaction over them
     * (both still include far-future sstables) once the writer clock or {@code USING TIMESTAMP} input
     * that produced them is fixed.
     */
    public Set<SSTableReader> getFarFutureSSTables()
    {
        return farFutureSSTables;
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
               + freezeBacklog
               + splitBacklog;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    /**
     * Flush and streaming both create sstables through this hook (via
     * {@code ColumnFamilyStore.createSSTableMultiWriter}); splitting them at window boundaries here
     * upholds the "every sstable belongs to exactly one window" invariant (design spec section 4)
     * that whole-window drops and per-window freezing rely on. Note this intentionally forgoes the
     * UCS delegate's token sharding for flushes (plan D6).
     */
    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       TimeUUID pendingRepair,
                                                       boolean isTransient,
                                                       IntervalSet<CommitLogPosition> commitLogPositions,
                                                       int sstableLevel,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       ILifecycleTransaction txn)
    {
        return new TimeWindowSplittingMultiWriter(cfs,
                                                  descriptor,
                                                  keyCount,
                                                  repairedAt,
                                                  pendingRepair,
                                                  isTransient,
                                                  commitLogPositions,
                                                  sstableLevel,
                                                  header,
                                                  indexGroups,
                                                  txn,
                                                  tsOptions::windowStartFor,
                                                  tsOptions.timestampResolution);
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
