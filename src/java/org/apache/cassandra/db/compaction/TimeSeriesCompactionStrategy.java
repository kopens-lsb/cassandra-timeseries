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

/**
 * Time-series compaction: SSTables are classified into fixed time windows by max timestamp.
 * Active windows (current, or closed less than freeze_after ago) delegate their compaction to an
 * internal {@link UnifiedCompactionStrategy}; windows past the configured retention are dropped
 * whole via {@link TimeSeriesCompactionTask}/{@link TimeSeriesCompactionController}
 * (see {@link TimeSeriesCompactionStrategyOptions#isExpiredWindow}). Freezing closed windows
 * to a single sstable and late-data isolation arrive in later increments (design spec section 10).
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

    public TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        this(cfs, options, new UnifiedCompactionStrategy(cfs, new TimeSeriesCompactionStrategyOptions(options).delegateOptions(options)));
    }

    @VisibleForTesting
    TimeSeriesCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options, UnifiedCompactionStrategy delegate)
    {
        super(cfs, options);
        this.tsOptions = new TimeSeriesCompactionStrategyOptions(options);
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
            LifecycleTransaction txn = cfs.getTracker().tryModify(expired, OperationType.COMPACTION);
            if (txn != null)
            {
                long cutoff = nowMillis - tsOptions.retentionMillis;
                logger.info("Dropping {} expired-window sstables from {} (retention cutoff {})",
                            expired.size(), cfs.getTableName(), cutoff);
                return List.of(new TimeSeriesCompactionTask(cfs, txn, gcBefore, cutoff, tsOptions.timestampResolution));
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
            if (tsOptions.isActiveWindow(tsOptions.windowStartFor(maxTimestampMillis(sstable)), nowMillis))
                active.add(sstable);

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

    @VisibleForTesting
    synchronized Map<Long, Set<SSTableReader>> windows(long nowMillis)
    {
        Map<Long, Set<SSTableReader>> result = new TreeMap<>();
        for (SSTableReader sstable : sstables)
            result.computeIfAbsent(tsOptions.windowStartFor(maxTimestampMillis(sstable)), start -> new HashSet<>()).add(sstable);
        return result;
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
        // Preserve the window invariant for maximal compaction too: never cross windows, one task per window.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Set<SSTableReader> window : windows(Clock.Global.currentTimeMillis()).values())
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(window, OperationType.COMPACTION);
            if (txn != null)
                tasks.add(new CompactionTask(cfs, txn, gcBefore));
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
        return delegate.getEstimatedRemainingTasks() + (lastExpiredSelection.isEmpty() ? 0 : 1);
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
