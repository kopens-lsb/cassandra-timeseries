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

import java.util.Collection;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.timeseries.WindowFrozenListeners;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Freezes one closed time window of {@link TimeSeriesCompactionStrategy}: a major compaction of every sstable
 * in the window (of this strategy-instance slice) down to a single sstable, after which the window classifies
 * FROZEN. Runs a real {@link CompactionController} with the caller's gcBefore, so TTL/tombstone data in the
 * closed window is purged here - this is what structurally closes T1's "closed windows need retention to
 * reclaim TTL'd data" gap. On failure the standard compaction transaction rolls back and the window simply
 * classifies FREEZING again next round (design spec section 8).
 */
public class FreezeCompactionTask extends CompactionTask
{
    private final long windowStartMillis;

    public FreezeCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore, long windowStartMillis)
    {
        super(cfs, txn, gcBefore);
        this.windowStartMillis = windowStartMillis;
    }

    /**
     * A freeze must be deterministic: silently dropping the largest input on low disk (the CompactionTask
     * default) breaks the whole-window-to-one-sstable contract. Better to fail whole and retry next round.
     * <p>
     * This is expressed by refusing <em>partial</em> compactions rather than by overriding
     * {@code shouldReduceScopeForSpace()}, which reads as if it meant the same thing and does not:
     * {@link CompactionTask#runMayThrow} guards the whole disk-space pre-flight with it
     * ({@code shouldReduceScopeForSpace() && !buildCompactionCandidatesForAvailableDiskSpace(...)}), so
     * returning false there skips the check entirely. A freeze of a window larger than the free space
     * would then start writing, hit ENOSPC, throw {@code FSWriteError}, and under the shipped
     * {@code disk_failure_policy: stop} take the node out of the ring. With
     * {@code partialCompactionsAcceptable() == false} the pre-flight runs, {@code reduceScopeForLimitedSpace}
     * refuses to shrink the freeze, and the task aborts cleanly with "Not enough space for compaction"
     * before writing a byte - the same choice {@link LeveledCompactionTask} and {@link SSTableSplitter} make.
     */
    @Override
    protected boolean partialCompactionsAcceptable()
    {
        return false;
    }

    /**
     * Fires the listeners strictly post-commit: {@code super.finish} is the "point of no return" in
     * {@link CompactionTask#runMayThrow} - once it returns, the single output sstable is durably committed.
     * Zero outputs means the whole window was expired data and no longer exists - no event (there is nothing
     * to hand to a consumer).
     * <p>
     * More than one output is unlikely but <b>possible</b>: {@code DefaultCompactionWriter} only suppresses
     * {@code shouldSwitchWriterInCurrentLocation}, while {@code CompactionAwareWriter.maybeSwitchLocation}
     * still switches at JBOD disk boundaries, and an sstable is assigned to a per-disk strategy instance by
     * its <em>first</em> key, so a boundary or topology change can leave one instance holding sstables that
     * cross the new boundary. The window then still has two sstables and classifies FREEZING again; the
     * strategy's no-progress guard is what stops that becoming a permanent refreeze loop
     * ({@code TimeSeriesCompactionStrategy#allowRewrite}). No event fires for a split output, because no
     * single sstable represents the window.
     */
    @Override
    protected Collection<SSTableReader> finish(AbstractCompactionPipeline pipeline)
    {
        Collection<SSTableReader> newSStables = super.finish(pipeline);
        if (newSStables.size() == 1)
            WindowFrozenListeners.fire(cfs.metadata(), windowStartMillis, newSStables.iterator().next());
        else if (newSStables.isEmpty())
            logger.debug("Freeze of window {} in {} produced no sstable (window fully expired); no event fired",
                         windowStartMillis, cfs.getTableName());
        else
            logger.warn("Freeze of window {} in {} produced {} sstables instead of 1 (a JBOD disk-boundary " +
                        "writer switch); no event fired. The window stays FREEZING; nodetool relocatesstables " +
                        "will realign it. Until then the strategy's no-progress guard stops it refreezing forever.",
                        windowStartMillis, cfs.getTableName(), newSStables.size());
        return newSStables;
    }
}
