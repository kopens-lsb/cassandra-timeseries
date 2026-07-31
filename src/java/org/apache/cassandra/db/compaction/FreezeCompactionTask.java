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
     */
    @Override
    protected boolean shouldReduceScopeForSpace()
    {
        return false;
    }

    /**
     * Fires the listeners strictly post-commit: {@code super.finish} is the "point of no return" in
     * {@link CompactionTask#runMayThrow} - once it returns, the single output sstable is durably committed.
     * Zero outputs means the whole window was expired data and no longer exists - no event (there is nothing
     * to hand to a consumer). More than one output cannot happen with {@code DefaultCompactionWriter}, which
     * never switches writers; log rather than half-fire if that invariant is ever broken upstream.
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
            logger.warn("Freeze of window {} in {} unexpectedly produced {} sstables; no event fired",
                        windowStartMillis, cfs.getTableName(), newSStables.size());
        return newSStables;
    }
}
