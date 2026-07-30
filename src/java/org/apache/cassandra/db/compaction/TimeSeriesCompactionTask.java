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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * A whole-window retention drop: every sstable handed to this task belongs to a window that closed before
 * the configured retention cutoff, so {@link TimeSeriesCompactionController} reports them all as fully
 * expired and {@link CompactionTask#runMayThrow} obsoletes them outright without rewriting anything.
 */
public class TimeSeriesCompactionTask extends CompactionTask
{
    private final long retentionCutoffMillis;
    private final TimeUnit timestampResolution;

    public TimeSeriesCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore,
                                    long retentionCutoffMillis, TimeUnit timestampResolution)
    {
        super(cfs, txn, gcBefore);
        this.retentionCutoffMillis = retentionCutoffMillis;
        this.timestampResolution = timestampResolution;
    }

    @Override
    public CompactionController getCompactionController(Set<SSTableReader> toCompact, long gcBefore)
    {
        return new TimeSeriesCompactionController(cfs, toCompact, gcBefore, retentionCutoffMillis, timestampResolution);
    }
}
