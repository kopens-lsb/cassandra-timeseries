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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeSeriesCompactionStrategyTest
{
    private static final long NOW = 1_700_000_000_000L;
    private static final long HOUR = 3_600_000L;

    // AbstractCompactionStrategy's constructor calls cfs.getDirectories(); with RETURNS_DEEP_STUBS that makes
    // Mockito build a real Directories mock, whose class init touches DatabaseDescriptor (see
    // UnifiedCompactionStrategyTest#setUpClass for the same requirement).
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static Map<String, String> options()
    {
        Map<String, String> map = new HashMap<>();
        map.put(TimeSeriesCompactionStrategyOptions.WINDOW_SIZE, "1h");
        map.put(TimeSeriesCompactionStrategyOptions.FREEZE_AFTER, "2h");
        return map;
    }

    /** UnifiedCompactionStrategyTest.mockSSTable 축소판: 창 분류에 필요한 것만 스텁 */
    static SSTableReader sstableAt(long maxTimestampMillis)
    {
        SSTableReader sstable = mock(SSTableReader.class, Mockito.RETURNS_DEEP_STUBS);
        // 전략은 timestampResolution(MICROSECONDS 기본)으로 변환하므로 마이크로초로 스텁
        when(sstable.getMaxTimestamp()).thenReturn(maxTimestampMillis * 1000);
        when(sstable.getMinTimestamp()).thenReturn(maxTimestampMillis * 1000 - 1000);
        return sstable;
    }

    private static TimeSeriesCompactionStrategy strategy(UnifiedCompactionStrategy delegate)
    {
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        return new TimeSeriesCompactionStrategy(cfs, options(), delegate);
    }

    @Test
    public void classifiesSSTablesIntoWindows()
    {
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        SSTableReader current = sstableAt(NOW - 60_000);
        SSTableReader old = sstableAt(NOW - 10 * HOUR);
        tscs.addSSTable(current);
        tscs.addSSTable(old);

        Map<Long, Set<SSTableReader>> windows = tscs.windows();
        assertEquals(2, windows.size());
        for (Map.Entry<Long, Set<SSTableReader>> window : windows.entrySet())
            for (SSTableReader sstable : window.getValue())
            {
                long ts = sstable.getMaxTimestamp() / 1000;
                assertTrue(window.getKey() <= ts && ts < window.getKey() + HOUR);
            }
    }

    @Test
    public void addAndRemoveAreIdempotent()
    {
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        SSTableReader sstable = sstableAt(NOW);
        tscs.addSSTable(sstable);
        tscs.addSSTable(sstable);                       // 중복 통지 (CSM:1054)
        assertEquals(1, tscs.getSSTables().size());
        tscs.removeSSTable(sstable);
        tscs.removeSSTable(sstable);
        assertEquals(0, tscs.getSSTables().size());
    }

    @Test
    public void delegateSeesOnlyActiveWindows()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader current = sstableAt(NOW - 60_000);                 // CURRENT
        SSTableReader closing = sstableAt(NOW - 2 * HOUR - 60_000);      // CLOSING (freeze_after 2h 이내)
        SSTableReader frozen = sstableAt(NOW - 10 * HOUR);               // 비활성
        tscs.addSSTable(current);
        tscs.addSSTable(closing);
        tscs.addSSTable(frozen);

        tscs.getNextBackgroundTasksAt(NOW, 0);                           // 테스트용 시각 주입 오버로드

        verify(delegate).addSSTables(Mockito.argThat(iterable -> {
            Set<SSTableReader> added = com.google.common.collect.Sets.newHashSet(iterable);
            return added.contains(current) && added.contains(closing) && !added.contains(frozen);
        }));
    }

    @Test
    public void delegateIsPrunedWhenWindowsAge()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader sstable = sstableAt(NOW - 60_000);
        tscs.addSSTable(sstable);
        tscs.getNextBackgroundTasksAt(NOW, 0);                           // 위임에 추가됨
        when(delegate.getSSTables()).thenReturn(Set.of(sstable));

        tscs.getNextBackgroundTasksAt(NOW + 4 * HOUR, 0);                // 창이 늙음 → 위임에서 제거
        verify(delegate).removeSSTables(Mockito.argThat(iterable ->
            com.google.common.collect.Sets.newHashSet(iterable).contains(sstable)));
    }

    @Test
    public void backgroundTasksComeFromDelegate()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask task = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(task));
        TimeSeriesCompactionStrategy tscs = strategy(delegate);
        tscs.addSSTable(sstableAt(NOW - 60_000));

        assertEquals(List.of(task), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
    }

    @Test
    public void estimatedTasksSumsDelegate()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getEstimatedRemainingTasks()).thenReturn(3);
        TimeSeriesCompactionStrategy tscs = strategy(delegate);
        assertEquals(3, tscs.getEstimatedRemainingTasks());
    }

    @Test
    public void expiredWindowsAreSelectedForWholeDrop()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        Map<String, String> opts = options();
        opts.put(TimeSeriesCompactionStrategyOptions.RETENTION, "30d");
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, opts, delegate);

        SSTableReader live = sstableAt(NOW - HOUR);
        SSTableReader expired1 = sstableAt(NOW - 31L * 24 * HOUR);
        SSTableReader expired2 = sstableAt(NOW - 31L * 24 * HOUR + 60_000);   // same expired window
        tscs.addSSTable(live);
        tscs.addSSTable(expired1);
        tscs.addSSTable(expired2);

        Set<SSTableReader> selected = tscs.expiredSSTables(NOW);
        assertEquals(Set.of(expired1, expired2), selected);
    }

    @Test
    public void nothingExpiresWithoutRetention()
    {
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        tscs.addSSTable(sstableAt(NOW - 400L * 24 * HOUR));
        assertEquals(Set.of(), tscs.expiredSSTables(NOW));
    }

    @Test
    public void expiredSelectionIsFilteredThroughLiveSSTablesAndNeverTriesTheTracker()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask delegateTask = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(delegateTask));

        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        // Simulate a zombie: this instance's own bookkeeping still classifies the sstable as an expired
        // window, but the tracker no longer considers it live (e.g. removed via another path already).
        // Mirrors UnifiedCompactionStrategy#getSSTables' zombie filter (CASSANDRA-18342).
        when(cfs.getLiveSSTables()).thenReturn(Set.of());

        Map<String, String> opts = options();
        opts.put(TimeSeriesCompactionStrategyOptions.RETENTION, "30d");
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, opts, delegate);
        tscs.addSSTable(sstableAt(NOW - 31L * 24 * HOUR));               // expired per classification, but not live

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        // Nothing survived the live-set filter, so the tracker is never asked to mark anything compacting,
        // and background tasks fall through to the delegate untouched.
        Mockito.verify(cfs.getTracker(), Mockito.never()).tryModify(Mockito.anyCollection(), Mockito.any(OperationType.class));
        assertEquals(List.of(delegateTask), List.copyOf(tasks));
    }
}
