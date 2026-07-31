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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
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

    /** min·max를 각각 지정하는 상태 기계용 확장판 (기존 sstableAt(max)는 min = max − 1ms 고정) */
    static SSTableReader sstableSpanning(long minTimestampMillis, long maxTimestampMillis)
    {
        SSTableReader sstable = mock(SSTableReader.class, Mockito.RETURNS_DEEP_STUBS);
        when(sstable.getMaxTimestamp()).thenReturn(maxTimestampMillis * 1000);   // timestampResolution 기본 MICROSECONDS
        when(sstable.getMinTimestamp()).thenReturn(minTimestampMillis * 1000);
        return sstable;
    }

    /**
     * tryModify가 "요청 집합을 originals로 갖는 오프라인 txn 목"을 내주도록 스텁.
     * isOffline=true로 AbstractCompactionTask 생성자의 compacting-marked 단언(:52-58)을 우회한다 —
     * 딥스텁 기본값에 맡기면 originals가 목 Set이 되어 태스크 라우팅 단언이 불가능하다.
     */
    @SuppressWarnings("unchecked")
    private static void stubTracker(ColumnFamilyStore cfs)
    {
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class)))
            .thenAnswer(invocation -> {
                Set<SSTableReader> requested = new HashSet<>((Collection<SSTableReader>) invocation.getArgument(0));
                LifecycleTransaction txn = mock(LifecycleTransaction.class);
                when(txn.originals()).thenReturn(requested);
                when(txn.isOffline()).thenReturn(true);
                return txn;
            });
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

    @Test
    public void classifierDerivesAllFiveStates()
    {
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        Map<String, String> opts = options();
        opts.put(TimeSeriesCompactionStrategyOptions.RETENTION, "30d");
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, opts, mock(UnifiedCompactionStrategy.class));
        TimeSeriesCompactionStrategyOptions o = new TimeSeriesCompactionStrategyOptions(opts);

        SSTableReader current = sstableAt(NOW - 60_000);
        SSTableReader closing = sstableAt(NOW - 2 * HOUR - 60_000);
        SSTableReader freezing1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader freezing2 = sstableAt(NOW - 10 * HOUR + 60_000);
        SSTableReader frozen = sstableAt(NOW - 20 * HOUR);                       // 단일 + min·max 같은 창(포함)
        SSTableReader expired = sstableAt(NOW - 31L * 24 * HOUR);

        assertEquals(TimeSeriesCompactionStrategy.WindowState.CURRENT,
                     tscs.classify(o.windowStartFor(NOW - 60_000), Set.of(current), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.CLOSING,
                     tscs.classify(o.windowStartFor(NOW - 2 * HOUR - 60_000), Set.of(closing), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FREEZING,
                     tscs.classify(o.windowStartFor(NOW - 10 * HOUR), Set.of(freezing1, freezing2), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FROZEN,
                     tscs.classify(o.windowStartFor(NOW - 20 * HOUR), Set.of(frozen), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.EXPIRED,
                     tscs.classify(o.windowStartFor(NOW - 31L * 24 * HOUR), Set.of(expired), NOW));
    }

    @Test
    public void singleSpanningSSTableIsFreezingNotFrozen()
    {
        // FROZEN은 "창 경계 안에 완전히 포함된 단일 sstable" — min이 이전 창에 걸치면 동결 상태가 아니다
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        TimeSeriesCompactionStrategyOptions o = new TimeSeriesCompactionStrategyOptions(options());
        SSTableReader spanning = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FREEZING,
                     tscs.classify(o.windowStartFor(NOW - 10 * HOUR), Set.of(spanning), NOW));
    }

    @Test
    public void lateDataRevertsFrozenToFreezingByPureDerivation()
    {
        // 상태 저장이 없으므로 FROZEN 창에 sstable이 더해지면 판정이 그 자리에서 FREEZING으로 되돌아간다 (스펙 §4)
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        TimeSeriesCompactionStrategyOptions o = new TimeSeriesCompactionStrategyOptions(options());
        long windowStart = o.windowStartFor(NOW - 10 * HOUR);
        SSTableReader frozen = sstableAt(NOW - 10 * HOUR);
        SSTableReader late = sstableAt(NOW - 10 * HOUR + 30_000);
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FROZEN, tscs.classify(windowStart, Set.of(frozen), NOW));
        assertEquals(TimeSeriesCompactionStrategy.WindowState.FREEZING, tscs.classify(windowStart, Set.of(frozen, late), NOW));
    }

    @Test
    public void farFutureSSTablesAreExcludedFromDelegate()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader current = sstableAt(NOW - 60_000);
        SSTableReader farFuture = sstableAt(NOW + 3L * 24 * HOUR);   // 기본 max_future_window 1d 초과
        tscs.addSSTable(current);
        tscs.addSSTable(farFuture);

        tscs.getNextBackgroundTasksAt(NOW, 0);

        verify(delegate).addSSTables(Mockito.argThat(iterable -> {
            Set<SSTableReader> added = com.google.common.collect.Sets.newHashSet(iterable);
            return added.contains(current) && !added.contains(farFuture);
        }));
    }

    @Test
    public void farFutureSSTablesArePrunedFromDelegateIfAlreadyThere()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        TimeSeriesCompactionStrategy tscs = strategy(delegate);

        SSTableReader farFuture = sstableAt(NOW + 3L * 24 * HOUR);
        tscs.addSSTable(farFuture);
        when(delegate.getSSTables()).thenReturn(Set.of(farFuture));  // 가드 도입 전에 위임에 들어가 있던 상황

        tscs.getNextBackgroundTasksAt(NOW, 0);

        verify(delegate).removeSSTables(Mockito.argThat(iterable ->
            com.google.common.collect.Sets.newHashSet(iterable).contains(farFuture)));
    }

    @Test
    public void maximalTasksBuildOneTaskPerWindowAndRouteExpiredThroughRetentionTask()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        Map<String, String> opts = options();
        opts.put(TimeSeriesCompactionStrategyOptions.RETENTION, "30d");
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, opts, delegate);

        SSTableReader live = sstableAt(NOW - HOUR);
        SSTableReader expired = sstableAt(NOW - 31L * 24 * HOUR);
        tscs.addSSTable(live);
        tscs.addSSTable(expired);

        List<AbstractCompactionTask> tasks = tscs.getMaximalTasksAt(NOW, 0, false);
        assertEquals(2, tasks.size());                               // 창당 1개, 창을 절대 넘지 않는다
        for (AbstractCompactionTask task : tasks)
        {
            boolean coversExpired = task.transaction.originals().contains(expired);
            assertEquals("expired 창만 retention 태스크로 라우팅", coversExpired, task instanceof TimeSeriesCompactionTask);
        }
    }

    @Test
    public void maximalTasksReturnEmptyListWhenTrackerRefuses()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class))).thenReturn(null);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);
        tscs.addSSTable(sstableAt(NOW - HOUR));

        assertEquals(List.of(), tscs.getMaximalTasksAt(NOW, 0, false));   // null이 아니라 빈 리스트 (TWCS 함정)
    }

    @Test
    public void userDefinedTaskMarksAndWraps()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);
        SSTableReader sstable = sstableAt(NOW - HOUR);

        AbstractCompactionTask task = tscs.getUserDefinedTask(List.of(sstable), 0);
        assertNotNull(task);
        assertTrue(task.isUserDefined);                              // 같은 패키지라 protected 필드 접근 가능
        assertEquals(Set.of(sstable), task.transaction.originals());
    }

    @Test
    public void userDefinedTaskReturnsNullWhenTrackerRefuses()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class))).thenReturn(null);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        assertNull(tscs.getUserDefinedTask(List.of(sstableAt(NOW - HOUR)), 0));
    }
}
