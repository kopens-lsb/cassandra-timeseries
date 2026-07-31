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
        return sstableSpanning(maxTimestampMillis - 1, maxTimestampMillis);
    }

    /** min·max를 각각 지정하는 상태 기계용 확장판 (기존 sstableAt(max)는 min = max − 1ms 고정) */
    static SSTableReader sstableSpanning(long minTimestampMillis, long maxTimestampMillis)
    {
        SSTableReader sstable = mock(SSTableReader.class, Mockito.RETURNS_DEEP_STUBS);
        when(sstable.getMaxTimestamp()).thenReturn(maxTimestampMillis * 1000);   // timestampResolution 기본 MICROSECONDS
        when(sstable.getMinTimestamp()).thenReturn(minTimestampMillis * 1000);
        // AbstractCompactionTask.validateSSTables는 2개 이상 묶을 때 수리 상태 일관성을 검사한다 — 딥스텁은
        // getPendingRepair()에 목 TimeUUID를 내주면서 isPendingRepair()는 false라 자기모순이 된다
        // (UnifiedCompactionStrategyTest.mockSSTable과 같은 처방)
        when(sstable.isRepaired()).thenReturn(false);
        when(sstable.getPendingRepair()).thenReturn(null);
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

    @Test
    public void freezeTaskIsCreatedForClosedMultiSSTableWindow()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader current = sstableAt(NOW - 60_000);
        SSTableReader old1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader old2 = sstableAt(NOW - 10 * HOUR + 60_000);    // 같은 닫힌 창
        tscs.addSSTable(current);
        tscs.addSSTable(old1);
        tscs.addSSTable(old2);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(current, old1, old2));

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        AbstractCompactionTask task = List.copyOf(tasks).get(0);
        assertEquals(1, tasks.size());                               // 라운드당 동결 최대 1개
        assertTrue(task instanceof FreezeCompactionTask);
        assertEquals(Set.of(old1, old2), task.transaction.originals());
        verify(delegate, Mockito.never()).getNextBackgroundTasks(anyLong());   // 동결이 위임보다 우선
    }

    @Test
    public void oldestFreezingWindowIsSelectedFirst()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader newer1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader newer2 = sstableAt(NOW - 10 * HOUR + 60_000);
        SSTableReader older1 = sstableAt(NOW - 20 * HOUR);
        SSTableReader older2 = sstableAt(NOW - 20 * HOUR + 60_000);
        for (SSTableReader s : List.of(newer1, newer2, older1, older2))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(newer1, newer2, older1, older2));

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(1, tasks.size());
        assertEquals(Set.of(older1, older2), List.copyOf(tasks).get(0).transaction.originals());   // 오래된 창 우선
    }

    @Test
    public void frozenWindowIsNotReselected()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        tscs.addSSTable(sstableAt(NOW - 10 * HOUR));                 // 단일 + 포함(min = max − 1ms) = FROZEN
        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(List.of(), List.copyOf(tasks));
        verify(cfs.getTracker(), Mockito.never()).tryModify(anyCollection(), any(OperationType.class));
    }

    @Test
    public void spanningSingleSSTableIsNeverFreezeSelected()
    {
        // FREEZING로 분류돼도(포함 실패) 단일 sstable은 재작성해 봐야 포함이 안 고쳐진다(T3 스플릿 전) →
        // 선택 대상이 아니어야 무한 재컴팩션 루프가 없다
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        tscs.addSSTable(sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR));

        assertNull(tscs.nextFreezeCandidate(NOW));
    }

    @Test
    public void farFutureWindowNeverCountsTowardFreeze()
    {
        // Pins end-to-end behavior (spec section 8): a far-future window is never a freeze candidate and
        // never counts toward the backlog, however many sstables it has. Note this cannot distinguish the
        // explicit far-future filter in nextFreezeCandidate from the classifier's verdict (any future
        // window classifies CURRENT under today's predicates, which also skips it) - it pins the
        // behavior, not the filter; see the precondition-hygiene comment at the filter site.
        TimeSeriesCompactionStrategy tscs = strategy(mock(UnifiedCompactionStrategy.class));
        tscs.addSSTable(sstableAt(NOW + 3L * 24 * HOUR));
        tscs.addSSTable(sstableAt(NOW + 3L * 24 * HOUR + 60_000));

        assertNull(tscs.nextFreezeCandidate(NOW));
        assertEquals(0, tscs.getEstimatedRemainingTasks());
    }

    @Test
    public void freezeSkipsWhenZombieFilterLeavesSingleSurvivor()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader old1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader old2 = sstableAt(NOW - 10 * HOUR + 60_000);
        tscs.addSSTable(old1);
        tscs.addSSTable(old2);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(old1));        // old2는 좀비: 트래커 기준 이미 비활성

        tscs.getNextBackgroundTasksAt(NOW, 0);

        // 생존자 1개짜리 동결은 무의미 — 다음 라운드에 집합이 안정되면 자연 치유된다
        verify(cfs.getTracker(), Mockito.never()).tryModify(anyCollection(), any(OperationType.class));
    }

    @Test
    public void tryModifyRefusalSkipsRoundAndRetriesUntilItSucceeds()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        AbstractCompactionTask delegateTask = mock(AbstractCompactionTask.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of(delegateTask));
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader old1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader old2 = sstableAt(NOW - 10 * HOUR + 60_000);
        tscs.addSSTable(old1);
        tscs.addSSTable(old2);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(old1, old2));
        when(cfs.getTracker().tryModify(anyCollection(), any(OperationType.class))).thenReturn(null);

        // 1·2라운드: 트래커 거부(예: CLOSING 시절 시작된 위임 컴팩션 진행 중) → 위임으로 폴스루.
        // 2라운드째 동일 후보 재실패는 TWCS previousCandidate 가드(:100-106)의 크로스-라운드 판으로 WARN만 남긴다.
        assertEquals(List.of(delegateTask), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
        assertEquals(List.of(delegateTask), List.copyOf(tscs.getNextBackgroundTasksAt(NOW, 0)));
        verify(cfs.getTracker(), Mockito.times(2)).tryModify(anyCollection(), any(OperationType.class));

        // 3라운드: 경합이 풀리면 정상 동결 — 가드가 살아있는 후보를 영구히 굶기지 않는다는 증명
        stubTracker(cfs);
        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);
        assertEquals(1, tasks.size());
        assertTrue(List.copyOf(tasks).get(0) instanceof FreezeCompactionTask);
    }

    @Test
    public void freezeBacklogCountsInEstimatedRemainingTasks()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenReturn(3);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader a1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader a2 = sstableAt(NOW - 10 * HOUR + 60_000);
        SSTableReader b1 = sstableAt(NOW - 20 * HOUR);
        SSTableReader b2 = sstableAt(NOW - 20 * HOUR + 60_000);
        for (SSTableReader s : List.of(a1, a2, b1, b2))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(a1, a2, b1, b2));

        tscs.getNextBackgroundTasksAt(NOW, 0);                       // 백로그 계산은 배경 라운드의 부수효과

        // 위임 3 + 만료 0 + FREEZING 창 2 — 백로그를 안 세면 CSM 정렬에서 다른 테이블에 밀려 동결이 기아한다
        assertEquals(5, tscs.getEstimatedRemainingTasks());
    }

    @Test
    public void spanningSingleIsSelectedForSplitRefreeze()
    {
        // T3: 레거시 걸침 단일 sstable은 동결 후보는 아니지만(위 테스트) 분할 재작성 후보다 (스펙 §4/§10)
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader spanning = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        tscs.addSSTable(spanning);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(spanning));

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(1, tasks.size());
        AbstractCompactionTask task = List.copyOf(tasks).get(0);
        assertTrue(task instanceof SplitRefreezeCompactionTask);
        assertEquals(Set.of(spanning), task.transaction.originals());
        verify(delegate, Mockito.never()).getNextBackgroundTasks(anyLong());   // 분할이 위임보다 우선
    }

    @Test
    public void normalFreezeBeatsSplitRefreeze()
    {
        // 우선순위: 만료 > 동결 > 분할 재작성 > 위임 (라운드당 태스크 1개)
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader spanning = sstableSpanning(NOW - 31 * HOUR, NOW - 30 * HOUR);   // 더 오래된 걸침 창
        SSTableReader f1 = sstableAt(NOW - 10 * HOUR);
        SSTableReader f2 = sstableAt(NOW - 10 * HOUR + 60_000);
        for (SSTableReader s : List.of(spanning, f1, f2))
            tscs.addSSTable(s);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(spanning, f1, f2));

        Collection<AbstractCompactionTask> tasks = tscs.getNextBackgroundTasksAt(NOW, 0);

        assertEquals(1, tasks.size());
        assertTrue(List.copyOf(tasks).get(0) instanceof FreezeCompactionTask);   // 걸침이 더 오래돼도 동결 우선
    }

    @Test
    public void splitBacklogCountsInEstimatedRemainingTasks()
    {
        UnifiedCompactionStrategy delegate = mock(UnifiedCompactionStrategy.class);
        when(delegate.getNextBackgroundTasks(anyLong())).thenReturn(List.of());
        when(delegate.getEstimatedRemainingTasks()).thenReturn(0);
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class, Mockito.RETURNS_DEEP_STUBS);
        stubTracker(cfs);
        TimeSeriesCompactionStrategy tscs = new TimeSeriesCompactionStrategy(cfs, options(), delegate);

        SSTableReader spanA = sstableSpanning(NOW - 11 * HOUR, NOW - 10 * HOUR);
        SSTableReader spanB = sstableSpanning(NOW - 21 * HOUR, NOW - 20 * HOUR);
        tscs.addSSTable(spanA);
        tscs.addSSTable(spanB);
        when(cfs.getLiveSSTables()).thenReturn(Set.of(spanA, spanB));

        tscs.getNextBackgroundTasksAt(NOW, 0);

        // 위임 0 + 만료 0 + 동결 0 + 걸침 2 — 분할 백로그도 CSM 정렬에 보여야 기아하지 않는다
        assertEquals(2, tscs.getEstimatedRemainingTasks());
    }
}
