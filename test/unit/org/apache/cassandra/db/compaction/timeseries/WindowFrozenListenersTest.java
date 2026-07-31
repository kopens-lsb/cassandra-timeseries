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

package org.apache.cassandra.db.compaction.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class WindowFrozenListenersTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();   // TableMetadata/SSTableReader 목의 클래스 초기화 안전망
    }

    @After
    public void clear()
    {
        WindowFrozenListeners.unsafeClearListeners();
    }

    @Test
    public void listenerExceptionsAreIsolatedAndDoNotPropagate()
    {
        AtomicInteger fired = new AtomicInteger();
        WindowFrozenListeners.registerListener((table, windowStart, frozen) -> { throw new RuntimeException("boom"); });
        WindowFrozenListeners.registerListener((table, windowStart, frozen) -> fired.incrementAndGet());

        WindowFrozenListeners.fire(mock(TableMetadata.class), 42L, mock(SSTableReader.class));

        assertEquals(1, fired.get());   // 예외 리스너 "뒤"의 리스너도 호출되고, 예외는 호출자(컴팩션)로 새지 않는다
    }

    @Test
    public void unregisterAndClearStopDelivery()
    {
        AtomicInteger fired = new AtomicInteger();
        WindowFrozenListener listener = (table, windowStart, frozen) -> fired.incrementAndGet();
        WindowFrozenListeners.registerListener(listener);
        WindowFrozenListeners.fire(mock(TableMetadata.class), 1L, mock(SSTableReader.class));
        WindowFrozenListeners.unregisterListener(listener);
        WindowFrozenListeners.fire(mock(TableMetadata.class), 2L, mock(SSTableReader.class));
        assertEquals(1, fired.get());
    }

    @Test
    public void fireDeliversArgumentsUnchanged()
    {
        TableMetadata table = mock(TableMetadata.class);
        SSTableReader frozen = mock(SSTableReader.class);
        List<Object> seen = new ArrayList<>();
        WindowFrozenListeners.registerListener((t, w, f) -> { seen.add(t); seen.add(w); seen.add(f); });

        WindowFrozenListeners.fire(table, 42L, frozen);

        assertEquals(List.of(table, 42L, frozen), seen);
    }
}
