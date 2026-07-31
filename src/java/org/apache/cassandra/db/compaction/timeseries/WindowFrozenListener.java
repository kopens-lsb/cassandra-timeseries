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

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Notification hook for {@link org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy}: fired every
 * time a window is frozen (or re-frozen after late data) into a single sstable. Design spec section 5.
 * <p>
 * The event is a latency optimisation, not the sole trigger: consumers must be able to fall back to scanning
 * for "frozen windows without downstream output" themselves (e.g. the tiering re-encoder's watermark scan),
 * because events can be lost across restarts. Events are scoped to one strategy-instance slice (the
 * CompactionStrategyManager splits instances per repair status and per disk).
 */
public interface WindowFrozenListener
{
    /** 창이 단일 SSTable로 동결(또는 재동결)될 때마다 호출. 멱등해야 한다. */
    void onWindowFrozen(TableMetadata table, long windowStartMillis, SSTableReader frozen);
}
