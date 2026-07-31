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
package org.apache.cassandra.db.timeseries.tiering;

import java.util.List;

/**
 * JMX interface for {@link TieredStorageService}, backing {@code nodetool tieringstatus} and
 * {@code nodetool retier} (see {@code org.apache.cassandra.tools.nodetool.TieringStatus} and
 * {@code org.apache.cassandra.tools.nodetool.Retier}).
 */
public interface TieredStorageServiceMBean
{
    /**
     * Runs one re-encode cycle for {@code keyspace.table} synchronously, right now, bypassing the
     * scheduled sweep's interval check.
     *
     * @throws IllegalStateException if a run for this table -- sweep- or {@code retier}-triggered --
     * is already in flight
     */
    void retier(String keyspace, String table);

    /**
     * @return one tab-delimited status line per table that has a {@code timeseries_tiering} policy:
     * {@code keyspace, table, interval_ms, last_run_at (epoch millis, or -1 if it has never run),
     * windows_encoded, rows_encoded, late_merges, chunks_expired}.
     */
    List<String> statusRows();
}
