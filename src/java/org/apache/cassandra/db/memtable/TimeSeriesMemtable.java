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

package org.apache.cassandra.db.memtable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.TimeSeriesCompactionStrategy;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Memtable for tables compacted by {@link TimeSeriesCompactionStrategy}, sharded by the compaction
 * window a row's write timestamp falls in.
 *
 * <p><b>Why shard by window at write time.</b> Splitting a flush across window boundaries already
 * works, but it costs: {@code SSTableWriter} consumes a partition in one pass and accepts its key
 * exactly once, so routing one across windows means buffering the whole partition on heap.
 * {@code WindowRoutingIterator} caps that at 64 MiB and, beyond it, gives up splitting and writes
 * the partition whole — which yields a window-spanning sstable, an unchanged shape on the next
 * split-refreeze, and a window the no-progress guard parks. Deciding the window when the row
 * arrives removes the buffering, and with it the ceiling: a memtable is a mutable map, so a row
 * goes into its window in O(1) no matter how large the partition grows.
 *
 * <p><b>Configuration.</b> Memtables are selected by configuration <em>key</em>, not class name, so
 * this needs an entry in {@code cassandra.yaml} on every node before any table can use it:
 * <pre>
 * memtable:
 *   configurations:
 *     timeseries:
 *       class_name: TimeSeriesMemtable
 * </pre>
 * and then per table:
 * <pre>
 * ALTER TABLE ks.tbl WITH memtable = 'timeseries';
 * </pre>
 * The window size is read from the table's compaction options rather than taken as a parameter here
 * — a window size configured in two places is one that can disagree with itself, and the flush would
 * then land in a window the strategy does not expect.
 *
 * <p><b>Unsupported schemas fall back, they do not fail.</b> A memtable is on the write path, so
 * refusing a schema by throwing would fail every write to that table and turn a missed optimisation
 * into an outage. {@link #unsupportedReason} decides, and {@link Factory#create} falls back to the
 * default memtable with one warning per table per hour. Tiering's {@code TieringPolicy} declines
 * unsupported schemas the same way and for the same reason.
 */
public class TimeSeriesMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMemtable.class);

    /**
     * Entry point {@code MemtableParams} finds by reflection, given a copy of the configured
     * parameters. It rejects the configuration if any are left unconsumed, which is how a typo in a
     * memtable parameter is caught when the node starts rather than at the first flush. This
     * memtable takes none.
     */
    public static Memtable.Factory factory(Map<String, String> options)
    {
        return Factory.INSTANCE;
    }

    /**
     * @return why this memtable cannot hold {@code metadata}, or {@code null} if it can.
     */
    public static String unsupportedReason(TableMetadata metadata)
    {
        Class<?> compaction = metadata.params.compaction.klass();
        if (!TimeSeriesCompactionStrategy.class.isAssignableFrom(compaction))
            return "table uses " + compaction.getSimpleName() + ", not TimeSeriesCompactionStrategy, "
                   + "so there is no window_size to shard by";

        if (metadata.clusteringColumns().size() != 1)
            return "expected exactly one clustering column, found " + metadata.clusteringColumns().size();

        ColumnMetadata clustering = metadata.clusteringColumns().get(0);
        if (!(clustering.type.unwrap() instanceof TimestampType))
            return "clustering column " + clustering.name + " is " + clustering.type.asCQL3Type()
                   + ", not a timestamp";

        for (ColumnMetadata column : metadata.columns())
        {
            // A counter cannot be re-inserted once deleted, so any path that moves a counter cell
            // between sstables is destructive rather than merely slow.
            if (column.type.isCounter())
                return "counter column " + column.name + " cannot be re-inserted after deletion";
            // Multi-cell columns have no single write timestamp for the row, which is the whole
            // basis of assigning the row to a window.
            if (column.type.isMultiCell())
                return "non-frozen column " + column.name + " (" + column.type.asCQL3Type()
                       + ") is multi-cell";
        }
        return null;
    }

    public static class Factory implements Memtable.Factory
    {
        static final Factory INSTANCE = new Factory();

        @Override
        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadataRef,
                               Memtable.Owner owner)
        {
            TableMetadata metadata = metadataRef.get();
            String reason = unsupportedReason(metadata);
            if (reason != null)
            {
                // Keyed per table, not per memtable: a busy table flushes constantly, and one line
                // per flush for a condition that only changes on ALTER buries the line that matters.
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN,
                                 "timeseries-memtable-unsupported:" + metadata.keyspace + '.' + metadata.name,
                                 1, TimeUnit.HOURS,
                                 "memtable = 'timeseries' is set on {}.{} but cannot be used: {}. Falling " +
                                 "back to the default memtable — writes are unaffected, only the " +
                                 "window-sharding optimisation is lost.",
                                 metadata.keyspace, metadata.name, reason);
                return SkipListMemtableFactory.INSTANCE.create(commitLogLowerBound, metadataRef, owner);
            }

            // Task 2 substitutes the sharded implementation here. Until then the gate is the whole
            // deliverable: it decides correctly and changes no behaviour, so it is safe to ship.
            return SkipListMemtableFactory.INSTANCE.create(commitLogLowerBound, metadataRef, owner);
        }

        @Override
        public boolean equals(Object o)
        {
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getClass());
        }
    }
}
