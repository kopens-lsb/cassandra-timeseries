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

import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Enforces the one rule that makes tiering's read path sound: <b>cold data is immutable</b>.
 * <p>
 * Once the re-encoder has moved a window into the {@code __chunks} shadow table, the base rows are
 * gone and the chunk is the only copy. A tombstone written against that window can therefore only
 * ever be a <em>temporary</em> mask: {@link TransparentReads} merges the chunk's rows back in, and
 * the tombstone shadows them only until {@code gc_grace_seconds} purges it -- after which the
 * deleted data reappears, permanently and silently. Documenting that would have been shipping a
 * data-resurrection bug on a timer, so instead such writes are rejected outright, and
 * {@code cold_window} is the supported way to remove cold data.
 * <p>
 * Rejected on a tiering-enabled table, when the write reaches a clustering older than
 * {@code now - hot_window}: partition-level deletes (which necessarily reach cold data -- they have
 * no clustering bound at all), range deletes, row deletes, and deletes of individual cells. A cell
 * tombstone is also what {@code UPDATE ... SET col = null} and {@code INSERT ... VALUES (..., null)}
 * write, so those are caught by the same check -- the test is what the mutation actually contains,
 * not which statement produced it.
 * <p>
 * <b>Not</b> rejected: anything confined to the hot window (ordinary current-data deletes are
 * untouched), and writes of real values to cold clusterings -- a late {@code UPDATE ... SET x = 1}
 * against a chunked row is a supported operation that the re-encoder merges into the chunk per
 * column on its next cycle. Only <em>un</em>writing cold data is refused.
 */
public final class TieredWrites
{
    private TieredWrites()
    {
    }

    /**
     * @throws InvalidRequestException if any of {@code mutations} tombstones data at or below the
     * hot boundary of a tiering-enabled table
     */
    public static void guardColdMutations(List<? extends IMutation> mutations)
    {
        // The re-encoder's own range delete is exactly the write this guard exists to reject -- it is
        // also the one write that is allowed to do it, because it deletes rows it has just copied
        // into a chunk. It brackets its whole cycle with the same bypass the read path uses.
        if (TransparentReads.inInternalBypass())
            return;

        for (IMutation mutation : mutations)
            for (PartitionUpdate update : mutation.getPartitionUpdates())
                guard(update);
    }

    /**
     * As {@link #guardColdMutations}, for the one update a conditional (LWT/CAS) write builds --
     * {@code CQL3CasRequest.makeUpdates} never goes through {@code getMutations}, so without this
     * {@code DELETE ... IF EXISTS} would walk straight past the rule. Covers conditional BATCH too:
     * that path builds its update through the same request object.
     */
    public static void guardColdUpdate(PartitionUpdate update)
    {
        if (TransparentReads.inInternalBypass())
            return;
        guard(update);
    }

    private static void guard(PartitionUpdate update)
    {
        TableMetadata metadata = update.metadata();
        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(metadata);
        }
        catch (ConfigurationException e)
        {
            return;                                       // an invalid policy must not break writes
        }
        if (policy == null)
            return;
        // Same guard the read path applies: without exactly one timestamp clustering column nothing
        // has ever been chunked, so there is no cold copy for a tombstone to mask.
        if (metadata.clusteringColumns().size() != 1)
            return;

        long hotBoundary = ColdBoundary.hotBoundaryMs(policy);
        boolean descending = ColdBoundary.isDescending(metadata);

        if (!update.partitionLevelDeletion().isLive())
            throw reject(metadata, "a partition-level DELETE", "it has no clustering bound, so it necessarily " +
                                                               "covers data that has already been chunked");

        DeletionInfo deletionInfo = update.deletionInfo();
        if (deletionInfo.hasRanges())
        {
            Iterator<RangeTombstone> ranges = deletionInfo.rangeIterator(false);
            while (ranges.hasNext())
            {
                RangeTombstone range = ranges.next();
                if (ColdBoundary.lowestMs(range.deletedSlice(), descending) < hotBoundary)
                    throw reject(metadata, "a range DELETE", describeBoundary(hotBoundary));
            }
        }

        for (Row row : update)
        {
            // Static cells are never chunked (the re-encoder deletes a clustering RANGE, which cannot
            // touch them), so deleting one is always safe.
            if (row.clustering() == Clustering.STATIC_CLUSTERING)
                continue;
            if (!hasTombstone(row))
                continue;
            if (ColdBoundary.clusteringMs(row.clustering()) < hotBoundary)
                throw reject(metadata,
                             row.deletion().isLive() ? "deleting a column (or writing a null value into one)"
                                                     : "a row DELETE",
                             describeBoundary(hotBoundary));
        }
    }

    private static boolean hasTombstone(Row row)
    {
        if (!row.deletion().isLive() || row.hasComplexDeletion())
            return true;
        for (Cell<?> cell : row.cells())
            if (cell.isTombstone())
                return true;
        return false;
    }

    private static String describeBoundary(long hotBoundary)
    {
        return "it reaches clusterings older than the hot window (before epoch millis " + hotBoundary + ')';
    }

    private static InvalidRequestException reject(TableMetadata metadata, String what, String why)
    {
        return new InvalidRequestException(
            String.format("%s is not allowed on %s.%s: %s, and data older than the hot window is immutable once " +
                          "timeseries_tiering has encoded it into %s (the base rows are gone, so a tombstone would " +
                          "only mask the chunk's copy until gc_grace_seconds purged it, after which the deleted data " +
                          "would silently reappear). Use the policy's cold_window to expire cold data; to remove a " +
                          "partition outright, delete it from %s as well.",
                          what, metadata.keyspace, metadata.name, why,
                          ChunkTables.chunkTableName(metadata.name), ChunkTables.chunkTableName(metadata.name)));
    }
}
