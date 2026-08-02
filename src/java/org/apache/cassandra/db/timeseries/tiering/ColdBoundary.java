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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;

/**
 * Reads epoch-millis timestamps out of clusterings and slice bounds on a table whose single
 * clustering column is a {@code timestamp}, and says where the hot/cold boundary currently sits.
 * <p>
 * Shared by the read path ({@link TransparentReads}, deciding which chunks a SELECT needs) and the
 * write path ({@link TieredWrites}, deciding whether a mutation reaches data that has been chunked)
 * so the two can never disagree about what "cold" means.
 * <p>
 * The one subtlety both sides need: {@code WITH CLUSTERING ORDER BY (ts DESC)} wraps the clustering
 * type in {@code ReversedType}, so {@link Slice} bounds are ordered by <b>descending</b> timestamp
 * -- the comparator-order start bound is then the range's <b>upper</b> time bound and vice versa.
 * {@link #lowestMs} and {@link #highestMsExclusive} take the reversal as a parameter rather than
 * letting each caller re-derive it.
 */
public final class ColdBoundary
{
    private ColdBoundary()
    {
    }

    /** True if {@code metadata}'s clustering is declared DESC (its type is a {@code ReversedType}). */
    public static boolean isDescending(TableMetadata metadata)
    {
        return metadata.clusteringColumns().get(0).isReversedType();
    }

    /**
     * The precondition every timestamp reader below has: exactly one clustering column, of type
     * {@code timestamp}. It is what {@link TieringPolicy#unsupportedSchemaError} enforces before
     * anything is ever chunked, but both the read gate and the write guard run on tables that have
     * <em>not</em> been through that check -- every non-tiered table in the cluster, and any table
     * carrying the extension on a shape tiering refuses. Composing a timestamp out of, say, an
     * {@code int} clustering throws {@code MarshalException} in the middle of an ordinary query, so
     * the shape is tested first and cheaply, off in-memory metadata.
     * <p>
     * {@code unwrap()}: {@code CLUSTERING ORDER BY (ts DESC)} wraps the type in {@code ReversedType},
     * and both orders are supported.
     */
    public static boolean hasTimestampClustering(TableMetadata metadata)
    {
        return metadata.clusteringColumns().size() == 1
               && metadata.clusteringColumns().get(0).type.unwrap().equals(TimestampType.instance);
    }

    /**
     * @return the epoch-millis instant at which data stops being hot: anything strictly older has
     * either been chunked already or is eligible to be on the next sweep.
     */
    public static long hotBoundaryMs(TieringPolicy policy)
    {
        return Clock.Global.currentTimeMillis() - policy.hotWindowMillis;
    }

    /**
     * <b>The</b> definition of cold, for both sides of the feature: the epoch-millis instant below
     * which a chunk may hold data. A read whose range starts at or above it needs no chunk merge; a
     * write that tombstones a clustering below it is refused, because the chunk is the only copy of
     * that row and a tombstone could only mask it until {@code gc_grace_seconds} purged it.
     * <p>
     * Both terms are needed, and the maximum of them is what makes staleness safe:
     * <ul>
     *     <li><b>Coverage</b> is what the chunk table <em>actually</em> holds
     *     ({@link ChunkCoverage.Coverage#topExclusiveMs()}), so raising {@code hot_window}, dropping
     *     the extension or writing an invalid one cannot make already-encoded data unreadable --
     *     or, on the write side, make it deletable.</li>
     *     <li>The <b>hot boundary</b> floors that answer while a policy is installed: a chunk the
     *     re-encoder has just written may not be in this node's cached coverage yet, but it is by
     *     definition below the current hot boundary, so it is still covered.</li>
     * </ul>
     * Two sentinels come straight through from coverage and both are deliberate:
     * {@link Long#MIN_VALUE} when nothing is encoded and no policy is installed (nothing is cold,
     * and neither side needs to do anything), and {@link Long#MAX_VALUE} when coverage cannot be
     * established -- chunks may reach anywhere, so reads merge everything and writes reject
     * everything. Failing that way round is the cheap mistake: a merge that was not needed costs a
     * query, and a refused write is an immediate, visible error, while the other direction is silent
     * data resurrection on a {@code gc_grace_seconds} timer.
     *
     * @param policy the table's tiering policy, or {@code null} if it has none (or an invalid one --
     *               a typo in the extension JSON is a configuration error, not an instruction to
     *               un-protect the data already encoded under it)
     */
    public static long coldBelowMs(ChunkCoverage.Coverage coverage, TieringPolicy policy)
    {
        long coldBelow = coverage.topExclusiveMs();
        return policy == null ? coldBelow : Math.max(coldBelow, hotBoundaryMs(policy));
    }

    /** @return the timestamp of a (non-static) clustering, in epoch millis. */
    public static long clusteringMs(Clustering<?> clustering)
    {
        return TimestampType.instance.compose(clustering.bufferAt(0)).getTime();
    }

    /** @return the oldest timestamp {@code slice} can contain, or {@link Long#MIN_VALUE} if unbounded. */
    public static long lowestMs(Slice slice, boolean descending)
    {
        return lowerBoundMs(descending ? slice.end() : slice.start());
    }

    /** @return one past the newest timestamp {@code slice} can contain, or {@link Long#MAX_VALUE} if unbounded. */
    public static long highestMsExclusive(Slice slice, boolean descending)
    {
        return upperBoundMsExclusive(descending ? slice.start() : slice.end());
    }

    /**
     * The inclusive lower TIME bound expressed by a slice bound that restricts timestamps from below
     * -- the comparator-order start on an ASC clustering, the comparator-order end on a DESC one. An
     * unbounded bound is either BOTTOM (ASC) or TOP (DESC), hence both are tested.
     */
    public static long lowerBoundMs(ClusteringBound<?> bound)
    {
        // A bound with no clustering component restricts nothing. BOTTOM and TOP are the usual
        // shapes, but they are not the only ones: the covered range of an sstable holding only
        // static rows surfaces here as an empty bound of other kinds. Production hit exactly that
        // (AIOOBE at bufferAt(0)) on the first cold-window flush of a table whose seed writes were
        // static-only, and the whole flush fell back to rows because of it.
        if (bound.size() == 0)
            return Long.MIN_VALUE;
        long ms = boundMs(bound);
        return bound.isInclusive() ? ms : ms + 1;
    }

    /** The exclusive upper TIME bound of a slice bound that restricts timestamps from above (see above). */
    public static long upperBoundMsExclusive(ClusteringBound<?> bound)
    {
        // See lowerBoundMs for why this tests emptiness rather than BOTTOM/TOP.
        if (bound.size() == 0)
            return Long.MAX_VALUE;
        long ms = boundMs(bound);
        return bound.isInclusive() ? ms + 1 : ms;
    }

    private static long boundMs(ClusteringBound<?> bound)
    {
        return TimestampType.instance.compose(bound.bufferAt(0)).getTime();
    }
}
