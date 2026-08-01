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
     * @return the epoch-millis instant at which data stops being hot: anything strictly older has
     * either been chunked already or is eligible to be on the next sweep.
     */
    public static long hotBoundaryMs(TieringPolicy policy)
    {
        return Clock.Global.currentTimeMillis() - policy.hotWindowMillis;
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
        if (bound.isBottom() || bound.isTop())
            return Long.MIN_VALUE;
        long ms = boundMs(bound);
        return bound.isInclusive() ? ms : ms + 1;
    }

    /** The exclusive upper TIME bound of a slice bound that restricts timestamps from above (see above). */
    public static long upperBoundMsExclusive(ClusteringBound<?> bound)
    {
        if (bound.isTop() || bound.isBottom())
            return Long.MAX_VALUE;
        long ms = boundMs(bound);
        return bound.isInclusive() ? ms + 1 : ms;
    }

    private static long boundMs(ClusteringBound<?> bound)
    {
        return TimestampType.instance.compose(bound.bufferAt(0)).getTime();
    }
}
