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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.timeseries.Chimp128Codec;
import org.apache.cassandra.db.timeseries.ChunkCodecs;
import org.apache.cassandra.db.timeseries.GorillaCodec;
import org.apache.cassandra.db.timeseries.SampleCursor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;

/**
 * Background re-encoder that turns closed, hot-window-expired time-series rows into Gorilla/Chimp128
 * chunks in a shadow {@code "<table>__chunks"} table (see {@link ChunkTables}), then tombstones the
 * source rows it just encoded.
 * <p>
 * {@link #runOnce} is the whole cycle for one (keyspace, table) pair, run synchronously and
 * idempotently -- see docs/superpowers/plans/2026-07-31-chunk-store-sp2.md ("재인코딩 사이클") for the
 * normative algorithm this implements. This class has no scheduler yet (see Task 3): callers drive
 * {@link #runOnce} directly.
 * <p>
 * <b>Invariants (violating either fails review):</b>
 * <ul>
 *     <li>Every data read/write/delete goes through {@link QueryProcessor#process} at the policy's
 *     configured {@link ConsistencyLevel} -- never {@code executeInternal}, which would only see
 *     locally-held data and could tombstone rows a distributed delete should have fanned out to.</li>
 *     <li>The range delete of source rows for a re-encoded window always uses
 *     {@code USING TIMESTAMP <maxWritetimeOfRowsEncodedIntoTheChunk>}, so a row written after this
 *     cycle read the window (a "late" row) has a newer write timestamp than the tombstone and
 *     survives it -- to be merged into the chunk on a subsequent cycle.</li>
 * </ul>
 */
public class TieredStorageService
{
    private static final Logger logger = LoggerFactory.getLogger(TieredStorageService.class);

    /** Network page size for the paged scans this cycle issues (tag enumeration, row reads, cold-expiry candidates). */
    private static final int PAGE_SIZE = 5000;

    /** Per-{@link #runOnce} call counters, also meant for the (not-yet-built) virtual table / nodetool status. */
    public static class TierRunStats
    {
        public long windowsEncoded;
        public long rowsEncoded;
        public long lateMerges;
        public long chunksExpired;
        public long bytesWritten;
    }

    /**
     * Runs one re-encode cycle for {@code keyspace.table}. No-ops (returning all-zero stats) if the
     * table has no {@code timeseries_tiering} policy, the policy is invalid, or the table's schema is
     * not the canonical single-partition-key/timestamp-clustering/double-value shape the re-encoder
     * requires -- in the latter two cases an error is logged rather than failing silently.
     */
    public TierRunStats runOnce(String keyspace, String table, long nowMillis)
    {
        TierRunStats stats = new TierRunStats();

        TableMetadata base = Schema.instance.getTableMetadata(keyspace, table);
        if (base == null)
        {
            logger.warn("Tiered storage runOnce skipped: {}.{} does not exist", keyspace, table);
            return stats;
        }

        TieringPolicy policy;
        try
        {
            policy = TieringPolicy.fromTable(base);
        }
        catch (ConfigurationException e)
        {
            logger.error("Tiered storage runOnce skipped: {}.{} has an invalid timeseries_tiering policy: {}",
                         keyspace, table, e.getMessage());
            return stats;
        }
        if (policy == null)
            return stats;

        String schemaError = TieringPolicy.canonicalSchemaError(base);
        if (schemaError != null)
        {
            logger.error("Tiered storage runOnce skipped: {}.{} has a timeseries_tiering policy but is not a " +
                         "canonical time-series table: {}", keyspace, table, schemaError);
            return stats;
        }

        ChunkTables.ensureChunkTable(base);
        String chunkRef = quotedRef(keyspace, ChunkTables.chunkTableName(table));

        ColumnMetadata tagColumn = base.partitionKeyColumns().get(0);
        ColumnMetadata tsColumn = base.clusteringColumns().get(0);
        ColumnMetadata valueColumn = base.regularColumns().iterator().next();

        String tagCql = tagColumn.name.toCQLString();
        String tsCql = tsColumn.name.toCQLString();
        String valueCql = valueColumn.name.toCQLString();
        String tagRaw = tagColumn.name.toString();
        String tsRaw = tsColumn.name.toString();
        String valueRaw = valueColumn.name.toString();
        String baseRef = base.toString();

        ConsistencyLevel cl = policy.consistency;
        long cutoff = policy.windowStartFor(nowMillis - policy.hotWindowMillis);

        long[] tsBuf = new long[1024];
        double[] valBuf = new double[1024];

        String rowsQuery = format("SELECT %s, %s, WRITETIME(%s) AS wt FROM %s WHERE %s = ? AND %s < ? ORDER BY %s ASC",
                                  tsCql, valueCql, valueCql, baseRef, tagCql, tsCql, tsCql);
        String existingChunkQuery = format("SELECT payload, max_row_writetime FROM %s WHERE %s = ? AND window_start = ?",
                                           chunkRef, tagCql);
        String insertChunkQuery = format("INSERT INTO %s (%s, window_start, codec, samples, max_row_writetime, payload) " +
                                         "VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?", chunkRef, tagCql);
        String deleteRowsQuery = format("DELETE FROM %s USING TIMESTAMP ? WHERE %s = ? AND %s >= ? AND %s < ?",
                                        baseRef, tagCql, tsCql, tsCql);
        String selectExpiredQuery = format("SELECT window_start FROM %s WHERE %s = ? AND window_start < ?", chunkRef, tagCql);
        String deleteExpiredQuery = format("DELETE FROM %s WHERE %s = ? AND window_start < ?", chunkRef, tagCql);

        for (ByteBuffer tag : enumerateTags(base, tagCql, tagRaw, baseRef, cl))
        {
            List<UntypedResultSet.Row> rows = pagedSelect(rowsQuery, cl,
                                                           Arrays.asList(tag, TimestampType.instance.fromTimeInMillis(cutoff)));

            int i = 0;
            int n = rows.size();
            while (i < n)
            {
                long windowStart = policy.windowStartFor(rows.get(i).getTimestamp(tsRaw).getTime());
                long windowEnd = windowStart + policy.chunkWindowMillis;

                int j = i;
                while (j < n && rows.get(j).getTimestamp(tsRaw).getTime() < windowEnd)
                    j++;

                UntypedResultSet existingRs = QueryProcessor.process(existingChunkQuery, cl,
                        Arrays.asList(tag, TimestampType.instance.fromTimeInMillis(windowStart)));
                UntypedResultSet.Row existingRow = (existingRs == null || existingRs.isEmpty()) ? null : existingRs.one();

                TreeMap<Long, Double> merged = new TreeMap<>();
                long maxWt = Long.MIN_VALUE;
                if (existingRow != null)
                {
                    SampleCursor cursor = ChunkCodecs.cursor(existingRow.getBytes("payload"));
                    while (cursor.advance())
                        merged.put(cursor.timestamp(), cursor.value());
                    maxWt = existingRow.getLong("max_row_writetime");
                }

                for (int k = i; k < j; k++)
                {
                    UntypedResultSet.Row row = rows.get(k);
                    // Rows always win over the previously-encoded chunk on a ts collision -- they are
                    // by definition a rewrite of that timestamp with a newer write time.
                    merged.put(row.getTimestamp(tsRaw).getTime(), row.getDouble(valueRaw));
                    maxWt = Math.max(maxWt, row.getLong("wt"));
                }

                int count = merged.size();
                if (tsBuf.length < count)
                {
                    int newLength = tsBuf.length;
                    while (newLength < count)
                        newLength *= 2;
                    tsBuf = Arrays.copyOf(tsBuf, newLength);
                    valBuf = Arrays.copyOf(valBuf, newLength);
                }
                int idx = 0;
                for (Map.Entry<Long, Double> sample : merged.entrySet())
                {
                    tsBuf[idx] = sample.getKey();
                    valBuf[idx] = sample.getValue();
                    idx++;
                }

                ByteBuffer payload = encode(policy.codec, tsBuf, valBuf, count);
                byte codecByte = codecVersionByte(ChunkCodecs.codecOf(payload));

                QueryProcessor.process(insertChunkQuery, cl, Arrays.asList(
                        tag,
                        TimestampType.instance.fromTimeInMillis(windowStart),
                        ByteType.instance.decompose(codecByte),
                        Int32Type.instance.decompose(count),
                        LongType.instance.decompose(maxWt),
                        payload,
                        LongType.instance.decompose(maxWt + 1)));

                QueryProcessor.process(deleteRowsQuery, cl, Arrays.asList(
                        LongType.instance.decompose(maxWt),
                        tag,
                        TimestampType.instance.fromTimeInMillis(windowStart),
                        TimestampType.instance.fromTimeInMillis(windowEnd)));

                stats.windowsEncoded++;
                stats.rowsEncoded += (j - i);
                if (existingRow != null)
                    stats.lateMerges++;
                stats.bytesWritten += payload.remaining();

                i = j;
            }

            if (policy.coldWindowMillis >= 0)
            {
                List<ByteBuffer> coldValues = Arrays.asList(
                        tag, TimestampType.instance.fromTimeInMillis(nowMillis - policy.coldWindowMillis));
                List<UntypedResultSet.Row> expired = pagedSelect(selectExpiredQuery, cl, coldValues);
                if (!expired.isEmpty())
                {
                    stats.chunksExpired += expired.size();
                    QueryProcessor.process(deleteExpiredQuery, cl, coldValues);
                }
            }
        }

        return stats;
    }

    /**
     * Enumerates the distinct partition keys ("tags") of {@code base} restricted to this node's local
     * primary token ranges ({@link StorageService#getPrimaryRanges}), so a multi-node cluster's
     * re-encoders partition the tag space instead of every node redundantly re-encoding every tag.
     * <p>
     * A primary range can wrap the ring's zero point (e.g. a single-node cluster's sole range, which
     * is degenerate: {@code (t, t]} covering the whole ring); {@link Range#unwrap()} normalizes that
     * into 1-2 non-wrapping sub-ranges, each turned into its own {@code token(tag) > ? AND <= ?}
     * restriction (a bound equal to the partitioner's minimum token is omitted -- it is a sentinel,
     * not a real, ownable token). If {@code getPrimaryRanges} itself reports no ranges at all -- which
     * should not happen once a node has joined the ring, but is treated defensively rather than
     * silently scanning nothing -- this falls back to an unrestricted scan of every tag.
     */
    private static List<ByteBuffer> enumerateTags(TableMetadata base, String tagCql, String tagRaw, String baseRef,
                                                   ConsistencyLevel cl)
    {
        Set<ByteBuffer> tags = new LinkedHashSet<>();
        Collection<Range<Token>> primaryRanges = StorageService.instance.getPrimaryRanges(base.keyspace);

        if (primaryRanges.isEmpty())
        {
            logger.warn("Tiered storage: {}.{} has no local primary ranges reported for this node; falling back " +
                        "to an unrestricted tag scan rather than silently skipping data", base.keyspace, base.name);
            collectTags(tags, format("SELECT DISTINCT %s FROM %s", tagCql, baseRef), tagRaw, cl, Collections.emptyList());
            return new ArrayList<>(tags);
        }

        for (Range<Token> range : primaryRanges)
            for (Range<Token> sub : range.unwrap())
                collectTags(tags, tagRangeQuery(tagCql, baseRef, sub), tagRaw, cl, Collections.emptyList());

        return new ArrayList<>(tags);
    }

    private static String tagRangeQuery(String tagCql, String baseRef, Range<Token> sub)
    {
        boolean hasLower = !sub.left.isMinimum();
        boolean hasUpper = !sub.right.isMinimum();

        StringBuilder query = new StringBuilder("SELECT DISTINCT ").append(tagCql).append(" FROM ").append(baseRef);
        if (hasLower || hasUpper)
        {
            query.append(" WHERE ");
            if (hasLower)
                query.append("token(").append(tagCql).append(") > ").append(sub.left);
            if (hasLower && hasUpper)
                query.append(" AND ");
            if (hasUpper)
                query.append("token(").append(tagCql).append(") <= ").append(sub.right);
        }
        return query.toString();
    }

    private static void collectTags(Set<ByteBuffer> out, String query, String tagRaw, ConsistencyLevel cl,
                                     List<ByteBuffer> values)
    {
        for (UntypedResultSet.Row row : pagedSelect(query, cl, values))
            out.add(row.getBytes(tagRaw));
    }

    private static ByteBuffer encode(TieringPolicy.CodecChoice codec, long[] timestamps, double[] values, int count)
    {
        switch (codec)
        {
            case GORILLA:
                return ChunkCodecs.encode(ChunkCodecs.Codec.GORILLA, timestamps, values, count);
            case CHIMP128:
                return ChunkCodecs.encode(ChunkCodecs.Codec.CHIMP128, timestamps, values, count);
            case AUTO:
            default:
                return ChunkCodecs.encodeSmallest(timestamps, values, count);
        }
    }

    private static byte codecVersionByte(ChunkCodecs.Codec codec)
    {
        switch (codec)
        {
            case GORILLA:
                return GorillaCodec.VERSION;
            case CHIMP128:
                return Chimp128Codec.VERSION;
            default:
                throw new AssertionError("Unhandled codec: " + codec);
        }
    }

    private static String quotedRef(String keyspace, String table)
    {
        return format("%s.%s", ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(table));
    }

    /**
     * Runs {@code query} to completion across as many pages as needed (network page size
     * {@link #PAGE_SIZE}), via {@link QueryProcessor#instance} directly rather than the static
     * {@link QueryProcessor#process(String, ConsistencyLevel, List)} convenience method, since that
     * overload has no way to carry a {@link PagingState} between calls.
     */
    private static List<UntypedResultSet.Row> pagedSelect(String query, ConsistencyLevel cl, List<ByteBuffer> values)
    {
        List<UntypedResultSet.Row> rows = new ArrayList<>();
        QueryState queryState = QueryState.forInternalCalls();
        PagingState pagingState = null;
        while (true)
        {
            QueryOptions options = QueryOptions.create(cl, values, false, PAGE_SIZE, pagingState, null,
                                                        ProtocolVersion.CURRENT, null);
            CQLStatement statement = QueryProcessor.instance.parse(query, queryState, options);
            ResultMessage result = QueryProcessor.instance.process(statement, queryState, options,
                                                                    Dispatcher.RequestTime.forImmediateExecution());
            if (!(result instanceof ResultMessage.Rows))
                break;

            ResultSet resultSet = ((ResultMessage.Rows) result).result;
            for (UntypedResultSet.Row row : UntypedResultSet.create(resultSet))
                rows.add(row);

            pagingState = resultSet.metadata.getPagingState();
            if (pagingState == null)
                break;
        }
        return rows;
    }
}
