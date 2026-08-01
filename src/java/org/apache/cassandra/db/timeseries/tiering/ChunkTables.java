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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.Clock;

/**
 * Creates and names the per-table shadow "chunk table" that the background re-encoder
 * ({@link TieredStorageService}) writes columnar chunks into.
 * <p>
 * The chunk table mirrors the base table's <b>whole</b> partition key -- every column, same names,
 * same types, same order -- so a chunk partition maps one-to-one onto a base partition whatever the
 * base key's arity, and adds a fixed set of clustering/regular columns describing the encoded chunk
 * ({@value #CLUSTERING_COLUMN} plus {@link #RESERVED_COLUMN_NAMES}).
 * <p>
 * One chunk row is one window of one partition, holding <b>every regular column</b> of the base
 * table over that window's shared timestamp axis -- not a stream of {@code (timestamp, value)}
 * samples. Its {@value #SAMPLES_COLUMN} is the number of rows encoded (not the number of values),
 * and its {@value #CODEC_COLUMN} is the payload's own leading version byte -- copied out of the blob
 * after encoding rather than named by the writer, so it always describes what was actually stored
 * (3 = {@link org.apache.cassandra.db.timeseries.ColumnarChunkCodec}, the only format written).
 */
public final class ChunkTables
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkTables.class);

    /**
     * How long {@link #ensureChunkTable} will wait for a freshly created chunk table to become
     * visible cluster-wide before giving up and letting the cycle proceed anyway. Matches the budget
     * {@code ViewBuilderTask} allows itself for the same hazard.
     */
    private static final long SCHEMA_AGREEMENT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    /**
     * {@code "keyspace.table"} of every chunk table this node has already watched the whole cluster
     * converge on. Schema only moves forwards and a chunk table is never dropped by this code, so a
     * table that was once visible everywhere stays visible: the check is worth paying exactly once
     * per chunk table per process, not on every sweep tick.
     */
    private static final Set<String> agreedChunkTables = ConcurrentHashMap.newKeySet();

    static final String CLUSTERING_COLUMN = "window_start";
    static final String CODEC_COLUMN = "codec";
    static final String SAMPLES_COLUMN = "samples";
    static final String MAX_ROW_WRITETIME_COLUMN = "max_row_writetime";
    static final String PAYLOAD_COLUMN = "payload";

    /**
     * The names the chunk table defines for itself. A base table whose partition key uses one of
     * these cannot be mirrored (two columns of the same name), so
     * {@link TieringPolicy#unsupportedSchemaError} rejects it up front rather than letting the
     * chunk-table creation fail with a schema-level error every sweep.
     */
    public static final Set<String> RESERVED_COLUMN_NAMES =
        ImmutableSet.of(CLUSTERING_COLUMN, CODEC_COLUMN, SAMPLES_COLUMN, MAX_ROW_WRITETIME_COLUMN, PAYLOAD_COLUMN);

    private ChunkTables()
    {
    }

    /** @return the shadow chunk table name for {@code baseTable}, e.g. {@code "metrics__chunks"}. */
    public static String chunkTableName(String baseTable)
    {
        return baseTable + "__chunks";
    }

    /**
     * Builds the (not-yet-registered) {@link TableMetadata} for {@code base}'s chunk table:
     * {@code PRIMARY KEY ((<every base partition key column>), window_start)}, each key column copied
     * verbatim from {@code base} (same name, same type, same order), and
     * {@code UnifiedCompactionStrategy} with {@code scaling_parameters = T4}.
     * <p>
     * Mirroring the full key -- rather than only its first column -- is what lets a table declared
     * {@code PRIMARY KEY ((asset_id, date, hour), ts)} be tiered at all: every chunk query is a
     * single-partition query on the same key values the base row had.
     */
    public static TableMetadata chunkTableMetadata(TableMetadata base)
    {
        CompactionParams compaction = CompactionParams.create(UnifiedCompactionStrategy.class,
                                                                ImmutableMap.of("scaling_parameters", "T4"));

        TableMetadata.Builder builder = TableMetadata.builder(base.keyspace, chunkTableName(base.name));
        for (ColumnMetadata partitionKeyColumn : base.partitionKeyColumns())
            builder.addPartitionKeyColumn(partitionKeyColumn.name, partitionKeyColumn.type);

        return builder.addClusteringColumn(CLUSTERING_COLUMN, TimestampType.instance)
                       .addRegularColumn(CODEC_COLUMN, ByteType.instance)
                       .addRegularColumn(SAMPLES_COLUMN, Int32Type.instance)
                       .addRegularColumn(MAX_ROW_WRITETIME_COLUMN, LongType.instance)
                       .addRegularColumn(PAYLOAD_COLUMN, BytesType.instance)
                       .params(TableParams.builder().compaction(compaction).build())
                       .build();
    }

    /**
     * Renders {@link #chunkTableMetadata} as the {@code CREATE TABLE IF NOT EXISTS} DDL that
     * {@link #ensureChunkTable} executes -- the whole partition key in the base table's own order, the
     * {@value #CLUSTERING_COLUMN} clustering column with its order, the four chunk payload columns, and
     * the one table option the chunk table sets (its compaction strategy). Everything else is left to
     * the CQL defaults, deliberately: the chunk table must not inherit the base table's
     * {@code default_time_to_live}, TTL-bearing params or extensions.
     * <p>
     * The DDL is generated from the {@link TableMetadata} rather than from {@code base} directly, so
     * the statement and the metadata cannot drift apart.
     * <p>
     * Every identifier is emitted through {@link CqlBuilder}, i.e. {@code ColumnIdentifier.maybeQuote}:
     * the keyspace, the table and every mirrored key column may be mixed-case, a reserved word, or
     * contain a quote, and all three sides have to survive that. (This is why the statement is built
     * here rather than interpolated at the call site.)
     */
    public static String createChunkTableStatement(TableMetadata base)
    {
        TableMetadata chunk = chunkTableMetadata(base);
        List<ColumnMetadata> keyColumns = chunk.partitionKeyColumns();
        List<ColumnMetadata> clusteringColumns = chunk.clusteringColumns();

        CqlBuilder builder = new CqlBuilder(512);
        builder.append("CREATE TABLE IF NOT EXISTS ")
               .appendQuotingIfNeeded(chunk.keyspace)
               .append('.')
               .appendQuotingIfNeeded(chunk.name)
               .append(" (");

        for (ColumnMetadata column : keyColumns)
            appendColumnDefinition(builder, column);
        for (ColumnMetadata column : clusteringColumns)
            appendColumnDefinition(builder, column);
        // Columns iterates in name order, which is deterministic; the create order of regular columns
        // has no semantic effect, but a stable one keeps the emitted DDL byte-identical run to run.
        for (ColumnMetadata column : chunk.regularColumns())
            appendColumnDefinition(builder, column);

        builder.append("PRIMARY KEY (");
        // A single-column partition key must NOT be parenthesised -- "PRIMARY KEY ((k), c)" is legal but
        // "PRIMARY KEY (k, c)" is what a composite-vs-simple key actually distinguishes for readers.
        if (keyColumns.size() > 1)
            builder.append('(')
                   .appendWithSeparators(keyColumns, (b, c) -> b.append(c.name), ", ")
                   .append(')');
        else
            builder.append(keyColumns.get(0).name);
        for (ColumnMetadata column : clusteringColumns)
            builder.append(", ").append(column.name);
        builder.append("))");

        builder.append(" WITH CLUSTERING ORDER BY (")
               .appendWithSeparators(clusteringColumns, (b, c) -> c.appendNameAndOrderTo(b), ", ")
               .append(')')
               .append(" AND compaction = ")
               .append(chunk.params.compaction.asMap());

        return builder.toString();
    }

    private static void appendColumnDefinition(CqlBuilder builder, ColumnMetadata column)
    {
        // Name and type only. Deliberately not ColumnMetadata.appendCqlTo, which would also carry a
        // mirrored key column's column mask and CHECK constraints across to the chunk table --
        // chunkTableMetadata copies neither.
        builder.append(column.name)
               .append(' ')
               .append(column.type)
               .append(", ");
    }

    /**
     * Idempotently ensures {@code base}'s chunk table exists, creating it by executing real
     * {@code CREATE TABLE IF NOT EXISTS} DDL through the ordinary CQL path.
     * <p>
     * It has to be real DDL. TCM serializes a committed schema change into the cluster metadata log
     * <b>as CQL text</b> ({@code SchemaTransformation.SchemaTransformationSerializer}), and a
     * programmatic {@code SchemaTransformations.addTable(...)} does not override
     * {@link org.apache.cassandra.schema.SchemaTransformation#cql()}, whose default return value is the
     * literal string {@code "null"}. Submitting one therefore writes an entry that nothing can ever
     * read back: every peer fetching the log, and this node replaying its own log after a restart,
     * fails with {@code SyntaxException: no viable alternative at input 'null'} -- which stalls schema
     * propagation cluster-wide from that entry onward. A {@code CreateTableStatement} parsed from text
     * carries that text in {@code cql()}, so it round-trips. (The precedent this used to cite,
     * {@code CQLSSTableWriter}, is the <em>offline</em> SSTable writer: no peers, no log replay.)
     * <p>
     * {@link QueryProcessor#process} rather than {@code executeInternal}: schema statements must go to
     * the CMS, not just to local state. The consistency level is irrelevant to a DDL statement (schema
     * agreement is the metadata log's job, not the read/write path's) and is never consulted. The
     * internal {@code QueryState} carries no user, so no authorization, guardrail or auto-grant applies.
     * <p>
     * <b>The postcondition is cluster-wide, not local</b> (see {@link #awaitClusterWideVisibility}):
     * committing the DDL only guarantees that the CMS accepted it and that <em>this</em> node has
     * applied it, and the re-encoder's very next act is to read the chunk table at the policy's
     * consistency level. A replica that has not yet caught up to that metadata epoch answers such a
     * read with {@code INCOMPATIBLE_SCHEMA}, which the coordinator surfaces as a
     * {@code ReadFailureException} -- and the re-encoder's per-tag error handler then skips that tag
     * for the whole cycle. On a 3-node cluster that silently loses an arbitrary, run-to-run varying
     * subset of tags from the first cycle after the chunk table is created.
     */
    public static void ensureChunkTable(TableMetadata base)
    {
        QueryProcessor.process(createChunkTableStatement(base), ConsistencyLevel.ONE);
        awaitClusterWideVisibility(base.keyspace, chunkTableName(base.name));
    }

    /**
     * Blocks (up to {@link #SCHEMA_AGREEMENT_TIMEOUT_MILLIS}) until every reachable node reports this
     * node's schema version, i.e. until the chunk table just ensured is visible to every replica the
     * re-encoder is about to query. This is the same precaution {@code ViewBuilderTask} takes before
     * sending view mutations for a just-created materialized view, and for the same reason: a peer
     * that has not seen the schema change cannot serve a request naming the new table.
     * <p>
     * Schema versions are collected with {@link StorageProxy#describeSchemaVersions}, which asks each
     * live node for its <em>current</em> version over the wire, rather than with
     * {@code Gossiper.waitForSchemaAgreement}, which compares gossip-cached versions: those lag the
     * schema change itself, so every node can still be advertising the <em>old</em> version and
     * "agree" on it, returning immediately and leaving the race exactly as it was.
     * <p>
     * Unreachable nodes are ignored: a node that cannot answer a schema-version request cannot serve
     * the re-encoder's reads either, and waiting for it would only delay the tags whose replicas are
     * up. A single-node cluster short-circuits before sending anything, so this costs nothing off a
     * real cluster. On timeout the cycle proceeds anyway (and is not memoized, so the next cycle
     * re-checks): the per-tag error handling still holds the line, and refusing to run at all would
     * turn a slow peer into a permanently stalled re-encoder.
     */
    private static void awaitClusterWideVisibility(String keyspace, String table)
    {
        String key = keyspace + '.' + table;
        if (agreedChunkTables.contains(key))
            return;

        // Nobody to propagate to. Also the shape every single-node unit test runs in, where the
        // messaging layer this would otherwise drive may not even be listening.
        if (Gossiper.instance.getLiveMembers().size() <= 1)
            return;

        long deadline = Clock.Global.currentTimeMillis() + SCHEMA_AGREEMENT_TIMEOUT_MILLIS;
        long backoffMillis = 50;
        while (true)
        {
            if (everyReachableNodeHasOurSchema())
            {
                agreedChunkTables.add(key);
                return;
            }

            if (Clock.Global.currentTimeMillis() >= deadline)
                break;

            Uninterruptibles.sleepUninterruptibly(backoffMillis, TimeUnit.MILLISECONDS);
            backoffMillis = Math.min(1000, backoffMillis * 2);
        }

        logger.warn("Tiered storage: {} did not become visible cluster-wide within {}ms of being ensured. " +
                    "Tags whose replicas are still behind will fail this re-encode cycle with " +
                    "INCOMPATIBLE_SCHEMA and be retried on the next one.", key, SCHEMA_AGREEMENT_TIMEOUT_MILLIS);
    }

    /** @return {@code true} if no reachable node reports a schema version other than this node's. */
    private static boolean everyReachableNodeHasOurSchema()
    {
        String ourVersion = Schema.instance.getVersion().toString();
        Map<String, List<String>> versions = StorageProxy.describeSchemaVersions(false);
        for (String version : versions.keySet())
        {
            if (!version.equals(StorageProxy.UNREACHABLE) && !version.equals(ourVersion))
                return false;
        }
        return true;
    }
}
