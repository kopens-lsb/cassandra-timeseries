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
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;

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
     */
    public static void ensureChunkTable(TableMetadata base)
    {
        QueryProcessor.process(createChunkTableStatement(base), ConsistencyLevel.ONE);
    }
}
