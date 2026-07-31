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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;

/**
 * Creates and names the per-table shadow "chunk table" that the background re-encoder
 * ({@link TieredStorageService}) writes Gorilla/Chimp128-encoded chunks into.
 * <p>
 * The chunk table schema is normative (see docs/superpowers/plans/2026-07-31-chunk-store-sp2.md,
 * "섀도 테이블 스키마"): it copies the base table's single partition key column verbatim (same name
 * and type) and adds a fixed set of clustering/regular columns describing the encoded chunk.
 */
public final class ChunkTables
{
    private static final String CLUSTERING_COLUMN = "window_start";
    private static final String CODEC_COLUMN = "codec";
    private static final String SAMPLES_COLUMN = "samples";
    private static final String MAX_ROW_WRITETIME_COLUMN = "max_row_writetime";
    private static final String PAYLOAD_COLUMN = "payload";

    private ChunkTables()
    {
    }

    /** @return the shadow chunk table name for {@code baseTable}, e.g. {@code "metrics__chunks"}. */
    public static String chunkTableName(String baseTable)
    {
        return baseTable + "__chunks";
    }

    /**
     * Builds the (not-yet-registered) {@link TableMetadata} for {@code base}'s chunk table, per the
     * normative schema: {@code PRIMARY KEY (tag, window_start)} with {@code tag} copied from
     * {@code base}'s partition key column, and {@code UnifiedCompactionStrategy} with
     * {@code scaling_parameters = T4}.
     */
    public static TableMetadata chunkTableMetadata(TableMetadata base)
    {
        ColumnMetadata basePartitionKey = base.partitionKeyColumns().get(0);

        CompactionParams compaction = CompactionParams.create(UnifiedCompactionStrategy.class,
                                                                ImmutableMap.of("scaling_parameters", "T4"));

        return TableMetadata.builder(base.keyspace, chunkTableName(base.name))
                             .addPartitionKeyColumn(basePartitionKey.name, basePartitionKey.type)
                             .addClusteringColumn(CLUSTERING_COLUMN, TimestampType.instance)
                             .addRegularColumn(CODEC_COLUMN, ByteType.instance)
                             .addRegularColumn(SAMPLES_COLUMN, Int32Type.instance)
                             .addRegularColumn(MAX_ROW_WRITETIME_COLUMN, LongType.instance)
                             .addRegularColumn(PAYLOAD_COLUMN, BytesType.instance)
                             .params(TableParams.builder().compaction(compaction).build())
                             .build();
    }

    /**
     * Idempotently ensures {@code base}'s chunk table exists, creating it via the normal
     * CMS-serialized schema path if not (precedent: {@code CQLSSTableWriter.java:715}).
     */
    public static void ensureChunkTable(TableMetadata base)
    {
        Schema.instance.submit(SchemaTransformations.addTable(chunkTableMetadata(base), true));
    }
}
