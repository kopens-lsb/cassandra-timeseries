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
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;
import org.apache.cassandra.db.timeseries.ColumnarCursor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SP3 transparent reads: decodes a chunk payload into synthetic CQL rows for coordinator-side
 * merging with hot rows (design spec section 3.3.1, plan R3/R4).
 *
 * A chunk carries every regular column of the base table (SP4: {@link ColumnarChunkCodec}, format
 * version 3), so one sample becomes one row with one cell per column that is non-null on that
 * sample. Columns are matched to the table <b>by name</b>: a column the chunk carries but the table
 * has since dropped is ignored, and a column added after the chunk was written simply reads as null.
 *
 * Every synthetic cell carries {@code max_row_writetime + 1} as its writetime. The {@code + 1} is
 * <b>forced, not chosen</b>: the re-encoder deletes the source rows with
 * {@code USING TIMESTAMP maxWt} where {@code maxWt == max_row_writetime}, and Cassandra's
 * {@code DeletionTime.deletes()} is {@code timestamp <= markedForDeleteAt}, so rows reconstructed at
 * {@code max_row_writetime} are shadowed by tiering's own tombstone the moment they are merged
 * against the unfiltered stream ({@link ChunkMergeUnfilteredIterator}). Reconstructing rows that a
 * tombstone removed requires post-dating that tombstone.
 * <p>
 * What that buys, and what it costs:
 * <ul>
 *   <li>A real user tombstone always post-dates the sweep that chunked the window, so it still
 *       shadows the reconstruction -- deletes are honoured. (Writing one is nonetheless refused; see
 *       {@link TieredWrites} for why a delete that only masks the chunk is not good enough.)</li>
 *   <li>An un-swept hot row survives the tiering tombstone only if its writetime is
 *       &gt; {@code maxWt}, i.e. &gt;= {@code maxWt + 1} -- so the old strict guarantee "a hot row
 *       always beats the chunk" weakens to "ties are possible at exactly {@code maxWt + 1}".
 *       Cassandra breaks cell ties by comparing values, so a late correction landing on exactly that
 *       microsecond can lose to the chunk's stale value if its value sorts lower. Reachable only
 *       with client-supplied {@code USING TIMESTAMP}; server-assigned microsecond timestamps make it
 *       vanishingly unlikely, and it is the same class of hazard explicit timestamps already carry
 *       in plain Cassandra.</li>
 * </ul>
 * The writetime is still an approximation for {@code writetime(col)} selections (documented
 * limitation): every column of every reconstructed row reports the same value.
 */
public final class ChunkReadSupport
{
    private ChunkReadSupport()
    {
    }

    /**
     * @param metadata          the base table's metadata (one timestamp clustering column; any set
     *                          of regular columns)
     * @param payload           the chunk blob ({@link ColumnarChunkCodec}, version 3)
     * @param maxRowWritetime   the chunk row's max_row_writetime (micros); cells are stamped one
     *                          microsecond LATER, see the class javadoc
     * @param startMsInclusive  emit samples with timestamp &gt;= this (epoch ms)
     * @param endMsExclusive    emit samples with timestamp &lt; this (epoch ms)
     * @param descending        emit newest timestamp first. NOT the same as the read's "reversed"
     *                          flag: on a table declared {@code WITH CLUSTERING ORDER BY (ts DESC)}
     *                          an <em>un</em>-reversed read already runs newest-first
     *                          (see {@code emitDescending} in {@link TransparentReads})
     * @return synthetic rows in the requested order
     * @throws IllegalArgumentException on a corrupt payload - callers decide whether to
     *         skip-and-warn (read path, plan R4) or propagate (tests, re-encoder)
     * @throws org.apache.cassandra.db.timeseries.UnsupportedChunkFormatException when the payload
     *         names a chunk format this build does not read; callers must NOT swallow that one -
     *         it is systematic, so skipping would silently truncate history on every read
     */
    public static List<Row> rowsFromChunk(TableMetadata metadata,
                                          ByteBuffer payload,
                                          long maxRowWritetime,
                                          long startMsInclusive,
                                          long endMsExclusive,
                                          boolean descending)
    {
        ColumnarCursor cursor = ColumnarChunkCodec.cursor(payload, null);
        // See the class javadoc: at max_row_writetime itself the re-encoder's own range tombstone
        // (issued at exactly that timestamp) shadows every row rebuilt here. Saturate rather than
        // overflow -- a corrupt chunk could carry Long.MAX_VALUE.
        long cellTimestamp = maxRowWritetime == Long.MAX_VALUE ? Long.MAX_VALUE : maxRowWritetime + 1;

        // Resolve the chunk's column names against the table once, not per row.
        List<String> names = new ArrayList<>(cursor.columns().size());
        List<ColumnMetadata> columns = new ArrayList<>(cursor.columns().size());
        for (String name : cursor.columns())
        {
            ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes(name));
            // Dropped since the chunk was written (or, defensively, no longer a regular column):
            // there is nowhere to put its cells, so leave it out rather than failing the read.
            if (column != null && column.isRegular())
            {
                names.add(name);
                columns.add(column);
            }
        }

        List<Row> rows = new ArrayList<>();
        while (cursor.advance())
        {
            long ts = cursor.timestamp();
            if (ts < startMsInclusive || ts >= endMsExclusive)
                continue;

            Clustering<?> clustering = Clustering.make(TimestampType.instance.decompose(new Date(ts)));
            // unsorted, not sorted: the chunk's directory is ordered by Java String comparison while
            // a row's columns are ordered by UTF-8 byte comparison of their names, and the two differ
            // for non-ASCII names.
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(clustering);
            boolean anyCell = false;
            for (int i = 0; i < columns.size(); i++)
            {
                ByteBuffer value = cursor.getBytes(names.get(i));
                if (value == null)
                    continue;                             // null cell: stays null, no cell emitted
                builder.addCell(BufferCell.live(columns.get(i), cellTimestamp, value));
                anyCell = true;
            }
            if (!anyCell)
            {
                // Every column null on this sample. The row still EXISTS - the re-encoder chunked it
                // precisely so its existence would not be lost to the range delete - and a row with
                // neither cells nor primary-key liveness is indistinguishable from no row at all, so
                // give it the liveness a bare `INSERT INTO t (key, ts) VALUES (...)` would have had.
                builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(cellTimestamp));
            }
            rows.add(builder.build());
        }
        if (descending)
            Collections.reverse(rows);
        return rows;
    }
}
