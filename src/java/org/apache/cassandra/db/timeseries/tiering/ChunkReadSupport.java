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
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.timeseries.ChunkCodecs;
import org.apache.cassandra.db.timeseries.SampleCursor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SP3 transparent reads: decodes a chunk payload into synthetic CQL rows for coordinator-side
 * merging with hot rows (design spec section 3.3.1, plan R3/R4).
 *
 * Every synthetic row carries the chunk's {@code max_row_writetime} as its cell writetime. That is
 * an approximation for {@code writetime(value)} selections (documented limitation), but it is
 * exactly what makes the merge rule correct with zero custom conflict logic: an un-swept hot row
 * for the same timestamp always has writetime &gt; the chunk's max_row_writetime (else the
 * re-encoder would have absorbed and deleted it), so standard timestamp-based reconciliation picks
 * the hot row - identical to the re-encoder's rows-win rule.
 */
public final class ChunkReadSupport
{
    private ChunkReadSupport()
    {
    }

    /**
     * @param metadata          the base table's metadata (canonical schema: timestamp clustering, double value)
     * @param payload           the chunk blob (version-dispatched via {@link ChunkCodecs#cursor})
     * @param maxRowWritetime   the chunk row's max_row_writetime (micros) - used as the cells' writetime
     * @param startMsInclusive  emit samples with timestamp &gt;= this (epoch ms)
     * @param endMsExclusive    emit samples with timestamp &lt; this (epoch ms)
     * @param reversed          emit in descending clustering order (for DESC / reversed slices)
     * @return synthetic rows in the requested order
     * @throws IllegalArgumentException on a corrupt/unknown payload - callers decide whether to
     *         skip-and-warn (read path, plan R4) or propagate (tests, re-encoder)
     */
    public static List<Row> rowsFromChunk(TableMetadata metadata,
                                          ByteBuffer payload,
                                          long maxRowWritetime,
                                          long startMsInclusive,
                                          long endMsExclusive,
                                          boolean reversed)
    {
        ColumnMetadata valueColumn = metadata.getColumn(ByteBufferUtil.bytes("value"));
        List<Row> rows = new ArrayList<>();
        SampleCursor cursor = ChunkCodecs.cursor(payload);
        while (cursor.advance())
        {
            long ts = cursor.timestamp();
            if (ts < startMsInclusive || ts >= endMsExclusive)
                continue;
            Clustering<?> clustering = Clustering.make(TimestampType.instance.decompose(new Date(ts)));
            BufferCell cell = BufferCell.live(valueColumn, maxRowWritetime, DoubleType.instance.decompose(cursor.value()));
            rows.add(BTreeRow.singleCellRow(clustering, cell));
        }
        if (reversed)
            Collections.reverse(rows);
        return rows;
    }
}
