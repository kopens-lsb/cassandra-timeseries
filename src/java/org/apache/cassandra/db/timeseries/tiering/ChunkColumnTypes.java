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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.ColumnarChunkCodec;

/**
 * Maps a base-table column's {@link AbstractType} onto the columnar (v3) chunk type code the
 * re-encoder should encode its values with.
 * <p>
 * The <b>only</b> thing a type code selects is which compression the column's <em>serialized</em>
 * bytes go through -- it never changes what those bytes are. Every code round-trips the exact
 * {@code ByteBuffer} Cassandra stored, so nothing here needs to know how a type is composed or
 * decomposed, and a column decoded out of a chunk is byte-identical to the cell that went in:
 * <ul>
 *   <li>{@code double} -> {@link ColumnarChunkCodec#TYPE_DOUBLE_CHIMP} (8-byte IEEE-754 payload,
 *       fed through the chimp128 value stream)</li>
 *   <li>{@code boolean} -> {@link ColumnarChunkCodec#TYPE_BOOLEAN} (1 byte, bit-packed)</li>
 *   <li>{@code int} and {@code date} -> {@link ColumnarChunkCodec#TYPE_INT32} (both are exactly 4
 *       bytes big-endian; {@code date} is an <em>unsigned</em> day count, so it is read back as a
 *       signed int and delta-encoded as one -- the zigzag varint stream is lossless over the whole
 *       32-bit range either way, and the 4 bytes written back out are the ones that came in)</li>
 *   <li>{@code bigint}, {@code timestamp} and {@code time} -> {@link ColumnarChunkCodec#TYPE_INT64}
 *       (all three are exactly 8 bytes big-endian: raw value, epoch millis, and nanoseconds since
 *       midnight respectively -- no normalisation is applied or needed)</li>
 *   <li>{@code text}/{@code varchar}/{@code ascii} -> {@link ColumnarChunkCodec#TYPE_TEXT}
 *       (dictionary-encoded up to 256 distinct values, else length-prefixed raw)</li>
 *   <li><b>everything else</b> -> {@link ColumnarChunkCodec#TYPE_OPAQUE}: the value's serialized
 *       bytes verbatim, dictionary/RLE-compressed. This fallback is what makes "any time-series
 *       table can be tiered" true -- {@code blob}, {@code uuid}, {@code timeuuid}, {@code decimal},
 *       {@code varint}, {@code inet}, {@code smallint}, {@code tinyint}, {@code float},
 *       {@code duration}, frozen collections, frozen UDTs and tuples all land here.</li>
 * </ul>
 * Widths matter: a type is only mapped onto a fixed-width code when its serialized form is exactly
 * that width. {@code smallint} (2 bytes), {@code tinyint} (1 byte) and {@code float} (4 bytes) are
 * deliberately opaque rather than squeezed into {@code INT32}/{@code BOOLEAN}, because those codes
 * re-serialize at their own width (or, for {@code BOOLEAN}, collapse the value to a 0/1 bit) and
 * would silently corrupt the round trip.
 * <p>
 * Counter columns and non-frozen (multi-cell) columns never reach here --
 * {@link TieringPolicy#unsupportedSchemaError} rejects those tables outright.
 * <p>
 * The mapping is per column <em>type</em>, but a chunk records a type code per column per chunk, and
 * {@link ColumnarChunkCodec#encode} downgrades a column to {@code TYPE_OPAQUE} for that one chunk if
 * any of its present values is not exactly the fixed width the code re-serializes at. That covers
 * the values Cassandra accepts but the natural code cannot represent -- {@code blobAsInt(0x)} is
 * legal and deserializes as a 0-byte {@code int} -- without a schema-level special case.
 * <p>
 * One documented non-byte-exactness: {@code boolean} values are stored as one bit, so a cell whose
 * serialized byte is neither {@code 0x00} nor {@code 0x01} (e.g. {@code blobAsBoolean(0x02)}, which
 * Cassandra reads as {@code true}) decodes back as {@code 0x01}. The composed CQL value is
 * unchanged; only the byte pattern is normalised.
 */
public final class ChunkColumnTypes
{
    private ChunkColumnTypes()
    {
    }

    /** @return the v3 type code to encode {@code type}'s serialized values with; never fails. */
    public static byte typeCodeFor(AbstractType<?> type)
    {
        AbstractType<?> unwrapped = type.unwrap();
        if (unwrapped instanceof DoubleType)
            return ColumnarChunkCodec.TYPE_DOUBLE_CHIMP;
        if (unwrapped instanceof BooleanType)
            return ColumnarChunkCodec.TYPE_BOOLEAN;
        if (unwrapped instanceof Int32Type || unwrapped instanceof SimpleDateType)
            return ColumnarChunkCodec.TYPE_INT32;
        if (unwrapped instanceof LongType || unwrapped instanceof TimestampType || unwrapped instanceof TimeType)
            return ColumnarChunkCodec.TYPE_INT64;
        if (unwrapped instanceof UTF8Type || unwrapped instanceof AsciiType)
            return ColumnarChunkCodec.TYPE_TEXT;
        return ColumnarChunkCodec.TYPE_OPAQUE;
    }
}
