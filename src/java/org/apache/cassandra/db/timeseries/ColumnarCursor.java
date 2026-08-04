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
package org.apache.cassandra.db.timeseries;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * A forward-only cursor over the rows of a version-3 (columnar) chunk codec payload, restricted
 * to the columns the caller projected (see {@link ColumnarChunkCodec#cursor}). Call
 * {@link #advance()} before every other accessor; it returns {@code false} once the payload is
 * exhausted.
 * <p>
 * Unlike the removed single-column format's {@code SampleCursor} (one value stream, no names),
 * a chunk can outlive a table's schema at encode time, so columns
 * are looked up by name rather than assumed to exist: {@link #hasColumn} reports whether a
 * column is known to this cursor at all (false for a column ADDed to the table after this chunk
 * was written, or for one excluded from the projection), and {@link #isNull}/{@link #getBytes}
 * report per-row presence for columns that are known.
 */
public interface ColumnarCursor
{
    boolean advance();

    long timestamp();

    /** True if {@code name} is a column this cursor knows about (see class javadoc). */
    boolean hasColumn(String name);

    /** True if {@code name} is unknown to this cursor, or known but null on the current row. */
    boolean isNull(String name);

    /**
     * The canonical serialized value of column {@code name} on the current row, or {@code null}
     * if the column is absent (unknown to this cursor, or not projected) or null on this row.
     */
    ByteBuffer getBytes(String name);

    /** The columns this cursor knows about -- the chunk's own columns intersected with the projection. */
    Set<String> columns();
}
