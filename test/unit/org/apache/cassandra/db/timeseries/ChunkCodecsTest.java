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
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link ChunkCodecs} entry point -- the only one the chunk store is meant to consume for
 * the single-column format. Chimp128 (v2) is the sole single-column codec; the removed gorilla
 * codec's version byte (1) is now just an unknown version, and columnar (v3) payloads are rejected
 * with a pointer at {@link ColumnarChunkCodec}.
 */
public class ChunkCodecsTest
{
    @Test
    public void encodesAndCursorsChimpPayloads()
    {
        long[] ts = { 1L, 2L, 3L };
        double[] vs = { 1.0, 2.0, 3.0 };
        ByteBuffer payload = ChunkCodecs.encode(ts, vs, 3);

        assertEquals(Chimp128Codec.VERSION, payload.get(payload.position()));
        SampleCursor cursor = ChunkCodecs.cursor(payload);
        for (int i = 0; i < 3; i++)
        {
            assertTrue(cursor.advance());
            assertEquals(ts[i], cursor.timestamp());
            assertEquals(vs[i], cursor.value(), 0.0);
        }
        assertEquals(3, ChunkCodecs.sampleCount(payload));
    }

    @Test
    public void rejectsUnknownVersion()
    {
        ByteBuffer payload = ChunkCodecs.encode(new long[]{ 1L }, new double[]{ 1.0 }, 1);
        payload.put(payload.position(), (byte) 9);
        assertThatThrownBy(() -> ChunkCodecs.cursor(payload)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void rejectsRemovedGorillaVersion()
    {
        // version byte 1 was the gorilla single-column format; it must fail loudly, never be
        // half-decoded as a chimp payload (the headers are byte-identical in layout)
        ByteBuffer payload = ChunkCodecs.encode(new long[]{ 1L }, new double[]{ 1.0 }, 1);
        payload.put(payload.position(), (byte) 1);
        assertThatThrownBy(() -> ChunkCodecs.cursor(payload)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkCodecs.sampleCount(payload)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void firstAndLastTimestampPeeks()
    {
        ByteBuffer payload = ChunkCodecs.encode(new long[]{ 10L, 20L, 30L }, new double[]{ 1.0, 2.0, 3.0 }, 3);
        assertEquals(10L, ChunkCodecs.firstTimestamp(payload));
        assertEquals(30L, ChunkCodecs.lastTimestamp(payload));
    }

    @Test
    public void cursorRejectsColumnarPayloadsWithAPointer()
    {
        SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new TreeMap<>();
        columns.put("v", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32,
                                                            new ByteBuffer[]{ intBytes(1), intBytes(2), intBytes(3) }));
        ByteBuffer payload = ColumnarChunkCodec.encode(new long[]{ 1L, 2L, 3L }, 3, columns);

        assertThatThrownBy(() -> ChunkCodecs.cursor(payload))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("ColumnarChunkCodec.cursor()");
    }

    @Test
    public void headerConstantsAgree()
    {
        assertEquals(Chimp128Codec.HEADER_SIZE, ChunkCodecs.HEADER_SIZE);
        assertEquals(Chimp128Codec.MAX_SAMPLES, ChunkCodecs.MAX_SAMPLES);
    }

    private static ByteBuffer intBytes(int v)
    {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(v);
        b.flip();
        return b;
    }
}
