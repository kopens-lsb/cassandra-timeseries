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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link ChunkCodecs#unsupportedVersion}, the single place that classifies chunk version
 * bytes: one naming a real-but-removed format (gorilla v1, chimp128 v2, columnar v3) is an
 * {@link UnsupportedChunkFormatException} the read path must never swallow, while one naming
 * nothing is indistinguishable from a scrambled first byte and stays a plain
 * {@link IllegalArgumentException} the read path skips one chunk at a time.
 * <p>
 * Exercised through {@link ColumnarChunkCodec}'s entry points -- the ones the production read path
 * uses -- rather than by calling the classifier directly, so a regression in the routing (an entry
 * point that stops consulting the classifier) fails here too. The payloads are a bare version byte
 * plus padding: since v1/v2/v3 have no decoders any more, classification must happen on that byte
 * alone, before any other byte is interpreted.
 */
public class ChunkCodecsTest
{
    @Test
    public void removedGorillaVersionIsReportedAsAnUnsupportedFormat()
    {
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(payloadWithVersion((byte) 1), null))
            .isInstanceOf(UnsupportedChunkFormatException.class)
            .hasMessageContaining("gorilla");
        assertThatThrownBy(() -> ColumnarChunkCodec.rowCount(payloadWithVersion((byte) 1)))
            .isInstanceOf(UnsupportedChunkFormatException.class);
    }

    @Test
    public void removedChimpVersionIsReportedAsAnUnsupportedFormat()
    {
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(payloadWithVersion((byte) 2), null))
            .isInstanceOf(UnsupportedChunkFormatException.class)
            .hasMessageContaining("known chunk format");
        assertThatThrownBy(() -> ColumnarChunkCodec.rowCount(payloadWithVersion((byte) 2)))
            .isInstanceOf(UnsupportedChunkFormatException.class);
    }

    @Test
    public void removedColumnarV3VersionIsReportedAsAnUnsupportedFormat()
    {
        assertThatThrownBy(() -> ColumnarChunkCodec.cursor(payloadWithVersion((byte) 3), null))
            .isInstanceOf(UnsupportedChunkFormatException.class)
            .hasMessageContaining("version: 3");
        assertThatThrownBy(() -> ColumnarChunkCodec.rowCount(payloadWithVersion((byte) 3)))
            .isInstanceOf(UnsupportedChunkFormatException.class);
    }

    @Test
    public void unrecognisedVersionIsReportedAsCorruption()
    {
        // A version byte naming no known format is indistinguishable from a header whose first byte
        // got scrambled, so it must stay a plain IllegalArgumentException -- the read path skips
        // those one at a time rather than failing the query.
        for (byte version : new byte[]{ 0, 5, 9, (byte) 0xFF })
        {
            assertThatThrownBy(() -> ColumnarChunkCodec.cursor(payloadWithVersion(version), null))
                .as("version " + version)
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(UnsupportedChunkFormatException.class);
            assertThatThrownBy(() -> ColumnarChunkCodec.rowCount(payloadWithVersion(version)))
                .as("version " + version)
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(UnsupportedChunkFormatException.class);
        }
    }

    private static ByteBuffer payloadWithVersion(byte version)
    {
        byte[] bytes = new byte[64];
        bytes[0] = version;
        return ByteBuffer.wrap(bytes);
    }
}
