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
import java.nio.ByteOrder;

/**
 * Version-checking entry point over the single-column chunk format: peeks the version byte at
 * {@code payload.position()} and routes to {@link Chimp128Codec} (version 2), the only
 * single-column codec. This is the sole entry point the chunk store is meant to consume for that
 * format -- callers should not invoke {@code Chimp128Codec} directly. Unrecognised version bytes
 * throw {@link IllegalArgumentException}, matching the per-codec behaviour these methods delegate
 * to; version 1 (the removed gorilla codec) is just another unrecognised version.
 * <p>
 * Version 3 ({@link ColumnarChunkCodec}, the many-columns-per-chunk format) is a different shape
 * entirely -- many named columns instead of one {@link SampleCursor}-style value stream -- so it is
 * out of scope for the methods here, which assume a single value per timestamp. {@link #cursor}
 * rejects it with a clear error pointing at {@link ColumnarChunkCodec#cursor} instead.
 * <p>
 * Payloads must span exactly one chunk (position..limit); corruption surfaces as
 * {@link IllegalArgumentException}, {@link IndexOutOfBoundsException} or
 * {@link java.nio.BufferUnderflowException} — callers must treat all three as a corrupt chunk.
 * Buffers are read big-endian and never mutated.
 */
public final class ChunkCodecs
{
    public static final int HEADER_SIZE = 21;
    public static final int MAX_SAMPLES = Chimp128Codec.MAX_SAMPLES;

    /**
     * Version byte 1 was the gorilla single-column format, removed when chimp128 became the only
     * chunk codec. Never reassigned: it is recognised here purely so a chunk written by that build
     * is reported as an unreadable format rather than as random corruption.
     */
    static final byte GORILLA_VERSION_REMOVED = 1;

    private ChunkCodecs()
    {
    }

    public static ByteBuffer encode(long[] timestamps, double[] values, int count)
    {
        return Chimp128Codec.encode(timestamps, values, count);
    }

    public static SampleCursor cursor(ByteBuffer payload)
    {
        switch (versionByte(payload))
        {
            case Chimp128Codec.VERSION:
                return Chimp128Codec.cursor(payload);
            case ColumnarChunkCodec.VERSION:
                throw new UnsupportedChunkFormatException("ChunkCodecs.cursor() does not support columnar (v3) " +
                                                          "payloads -- use ColumnarChunkCodec.cursor() instead");
            default:
                throw unsupportedVersion(versionByte(payload), "chunk codec");
        }
    }

    public static int sampleCount(ByteBuffer payload)
    {
        return Chimp128Codec.sampleCount(payload);
    }

    public static long firstTimestamp(ByteBuffer payload)
    {
        return Chimp128Codec.firstTimestamp(payload);
    }

    public static long lastTimestamp(ByteBuffer payload)
    {
        return Chimp128Codec.lastTimestamp(payload);
    }

    /**
     * Builds the exception for a version byte this decoder will not read, choosing its <em>type</em>
     * by whether the byte names a format at all.
     * <p>
     * A byte naming a known format (1, 2, 3) means the payload is real and this build cannot decode
     * it -- systematic, affecting every chunk written that way, so it is an
     * {@link UnsupportedChunkFormatException} that callers must not swallow. A byte naming nothing
     * is exactly what a header corrupted in its first byte looks like, so it stays a plain
     * {@link IllegalArgumentException} and the read path keeps skipping it as one bad chunk. There
     * is no way to tell a scrambled byte from a hypothetical future version, and the availability
     * trade-off SP3 chose for corruption is the right default for the ambiguous case.
     */
    static RuntimeException unsupportedVersion(byte version, String what)
    {
        if (version == GORILLA_VERSION_REMOVED)
            return new UnsupportedChunkFormatException(
                "Unsupported " + what + " version: 1 -- version 1 was the gorilla single-column format, removed " +
                "when chimp128 became the only chunk codec; chunks written by that build cannot be read by this one");
        if (version == Chimp128Codec.VERSION || version == ColumnarChunkCodec.VERSION)
            return new UnsupportedChunkFormatException(
                "Unsupported " + what + " version: " + version + " -- that is a known chunk format, but not one " +
                "this decoder reads");
        return new IllegalArgumentException("Unsupported " + what + " version: " + version +
                                            " -- names no known chunk format (treated as a corrupt payload)");
    }

    /** Byte-order-independent peek at the version byte, matching each codec's own header peeks. */
    private static byte versionByte(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.get(buffer.position());
    }
}
