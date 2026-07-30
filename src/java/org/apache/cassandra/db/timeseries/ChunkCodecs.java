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
 * Version-dispatching entry point over the chunk codecs: peeks the version byte at
 * {@code payload.position()} and routes to {@link GorillaCodec} (version 1) or
 * {@link Chimp128Codec} (version 2). This is the sole codec entry point the chunk store is meant
 * to consume -- callers should not invoke {@code GorillaCodec}/{@code Chimp128Codec} directly.
 * Unrecognised version bytes throw {@link IllegalArgumentException}, matching the per-codec
 * behaviour these methods delegate to.
 * <p>
 * Payloads must span exactly one chunk (position..limit); corruption surfaces as
 * {@link IllegalArgumentException}, {@link IndexOutOfBoundsException} or
 * {@link java.nio.BufferUnderflowException} — callers must treat all three as a corrupt chunk.
 * Buffers are read big-endian and never mutated.
 */
public final class ChunkCodecs
{
    private ChunkCodecs()
    {
    }

    public enum Codec
    {
        GORILLA, CHIMP128
    }

    public static ByteBuffer encode(Codec codec, long[] timestamps, double[] values, int count)
    {
        switch (codec)
        {
            case GORILLA:
                return GorillaCodec.encode(timestamps, values, count);
            case CHIMP128:
                return Chimp128Codec.encode(timestamps, values, count);
            default:
                throw new AssertionError("Unhandled codec: " + codec);
        }
    }

    public static GorillaCodec.SampleCursor cursor(ByteBuffer payload)
    {
        switch (versionByte(payload))
        {
            case GorillaCodec.VERSION:
                return GorillaCodec.cursor(payload);
            case Chimp128Codec.VERSION:
                return Chimp128Codec.cursor(payload);
            default:
                throw new IllegalArgumentException("Unsupported chunk codec version: " + versionByte(payload));
        }
    }

    public static int sampleCount(ByteBuffer payload)
    {
        switch (versionByte(payload))
        {
            case GorillaCodec.VERSION:
                return GorillaCodec.sampleCount(payload);
            case Chimp128Codec.VERSION:
                return Chimp128Codec.sampleCount(payload);
            default:
                throw new IllegalArgumentException("Unsupported chunk codec version: " + versionByte(payload));
        }
    }

    public static long firstTimestamp(ByteBuffer payload)
    {
        switch (versionByte(payload))
        {
            case GorillaCodec.VERSION:
                return GorillaCodec.firstTimestamp(payload);
            case Chimp128Codec.VERSION:
                return Chimp128Codec.firstTimestamp(payload);
            default:
                throw new IllegalArgumentException("Unsupported chunk codec version: " + versionByte(payload));
        }
    }

    public static long lastTimestamp(ByteBuffer payload)
    {
        switch (versionByte(payload))
        {
            case GorillaCodec.VERSION:
                return GorillaCodec.lastTimestamp(payload);
            case Chimp128Codec.VERSION:
                return Chimp128Codec.lastTimestamp(payload);
            default:
                throw new IllegalArgumentException("Unsupported chunk codec version: " + versionByte(payload));
        }
    }

    /** Byte-order-independent peek at the version byte, matching each codec's own header peeks. */
    private static byte versionByte(ByteBuffer payload)
    {
        ByteBuffer buffer = payload.duplicate();
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.get(buffer.position());
    }
}
