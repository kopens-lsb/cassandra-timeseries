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
 * Version-dispatching entry point over the single-column chunk codecs: peeks the version byte at
 * {@code payload.position()} and routes to {@link GorillaCodec} (version 1) or
 * {@link Chimp128Codec} (version 2). This is the sole codec entry point the chunk store is meant
 * to consume for those two formats -- callers should not invoke
 * {@code GorillaCodec}/{@code Chimp128Codec} directly.
 * Unrecognised version bytes throw {@link IllegalArgumentException}, matching the per-codec
 * behaviour these methods delegate to.
 * <p>
 * Version 3 ({@link ColumnarChunkCodec}, the many-columns-per-chunk format) is a different shape
 * entirely -- many named columns instead of one {@link SampleCursor}-style value stream -- so it
 * is out of scope for the methods here that assume a single value per timestamp. {@link #codecOf}
 * still recognises it (returning {@link Codec#COLUMNAR}) so callers can tell v1/v2 apart from v3
 * without depending on {@code ColumnarChunkCodec} directly, but {@link #cursor} rejects it with a
 * clear error pointing at {@link ColumnarChunkCodec#cursor} instead.
 * <p>
 * Payloads must span exactly one chunk (position..limit); corruption surfaces as
 * {@link IllegalArgumentException}, {@link IndexOutOfBoundsException} or
 * {@link java.nio.BufferUnderflowException} — callers must treat all three as a corrupt chunk.
 * Buffers are read big-endian and never mutated.
 */
public final class ChunkCodecs
{
    public static final int HEADER_SIZE = 21;
    public static final int MAX_SAMPLES = GorillaCodec.MAX_SAMPLES;

    private ChunkCodecs()
    {
    }

    public enum Codec
    {
        GORILLA, CHIMP128, COLUMNAR
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

    /**
     * Encodes {@code timestamps}/{@code values} with both codecs and returns whichever payload's
     * {@link ByteBuffer#remaining()} is smaller -- ties favor {@link Codec#GORILLA}.
     * <p>
     * This is the "auto" chunk-codec policy: it costs one extra encode per window (on the order of
     * milliseconds), which the bake-off in
     * docs/superpowers/specs/2026-07-31-chimp128-codec-design.md and doc/timeseries/codec-bakeoff.md
     * justifies -- chimp128 is roughly 3.2x smaller than gorilla on quantized data, but roughly 5x
     * larger on constant series, so neither codec is a safe static default across workloads.
     */
    public static ByteBuffer encodeSmallest(long[] timestamps, double[] values, int count)
    {
        ByteBuffer gorilla = GorillaCodec.encode(timestamps, values, count);
        ByteBuffer chimp = Chimp128Codec.encode(timestamps, values, count);
        return chimp.remaining() < gorilla.remaining() ? chimp : gorilla;
    }

    /**
     * Maps the version byte at {@code payload.position()} to the {@link Codec} that produced it,
     * without mutating {@code payload}. Throws {@link IllegalArgumentException} naming the byte if
     * it does not match a known codec version.
     */
    public static Codec codecOf(ByteBuffer payload)
    {
        byte version = versionByte(payload);
        switch (version)
        {
            case GorillaCodec.VERSION:
                return Codec.GORILLA;
            case Chimp128Codec.VERSION:
                return Codec.CHIMP128;
            case ColumnarChunkCodec.VERSION:
                return Codec.COLUMNAR;
            default:
                throw new IllegalArgumentException("Unsupported chunk codec version: " + version);
        }
    }

    public static SampleCursor cursor(ByteBuffer payload)
    {
        switch (versionByte(payload))
        {
            case GorillaCodec.VERSION:
                return GorillaCodec.cursor(payload);
            case Chimp128Codec.VERSION:
                return Chimp128Codec.cursor(payload);
            case ColumnarChunkCodec.VERSION:
                throw new IllegalArgumentException("ChunkCodecs.cursor() does not support columnar (v3) " +
                                                   "payloads -- use ColumnarChunkCodec.cursor() instead");
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
