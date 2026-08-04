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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.timeseries.AlpBlockCodec;
import org.apache.cassandra.db.timeseries.BitPacking;
import org.apache.cassandra.db.timeseries.BlockEncodings;
import org.apache.cassandra.db.timeseries.BlockPresence;
import org.apache.cassandra.db.timeseries.ChunkV4BlockTable;
import org.apache.cassandra.db.timeseries.ChunkV4Directory;
import org.apache.cassandra.db.timeseries.ExceptionArea;

/**
 * <b>The denominator bench</b> for doc/timeseries/simd-decode-design.md §5: every component of one
 * v4 block decode, isolated on the same two fixed blocks, so that the decision rule
 *
 * <pre>  (unpackOnly + alpReconstructOnly) / wholeBlockDouble &gt;= 15%  =&gt;  reconsider vector kernels</pre>
 *
 * is directly computable from one JMH report. A bench that fused unpacking with reconstruction
 * could not answer that question, so nothing here fuses anything.
 *
 * <p>The two blocks, fixed at setup with pinned seeds:
 * <ul>
 * <li><b>double block</b>: 1,024 rows of the 2-decimal sensor walk from
 *     {@code DoubleBlockCodecTest}'s distribution vocabulary, ~1% random nulls, and four
 *     full-precision gaussians injected so the ALP payload carries a real exception area. The
 *     argmin is required (asserted) to pick {@code 0x20 ALP} for it.</li>
 * <li><b>timestamp block</b>: 1,024 {@code INT64} values at 1 s cadence with a single 30 s gap, all
 *     present. The argmin is required (asserted) to pick {@code 0x11 DELTA_FOR_BITPACK} with
 *     exactly the one gap exception -- width 0, so its decode cost is the prefix sum, which is the
 *     honest shape of a production timestamp block.</li>
 * </ul>
 *
 * <p>Component map (all on the double block unless said otherwise):
 * <ul>
 * <li>{@link #presenceOnly} -- {@code BlockPresence.decode} of the block's own presence bytes
 *     (~1% nulls encodes as RLE under the exact-size argmin).</li>
 * <li>{@link #unpackOnly} -- {@code BitPacking.unpack} of the ALP payload's lane, at the width the
 *     encoder actually chose (the width sweep lives in {@code ChunkBitUnpackBench}).</li>
 * <li>{@link #forReconstructOnly} -- the FOR-shaped reconstruction: one add of the frame of
 *     reference per value over an already-unpacked {@code long[]}.</li>
 * <li>{@link #alpReconstructOnly} -- the ALP reconstruction over the already-unpacked lane:
 *     reference add, {@code AlpCodec.decodeOne}'s two-multiply scaled-integer math,
 *     {@code doubleToRawLongBits}, and the exception scatter. {@code AlpCodec} is package-private,
 *     so the two-multiply is replicated here verbatim and <em>verified bit-for-bit at setup</em>
 *     against the real {@code AlpBlockCodec} decode of the same payload -- if the replica ever
 *     drifts, setup throws and the bench refuses to run.</li>
 * <li>{@link #wholeBlockDouble} / {@link #wholeBlockTimestamp} -- {@code BlockEncodings.decodeFixed}
 *     end to end: the denominators.</li>
 * </ul>
 *
 * <p>Allocation: every measured method is allocation-free -- payload buffers, lanes, scratch and
 * destinations are setup state ({@code BlockEncodings.Scratch} preallocates in its constructor, and
 * the {@code AlpBlockCodec} thread-local scratch is pre-warmed by the setup verification decode on
 * the worker thread). The only per-invocation buffer mutation is the absolute position reset.
 *
 * <p>Run: {@code ant microbench -Dbenchmark.name=ChunkBlockDecodeBench}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
@SuppressWarnings("strictfp")   // redundant from Java 17 and javac says so; kept to mirror AlpCodec
public class ChunkBlockDecodeBench
{
    private static final int ROWS = 1024;

    /** {@code exponent u8 | factor u8 | width u8 | flags u8 | pad u32 | reference i64} -- AlpBlockCodec.ALP_HEADER_BYTES. */
    private static final int ALP_HEADER_BYTES = 16;

    /**
     * AlpCodec's POW10/INV_POW10 tables, replicated because that class is package-private. The
     * decimal literals have exactly one legal double value each, and the setup verification pins
     * the whole reconstruction against the real decoder, so a drift here fails loudly.
     */
    private static final double[] POW10 =
    {
        1.0E0, 1.0E1, 1.0E2, 1.0E3, 1.0E4, 1.0E5, 1.0E6, 1.0E7, 1.0E8, 1.0E9,
        1.0E10, 1.0E11, 1.0E12, 1.0E13, 1.0E14, 1.0E15, 1.0E16, 1.0E17, 1.0E18
    };
    private static final double[] INV_POW10 =
    {
        1.0E0, 1.0E-1, 1.0E-2, 1.0E-3, 1.0E-4, 1.0E-5, 1.0E-6, 1.0E-7, 1.0E-8, 1.0E-9,
        1.0E-10, 1.0E-11, 1.0E-12, 1.0E-13, 1.0E-14, 1.0E-15, 1.0E-16, 1.0E-17, 1.0E-18
    };

    // -- double block --------------------------------------------------------------------------
    private int doubleCount;                 // present values; the lane length
    private long doubleMin;                  // block-table min under IEEE754 total order
    private int doubleEncoding;              // asserted ENC_ALP
    private int doublePayloadLength;
    private ByteBuffer doublePayload;
    private int doublePresenceMode;
    private ByteBuffer doublePresenceBytes;

    // parsed out of the ALP payload at setup
    private int alpExponent;
    private int alpFactor;
    private int alpWidth;
    private long alpReference;
    private long[] alpLane;                  // the unpacked lane, pristine (never mutated)
    private int alpExceptionCount;
    private int[] alpExceptionPositions;
    private long[] alpExceptionValues;

    // -- timestamp block -----------------------------------------------------------------------
    private long tsMin;
    private int tsEncoding;                  // asserted ENC_DELTA_FOR_BITPACK
    private boolean tsHasExceptions;
    private int tsPayloadLength;
    private ByteBuffer tsPayload;

    // -- shared --------------------------------------------------------------------------------
    private BlockEncodings.Scratch scratch;
    private long[] dst;
    private long[] presenceDst;

    @Setup
    public void setup()
    {
        scratch = new BlockEncodings.Scratch(ROWS);
        dst = new long[ROWS];
        presenceDst = new long[BlockPresence.wordCount(ROWS)];
        setupDoubleBlock();
        setupTimestampBlock();
        verifyAlpReplica();
    }

    private void setupDoubleBlock()
    {
        // ~1% random nulls, fixed seed.
        Random nulls = new Random(42);
        boolean[] present = new boolean[ROWS];
        int count = 0;
        for (int i = 0; i < ROWS; i++)
        {
            present[i] = nulls.nextInt(100) != 0;
            if (present[i])
                count++;
        }
        doubleCount = count;

        // The 2-decimal sensor walk of DoubleBlockCodecTest, over the dense (present) values.
        Random walk = new Random(7);
        double[] steps = { -0.01, 0.0, 0.0, 0.01 };
        double value = 10.0 + walk.nextDouble() * 80.0;
        long[] dense = new long[count];
        for (int i = 0; i < count; i++)
        {
            value = Math.min(100.0, Math.max(0.0, value + steps[walk.nextInt(4)]));
            dense[i] = Double.doubleToRawLongBits(Math.round(value * 100.0) / 100.0);
        }
        // A few full-precision values ALP cannot represent at any (e, f): the exception path.
        Random full = new Random(99);
        int[] at = { 97, 397, 601, 907 };
        for (int index : at)
            if (index < count)
                dense[index] = Double.doubleToRawLongBits(50.0 + full.nextGaussian());

        long min = dense[0];
        for (int i = 1; i < count; i++)
            if (BlockEncodings.compareValues(dense[i], min, ChunkV4Directory.TYPE_DOUBLE) < 0)
                min = dense[i];
        doubleMin = min;

        // Presence, encoded under the exact-size argmin (~1% nulls => RLE at this shape).
        long[] words = BlockPresence.toWords(present, ROWS);
        doublePresenceMode = BlockPresence.chooseMode(words, ROWS);
        doublePresenceBytes = ByteBuffer.wrap(BlockPresence.encode(doublePresenceMode, words, ROWS));

        // The §5 argmin, then the payload it scored.
        BlockEncodings.Choice choice = new BlockEncodings.Choice();
        BlockEncodings.chooseFixed(ChunkV4Directory.TYPE_DOUBLE, dense, count, min, null,
                                   AlpBlockCodec.INSTANCE, scratch, choice);
        if (choice.encoding != ChunkV4BlockTable.ENC_ALP)
            throw new IllegalStateException("the 2-decimal walk no longer selects ALP but 0x" +
                                            Integer.toHexString(choice.encoding) +
                                            "; the component benches would not measure the ALP path");
        doubleEncoding = choice.encoding;
        doublePayloadLength = choice.payloadLength;
        doublePayload = ByteBuffer.allocate(choice.payloadLength);
        BlockEncodings.encodeFixed(choice, ChunkV4Directory.TYPE_DOUBLE, dense, count, min,
                                   AlpBlockCodec.INSTANCE, scratch, doublePayload);
        doublePayload.position(0);

        // Parse the ALP payload once: header, lane, exception area.
        alpExponent = doublePayload.get(0) & 0xFF;
        alpFactor = doublePayload.get(1) & 0xFF;
        alpWidth = doublePayload.get(2) & 0xFF;
        int flags = doublePayload.get(3) & 0xFF;
        alpReference = doublePayload.getLong(8);
        if ((flags & 0x01) == 0)
            throw new IllegalStateException("the injected gaussians produced no ALP exception area; " +
                                            "alpReconstructOnly would not measure the scatter");
        alpLane = new long[count];
        ByteBuffer lane = doublePayload.duplicate();
        lane.position(ALP_HEADER_BYTES);
        BitPacking.unpack(lane, count, alpWidth, alpLane);
        alpExceptionPositions = new int[count];
        alpExceptionValues = new long[count];
        alpExceptionCount = ExceptionArea.decode(lane, ExceptionArea.valueBytes(ChunkV4Directory.TYPE_DOUBLE),
                                                 count, alpExceptionPositions, alpExceptionValues);
    }

    private void setupTimestampBlock()
    {
        // 1 s cadence with one 30 s gap: deltas are a constant 1,000 except one 31,000, so the
        // argmin lands on DELTA_FOR_BITPACK at width 0 with exactly one exception.
        long[] timestamps = new long[ROWS];
        long base = 1_700_000_000_000L;
        for (int i = 0; i < ROWS; i++)
            timestamps[i] = base + i * 1000L + (i >= 600 ? 30_000L : 0L);
        tsMin = timestamps[0];

        BlockEncodings.Choice choice = new BlockEncodings.Choice();
        BlockEncodings.chooseFixed(ChunkV4Directory.TYPE_INT64, timestamps, ROWS, tsMin, null,
                                   AlpBlockCodec.INSTANCE, scratch, choice);
        if (choice.encoding != ChunkV4BlockTable.ENC_DELTA_FOR_BITPACK || choice.exceptionCount != 1)
            throw new IllegalStateException("the gapped cadence no longer selects DELTA_FOR_BITPACK with one " +
                                            "exception: encoding 0x" + Integer.toHexString(choice.encoding) +
                                            ", " + choice.exceptionCount + " exceptions");
        tsEncoding = choice.encoding;
        tsHasExceptions = choice.hasExceptions();
        tsPayloadLength = choice.payloadLength;
        tsPayload = ByteBuffer.allocate(choice.payloadLength);
        BlockEncodings.encodeFixed(choice, ChunkV4Directory.TYPE_INT64, timestamps, ROWS, tsMin,
                                   AlpBlockCodec.INSTANCE, scratch, tsPayload);
        tsPayload.position(0);
    }

    /**
     * Pins {@link #alpReconstruct} to the real decoder, bit for bit, and pre-warms the
     * {@code AlpBlockCodec} thread-local scratch on the JMH worker thread so the steady state is
     * what gets measured.
     */
    private void verifyAlpReplica()
    {
        long[] expected = new long[ROWS];
        doublePayload.position(0);
        BlockEncodings.decodeFixed(doubleEncoding, false, doublePayload, doublePayloadLength,
                                   ChunkV4Directory.TYPE_DOUBLE, doubleCount, doubleMin, null,
                                   AlpBlockCodec.INSTANCE, scratch, expected);
        doublePayload.position(0);
        long[] check = new long[ROWS];
        alpReconstruct(check);
        for (int i = 0; i < doubleCount; i++)
            if (expected[i] != check[i])
                throw new IllegalStateException("ALP reconstruction replica diverges from AlpBlockCodec at value " +
                                                i + ": 0x" + Long.toHexString(expected[i]) + " != 0x" +
                                                Long.toHexString(check[i]));
    }

    // -----------------------------------------------------------------------------------------
    // components
    // -----------------------------------------------------------------------------------------

    /** Presence expansion for the double block (~1% nulls => RLE under the argmin). */
    @Benchmark
    public long[] presenceOnly()
    {
        doublePresenceBytes.position(0);
        BlockPresence.decode(doublePresenceMode, doublePresenceBytes, ROWS, presenceDst);
        return presenceDst;
    }

    /** The ALP payload's lane unpack alone, at the width the encoder chose for this block. */
    @Benchmark
    public long[] unpackOnly()
    {
        doublePayload.position(ALP_HEADER_BYTES);
        BitPacking.unpack(doublePayload, doubleCount, alpWidth, dst);
        return dst;
    }

    /**
     * FOR-shaped reconstruction over an already-unpacked lane: one frame-of-reference add per
     * value, exactly the loop {@code decodeFixed}'s FOR_BITPACK branch runs after its unpack.
     */
    @Benchmark
    public long[] forReconstructOnly()
    {
        long reference = alpReference;
        long[] lane = alpLane;
        for (int i = 0; i < doubleCount; i++)
            dst[i] = lane[i] + reference;
        return dst;
    }

    /**
     * ALP reconstruction over the already-unpacked lane, exception scatter included -- the
     * "ALP math" term of the design §5 decision rule. Verified against the real decoder at setup.
     */
    @Benchmark
    public long[] alpReconstructOnly()
    {
        alpReconstruct(dst);
        return dst;
    }

    private strictfp void alpReconstruct(long[] out)
    {
        int e = alpExponent;
        int f = alpFactor;
        long reference = alpReference;
        long[] lane = alpLane;
        int exceptions = alpExceptionCount;
        int next = 0;
        for (int i = 0; i < doubleCount; i++)
        {
            if (next < exceptions && alpExceptionPositions[next] == i)
            {
                out[i] = alpExceptionValues[next++];
                continue;
            }
            // AlpCodec.decodeOne, verbatim: encoded * 10^f * 10^-e over the exact tables.
            double decoded = (reference + lane[i]) * POW10[f] * INV_POW10[e];
            out[i] = Double.doubleToRawLongBits(decoded);
        }
    }

    // -----------------------------------------------------------------------------------------
    // denominators
    // -----------------------------------------------------------------------------------------

    /** The whole double-block payload decode, end to end: the §5 denominator. */
    @Benchmark
    public long[] wholeBlockDouble()
    {
        doublePayload.position(0);
        BlockEncodings.decodeFixed(doubleEncoding, false, doublePayload, doublePayloadLength,
                                   ChunkV4Directory.TYPE_DOUBLE, doubleCount, doubleMin, null,
                                   AlpBlockCodec.INSTANCE, scratch, dst);
        return dst;
    }

    /** The whole timestamp-block payload decode: width-0 lane, prefix sum, one gap exception. */
    @Benchmark
    public long[] wholeBlockTimestamp()
    {
        tsPayload.position(0);
        BlockEncodings.decodeFixed(tsEncoding, tsHasExceptions, tsPayload, tsPayloadLength,
                                   ChunkV4Directory.TYPE_INT64, ROWS, tsMin, null,
                                   AlpBlockCodec.INSTANCE, scratch, dst);
        return dst;
    }
}
