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
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
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

import org.apache.cassandra.db.timeseries.BlockEncodings;
import org.apache.cassandra.db.timeseries.ChunkV4Codec;
import org.apache.cassandra.db.timeseries.ChunkV4Directory;

/**
 * {@code ChunkV4Codec.encode} of a production-shaped 3,600-row chunk: the chunk-format-v4 §12
 * re-encoder gate, unmeasured since {@code AlpBlockCodec} put three planning passes (the (e, f)
 * candidate search plus decimal-ALP and ALP-RD exact measurement) in front of every double block.
 *
 * <p><b>The gate: re-encode throughput must be &ge; 50,000 rows/s.</b> Scores here are ops/s
 * (one op = one 3,600-row chunk), so
 *
 * <pre>  rows/s = score(ops/s) * 3600     -- the gate passes at score &ge; 13.9 ops/s</pre>
 *
 * <p>{@link #encodeProduction} is the mixed chunk {@code ChunkReadBench} reads (2 constant text, a
 * 2-decimal ALP walk, a near-constant setpoint, mostly-constant ints, a boolean, a partially-null
 * double, the axis). {@link #encodeDoubleHeavy} replaces every value column with an independent
 * 2-decimal double walk -- 8 double columns of ALP planning per chunk -- to isolate the triple
 * planning cost: if production passes the gate and double-heavy fails it, the ALP planning passes
 * are the reason, and sampling/caching work targets them.
 *
 * <p>All inputs are prebuilt at setup with fixed seeds; the measured method is the encode alone.
 * The encoder allocates by design (planning scratch, section buffers, the output chunk); do not
 * read GC noise here as harness noise -- it is the cost being measured. Per the
 * doc/timeseries/simd-decode-design.md §3 rule, the encode path stays single-implementation scalar
 * forever, so this bench gates algorithmic work (planning-pass reduction), never vectorization.
 *
 * <p>Run: {@code ant microbench -Dbenchmark.name=ChunkEncodeBench}
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class ChunkEncodeBench
{
    private static final int ROWS = 3600;

    private long[] timestamps;
    private SortedMap<String, ChunkV4Codec.ColumnInput> production;
    private SortedMap<String, ChunkV4Codec.ColumnInput> doubleHeavy;

    @Setup
    public void setup()
    {
        timestamps = new long[ROWS];
        long base = 1_700_000_000_000L;
        for (int i = 0; i < ROWS; i++)
            timestamps[i] = base + i * 1000L;

        production = new TreeMap<>();
        production.put("tag", text(constantText("compressor-7")));
        production.put("site", text(constantText("plant-A")));
        production.put("value", doubles(sensorWalkTwoDecimals(7, false)));
        production.put("value2", doubles(nearConstant()));
        production.put("status", longs(mostlyConstantStatus()));
        production.put("flag", booleans());
        production.put("aux", doubles(sensorWalkTwoDecimals(11, true)));

        doubleHeavy = new TreeMap<>();
        for (int c = 0; c < 8; c++)
            doubleHeavy.put("v" + c, doubles(sensorWalkTwoDecimals(7 + c, false)));

        // Fail fast at setup rather than measuring a rejected input.
        ChunkV4Codec.encode(timestamps, ROWS, production);
        ChunkV4Codec.encode(timestamps, ROWS, doubleHeavy);
    }

    /** The mixed production chunk. rows/s = score * 3600; §12 gate at score >= 13.9. */
    @Benchmark
    public ByteBuffer encodeProduction()
    {
        return ChunkV4Codec.encode(timestamps, ROWS, production);
    }

    /** Eight ALP-planned double columns: the triple-planning cost, isolated. rows/s = score * 3600. */
    @Benchmark
    public ByteBuffer encodeDoubleHeavy()
    {
        return ChunkV4Codec.encode(timestamps, ROWS, doubleHeavy);
    }

    // -----------------------------------------------------------------------------------------
    // production-shaped inputs (distribution shapes from DoubleBlockCodecTest), fixed seeds
    // -----------------------------------------------------------------------------------------

    private static ChunkV4Codec.ColumnInput text(ByteBuffer[] values)
    {
        return input(ChunkV4Directory.TYPE_TEXT, values);
    }

    private static ChunkV4Codec.ColumnInput doubles(ByteBuffer[] values)
    {
        return input(ChunkV4Directory.TYPE_DOUBLE, values);
    }

    private static ChunkV4Codec.ColumnInput longs(ByteBuffer[] values)
    {
        return input(ChunkV4Directory.TYPE_INT64, values);
    }

    private static ChunkV4Codec.ColumnInput input(int typeCode, ByteBuffer[] values)
    {
        return new ChunkV4Codec.ColumnInput(typeCode, ChunkV4Codec.canonicalStatOrder(typeCode), values);
    }

    private static ByteBuffer[] constantText(String value)
    {
        ByteBuffer bytes = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
            out[i] = bytes;
        return out;
    }

    private static ByteBuffer[] sensorWalkTwoDecimals(long seed, boolean partiallyNull)
    {
        Random random = new Random(seed);
        double[] steps = { -0.01, 0.0, 0.0, 0.01 };
        double value = 10.0 + random.nextDouble() * 80.0;
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
        {
            value = Math.min(100.0, Math.max(0.0, value + steps[random.nextInt(4)]));
            out[i] = partiallyNull && i % 7 == 3 ? null : doubleBytes(Math.round(value * 100.0) / 100.0);
        }
        return out;
    }

    private static ByteBuffer[] nearConstant()
    {
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
            out[i] = doubleBytes(i % 997 == 0 ? 192.5 : 192.0);
        return out;
    }

    private static ByteBuffer[] mostlyConstantStatus()
    {
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
            out[i] = longBytes(i % 211 == 0 ? 3L : 0L);
        return out;
    }

    private static ChunkV4Codec.ColumnInput booleans()
    {
        ByteBuffer[] out = new ByteBuffer[ROWS];
        for (int i = 0; i < ROWS; i++)
            out[i] = ByteBuffer.wrap(BlockEncodings.toFixedBytes(i % 97 == 0 ? 0L : 1L,
                                                                 ChunkV4Directory.TYPE_BOOLEAN));
        return input(ChunkV4Directory.TYPE_BOOLEAN, out);
    }

    private static ByteBuffer doubleBytes(double value)
    {
        return ByteBuffer.wrap(BlockEncodings.toFixedBytes(Double.doubleToRawLongBits(value),
                                                           ChunkV4Directory.TYPE_DOUBLE));
    }

    private static ByteBuffer longBytes(long value)
    {
        return ByteBuffer.wrap(BlockEncodings.toFixedBytes(value, ChunkV4Directory.TYPE_INT64));
    }
}
