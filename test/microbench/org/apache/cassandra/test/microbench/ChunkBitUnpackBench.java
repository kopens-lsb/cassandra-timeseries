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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.timeseries.BitPacking;

/**
 * The v4 lane primitive in isolation: {@code BitPacking.unpack(ByteBuffer, count, width, long[])}
 * over one 1,024-value lane, swept across the widths that matter.
 *
 * <p>This is the baseline of doc/timeseries/simd-decode-design.md gate A: any future
 * width-specialized scalar kernel (design §1 item 3) or vector kernel (§1 item 4, gated at &ge; 2.0x
 * against the <em>specialized scalar</em>, not against this loop) is measured against these numbers.
 * The current loop is branchy and loop-carried (design §1.3), so it is not auto-vectorized; this
 * bench pins down what that costs per width so the specialized kernels have an honest denominator.
 *
 * <p>Width notes: 1 is the boolean lane, 2/4/7/8 are common FOR residual widths, 11-20 cover the
 * 2-decimal ALP walks seen in production, 32 is a worst-ish case straddling every other word, 55 is
 * {@code AlpCodec.MAX_BIT_WIDTH}, 64 is the verbatim lane (pure 8-byte copies).
 *
 * <p>Allocation: the measured method allocates nothing -- the lane bytes, the destination array and
 * the {@code ByteBuffer} are all setup state, and the only per-invocation mutation is the absolute
 * {@code position(0)} reset that undoes {@code unpack}'s position advance.
 *
 * <p>Run: {@code ant microbench -Dbenchmark.name=ChunkBitUnpackBench}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class ChunkBitUnpackBench
{
    /** §7's v4.0 default block size: the lane length every production block decode uses. */
    private static final int COUNT = 1024;

    @Param({ "1", "2", "4", "7", "8", "11", "16", "20", "32", "55", "64" })
    private int width;

    private ByteBuffer packed;
    private long[] dst;

    @Setup
    public void setup()
    {
        Random random = new Random(0xC0DEC + width);   // fixed seed, distinct stream per width
        long mask = width == 64 ? -1L : (1L << width) - 1;
        long[] values = new long[COUNT];
        for (int i = 0; i < COUNT; i++)
            values[i] = random.nextLong() & mask;
        // ByteBuffer.wrap is big-endian by default, which BitPacking requires.
        packed = ByteBuffer.wrap(BitPacking.pack(values, COUNT, width));
        dst = new long[COUNT];
    }

    /** One 1,024-value lane unpack; per-value ns = result / 1024 * 1000. */
    @Benchmark
    public long[] unpack()
    {
        packed.position(0);
        BitPacking.unpack(packed, COUNT, width, dst);
        return dst;    // returned so the JIT cannot treat the stores as dead
    }
}
