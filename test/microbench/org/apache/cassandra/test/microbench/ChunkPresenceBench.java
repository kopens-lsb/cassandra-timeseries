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
import java.util.Arrays;
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

import org.apache.cassandra.db.timeseries.BlockPresence;

/**
 * The v4 presence layer: {@code BlockPresence.decode} for each of the four §4 modes at 1,024 rows,
 * plus the two scan strategies over an already-decoded word array.
 *
 * <p>The decode benches feed doc/timeseries/simd-decode-design.md §1 items 1 and 8: RLE expansion is
 * the design's top-ranked (non-SIMD) win and BITMAP/ALL_PRESENT/ALL_NULL are claimed already
 * optimal. The RLE pattern here carries ~84 transitions per 1,024 rows -- the RLE/BITMAP size
 * crossover the {@code BlockPresence} javadoc records -- i.e. the <em>worst realistic</em> RLE
 * block: any sparser pattern has fewer runs to walk, any denser one is stored as BITMAP.
 *
 * <p>The scan benches quantify the normative rule on {@code BlockPresence.rank} (design §1 item 2):
 * a sequential scan must carry a running value index incremented by the row's own presence bit and
 * never call {@code rank} per row. {@code rankPerRow} and {@code runningIndex} compute the same sum
 * of value indices over the same words, so their ratio is exactly the cost of ignoring the rule.
 *
 * <p>Allocation: all measured methods are allocation-free; decode writes into a setup-owned word
 * array, and the only per-invocation buffer mutation is the absolute {@code position(0)} reset.
 *
 * <p>Run: {@code ant microbench -Dbenchmark.name=ChunkPresenceBench}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
public class ChunkPresenceBench
{
    private static final int ROWS = 1024;

    @State(Scope.Benchmark)
    public static class DecodeState
    {
        @Param({ "ALL_PRESENT", "ALL_NULL", "BITMAP", "RLE" })
        String mode;

        int modeCode;
        ByteBuffer encoded;
        long[] dst;

        @Setup
        public void setup()
        {
            boolean[] present = new boolean[ROWS];
            switch (mode)
            {
                case "ALL_PRESENT":
                    Arrays.fill(present, true);
                    modeCode = BlockPresence.MODE_ALL_PRESENT;
                    break;
                case "ALL_NULL":
                    modeCode = BlockPresence.MODE_ALL_NULL;
                    break;
                case "BITMAP":
                {
                    // Random 50/50 presence: the shape RLE cannot compress, stored as raw words.
                    Random random = new Random(0xB17);
                    for (int i = 0; i < ROWS; i++)
                        present[i] = random.nextBoolean();
                    modeCode = BlockPresence.MODE_BITMAP;
                    break;
                }
                case "RLE":
                {
                    // ~84 transitions per 1,024 rows: an expected run length of 1024/85 = ~12 puts
                    // the pattern at the RLE/BITMAP crossover the BlockPresence javadoc records.
                    Random random = new Random(0x51E);
                    boolean current = true;
                    for (int i = 0; i < ROWS; i++)
                    {
                        present[i] = current;
                        if (random.nextInt(12) == 0)
                            current = !current;
                    }
                    modeCode = BlockPresence.MODE_RLE;
                    break;
                }
                default:
                    throw new IllegalStateException("unknown mode " + mode);
            }
            long[] words = BlockPresence.toWords(present, ROWS);
            encoded = ByteBuffer.wrap(BlockPresence.encode(modeCode, words, ROWS));
            dst = new long[BlockPresence.wordCount(ROWS)];
        }
    }

    /** One block's presence expanded to bit words -- what every block open pays, once. */
    @Benchmark
    public long[] decode(DecodeState state)
    {
        state.encoded.position(0);
        BlockPresence.decode(state.modeCode, state.encoded, ROWS, state.dst);
        return state.dst;
    }

    @State(Scope.Benchmark)
    public static class ScanState
    {
        long[] words;

        @Setup
        public void setup()
        {
            // Same 50/50 pattern as the BITMAP decode param: the worst case for rank, whose
            // bitCount work cannot be short-circuited by empty or full words.
            Random random = new Random(0xB17);
            boolean[] present = new boolean[ROWS];
            for (int i = 0; i < ROWS; i++)
                present[i] = random.nextBoolean();
            words = BlockPresence.toWords(present, ROWS);
        }
    }

    /**
     * The anti-pattern the {@code rank} javadoc forbids for sequential scans: O(row/64) bitCount
     * work per row, O(n^2/64) per block. Kept as a measurement, not an endorsement.
     */
    @Benchmark
    public long rankPerRow(ScanState state)
    {
        long sum = 0;
        for (int row = 0; row < ROWS; row++)
            sum += BlockPresence.rank(state.words, row);
        return sum;
    }

    /**
     * The normative scan: a running value index incremented by the row's own presence bit, which is
     * {@code rank(words, row + 1) - rank(words, row)} with no bitCount at all. Computes the same
     * sum as {@link #rankPerRow}, so the two reports are directly comparable.
     */
    @Benchmark
    public long runningIndex(ScanState state)
    {
        long sum = 0;
        int valueIndex = 0;
        for (int row = 0; row < ROWS; row++)
        {
            sum += valueIndex;
            valueIndex += (int) ((state.words[row >>> 6] >>> (row & 63)) & 1L);
        }
        return sum;
    }
}
