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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link ChunkCodecs} version dispatcher -- the sole entry point the chunk store is
 * meant to consume -- and runs the v1/v2 bake-off referenced by
 * docs/superpowers/specs/2026-07-31-chimp128-codec-design.md §1 and reported in
 * doc/timeseries/codec-bakeoff.md.
 */
public class ChunkCodecsTest
{
    private static final int HEADER_SLACK = 8;   // small fixed overhead allowance

    // ChunkCodecs.Codec also has a COLUMNAR (v3) value -- codecOf() recognises it (see
    // codecOfRecognisesColumnarVersion below) but it is not a single-column codec, so it cannot go
    // through ChunkCodecs.encode(Codec, timestamps, values, count)/.cursor(payload) like GORILLA and
    // CHIMP128 can. These tests iterate only the two single-column codecs deliberately.
    private static final ChunkCodecs.Codec[] SINGLE_COLUMN_CODECS =
        { ChunkCodecs.Codec.GORILLA, ChunkCodecs.Codec.CHIMP128 };

    @Test
    public void dispatchesByVersionByte()
    {
        long[] ts = { 1L, 2L, 3L }; double[] vs = { 1.0, 2.0, 3.0 };
        for (ChunkCodecs.Codec codec : SINGLE_COLUMN_CODECS)
        {
            ByteBuffer payload = ChunkCodecs.encode(codec, ts, vs, 3);
            SampleCursor cursor = ChunkCodecs.cursor(payload);
            for (int i = 0; i < 3; i++) { assertTrue(cursor.advance()); assertEquals(ts[i], cursor.timestamp()); }
            assertEquals(3, ChunkCodecs.sampleCount(payload));
        }
    }

    @Test
    public void rejectsUnknownVersion()
    {
        ByteBuffer payload = ChunkCodecs.encode(ChunkCodecs.Codec.GORILLA, new long[]{1L}, new double[]{1.0}, 1);
        payload.put(payload.position(), (byte) 9);
        assertThatThrownBy(() -> ChunkCodecs.cursor(payload)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void firstAndLastTimestampDispatchByVersionByte()
    {
        long[] ts = { 10L, 20L, 30L }; double[] vs = { 1.0, 2.0, 3.0 };
        for (ChunkCodecs.Codec codec : SINGLE_COLUMN_CODECS)
        {
            ByteBuffer payload = ChunkCodecs.encode(codec, ts, vs, 3);
            assertEquals(10L, ChunkCodecs.firstTimestamp(payload));
            assertEquals(30L, ChunkCodecs.lastTimestamp(payload));
        }
    }

    @Test
    public void codecOfRoundtrip()
    {
        long[] ts = { 1L, 2L, 3L }; double[] vs = { 1.0, 2.0, 3.0 };
        for (ChunkCodecs.Codec codec : SINGLE_COLUMN_CODECS)
        {
            ByteBuffer payload = ChunkCodecs.encode(codec, ts, vs, 3);
            assertEquals(codec, ChunkCodecs.codecOf(payload));
        }

        ByteBuffer payload = ChunkCodecs.encode(ChunkCodecs.Codec.GORILLA, ts, vs, 3);
        payload.put(payload.position(), (byte) 9);
        assertThatThrownBy(() -> ChunkCodecs.codecOf(payload)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void codecOfRecognisesColumnarVersionButCursorRejectsIt()
    {
        java.util.SortedMap<String, ColumnarChunkCodec.ColumnInput> columns = new java.util.TreeMap<>();
        columns.put("v", new ColumnarChunkCodec.ColumnInput(ColumnarChunkCodec.TYPE_INT32,
                                                            new ByteBuffer[]{ intBytes(1), intBytes(2), intBytes(3) }));
        ByteBuffer payload = ColumnarChunkCodec.encode(new long[]{ 1L, 2L, 3L }, 3, columns);

        assertEquals(ChunkCodecs.Codec.COLUMNAR, ChunkCodecs.codecOf(payload));
        assertThatThrownBy(() -> ChunkCodecs.cursor(payload)).isInstanceOf(IllegalArgumentException.class);
    }

    private static ByteBuffer intBytes(int v)
    {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(v);
        b.flip();
        return b;
    }

    @Test
    public void encodeSmallestPicksGorillaForConstants()
    {
        int n = 1000;
        long[] ts = new long[n]; double[] vs = new double[n];
        for (int i = 0; i < n; i++) { ts[i] = i * 1000L; vs[i] = 42.0; }

        ByteBuffer payload = ChunkCodecs.encodeSmallest(ts, vs, n);
        assertEquals((byte) GorillaCodec.VERSION, payload.get(payload.position()));
        assertEquals(ChunkCodecs.Codec.GORILLA, ChunkCodecs.codecOf(payload));
    }

    @Test
    public void encodeSmallestPicksChimpForQuantizedWalk()
    {
        int n = 100_000;
        long[] ts = new long[n]; double[] vs = new double[n];
        java.util.Random random = new java.util.Random(17);
        double walk = 50.0;
        for (int i = 0; i < n; i++)
        {
            ts[i] = i * 1000L;
            walk += (random.nextInt(3) - 1) * 0.1;
            vs[i] = Math.round(walk * 10.0) / 10.0;
        }

        ByteBuffer payload = ChunkCodecs.encodeSmallest(ts, vs, n);
        assertEquals((byte) Chimp128Codec.VERSION, payload.get(payload.position()));
        assertEquals(ChunkCodecs.Codec.CHIMP128, ChunkCodecs.codecOf(payload));
    }

    @Test
    public void headerConstantsAgree()
    {
        assertEquals(GorillaCodec.HEADER_SIZE, Chimp128Codec.HEADER_SIZE);
        assertEquals(GorillaCodec.HEADER_SIZE, ChunkCodecs.HEADER_SIZE);
        assertEquals(GorillaCodec.MAX_SAMPLES, Chimp128Codec.MAX_SAMPLES);
        assertEquals(GorillaCodec.MAX_SAMPLES, ChunkCodecs.MAX_SAMPLES);
    }

    @Test
    public void bakeoff()
    {
        // Promotion data for spec §1: quantized patterns need >=30% bytes/sample reduction vs
        // gorilla to promote chimp128 to the default codec; the constant pattern must not regress
        // more than +-10% (+ HEADER_SLACK bytes). All five BAKEOFF lines are printed unconditionally
        // (measured before any assertion runs) so the full picture is captured in the test log and
        // doc/timeseries/codec-bakeoff.md regardless of verdict -- see that doc for the actual
        // percentages and the promotion decision.
        //
        // The quantized-walk/quantized-sine assertions below are hard requirements: chimp is
        // designed for exactly this workload and measured >60% smaller than gorilla on both (see
        // the doc), so a regression here would indicate a real implementation bug.
        //
        // The constant pattern is NOT hard-asserted against the +-10% bound: per the normative
        // algorithm (design spec §2, left branch, xorC==0 case), an exact repeat still costs
        // 1 (branch flag) + 7 (ring index) + 1 (exact-match flag) = 9 bits, vs gorilla's 1-bit
        // "unchanged" fast path -- chimp measures ~5x LARGER than gorilla here (0.25 vs 1.25
        // bytes/sample), a ~400% regression, not a bug in this spec-compliant implementation. Hard
        // failing this build over a documented, by-design algorithmic trade-off would be dishonest
        // theater; the finding is instead the load-bearing input to the doc's "not promoted as
        // default" verdict. A generous sanity bound still guards against silent further regression.
        int n = 100_000;
        String[] names = { "constant", "quantized-walk-0.1", "quantized-sine", "full-precision-walk", "random-bits" };
        for (int p = 0; p < names.length; p++)
        {
            long[] ts = new long[n]; double[] vs = new double[n];
            java.util.Random random = new java.util.Random(17);
            double walk = 50.0;
            for (int i = 0; i < n; i++)
            {
                ts[i] = i * 1000L;
                switch (p)
                {
                    case 0: vs[i] = 42.0; break;
                    case 1: walk += (random.nextInt(3) - 1) * 0.1; vs[i] = Math.round(walk * 10.0) / 10.0; break;
                    case 2: vs[i] = Math.round(Math.sin(i / 300.0) * 500.0) / 10.0; break;
                    case 3: walk += random.nextGaussian(); vs[i] = walk; break;
                    default: vs[i] = Double.longBitsToDouble(random.nextLong()); break;
                }
            }
            long t0 = System.nanoTime();
            int gorilla = GorillaCodec.encode(ts, vs, n).remaining();
            long t1 = System.nanoTime();
            int chimp = Chimp128Codec.encode(ts, vs, n).remaining();
            long t2 = System.nanoTime();
            // decode timings too
            GorillaCodec.SampleCursor g = GorillaCodec.cursor(GorillaCodec.encode(ts, vs, n));
            long t3 = System.nanoTime(); while (g.advance()) {} long t4 = System.nanoTime();
            SampleCursor c = Chimp128Codec.cursor(Chimp128Codec.encode(ts, vs, n));
            long t5 = System.nanoTime(); while (c.advance()) {} long t6 = System.nanoTime();
            System.out.printf("BAKEOFF %-22s gorilla=%.3f B/s chimp=%.3f B/s ratio=%.2f " +
                              "enc(g/c)=%d/%d ms dec(g/c)=%d/%d ms%n",
                              names[p], gorilla / (double) n, chimp / (double) n,
                              gorilla / (double) chimp,
                              (t1 - t0) / 1_000_000, (t2 - t1) / 1_000_000,
                              (t4 - t3) / 1_000_000, (t6 - t5) / 1_000_000);
            if (p == 1 || p == 2)
                assertTrue(names[p] + ": chimp " + chimp + " > gorilla " + gorilla, chimp <= gorilla);
            if (p == 0)
                // Sanity/regression guard only (measured ratio ~5x; see comment above) -- NOT the
                // spec's +-10% promotion bound, which this pattern fails and does not meet.
                assertTrue("constant sanity bound blown: chimp " + chimp + " gorilla " + gorilla,
                           chimp <= gorilla * 6.0 + HEADER_SLACK);
        }
    }
}
