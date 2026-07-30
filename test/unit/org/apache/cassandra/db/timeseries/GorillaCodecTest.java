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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GorillaCodecTest
{
    @Test
    public void roundtripRegularOneSecondSeries()
    {
        int n = 3600;                              // 1시간, 1초 주기
        long[] timestamps = new long[n];
        double[] values = new double[n];
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = 1_700_000_000_000L + i * 1000L;
            values[i] = 20.0 + Math.sin(i / 500.0) * 40.0;
        }

        assertRoundtrip(timestamps, values, n);
    }

    @Test
    public void headerPeeksDoNotConsumeBuffer()
    {
        long[] timestamps = { 1000L, 2000L, 3500L };
        double[] values = { 1.5, 1.5, -2.25 };
        ByteBuffer payload = GorillaCodec.encode(timestamps, values, 3);

        int position = payload.position();
        assertEquals(3, GorillaCodec.sampleCount(payload));
        assertEquals(1000L, GorillaCodec.firstTimestamp(payload));
        assertEquals(3500L, GorillaCodec.lastTimestamp(payload));
        assertEquals(position, payload.position());
    }

    @Test
    public void constantValuesCompressToUnderHalfByteAmortized()
    {
        int n = 3600;
        long[] timestamps = new long[n];
        double[] values = new double[n];
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = i * 1000L;
            values[i] = 42.0;                      // 산업 설정값(setpoint) 패턴
        }

        ByteBuffer payload = GorillaCodec.encode(timestamps, values, n);
        // dod=0(1비트) + xor=0(1비트) = 샘플당 2비트 + 헤더/첫샘플 상수 비용
        assertTrue("bytes=" + payload.remaining(), payload.remaining() <= n / 2);
        assertRoundtrip(timestamps, values, n);
    }

    @Test
    public void specialValuesRoundtripBitExactly()
    {
        long[] timestamps = { 1L, 2L, 3L, 4L, 5L, 6L, 7L };
        double[] values = { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                            -0.0, 0.0, Double.MIN_VALUE, Double.MAX_VALUE };
        assertRoundtrip(timestamps, values, values.length);
    }

    @Test
    public void singleSampleRoundtrip()
    {
        assertRoundtrip(new long[]{ 1_700_000_000_000L }, new double[]{ 3.14 }, 1);
    }

    @Test
    public void irregularGapsIncludingLargeOnesRoundtrip()
    {
        // 밀리초·초·시간·여러 날 갭이 섞인 불규칙 시계열 (dod 전 버킷 + 64비트 escape 경유)
        long[] timestamps = { 0L, 1L, 1001L, 1002L, 3_600_000L, 3_600_050L, 90_000_000_000L };
        double[] values = { 1, 2, 3, 4, 5, 6, 7 };
        assertRoundtrip(timestamps, values, timestamps.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsOutOfOrderTimestamps()
    {
        GorillaCodec.encode(new long[]{ 1000L, 999L }, new double[]{ 1, 2 }, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDuplicateTimestamps()
    {
        GorillaCodec.encode(new long[]{ 1000L, 1000L }, new double[]{ 1, 2 }, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsZeroCount()
    {
        GorillaCodec.encode(new long[0], new double[0], 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnknownVersion()
    {
        ByteBuffer payload = GorillaCodec.encode(new long[]{ 1L }, new double[]{ 1.0 }, 1);
        payload.put(payload.position(), (byte) 99);
        GorillaCodec.cursor(payload);
    }

    @Test(expected = RuntimeException.class)
    public void truncatedPayloadFailsInsteadOfReturningGarbage()
    {
        long[] timestamps = new long[100];
        double[] values = new double[100];
        for (int i = 0; i < 100; i++)
        {
            timestamps[i] = i * 1000L;
            values[i] = i * 0.1;
        }
        ByteBuffer payload = GorillaCodec.encode(timestamps, values, 100);
        payload.limit(payload.limit() - 10);   // 뒤 10바이트 절단

        GorillaCodec.SampleCursor cursor = GorillaCodec.cursor(payload);
        while (cursor.advance())
        {
            // 절단 지점에서 IndexOutOfBoundsException(RuntimeException) 이 나야 한다
        }
    }

    @Test
    public void propertyRoundtripAcrossSeedsAndPatterns()
    {
        for (long seed = 0; seed < 30; seed++)
        {
            java.util.Random random = new java.util.Random(seed);
            int n = 1 + random.nextInt(5000);
            long[] timestamps = new long[n];
            double[] values = new double[n];

            long timestamp = Math.abs(random.nextLong() % 4_000_000_000_000L);
            int pattern = (int) (seed % 4);
            double walk = random.nextDouble() * 100;
            for (int i = 0; i < n; i++)
            {
                // 타임스탬프: 규칙(1s) / 지터 / 불규칙 갭을 시드별로 섞는다
                long step;
                switch (pattern)
                {
                    case 0:  step = 1000; break;                                   // 규칙
                    case 1:  step = 995 + random.nextInt(11); break;               // ±5ms 지터
                    case 2:  step = 1 + random.nextInt(10_000_000); break;         // 불규칙
                    default: step = random.nextInt(100) == 0
                                    ? 86_400_000L + random.nextInt(1_000_000)      // 드문 하루+ 갭
                                    : 100; break;
                }
                timestamp += step;
                timestamps[i] = timestamp;

                switch (pattern)
                {
                    case 0:  values[i] = 42.0; break;                              // 상수
                    case 1:  walk += random.nextGaussian(); values[i] = walk; break;
                    case 2:  values[i] = Double.longBitsToDouble(random.nextLong()); break;  // 임의 비트(NaN 포함)
                    default: values[i] = 20 + Math.sin(i / 300.0) * 5; break;
                }
            }

            assertRoundtrip(timestamps, values, n);
        }
    }

    @Test
    public void sizeRegressionBaselines()
    {
        // 회귀 기준: 이 수치를 넘기는 인코딩 변경은 명시적 결정 없이는 금지
        assertTrue("constant: " + bytesPerSample(0), bytesPerSample(0) <= 0.5);   // 설정값 패턴
        // 풀정밀도 가우시안 워크는 매 샘플 가수부가 거의 전부 바뀌는 XOR 압축의 준최악 케이스(실측 ~7.1).
        // 실제 산업 센서(양자화된 값)는 훨씬 잘 압축된다 — 실측 비교는 tiering 벤치마크(스펙 §7)에서.
        assertTrue("walk: " + bytesPerSample(1),     bytesPerSample(1) <= 8.0);   // 풀정밀도 랜덤워크
        assertTrue("random: " + bytesPerSample(2),   bytesPerSample(2) <= 11.0);  // 최악(원본 16B 대비)
    }

    private static double bytesPerSample(int pattern)
    {
        int n = 10_000;
        java.util.Random random = new java.util.Random(7);
        long[] timestamps = new long[n];
        double[] values = new double[n];
        double walk = 50;
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = i * 1000L;
            switch (pattern)
            {
                case 0:  values[i] = 42.0; break;
                case 1:  walk += random.nextGaussian(); values[i] = walk; break;
                default: values[i] = Double.longBitsToDouble(random.nextLong()); break;
            }
        }
        return GorillaCodec.encode(timestamps, values, n).remaining() / (double) n;
    }

    static void assertRoundtrip(long[] timestamps, double[] values, int count)
    {
        ByteBuffer payload = GorillaCodec.encode(timestamps, values, count);
        GorillaCodec.SampleCursor cursor = GorillaCodec.cursor(payload);
        for (int i = 0; i < count; i++)
        {
            assertTrue("cursor ended early at " + i, cursor.advance());
            assertEquals("timestamp " + i, timestamps[i], cursor.timestamp());
            // NaN 포함 비트 단위 동일성 (== 비교는 NaN에서 깨진다)
            assertEquals("value bits " + i,
                         Double.doubleToRawLongBits(values[i]),
                         Double.doubleToRawLongBits(cursor.value()));
        }
        assertFalse(cursor.advance());
    }
}
