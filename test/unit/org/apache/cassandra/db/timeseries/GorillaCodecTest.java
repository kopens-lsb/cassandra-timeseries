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
