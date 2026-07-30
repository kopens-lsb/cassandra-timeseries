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
