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

public class Chimp128CodecTest
{
    @Test
    public void roundtripQuantizedWalk()
    {
        // 산업 센서 전형: 0.1 스텝 양자화 랜덤워크 — chimp의 주 타깃
        int n = 5000;
        long[] timestamps = new long[n];
        double[] values = new double[n];
        java.util.Random random = new java.util.Random(11);
        double v = 50.0;
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = 1_700_000_000_000L + i * 1000L;
            v += (random.nextInt(3) - 1) * 0.1;              // -0.1 / 0 / +0.1
            values[i] = Math.round(v * 10.0) / 10.0;
            }
        assertRoundtrip(timestamps, values, n);
    }

    @Test
    public void roundtripRepeatedValuesFarApart()
    {
        // 주기 신호: 같은 값이 128 이내 간격으로 재등장 → 링 후보 히트(좌분기) 경로 검증
        int n = 1000;
        long[] timestamps = new long[n];
        double[] values = new double[n];
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = i * 1000L;
            values[i] = 20.0 + (i % 60) * 0.5;               // 60 간격 주기 반복
        }
        assertRoundtrip(timestamps, values, n);
    }

    @Test
    public void roundtripAllRightBranchPatterns()
    {
        // 후보 미스가 계속되는 풀정밀 워크 → 우분기('10'/'11') 경로 검증
        int n = 2000;
        long[] timestamps = new long[n];
        double[] values = new double[n];
        java.util.Random random = new java.util.Random(7);
        double v = 0;
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = i * 1000L;
            v += random.nextGaussian();
            values[i] = v;
        }
        assertRoundtrip(timestamps, values, n);
    }

    @Test
    public void headerMatchesGorillaLayoutWithVersion2()
    {
        long[] timestamps = { 1000L, 2000L };
        double[] values = { 1.0, 2.0 };
        ByteBuffer payload = Chimp128Codec.encode(timestamps, values, 2);
        assertEquals(2, payload.get(payload.position()));      // version byte
        assertEquals(2, Chimp128Codec.sampleCount(payload));
        assertEquals(1000L, Chimp128Codec.firstTimestamp(payload));
        assertEquals(2000L, Chimp128Codec.lastTimestamp(payload));
    }

    @Test
    public void singleSampleRoundtrip()
    {
        assertRoundtrip(new long[]{ 42L }, new double[]{ 3.14 }, 1);
    }

    @Test
    public void quantizedBeatsGorillaProvisionally()
    {
        // 승격 판정은 Task 3 bake-off가 하지만, 코어 단계에서 방향성만 확인:
        // 양자화 워크에서 chimp payload가 gorilla payload보다 크면 구현이 잘못된 것
        int n = 10_000;
        long[] timestamps = new long[n];
        double[] values = new double[n];
        java.util.Random random = new java.util.Random(3);
        double v = 50.0;
        for (int i = 0; i < n; i++)
        {
            timestamps[i] = i * 1000L;
            v += (random.nextInt(3) - 1) * 0.1;
            values[i] = Math.round(v * 10.0) / 10.0;
        }
        int chimp = Chimp128Codec.encode(timestamps, values, n).remaining();
        int gorilla = GorillaCodec.encode(timestamps, values, n).remaining();
        assertTrue("chimp=" + chimp + " gorilla=" + gorilla, chimp <= gorilla);
    }

    static void assertRoundtrip(long[] timestamps, double[] values, int count)
    {
        ByteBuffer payload = Chimp128Codec.encode(timestamps, values, count);
        GorillaCodec.SampleCursor cursor = Chimp128Codec.cursor(payload);
        for (int i = 0; i < count; i++)
        {
            assertTrue("ended early at " + i, cursor.advance());
            assertEquals("ts " + i, timestamps[i], cursor.timestamp());
            assertEquals("bits " + i,
                         Double.doubleToRawLongBits(values[i]),
                         Double.doubleToRawLongBits(cursor.value()));
        }
        assertFalse(cursor.advance());
    }
}
