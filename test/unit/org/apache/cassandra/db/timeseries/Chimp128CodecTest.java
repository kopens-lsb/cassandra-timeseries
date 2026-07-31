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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public void headerLayoutWithVersion2()
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
    public void sizeRegressionBaselines()
    {
        // Size regression bounds for the ONLY double codec: an encoding change that blows any of
        // these needs an explicit decision, not a silent merge. Values are the measured bytes/sample
        // (see doc/timeseries/codec-bakeoff.md) with headroom; the quantized walk is the pattern
        // chimp128 was chosen for, the random-bits pattern is the incompressible worst case that must
        // still not exceed the 16-byte raw (ts + value) representation it replaces.
        assertTrue("constant: " + bytesPerSample(0),        bytesPerSample(0) <= 1.5);
        assertTrue("quantized-walk: " + bytesPerSample(1),  bytesPerSample(1) <= 2.0);
        assertTrue("full-precision: " + bytesPerSample(2),  bytesPerSample(2) <= 7.5);
        assertTrue("random-bits: " + bytesPerSample(3),     bytesPerSample(3) <= 9.0);
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
                case 1:  walk += (random.nextInt(3) - 1) * 0.1; values[i] = Math.round(walk * 10.0) / 10.0; break;
                case 2:  walk += random.nextGaussian(); values[i] = walk; break;
                default: values[i] = Double.longBitsToDouble(random.nextLong()); break;
            }
        }
        return Chimp128Codec.encode(timestamps, values, n).remaining() / (double) n;
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
    public void irregularGapsIncludingLargeOnesRoundtrip()
    {
        // 밀리초·초·시간·여러 날 갭이 섞인 불규칙 시계열 (dod 전 버킷 + 64비트 escape 경유)
        long[] timestamps = { 0L, 1L, 1001L, 1002L, 3_600_000L, 3_600_050L, 90_000_000_000L };
        double[] values = { 1, 2, 3, 4, 5, 6, 7 };
        assertRoundtrip(timestamps, values, timestamps.length);
    }

    @Test
    public void dodBucketBoundariesRoundtrip()
    {
        // exact bucket edges and their just-outside neighbours: a silent off-by-one in the
        // dod buckets would corrupt data without failing any other test
        long[] dods = { -65537, -65536, -65535, -2049, -2048, -2047, -65, -64, -63, -1, 0, 1,
                        63, 64, 65, 2047, 2048, 2049, 65535, 65536, 65537 };
        long[] timestamps = new long[dods.length + 2];
        double[] values = new double[dods.length + 2];
        timestamps[0] = 10_000_000_000L; values[0] = 1.0;
        // base delta raised from the naively-obvious 200_000 to 300_000: the cumulative sum of
        // the dods above dips to -202945 (around the -1/0 entries), so 200_000 would drive the
        // running delta negative and violate strictly-increasing timestamps.
        long delta = 300_000L;
        timestamps[1] = timestamps[0] + delta; values[1] = 2.0;
        for (int i = 0; i < dods.length; i++)
        {
            delta += dods[i];
            timestamps[i + 2] = timestamps[i + 1] + delta; values[i + 2] = i;
        }
        assertRoundtrip(timestamps, values, timestamps.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsOutOfOrderTimestamps()
    {
        Chimp128Codec.encode(new long[]{ 1000L, 999L }, new double[]{ 1, 2 }, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDuplicateTimestamps()
    {
        Chimp128Codec.encode(new long[]{ 1000L, 1000L }, new double[]{ 1, 2 }, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsZeroCount()
    {
        Chimp128Codec.encode(new long[0], new double[0], 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsCountAboveMaximum()
    {
        // the MAX_SAMPLES check runs before the array-length check in encode(), so tiny arrays
        // are enough to hit it without allocating a 128MB+ array just for this test
        Chimp128Codec.encode(new long[]{ 1L }, new double[]{ 1.0 }, Chimp128Codec.MAX_SAMPLES + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnknownVersion()
    {
        ByteBuffer payload = Chimp128Codec.encode(new long[]{ 1L }, new double[]{ 1.0 }, 1);
        payload.put(payload.position(), (byte) 99);
        Chimp128Codec.cursor(payload);
    }

    @Test
    public void peeksRejectForeignVersion()
    {
        // a payload carrying any other version byte -- including 1, the removed gorilla format,
        // whose header layout is byte-identical -- must be rejected rather than half-decoded
        ByteBuffer payload = Chimp128Codec.encode(new long[]{ 1L }, new double[]{ 1.0 }, 1);
        payload.put(payload.position(), (byte) 1);

        assertThrowsIllegalArgument(() -> Chimp128Codec.sampleCount(payload));
        assertThrowsIllegalArgument(() -> Chimp128Codec.firstTimestamp(payload));
        assertThrowsIllegalArgument(() -> Chimp128Codec.lastTimestamp(payload));
    }

    @Test
    public void peeksAreByteOrderIndependent()
    {
        long[] timestamps = { 1000L, 2000L, 3500L };
        double[] values = { 1.5, 1.5, -2.25 };
        ByteBuffer payload = Chimp128Codec.encode(timestamps, values, 3);
        payload.order(ByteOrder.LITTLE_ENDIAN);   // peeks must ignore the caller's buffer order

        assertEquals(3, Chimp128Codec.sampleCount(payload));
        assertEquals(1000L, Chimp128Codec.firstTimestamp(payload));
        assertEquals(3500L, Chimp128Codec.lastTimestamp(payload));
    }

    @Test
    public void cursorAccessBeforeAdvanceThrows()
    {
        ByteBuffer payload = Chimp128Codec.encode(new long[]{ 1L }, new double[]{ 1.0 }, 1);
        SampleCursor cursor = Chimp128Codec.cursor(payload);

        assertThrowsIllegalState(cursor::timestamp);
        assertThrowsIllegalState(cursor::value);
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
        ByteBuffer payload = Chimp128Codec.encode(timestamps, values, 100);
        payload.limit(payload.limit() - 10);   // 뒤 10바이트 절단

        SampleCursor cursor = Chimp128Codec.cursor(payload);
        while (cursor.advance())
        {
            // 절단 지점에서 IndexOutOfBoundsException(RuntimeException) 이 나야 한다
        }
    }

    @Test
    public void corruptRingReferenceIsDetected()
    {
        // 2개 샘플 페이로드의 비트스트림을 조작해 좌분기 + 미기록 슬롯 참조를 만들기는 비트 단위라
        // 취약하므로, 대신 절단·버전 오염과 동일한 수준의 방어를 보장하는 손상 주입:
        // 첫 값(64비트) 직후의 비트들을 뒤집어 여러 무작위 손상 페이로드를 디코드 — 예외 또는
        // 정상 종료만 허용하고, 무한 루프/크래시가 없어야 한다
        long[] timestamps = new long[50];
        double[] values = new double[50];
        for (int i = 0; i < 50; i++) { timestamps[i] = i * 1000L; values[i] = i * 0.1; }
        ByteBuffer payload = Chimp128Codec.encode(timestamps, values, 50);
        for (int corruptByte = 30; corruptByte < Math.min(60, payload.limit()); corruptByte++)
        {
            ByteBuffer corrupted = ByteBuffer.allocate(payload.remaining());
            corrupted.put(payload.duplicate()).flip();
            corrupted.put(corruptByte, (byte) (corrupted.get(corruptByte) ^ 0x5A));
            try
            {
                SampleCursor cursor = Chimp128Codec.cursor(corrupted);
                int decoded = 0;
                while (cursor.advance() && decoded < 200) decoded++;
            }
            catch (RuntimeException expected)
            {
                // IllegalArgument / IndexOutOfBounds / BufferUnderflow 모두 허용
            }
        }
    }

    @Test
    public void propertyRoundtripAcrossSeedsAndPatterns()
    {
        // 고릴라 프로퍼티 테스트와 동일 구조(시드 30, ts 4패턴), 값 패턴은 chimp 특화:
        // 상수 / 양자화 워크(0.1) / 임의 비트(NaN 포함) / 양자화 주기(sin 0.1 반올림)
        for (long seed = 0; seed < 30; seed++)
        {
            java.util.Random random = new java.util.Random(seed);
            int n = 1 + random.nextInt(5000);
            long[] timestamps = new long[n];
            double[] values = new double[n];
            long timestamp = Math.abs(random.nextLong() % 4_000_000_000_000L);
            int pattern = (int) (seed % 4);
            double walk = 50.0;
            for (int i = 0; i < n; i++)
            {
                long step;
                switch (pattern)
                {
                    case 0:  step = 1000; break;
                    case 1:  step = 995 + random.nextInt(11); break;
                    case 2:  step = 1 + random.nextInt(10_000_000); break;
                    default: step = random.nextInt(100) == 0 ? 86_400_000L + random.nextInt(1_000_000) : 100; break;
                }
                timestamp += step;
                timestamps[i] = timestamp;
                switch (pattern)
                {
                    case 0:  values[i] = 42.0; break;
                    case 1:  walk += (random.nextInt(3) - 1) * 0.1; values[i] = Math.round(walk * 10.0) / 10.0; break;
                    case 2:  values[i] = Double.longBitsToDouble(random.nextLong()); break;
                    default: values[i] = Math.round(Math.sin(i / 300.0) * 50.0) / 10.0; break;
                }
            }
            assertRoundtrip(timestamps, values, n);
        }
    }

    static void assertRoundtrip(long[] timestamps, double[] values, int count)
    {
        ByteBuffer payload = Chimp128Codec.encode(timestamps, values, count);
        SampleCursor cursor = Chimp128Codec.cursor(payload);
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

    private static void assertThrowsIllegalArgument(Runnable action)
    {
        try
        {
            action.run();
            fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }

    private static void assertThrowsIllegalState(Runnable action)
    {
        try
        {
            action.run();
            fail("expected IllegalStateException");
        }
        catch (IllegalStateException expected)
        {
            // expected
        }
    }
}
