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

package org.apache.cassandra.db.compaction;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TimeSeriesCompactionStrategyOptionsTest
{
    private static Map<String, String> options(String... kv)
    {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2)
            map.put(kv[i], kv[i + 1]);
        return map;
    }

    @Test
    public void parsesDurations()
    {
        TimeSeriesCompactionStrategyOptions opts =
            new TimeSeriesCompactionStrategyOptions(options("window_size", "1h", "freeze_after", "2h", "retention", "30d"));
        assertEquals(3_600_000L, opts.windowSizeMillis);
        assertEquals(7_200_000L, opts.freezeAfterMillis);
        assertEquals(30L * 24 * 3_600_000L, opts.retentionMillis);
    }

    @Test
    public void defaultsAreApplied()
    {
        TimeSeriesCompactionStrategyOptions opts = new TimeSeriesCompactionStrategyOptions(options());
        assertEquals(3_600_000L, opts.windowSizeMillis);          // 기본 1h
        assertEquals(2 * 3_600_000L, opts.freezeAfterMillis);     // 기본 2h
        assertEquals(-1L, opts.retentionMillis);                  // 기본: 없음
    }

    @Test
    public void windowStartIsFloorAligned()
    {
        TimeSeriesCompactionStrategyOptions opts = new TimeSeriesCompactionStrategyOptions(options("window_size", "1h"));
        long t = 1_700_003_723_456L;                              // 창 중간의 임의 시각
        long start = opts.windowStartFor(t);
        assertTrue(start <= t);
        assertTrue(t < start + opts.windowSizeMillis);
        assertEquals(0L, start % opts.windowSizeMillis);
        // 경계 정확성: 창 시작 자신과 창 끝 직전은 같은 창, 창 끝은 다음 창
        assertEquals(start, opts.windowStartFor(start));
        assertEquals(start, opts.windowStartFor(start + opts.windowSizeMillis - 1));
        assertEquals(start + opts.windowSizeMillis, opts.windowStartFor(start + opts.windowSizeMillis));
    }

    @Test
    public void activeAndExpiredClassification()
    {
        TimeSeriesCompactionStrategyOptions opts =
            new TimeSeriesCompactionStrategyOptions(options("window_size", "1h", "freeze_after", "2h", "retention", "30d"));
        long now = 1_700_000_000_000L;
        long currentWindow = opts.windowStartFor(now);
        assertTrue(opts.isActiveWindow(currentWindow, now));
        // 창 끝 + freeze_after 이내면 여전히 활성(CLOSING)
        assertTrue(opts.isActiveWindow(currentWindow - 2 * opts.windowSizeMillis, now));
        // 그보다 오래되면 비활성
        assertFalse(opts.isActiveWindow(currentWindow - 4 * opts.windowSizeMillis, now));
        // 만료: 창 끝이 now - retention 이전
        long expiredStart = opts.windowStartFor(now - opts.retentionMillis) - 2 * opts.windowSizeMillis;
        assertTrue(opts.isExpiredWindow(expiredStart, now));
        assertFalse(opts.isExpiredWindow(currentWindow, now));
    }

    @Test
    public void retentionUnsetNeverExpires()
    {
        TimeSeriesCompactionStrategyOptions opts = new TimeSeriesCompactionStrategyOptions(options("window_size", "1h"));
        assertFalse(opts.isExpiredWindow(0L, Long.MAX_VALUE / 2));
    }

    @Test
    public void validateRejectsBadDurations()
    {
        for (String bad : new String[]{ "0h", "-1h", "1w", "abc", "", "1.5h" })
            assertThatThrownBy(() -> TimeSeriesCompactionStrategyOptions.validateOptions(
                                   options("window_size", bad), new HashMap<>(options("window_size", bad))))
            .as("window_size=" + bad)
            .isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void validateRejectsRetentionShorterThanWindowPlusFreeze()
    {
        Map<String, String> map = options("window_size", "1h", "freeze_after", "2h", "retention", "2h");
        assertThatThrownBy(() -> TimeSeriesCompactionStrategyOptions.validateOptions(map, new HashMap<>(map)))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("retention");
    }

    @Test
    public void validateConsumesOwnKeysOnly()
    {
        Map<String, String> map = options("window_size", "1h", "retention", "30d", "scaling_parameters", "T4");
        Map<String, String> unchecked = TimeSeriesCompactionStrategyOptions.validateOptions(map, new HashMap<>(map));
        assertFalse(unchecked.containsKey("window_size"));
        assertFalse(unchecked.containsKey("retention"));
        assertTrue(unchecked.containsKey("scaling_parameters"));   // UCS 몫은 남긴다
    }

    @Test
    public void delegateOptionsStripOwnKeys()
    {
        TimeSeriesCompactionStrategyOptions opts =
            new TimeSeriesCompactionStrategyOptions(options("window_size", "1h", "retention", "30d"));
        Map<String, String> delegate =
            opts.delegateOptions(options("window_size", "1h", "retention", "30d", "scaling_parameters", "T4"));
        assertEquals(options("scaling_parameters", "T4"), delegate);
    }

    @Test
    public void cqlSurfaceAcceptsAndValidates()
    {
        // 짧은 이름 해석 + 옵션 수용
        Map<String, String> valid = new HashMap<>();
        valid.put("class", "TimeSeriesCompactionStrategy");
        valid.put("window_size", "1h");
        valid.put("retention", "30d");
        // CompactionParams 경유 검증 (CREATE TABLE과 동일 경로)
        CompactionParams params = CompactionParams.fromMap(valid);
        params.validate();
        assertEquals(TimeSeriesCompactionStrategy.class, params.klass());

        Map<String, String> bad = new HashMap<>(valid);
        bad.put("retention", "1h");                       // window+freeze 미만
        assertThatThrownBy(() -> CompactionParams.fromMap(bad).validate())
            .isInstanceOf(ConfigurationException.class);

        Map<String, String> unknown = new HashMap<>(valid);
        unknown.put("no_such_option", "x");
        assertThatThrownBy(() -> CompactionParams.fromMap(unknown).validate())
            .isInstanceOf(ConfigurationException.class);
    }
}
