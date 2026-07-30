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
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;

/**
 * Options and window arithmetic for {@link TimeSeriesCompactionStrategy}. Durations are simple
 * fixed-length strings: {@code <positive integer><m|h|d>} (minutes/hours/days). Windows are
 * floor-aligned to the epoch via {@link TimeWindowCompactionStrategy#getWindowBoundsInMillis}.
 */
public final class TimeSeriesCompactionStrategyOptions
{
    public static final String WINDOW_SIZE = "window_size";
    public static final String FREEZE_AFTER = "freeze_after";
    public static final String RETENTION = "retention";
    public static final String TIMESTAMP_RESOLUTION = "timestamp_resolution";

    static final String DEFAULT_WINDOW_SIZE = "1h";
    static final String DEFAULT_FREEZE_AFTER = "2h";

    private static final Pattern DURATION = Pattern.compile("([1-9][0-9]*)([mhd])");

    public final long windowSizeMillis;
    public final long freezeAfterMillis;
    public final long retentionMillis;          // -1 = unset
    public final TimeUnit windowUnit;
    public final int windowSizeInUnits;
    public final TimeUnit timestampResolution;

    public TimeSeriesCompactionStrategyOptions(Map<String, String> options)
    {
        Pair<TimeUnit, Integer> window = parseDuration(WINDOW_SIZE, options.getOrDefault(WINDOW_SIZE, DEFAULT_WINDOW_SIZE));
        this.windowUnit = window.left;
        this.windowSizeInUnits = window.right;
        this.windowSizeMillis = TimeUnit.MILLISECONDS.convert(windowSizeInUnits, windowUnit);
        Pair<TimeUnit, Integer> freeze = parseDuration(FREEZE_AFTER, options.getOrDefault(FREEZE_AFTER, DEFAULT_FREEZE_AFTER));
        this.freezeAfterMillis = TimeUnit.MILLISECONDS.convert(freeze.right, freeze.left);
        String retention = options.get(RETENTION);
        if (retention == null)
        {
            this.retentionMillis = -1;
        }
        else
        {
            Pair<TimeUnit, Integer> parsed = parseDuration(RETENTION, retention);
            this.retentionMillis = TimeUnit.MILLISECONDS.convert(parsed.right, parsed.left);
        }
        this.timestampResolution = TimeUnit.valueOf(options.getOrDefault(TIMESTAMP_RESOLUTION, "MICROSECONDS"));
    }

    public long windowStartFor(long timestampMillis)
    {
        return TimeWindowCompactionStrategy.getWindowBoundsInMillis(windowUnit, windowSizeInUnits, timestampMillis).left;
    }

    /**
     * Written in subtraction form ({@code windowStartMillis > nowMillis - windowSizeMillis - freezeAfterMillis})
     * rather than the equivalent addition form ({@code windowStartMillis + windowSizeMillis + freezeAfterMillis
     * > nowMillis}): {@code windowStartMillis} is derived from an sstable's max timestamp, which for a
     * garbage/adversarial write can be near {@code Long.MAX_VALUE}. Adding {@code windowSizeMillis +
     * freezeAfterMillis} to such a value silently wraps to a negative number, which would misclassify an
     * active window as inactive (and, in {@link #isExpiredWindow}, as expired - leading to whole-sstable
     * obsoletion with no rewrite). The subtraction's operands are both config-derived and bounded (the
     * duration parser caps at {@code Integer.MAX_VALUE} days), so {@code nowMillis - windowSizeMillis -
     * freezeAfterMillis} cannot itself wrap for any real clock value.
     */
    public boolean isActiveWindow(long windowStartMillis, long nowMillis)
    {
        return windowStartMillis > nowMillis - windowSizeMillis - freezeAfterMillis;
    }

    /** See {@link #isActiveWindow} for why this is written in subtraction form. */
    public boolean isExpiredWindow(long windowStartMillis, long nowMillis)
    {
        return retentionMillis >= 0 && windowStartMillis <= nowMillis - retentionMillis - windowSizeMillis;
    }

    /** A copy of {@code original} without this strategy's own keys - the options handed to the UCS delegate. */
    public Map<String, String> delegateOptions(Map<String, String> original)
    {
        Map<String, String> copy = new HashMap<>(original);
        copy.remove(WINDOW_SIZE);
        copy.remove(FREEZE_AFTER);
        copy.remove(RETENTION);
        copy.remove(TIMESTAMP_RESOLUTION);
        return copy;
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions)
        throws ConfigurationException
    {
        Pair<TimeUnit, Integer> window = parseDuration(WINDOW_SIZE, options.getOrDefault(WINDOW_SIZE, DEFAULT_WINDOW_SIZE));
        long windowMillis = TimeUnit.MILLISECONDS.convert(window.right, window.left);
        Pair<TimeUnit, Integer> freeze = parseDuration(FREEZE_AFTER, options.getOrDefault(FREEZE_AFTER, DEFAULT_FREEZE_AFTER));
        long freezeMillis = TimeUnit.MILLISECONDS.convert(freeze.right, freeze.left);
        String retention = options.get(RETENTION);
        if (retention != null)
        {
            Pair<TimeUnit, Integer> parsed = parseDuration(RETENTION, retention);
            long retentionMillis = TimeUnit.MILLISECONDS.convert(parsed.right, parsed.left);
            if (retentionMillis < windowMillis + freezeMillis)
                throw new ConfigurationException(String.format("%s (%s) must be at least %s + %s",
                                                               RETENTION, retention, WINDOW_SIZE, FREEZE_AFTER));
        }
        String resolution = options.get(TIMESTAMP_RESOLUTION);
        if (resolution != null)
        {
            try
            {
                TimeUnit.valueOf(resolution);
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException(TIMESTAMP_RESOLUTION + " must be a java TimeUnit name, got " + resolution);
            }
        }

        uncheckedOptions.remove(WINDOW_SIZE);
        uncheckedOptions.remove(FREEZE_AFTER);
        uncheckedOptions.remove(RETENTION);
        uncheckedOptions.remove(TIMESTAMP_RESOLUTION);
        return uncheckedOptions;
    }

    private static Pair<TimeUnit, Integer> parseDuration(String key, String value) throws ConfigurationException
    {
        Matcher matcher = DURATION.matcher(value == null ? "" : value.trim());
        if (!matcher.matches())
            throw new ConfigurationException(String.format("%s must look like 10m, 1h or 30d, got '%s'", key, value));
        int amount;
        try
        {
            amount = Integer.parseInt(matcher.group(1));
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s value '%s' is out of range", key, value));
        }
        switch (matcher.group(2))
        {
            case "m": return Pair.create(TimeUnit.MINUTES, amount);
            case "h": return Pair.create(TimeUnit.HOURS, amount);
            default:  return Pair.create(TimeUnit.DAYS, amount);
        }
    }
}
