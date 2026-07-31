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
package org.apache.cassandra.db.timeseries.tiering;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JsonUtils;

import static java.lang.String.format;

/**
 * Per-table tiering policy for the background chunk-store re-encoder, stored as JSON in the table's
 * {@code extensions['timeseries_tiering']} (see {@link org.apache.cassandra.cql3.statements.schema.TableAttributes}
 * for how that extension value gets set via {@code ALTER TABLE ... WITH extensions = {...}}).
 *
 * <p>Recognised JSON keys (unknown keys are rejected):
 * <ul>
 *     <li>{@code hot_window} (required) - rows younger than this are left alone.</li>
 *     <li>{@code chunk_window} (default {@code "1h"}) - fixed-length re-encoding window.</li>
 *     <li>{@code cold_window} (optional) - once a chunk is older than this, delete it outright.</li>
 *     <li>{@code codec} (default {@code "auto"}) - {@code auto|gorilla|chimp128}.</li>
 *     <li>{@code consistency} (default {@code "LOCAL_QUORUM"}) - a {@link ConsistencyLevel} name.</li>
 *     <li>{@code interval} (default {@code "5m"}) - re-encoder tick period.</li>
 * </ul>
 * Durations use the grammar {@code <positive int><m|h|d>} (minutes/hours/days).
 */
public final class TieringPolicy
{
    public static final String EXTENSION_KEY = "timeseries_tiering";

    private static final String HOT_WINDOW = "hot_window";
    private static final String CHUNK_WINDOW = "chunk_window";
    private static final String COLD_WINDOW = "cold_window";
    private static final String CODEC = "codec";
    private static final String CONSISTENCY = "consistency";
    private static final String INTERVAL = "interval";

    private static final String DEFAULT_CHUNK_WINDOW = "1h";
    private static final String DEFAULT_CODEC = "auto";
    private static final String DEFAULT_CONSISTENCY = "LOCAL_QUORUM";
    private static final String DEFAULT_INTERVAL = "5m";

    private static final Set<String> KNOWN_KEYS = ImmutableSet.of(HOT_WINDOW, CHUNK_WINDOW, COLD_WINDOW,
                                                                    CODEC, CONSISTENCY, INTERVAL);

    /**
     * Consistency levels weaker than a quorum are rejected: at e.g. {@code ONE}, the re-encoder's
     * existing-chunk read can miss the previous cycle's chunk (written at a higher CL, or simply not
     * yet visible to the replica this node read), so a subsequent merge produces a fresh-rows-only
     * chunk that *loses* to the old chunk on a later read (older write, but same or higher timestamp
     * doesn't apply here -- it's a distinct write racing on visibility, not ordering) while the range
     * delete still removes the source rows -- silent, permanent data loss with no error surfaced.
     */
    private static final Set<ConsistencyLevel> ALLOWED_CONSISTENCY_LEVELS =
            ImmutableSet.of(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM,
                            ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.ALL);

    private static final Pattern DURATION = Pattern.compile("([1-9][0-9]*)([mhd])");

    public enum CodecChoice
    {
        AUTO, GORILLA, CHIMP128
    }

    public final long hotWindowMillis;
    public final long chunkWindowMillis;
    public final long coldWindowMillis; // -1 = unset
    public final long intervalMillis;
    public final CodecChoice codec;
    public final ConsistencyLevel consistency;

    private TieringPolicy(long hotWindowMillis,
                           long chunkWindowMillis,
                           long coldWindowMillis,
                           long intervalMillis,
                           CodecChoice codec,
                           ConsistencyLevel consistency)
    {
        this.hotWindowMillis = hotWindowMillis;
        this.chunkWindowMillis = chunkWindowMillis;
        this.coldWindowMillis = coldWindowMillis;
        this.intervalMillis = intervalMillis;
        this.codec = codec;
        this.consistency = consistency;
    }

    /**
     * @return the policy configured on {@code metadata}, or {@code null} if the table has no
     * {@code timeseries_tiering} extension.
     * @throws ConfigurationException if the extension value is present but invalid (malformed JSON,
     * unknown keys, or a rule violation).
     */
    public static TieringPolicy fromTable(TableMetadata metadata)
    {
        ByteBuffer value = metadata.params.extensions.get(EXTENSION_KEY);
        if (value == null)
            return null;

        String json;
        try
        {
            json = ByteBufferUtil.string(value);
        }
        catch (CharacterCodingException e)
        {
            throw new ConfigurationException(format("%s extension value is not valid UTF-8: %s", EXTENSION_KEY, e.getMessage()));
        }

        return parse(json);
    }

    /**
     * Parses and validates a {@code timeseries_tiering} policy JSON document.
     *
     * @throws ConfigurationException on malformed JSON, unknown keys, or a rule violation.
     */
    public static TieringPolicy parse(String json)
    {
        Map<String, Object> raw;
        try
        {
            raw = JsonUtils.fromJsonMap(json);
        }
        catch (RuntimeException e)
        {
            throw new ConfigurationException(format("Invalid %s JSON: %s", EXTENSION_KEY, e.getMessage()));
        }

        for (String key : raw.keySet())
        {
            if (!KNOWN_KEYS.contains(key))
                throw new ConfigurationException(format("Unknown %s key '%s'", EXTENSION_KEY, key));
        }

        String hotWindow = requireString(raw, HOT_WINDOW);
        if (hotWindow == null)
            throw new ConfigurationException(format("%s.%s is required", EXTENSION_KEY, HOT_WINDOW));
        long hotWindowMillis = parseDurationMillis(HOT_WINDOW, hotWindow);

        String chunkWindow = requireString(raw, CHUNK_WINDOW);
        long chunkWindowMillis = parseDurationMillis(CHUNK_WINDOW, chunkWindow != null ? chunkWindow : DEFAULT_CHUNK_WINDOW);

        String coldWindow = requireString(raw, COLD_WINDOW);
        long coldWindowMillis = coldWindow == null ? -1 : parseDurationMillis(COLD_WINDOW, coldWindow);

        String codecStr = requireString(raw, CODEC);
        CodecChoice codec = parseCodec(codecStr != null ? codecStr : DEFAULT_CODEC);

        String consistencyStr = requireString(raw, CONSISTENCY);
        ConsistencyLevel consistency = parseConsistency(consistencyStr != null ? consistencyStr : DEFAULT_CONSISTENCY);

        String interval = requireString(raw, INTERVAL);
        long intervalMillis = parseDurationMillis(INTERVAL, interval != null ? interval : DEFAULT_INTERVAL);

        if (hotWindowMillis < chunkWindowMillis)
            throw new ConfigurationException(format("%s.%s (%s) must be >= %s (%s)",
                                                      EXTENSION_KEY, HOT_WINDOW, hotWindow, CHUNK_WINDOW,
                                                      chunkWindow != null ? chunkWindow : DEFAULT_CHUNK_WINDOW));

        if (coldWindowMillis >= 0 && coldWindowMillis <= hotWindowMillis)
            throw new ConfigurationException(format("%s.%s (%s) must be > %s (%s)",
                                                      EXTENSION_KEY, COLD_WINDOW, coldWindow, HOT_WINDOW, hotWindow));

        return new TieringPolicy(hotWindowMillis, chunkWindowMillis, coldWindowMillis, intervalMillis, codec, consistency);
    }

    /** Floors {@code tsMillis} to the start of its {@link #chunkWindowMillis}-wide, epoch-aligned window. */
    public long windowStartFor(long tsMillis)
    {
        return tsMillis - Math.floorMod(tsMillis, chunkWindowMillis);
    }

    /**
     * @return {@code null} if {@code metadata} is a canonical time-series table (exactly one partition key
     * column, exactly one {@code timestamp} clustering column, exactly one {@code double} regular column),
     * else a human-readable description of why it is not.
     */
    public static String canonicalSchemaError(TableMetadata metadata)
    {
        if (metadata.partitionKeyColumns().size() != 1)
            return format("expected exactly 1 partition key column, found %d", metadata.partitionKeyColumns().size());

        if (metadata.clusteringColumns().size() != 1)
            return format("expected exactly 1 clustering column, found %d", metadata.clusteringColumns().size());

        ColumnMetadata clustering = metadata.clusteringColumns().get(0);
        if (!clustering.type.equals(TimestampType.instance))
            return format("expected clustering column '%s' to be of type timestamp, found %s",
                           clustering.name.toCQLString(), clustering.type.asCQL3Type());

        if (metadata.regularColumns().size() != 1)
            return format("expected exactly 1 regular column, found %d", metadata.regularColumns().size());

        ColumnMetadata value = metadata.regularColumns().iterator().next();
        if (!value.type.equals(DoubleType.instance))
            return format("expected regular column '%s' to be of type double, found %s",
                           value.name.toCQLString(), value.type.asCQL3Type());

        return null;
    }

    @Override
    public String toString()
    {
        return format("TieringPolicy{hot_window=%dms, chunk_window=%dms, cold_window=%s, codec=%s, consistency=%s, interval=%dms}",
                       hotWindowMillis, chunkWindowMillis,
                       coldWindowMillis < 0 ? "unset" : coldWindowMillis + "ms",
                       codec, consistency, intervalMillis);
    }

    private static String requireString(Map<String, Object> raw, String key)
    {
        Object value = raw.get(key);
        if (value == null)
            return null;
        if (!(value instanceof String))
            throw new ConfigurationException(format("%s.%s must be a string, got '%s'", EXTENSION_KEY, key, value));
        return (String) value;
    }

    private static CodecChoice parseCodec(String value)
    {
        try
        {
            return CodecChoice.valueOf(value.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(format("%s.%s must be one of auto, gorilla, chimp128, got '%s'",
                                                      EXTENSION_KEY, CODEC, value));
        }
    }

    private static ConsistencyLevel parseConsistency(String value)
    {
        ConsistencyLevel consistency;
        try
        {
            consistency = ConsistencyLevel.fromString(value);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(format("%s.%s is not a valid consistency level: '%s'",
                                                      EXTENSION_KEY, CONSISTENCY, value));
        }

        if (!ALLOWED_CONSISTENCY_LEVELS.contains(consistency))
            throw new ConfigurationException(format(
                "%s.%s must be one of QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL (got '%s'): a weaker " +
                "consistency lets the re-encoder's existing-chunk read miss the previous cycle's chunk, " +
                "producing a merge that loses data while the source rows still get deleted",
                EXTENSION_KEY, CONSISTENCY, value));

        return consistency;
    }

    private static long parseDurationMillis(String key, String value)
    {
        Matcher matcher = DURATION.matcher(value == null ? "" : value.trim());
        if (!matcher.matches())
            throw new ConfigurationException(format("%s.%s must look like 10m, 1h or 30d, got '%s'", EXTENSION_KEY, key, value));

        long amount;
        try
        {
            amount = Long.parseLong(matcher.group(1));
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(format("%s.%s value '%s' is out of range", EXTENSION_KEY, key, value));
        }

        switch (matcher.group(2))
        {
            case "m": return TimeUnit.MINUTES.toMillis(amount);
            case "h": return TimeUnit.HOURS.toMillis(amount);
            default:  return TimeUnit.DAYS.toMillis(amount);
        }
    }
}
