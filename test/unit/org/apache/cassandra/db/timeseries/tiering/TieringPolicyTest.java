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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.timeseries.tiering.TieringPolicy.CodecChoice;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TieringPolicyTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static TableMetadata canonicalTable(String json)
    {
        TableMetadata.Builder builder = TableMetadata.builder("ks", "tbl")
                                                      .addPartitionKeyColumn("tag", UTF8Type.instance)
                                                      .addClusteringColumn("ts", TimestampType.instance)
                                                      .addRegularColumn("value", DoubleType.instance);
        if (json != null)
            builder.params(TableParams.builder()
                                      .extensions(ImmutableMap.of(TieringPolicy.EXTENSION_KEY, ByteBufferUtil.bytes(json)))
                                      .build());
        return builder.build();
    }

    // ---- parsing + defaults ----

    @Test
    public void testParsesAllFields()
    {
        TieringPolicy policy = TieringPolicy.parse(
            "{\"hot_window\":\"7d\", \"chunk_window\":\"1h\", \"cold_window\":\"365d\", " +
            "\"codec\":\"gorilla\", \"consistency\":\"QUORUM\", \"interval\":\"10m\"}");

        assertEquals(TimeUnit.DAYS.toMillis(7), policy.hotWindowMillis);
        assertEquals(TimeUnit.HOURS.toMillis(1), policy.chunkWindowMillis);
        assertEquals(TimeUnit.DAYS.toMillis(365), policy.coldWindowMillis);
        assertEquals(CodecChoice.GORILLA, policy.codec);
        assertEquals(ConsistencyLevel.QUORUM, policy.consistency);
        assertEquals(TimeUnit.MINUTES.toMillis(10), policy.intervalMillis);
    }

    @Test
    public void testDefaults()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"7d\"}");

        assertEquals(TimeUnit.DAYS.toMillis(7), policy.hotWindowMillis);
        assertEquals(TimeUnit.HOURS.toMillis(1), policy.chunkWindowMillis);
        assertEquals(-1, policy.coldWindowMillis);
        assertEquals(CodecChoice.AUTO, policy.codec);
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, policy.consistency);
        assertEquals(TimeUnit.MINUTES.toMillis(5), policy.intervalMillis);
    }

    @Test
    public void testHotWindowEqualToChunkWindowIsAllowed()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"1h\", \"chunk_window\":\"1h\"}");
        assertEquals(policy.chunkWindowMillis, policy.hotWindowMillis);
    }

    @Test
    public void testToStringSummarizesPolicy()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"7d\"}");
        String s = policy.toString();
        assertTrue(s.contains("hot_window"));
        assertTrue(s.contains("AUTO"));
        assertTrue(s.contains("LOCAL_QUORUM"));
    }

    // ---- validation rejections ----

    @Test
    public void testMissingHotWindowRejected()
    {
        assertConfigurationException("{\"chunk_window\":\"1h\"}", "hot_window");
    }

    @Test
    public void testUnknownKeyRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"bogus_key\":\"1h\"}", "bogus_key");
    }

    @Test
    public void testHotWindowLessThanChunkWindowRejected()
    {
        assertConfigurationException("{\"hot_window\":\"1h\", \"chunk_window\":\"2h\"}", null);
    }

    @Test
    public void testChunkWindowAtCapAccepted()
    {
        // 31d is the documented maximum chunk_window -- the boundary itself must parse.
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"40d\", \"chunk_window\":\"31d\"}");
        assertEquals(TimeUnit.DAYS.toMillis(31), policy.chunkWindowMillis);
    }

    @Test
    public void testChunkWindowOverCapRejected()
    {
        // An unbounded chunk_window defeats the re-encoder's one-window-at-a-time memory bound (and can
        // exceed the codec's per-chunk sample limit), so anything over 31d is rejected at parse time.
        // The message must name both the offending value and the cap so the operator can act on it.
        assertConfigurationException("{\"hot_window\":\"33d\", \"chunk_window\":\"32d\"}", "32d");
        assertConfigurationException("{\"hot_window\":\"33d\", \"chunk_window\":\"32d\"}", "31d");
        assertConfigurationException("{\"hot_window\":\"370d\", \"chunk_window\":\"365d\"}", "365d");
        assertConfigurationException("{\"hot_window\":\"370d\", \"chunk_window\":\"365d\"}", "31d");
    }

    @Test
    public void testColdWindowEqualToHotWindowRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"cold_window\":\"7d\"}", null);
    }

    @Test
    public void testColdWindowLessThanHotWindowRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"cold_window\":\"1d\"}", null);
    }

    @Test
    public void testBadDurationUnitRejected()
    {
        assertConfigurationException("{\"hot_window\":\"1w\"}", null);
    }

    @Test
    public void testZeroDurationRejected()
    {
        assertConfigurationException("{\"hot_window\":\"0h\"}", null);
    }

    @Test
    public void testEmptyDurationRejected()
    {
        assertConfigurationException("{\"hot_window\":\"\"}", null);
    }

    @Test
    public void testBadCodecRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"codec\":\"snappy\"}", "codec");
    }

    @Test
    public void testBadConsistencyRejected()
    {
        assertConfigurationException("{\"hot_window\":\"7d\", \"consistency\":\"NOT_A_LEVEL\"}", "consistency");
    }

    @Test
    public void testWeakConsistencyRejected()
    {
        // ONE (and TWO/THREE/LOCAL_ONE/ANY) would let the existing-chunk read miss a prior cycle's
        // chunk, so weaker-than-quorum levels are rejected outright -- see ALLOWED_CONSISTENCY_LEVELS.
        assertConfigurationException("{\"hot_window\":\"7d\", \"consistency\":\"ONE\"}", "consistency");
    }

    @Test
    public void testMalformedJsonRejected()
    {
        assertConfigurationException("{not json", null);
    }

    private static void assertConfigurationException(String json, String expectedSubstring)
    {
        try
        {
            TieringPolicy.parse(json);
            fail("Expected ConfigurationException for: " + json);
        }
        catch (ConfigurationException e)
        {
            if (expectedSubstring != null)
                assertTrue("Expected message to contain '" + expectedSubstring + "', was: " + e.getMessage(),
                           e.getMessage().contains(expectedSubstring));
        }
    }

    // ---- windowStartFor ----

    @Test
    public void testWindowStartForFloorsToChunkWindow()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"1h\", \"chunk_window\":\"1h\"}");
        long hourMillis = TimeUnit.HOURS.toMillis(1);

        assertEquals(0L, policy.windowStartFor(0L));
        assertEquals(0L, policy.windowStartFor(hourMillis - 1));
        assertEquals(hourMillis, policy.windowStartFor(hourMillis));
        assertEquals(hourMillis, policy.windowStartFor(hourMillis + 1));
    }

    @Test
    public void testWindowStartForNegativeTimestamps()
    {
        TieringPolicy policy = TieringPolicy.parse("{\"hot_window\":\"1h\", \"chunk_window\":\"1h\"}");
        long hourMillis = TimeUnit.HOURS.toMillis(1);

        // Math.floorMod behaviour: floors toward negative infinity, not toward zero.
        assertEquals(-hourMillis, policy.windowStartFor(-1L));
        assertEquals(-hourMillis, policy.windowStartFor(-hourMillis));
        assertEquals(-2 * hourMillis, policy.windowStartFor(-hourMillis - 1));
    }

    // ---- canonicalSchemaError ----

    @Test
    public void testCanonicalSchemaIsAccepted()
    {
        assertNull(TieringPolicy.canonicalSchemaError(canonicalTable(null)));
    }

    @Test
    public void testDescClusteredCanonicalSchemaIsAccepted()
    {
        // CLUSTERING ORDER BY (ts DESC) wraps the clustering column type in ReversedType(timestamp);
        // it is still canonical -- newest-first is the dominant time-series clustering idiom.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", ReversedType.getInstance(TimestampType.instance))
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        assertNull(TieringPolicy.canonicalSchemaError(table));
    }

    @Test
    public void testExtraRegularColumnRejected()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .addRegularColumn("extra", DoubleType.instance)
                                            .build();
        String error = TieringPolicy.canonicalSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("regular column"));
    }

    @Test
    public void testNonTimestampClusteringRejected()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag", UTF8Type.instance)
                                            .addClusteringColumn("ts", Int32Type.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        String error = TieringPolicy.canonicalSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("timestamp"));
    }

    @Test
    public void testCompositePartitionKeyRejected()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                            .addPartitionKeyColumn("tag1", UTF8Type.instance)
                                            .addPartitionKeyColumn("tag2", UTF8Type.instance)
                                            .addClusteringColumn("ts", TimestampType.instance)
                                            .addRegularColumn("value", DoubleType.instance)
                                            .build();
        String error = TieringPolicy.canonicalSchemaError(table);
        assertNotNull(error);
        assertTrue(error, error.contains("partition key"));
    }

    // ---- fromTable ----

    @Test
    public void testFromTableReturnsNullWhenExtensionAbsent()
    {
        assertNull(TieringPolicy.fromTable(canonicalTable(null)));
    }

    @Test
    public void testFromTableParsesExtension()
    {
        TableMetadata table = canonicalTable("{\"hot_window\":\"7d\"}");
        TieringPolicy policy = TieringPolicy.fromTable(table);
        assertNotNull(policy);
        assertEquals(TimeUnit.DAYS.toMillis(7), policy.hotWindowMillis);
    }
}
