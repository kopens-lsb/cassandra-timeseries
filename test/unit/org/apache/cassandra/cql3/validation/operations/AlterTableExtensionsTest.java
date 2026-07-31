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
package org.apache.cassandra.cql3.validation.operations;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.timeseries.tiering.TieringPolicy;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests that {@code ALTER TABLE ... WITH extensions = {...}} actually persists the extension bytes
 * (see {@link org.apache.cassandra.cql3.statements.schema.TableAttributes}), and that
 * {@link TieringPolicy#fromTable} can parse what lands there.
 */
public class AlterTableExtensionsTest extends CQLTester
{
    @Test
    public void testExtensionsRoundTripAndTieringPolicyParses() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts));");

        String json = "{\"hot_window\":\"7d\", \"chunk_window\":\"1h\"}";
        ByteBuffer expected = ByteBufferUtil.bytes(json);
        String hex = ByteBufferUtil.bytesToHex(expected);

        alterTable("ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 0x" + hex + "};");

        ByteBuffer actual = getCurrentColumnFamilyStore().metadata().params.extensions.get(TieringPolicy.EXTENSION_KEY);
        assertNotNull("extensions should contain " + TieringPolicy.EXTENSION_KEY, actual);
        assertEquals(expected, actual);

        TieringPolicy policy = TieringPolicy.fromTable(getCurrentColumnFamilyStore().metadata());
        assertNotNull(policy);
        assertEquals(TimeUnit.DAYS.toMillis(7), policy.hotWindowMillis);
        assertEquals(TimeUnit.HOURS.toMillis(1), policy.chunkWindowMillis);
    }

    @Test
    public void testNonHexExtensionValueRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (tag text, ts timestamp, value double, PRIMARY KEY (tag, ts));");

        assertInvalidThrow(InvalidRequestException.class,
                            "ALTER TABLE %s WITH extensions = {'" + TieringPolicy.EXTENSION_KEY + "': 'not-hex'};");
    }
}
