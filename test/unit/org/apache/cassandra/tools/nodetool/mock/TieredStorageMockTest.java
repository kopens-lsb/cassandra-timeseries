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
package org.apache.cassandra.tools.nodetool.mock;

import java.util.List;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.db.timeseries.tiering.TieredStorageService;
import org.apache.cassandra.db.timeseries.tiering.TieredStorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * NodeProbe-passthrough tests for the tiered storage nodetool commands
 * ({@code nodetool retier}, {@code nodetool tieringstatus} -- see
 * {@link org.apache.cassandra.tools.nodetool.Retier} and
 * {@link org.apache.cassandra.tools.nodetool.TieringStatus}) against a mocked
 * {@link TieredStorageServiceMBean}, mirroring the other command tests in this package.
 * <p>
 * The tiered storage MBean is not part of {@link AbstractNodetoolMock}'s static mock set (that list
 * mirrors upstream's MBeans), so this class registers/unregisters its own mock around each test.
 */
public class TieredStorageMockTest extends AbstractNodetoolMock
{
    private TieredStorageServiceMBean mock;

    @Before
    public void registerTieredStorageMock() throws NotCompliantMBeanException
    {
        mock = Mockito.mock(TieredStorageServiceMBean.class);
        MBeanWrapper.instance.unregisterMBean(TieredStorageService.MBEAN_NAME, MBeanWrapper.OnException.IGNORE);
        MBeanWrapper.instance.registerMBean(new StandardMBean(mock, TieredStorageServiceMBean.class),
                                            TieredStorageService.MBEAN_NAME);
    }

    @After
    public void unregisterTieredStorageMock()
    {
        MBeanWrapper.instance.unregisterMBean(TieredStorageService.MBEAN_NAME, MBeanWrapper.OnException.IGNORE);
    }

    @Test
    public void testRetierPassesKeyspaceAndTableThrough()
    {
        invokeNodetool("retier", "ks1", "metrics").assertOnCleanExit();
        Mockito.verify(mock).retier("ks1", "metrics");
    }

    @Test
    public void testRetierAlreadyInFlightSurfacesFailure()
    {
        // The MBean contract: a run already in flight for the table fails retier with
        // IllegalStateException rather than silently doing nothing.
        doThrow(new IllegalStateException("Tiered storage run already in flight for ks1.metrics"))
                .when(mock).retier("ks1", "metrics");
        ToolRunner.ToolResult result = invokeNodetool("retier", "ks1", "metrics");
        result.asserts().failure();
        assertTrue(result.getStdout().contains("already in flight"));
    }

    @Test
    public void testRetierSkippedTagsSurfaceAsFailure()
    {
        // A cycle that skipped tags did not do what retier was asked to do. The MBean fails, and the
        // command must exit non-zero rather than print nothing and report success.
        doThrow(new RuntimeException("Tiered storage run for ks1.metrics completed with 2 tag(s) skipped"))
                .when(mock).retier("ks1", "metrics");
        ToolRunner.ToolResult result = invokeNodetool("retier", "ks1", "metrics");
        result.asserts().failure().errorContains("2 tag(s) skipped");
    }

    @Test
    public void testTieringStatusRendersStatusRows()
    {
        when(mock.statusRows()).thenReturn(List.of("ks1\tmetrics\t300000\t1700000000000\t7\t42\t1\t3\t2"));
        ToolRunner.ToolResult result = invokeNodetool("tieringstatus");
        result.assertOnCleanExit();
        Mockito.verify(mock).statusRows();

        String stdout = result.getStdout();
        // Header plus every tab-separated field of the row, laid out by TableBuilder.
        assertTrue(stdout.contains("Keyspace"));
        assertTrue(stdout.contains("Windows Encoded"));
        assertTrue(stdout.contains("Tags Skipped"));
        assertTrue(stdout.contains("ks1"));
        assertTrue(stdout.contains("metrics"));
        assertTrue(stdout.contains("300000"));
        assertTrue(stdout.contains("1700000000000"));
        assertTrue(stdout.contains("42"));
    }

    @Test
    public void testTieringStatusFailureSurfaces()
    {
        // Unlike IllegalStateException (a "bad use", printed to stdout by NodeTool.badUse), a generic
        // RuntimeException goes through NodeTool.err, which prints "error: <message>" to stderr.
        doThrow(new RuntimeException("status collection failed"))
                .when(mock).statusRows();
        ToolRunner.ToolResult result = invokeNodetool("tieringstatus");
        result.asserts().failure().errorContains("status collection failed");
    }
}
