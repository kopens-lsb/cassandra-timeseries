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
package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import picocli.CommandLine.Command;

/**
 * Prints tiered storage policy and re-encode status for every time-series table with a
 * {@code timeseries_tiering} policy -- see {@code system_views.timeseries_tiering} for the same data
 * over CQL.
 */
@Command(name = "tieringstatus", description = "Print tiered storage policy and re-encode status for time-series tables")
public class TieringStatus extends AbstractCommand
{
    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;

        TableBuilder table = new TableBuilder();
        table.add("Keyspace", "Table", "Interval (ms)", "Last Run At", "Windows Encoded", "Rows Encoded",
                  "Late Merges", "Chunks Expired");
        for (String row : probe.tieringStatusRows())
            table.add(row.split("\t", -1));
        table.printTo(out);
    }
}
