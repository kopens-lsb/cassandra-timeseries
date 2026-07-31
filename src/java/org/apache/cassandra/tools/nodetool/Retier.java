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

import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Immediately runs one tiered storage re-encode cycle for a table, bypassing the scheduled sweep's
 * interval check. Fails if a run for this table is already in flight.
 */
@Command(name = "retier", description = "Immediately run one tiered storage re-encode cycle for a table")
public class Retier extends AbstractCommand
{
    @Parameters(index = "0", arity = "1", description = "The keyspace of the table to retier")
    private String keyspace;

    @Parameters(index = "1", arity = "1", description = "The table to retier")
    private String table;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.retier(keyspace, table);
    }
}
