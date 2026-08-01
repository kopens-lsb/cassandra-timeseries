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

/**
 * A chunk payload this build cannot read because of its <em>format</em>, not its contents: a codec
 * version byte or column type code that was valid when the chunk was written and has since been
 * removed (or that comes from a newer, unknown format).
 *
 * <p>This is deliberately a distinct type from the plain {@link IllegalArgumentException} the codecs
 * throw for a corrupt payload, because the two demand opposite handling. Corruption is local and
 * accidental -- one bad chunk out of many -- so the read path skips it and warns, trading
 * completeness for availability. An unsupported format is systematic: it affects <em>every</em>
 * chunk written by the old build, so skipping would let a {@code SELECT} succeed while silently
 * returning truncated history for as long as those chunks exist. Callers must let this propagate
 * and fail the operation instead.
 *
 * <p>It extends {@link IllegalArgumentException} so that callers which only care that decoding
 * failed keep working unchanged; callers that must tell the two apart catch this type first.
 */
public class UnsupportedChunkFormatException extends IllegalArgumentException
{
    public UnsupportedChunkFormatException(String message)
    {
        super(message);
    }
}
