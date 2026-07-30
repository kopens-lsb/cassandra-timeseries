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

/**
 * Bit stream reader mirroring {@link BitWriter}: the most significant bit of the first byte is
 * the first bit. Reading past the underlying buffer's limit throws {@link IndexOutOfBoundsException},
 * which callers treat as a truncated/corrupt payload.
 */
final class BitReader
{
    private final ByteBuffer buffer;
    private final int start;
    private int bitPosition;

    BitReader(ByteBuffer buffer)
    {
        this.buffer = buffer;
        this.start = buffer.position();
    }

    boolean readBit()
    {
        return readBits(1) == 1L;
    }

    long readBits(int numBits)
    {
        assert numBits >= 1 && numBits <= 64 : numBits;
        long result = 0;
        int remaining = numBits;
        while (remaining > 0)
        {
            int byteIndex = bitPosition >>> 3;
            int bitIndex = bitPosition & 7;
            int available = 8 - bitIndex;
            int take = Math.min(available, remaining);
            int current = buffer.get(start + byteIndex) & 0xFF;
            int taken = (current >>> (available - take)) & ((1 << take) - 1);
            result = (result << take) | taken;
            bitPosition += take;
            remaining -= take;
        }
        return result;
    }
}
