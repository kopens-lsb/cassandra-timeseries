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
import java.util.Arrays;

/**
 * Append-only bit stream writer. The first bit written becomes the most significant bit of the
 * first byte (big-endian bit order). {@code writeBits} appends the low {@code numBits} bits of
 * the value, most significant of those bits first.
 */
final class BitWriter
{
    private long[] words = new long[16];
    private int bitCount;

    void writeBit(boolean bit)
    {
        writeBits(bit ? 1L : 0L, 1);
    }

    void writeBits(long value, int numBits)
    {
        assert numBits >= 1 && numBits <= 64 : numBits;
        ensureCapacity(numBits);
        if (numBits < 64)
            value &= (1L << numBits) - 1;

        int wordIndex = bitCount >>> 6;
        int used = bitCount & 63;
        int space = 64 - used;
        if (numBits <= space)
        {
            words[wordIndex] |= value << (space - numBits);
        }
        else
        {
            int overflow = numBits - space;
            words[wordIndex] |= value >>> overflow;
            words[wordIndex + 1] |= value << (64 - overflow);
        }
        bitCount += numBits;
    }

    int sizeInBytes()
    {
        return (bitCount + 7) >>> 3;
    }

    void writeTo(ByteBuffer out)
    {
        int bytes = sizeInBytes();
        for (int i = 0; i < bytes; i++)
        {
            int word = i >>> 3;
            int shift = 56 - ((i & 7) << 3);
            out.put((byte) (words[word] >>> shift));
        }
    }

    private void ensureCapacity(int numBits)
    {
        int required = (bitCount + numBits + 63) >>> 6;
        if (required > words.length)
            words = Arrays.copyOf(words, Math.max(required, words.length * 2));
    }
}
