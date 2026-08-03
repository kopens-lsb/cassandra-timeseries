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

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * ALP -- Adaptive Lossless floating-Point (Afroozeh, Kuffo, Boncz, SIGMOD 2024) -- the only double
 * encoding of the columnar chunk format ({@link ColumnarChunkCodec}, type code
 * {@link ColumnarChunkCodec#TYPE_DOUBLE_ALP}). It replaced chimp128, which is retained only as the
 * version-2 single-column format ({@link Chimp128Codec}) and as this codec's size baseline in tests.
 *
 * <p><b>Two variants, one type code.</b> A section's first byte says which:
 * <ul>
 *   <li>{@link #SUBFORMAT_ALP} -- <i>decimal</i> ALP. Most doubles produced by measurement are
 *       decimals of few digits ({@code 20.76}, {@code 157.0}). Each is re-expressed as the integer
 *       {@code i = round(v * 10^e * 10^-f)}, which decodes back as {@code i * 10^f * 10^-e}; the
 *       integers are frame-of-reference'd against the block minimum and bit-packed at the width the
 *       block needs. Any value that does not survive that round trip <em>bit for bit</em> is an
 *       exception, stored verbatim as its 64-bit pattern next to its row position.</li>
 *   <li>{@link #SUBFORMAT_ALP_RD} -- ALP <i>Real Double</i>, for values that are not decimal-like.
 *       Each 64-bit pattern is cut into a {@code leftBitWidth}-bit high part and a
 *       {@code 64 - leftBitWidth}-bit low part. The high parts of a real column take few distinct
 *       values (same sign, same or adjacent binades), so they are dictionary-coded; the low part is
 *       stored verbatim. Rows whose high part missed the dictionary carry it explicitly.</li>
 * </ul>
 *
 * <p><b>Losslessness is structural, not statistical.</b> Neither variant can lose a bit:
 * decimal ALP verifies every value against {@code Double.doubleToRawLongBits} and demotes any
 * mismatch to a verbatim exception, and ALP-RD reassembles {@code (left << rightBitWidth) | right}
 * from parts that were both stored exactly. {@code -0.0}, every NaN payload (quiet and signalling),
 * the infinities, subnormals, {@code MIN_VALUE} and {@code MAX_VALUE} therefore all survive; the
 * decimal path simply refuses them and pays 8 bytes each, and ALP-RD never even inspects them as
 * numbers. Nothing here converts a {@code long} bit pattern to a {@code double} and back as a way of
 * carrying it: the value pipeline is {@code long} raw bits end to end, and the {@code double} view
 * exists only inside the decimal arithmetic, whose result is checked against the original bits.
 *
 * <p><b>Determinism.</b> Two replicas encoding the same window must produce byte-identical payloads
 * -- {@code TieredStorageService.chunkUnchanged} compares payload bytes -- so every choice this
 * codec makes is a pure function of the input array:
 * <ul>
 *   <li>The {@code (e, f)} search samples with a <b>fixed integer stride</b> (never an RNG), scores
 *       all {@code 0 <= f <= e <= 18} pairs, and keeps the best {@link #CANDIDATE_LIMIT} under the
 *       strict total order (estimated bits asc, {@code e} asc, {@code f} asc). Those candidates are
 *       then measured exactly over the whole block and ranked by (exact bytes asc, {@code e} asc,
 *       {@code f} asc).</li>
 *   <li>The ALP-RD dictionary is the top {@link #RD_MAX_DICTIONARY} high parts under (frequency
 *       desc, value asc) -- a total order over a histogram held in an <b>array</b>, never a
 *       {@code HashMap} whose iteration order is unspecified.</li>
 *   <li>The variant choice is "ALP-RD iff strictly fewer bytes", so an exact tie always resolves to
 *       decimal ALP. There is no comparison that can go either way.</li>
 *   <li>The only floating-point operations are {@code *} and {@link Math#rint}. Java multiplication
 *       is IEEE-754 correctly rounded and (since Java 17) unconditionally strict -- no x87 extended
 *       precision, and Java never contracts {@code a*b+c} into an FMA -- while {@code Math.rint}'s
 *       specification admits exactly one result for every input, with no ulp allowance of the kind
 *       {@code Math.sin}/{@code pow} carry. Both are therefore reproducible across JVMs and CPUs.
 *       <b>Any future use of a transcendental, of {@code Math.round}'s half-up rule, or of
 *       {@code strictfp}-sensitive code here must be treated as a determinism regression.</b></li>
 * </ul>
 *
 * <p>Corruption surfaces as {@link IllegalArgumentException}, matching
 * {@link ColumnarChunkCodec}'s uniform policy: every length and index read out of the payload is
 * bounds-checked before it sizes an allocation, and a body that runs off the end of the section is
 * rewrapped rather than escaping as {@link IndexOutOfBoundsException}.
 */
final class AlpCodec
{
    /** Decimal ALP: scaled integers, frame-of-reference'd and bit-packed, plus verbatim exceptions. */
    static final byte SUBFORMAT_ALP = 0x00;
    /** ALP-RD: dictionary-coded high bits + verbatim low bits. Lossless for every bit pattern. */
    static final byte SUBFORMAT_ALP_RD = 0x01;

    /**
     * Largest {@code e}/{@code f} considered. 10^18 is the largest power of ten below
     * {@code Long.MAX_VALUE} and the bound the ALP paper uses; 10^e is also exactly representable as
     * a {@code double} for every {@code e <= 22}, so no table entry is itself an approximation.
     */
    static final int MAX_EXPONENT = 18;

    /**
     * Scaled integers are held to {@code |i| <= 2^53}, the range over which {@code long} and
     * {@code double} represent the same integers exactly. Outside it the {@code (long)} cast and the
     * widening back to {@code double} stop being inverses, so a value that scales past this is an
     * exception rather than a silently lossy encode. It also bounds the frame-of-reference span at
     * {@code 2^54}, hence {@link #MAX_BIT_WIDTH}.
     */
    static final long ENCODED_LIMIT = 1L << 53;
    private static final double ENCODED_LIMIT_D = (double) ENCODED_LIMIT;

    /** {@code max - min <= 2^54} for encoded integers within {@link #ENCODED_LIMIT}, so 55 bits suffice. */
    static final int MAX_BIT_WIDTH = 55;

    /** Returned by {@link #tryEncode} for a value the decimal path cannot represent exactly. */
    static final long NOT_ENCODABLE = Long.MIN_VALUE;

    /** ALP-RD cuts at most 16 bits off the top, so an exception's high part always fits in 2 bytes. */
    static final int RD_MAX_LEFT_BITS = 16;
    /** Dictionary entries; 8 is the paper's bound and keeps the per-row code at most 3 bits. */
    static final int RD_MAX_DICTIONARY = 8;

    /** Values inspected when scoring {@code (e, f)} pairs. Sampled by fixed stride -- never randomly. */
    static final int SAMPLE_LIMIT = 64;
    /** {@code (e, f)} pairs promoted from the sample to an exact whole-block measurement. */
    static final int CANDIDATE_LIMIT = 5;

    /** 10^i for i in [0, 18]. Every entry is exact: 10^i is representable for i <= 22. */
    private static final double[] POW10 =
    {
        1.0E0, 1.0E1, 1.0E2, 1.0E3, 1.0E4, 1.0E5, 1.0E6, 1.0E7, 1.0E8, 1.0E9,
        1.0E10, 1.0E11, 1.0E12, 1.0E13, 1.0E14, 1.0E15, 1.0E16, 1.0E17, 1.0E18
    };

    /**
     * 10^-i for i in [0, 18]. Only 10^0 is exact; the rest are the correctly-rounded double nearest
     * the true value, which is what makes them deterministic (a Java decimal literal has exactly one
     * legal {@code double} value). ALP multiplies by these rather than dividing by {@link #POW10}
     * deliberately -- it is what the reference implementation does, and the round trip is verified
     * bit-for-bit afterwards regardless, so the approximation costs recall, never correctness.
     */
    private static final double[] INV_POW10 =
    {
        1.0E0, 1.0E-1, 1.0E-2, 1.0E-3, 1.0E-4, 1.0E-5, 1.0E-6, 1.0E-7, 1.0E-8, 1.0E-9,
        1.0E-10, 1.0E-11, 1.0E-12, 1.0E-13, 1.0E-14, 1.0E-15, 1.0E-16, 1.0E-17, 1.0E-18
    };

    private AlpCodec()
    {
    }

    // ---------------------------------------------------------------------------------------
    // Encoding
    // ---------------------------------------------------------------------------------------

    /**
     * Encodes one double column section from raw IEEE-754 bit patterns (not {@code double}s -- see
     * the class javadoc on why the pipeline never round-trips a pattern through a {@code double}).
     *
     * @param rawBits {@code Double.doubleToRawLongBits} of each present value, in row order
     * @param count   how many entries of {@code rawBits} are in play
     */
    static byte[] encode(long[] rawBits, int count)
    {
        return choose(rawBits, count).encode(rawBits, count);
    }

    /**
     * The variant this codec will use for {@code rawBits}, chosen by a strict total order: ALP-RD
     * wins only when it is <b>strictly</b> smaller, so an exact tie always goes to decimal ALP.
     * Both plans carry the exact byte length they will produce, so nothing is encoded twice.
     */
    static Plan choose(long[] rawBits, int count)
    {
        Plan alp = planAlp(rawBits, count);
        Plan rd = planRd(rawBits, count);
        return rd.sizeInBytes < alp.sizeInBytes ? rd : alp;
    }

    /** One encoding decision for a block, plus the exact size it will occupy. */
    abstract static class Plan
    {
        final int sizeInBytes;

        Plan(int sizeInBytes)
        {
            this.sizeInBytes = sizeInBytes;
        }

        abstract byte subFormat();

        abstract byte[] encode(long[] rawBits, int count);
    }

    // ---- decimal ALP -----------------------------------------------------------------------

    /**
     * The scaled integer representing {@code raw} under {@code (e, f)}, or {@link #NOT_ENCODABLE}
     * when it cannot be represented exactly.
     * <p>
     * The acceptance test is {@code Double.doubleToRawLongBits(decode(encode(v))) == raw} -- bit
     * equality, not {@code ==} on doubles. That distinction is the whole edge-case story:
     * {@code 0.0 == -0.0} is true but their patterns differ, so {@code -0.0} is correctly refused
     * here and stored verbatim; and NaN is refused twice over, once by the range guard (every
     * comparison against NaN is false, so the negated test fires) and once by bit inequality.
     */
    static long tryEncode(long raw, int e, int f)
    {
        double value = Double.longBitsToDouble(raw);
        double scaled = value * POW10[e] * INV_POW10[f];
        // Written negated so NaN and both infinities fall out here rather than reaching the cast,
        // where (long) saturates at Long.MAX_VALUE/MIN_VALUE and would fabricate a huge frame.
        if (!(scaled >= -ENCODED_LIMIT_D && scaled <= ENCODED_LIMIT_D))
            return NOT_ENCODABLE;
        long encoded = (long) Math.rint(scaled);
        return Double.doubleToRawLongBits(decodeOne(encoded, e, f)) == raw ? encoded : NOT_ENCODABLE;
    }

    /** The value a scaled integer stands for: {@code i * 10^f * 10^-e}. Never NaN or infinite. */
    private static double decodeOne(long encoded, int e, int f)
    {
        return encoded * POW10[f] * INV_POW10[e];
    }

    static AlpPlan planAlp(long[] rawBits, int count)
    {
        // Stage 1: score every (e, f) pair on a fixed-stride sample. `ceil` rather than `floor` so
        // the sample spans the whole block for every count, instead of clustering at the head for
        // counts just above SAMPLE_LIMIT.
        int stride = Math.max(1, (count + SAMPLE_LIMIT - 1) / SAMPLE_LIMIT);
        int[] candidateE = new int[CANDIDATE_LIMIT];
        int[] candidateF = new int[CANDIDATE_LIMIT];
        long[] candidateScore = new long[CANDIDATE_LIMIT];
        int candidates = 0;

        for (int e = 0; e <= MAX_EXPONENT; e++)
        {
            for (int f = 0; f <= e; f++)
            {
                long minimum = Long.MAX_VALUE;
                long maximum = Long.MIN_VALUE;
                int sampled = 0;
                int exceptions = 0;
                for (int i = 0; i < count && sampled < SAMPLE_LIMIT; i += stride, sampled++)
                {
                    long encoded = tryEncode(rawBits[i], e, f);
                    if (encoded == NOT_ENCODABLE)
                    {
                        exceptions++;
                    }
                    else
                    {
                        minimum = Math.min(minimum, encoded);
                        maximum = Math.max(maximum, encoded);
                    }
                }
                int width = exceptions == sampled ? 0 : bitWidthOf(maximum - minimum);
                // Estimated bits: packed payload plus the 64-bit value and ~16-bit position every
                // exception costs. Only used to rank candidates -- the winner is re-measured exactly.
                long score = (long) (sampled - exceptions) * width + (long) exceptions * (64 + 16);
                candidates = offer(candidateE, candidateF, candidateScore, candidates, e, f, score);
            }
        }

        // Stage 2: measure each surviving candidate exactly over the whole block. Ranked by
        // (bytes asc, e asc, f asc) -- candidates are already in (score, e, f) order, so a strict
        // `<` on size keeps the first of any tie, which is the (e, f)-smallest.
        AlpPlan best = null;
        for (int c = 0; c < candidates; c++)
        {
            AlpPlan plan = measureAlp(rawBits, count, candidateE[c], candidateF[c]);
            if (best == null || plan.sizeInBytes < best.sizeInBytes)
                best = plan;
        }
        return best;
    }

    /**
     * Inserts {@code (e, f, score)} into the descending-quality candidate list, keeping it sorted by
     * the strict total order (score asc, e asc, f asc). Called with {@code e}/{@code f} already
     * ascending, so the {@code <} test alone realises that order.
     *
     * @return the new candidate count
     */
    private static int offer(int[] es, int[] fs, long[] scores, int size, int e, int f, long score)
    {
        int position = size;
        while (position > 0 && score < scores[position - 1])
            position--;
        if (position >= CANDIDATE_LIMIT)
            return size;
        for (int i = Math.min(size, CANDIDATE_LIMIT - 1); i > position; i--)
        {
            es[i] = es[i - 1];
            fs[i] = fs[i - 1];
            scores[i] = scores[i - 1];
        }
        es[position] = e;
        fs[position] = f;
        scores[position] = score;
        return Math.min(size + 1, CANDIDATE_LIMIT);
    }

    /** Exact whole-block cost of encoding under {@code (e, f)}. */
    private static AlpPlan measureAlp(long[] rawBits, int count, int e, int f)
    {
        long minimum = Long.MAX_VALUE;
        long maximum = Long.MIN_VALUE;
        int exceptions = 0;
        int exceptionBytes = 0;
        int previousPosition = -1;
        for (int i = 0; i < count; i++)
        {
            long encoded = tryEncode(rawBits[i], e, f);
            if (encoded == NOT_ENCODABLE)
            {
                exceptions++;
                exceptionBytes += varLen(i - previousPosition - 1) + Long.BYTES;
                previousPosition = i;
            }
            else
            {
                minimum = Math.min(minimum, encoded);
                maximum = Math.max(maximum, encoded);
            }
        }

        long reference = exceptions == count ? 0L : minimum;
        int bitWidth = exceptions == count ? 0 : bitWidthOf(maximum - minimum);
        int size = 4                                                        // subFormat, e, f, bitWidth
                   + varLen(ColumnarChunkCodec.zigzagEncode(reference))
                   + varLen(exceptions)
                   + exceptionBytes
                   + packedBytes((long) (count - exceptions) * bitWidth);
        return new AlpPlan(size, e, f, bitWidth, reference, exceptions);
    }

    static final class AlpPlan extends Plan
    {
        final int exponent;
        final int factor;
        final int bitWidth;
        final long reference;
        final int exceptionCount;

        AlpPlan(int sizeInBytes, int exponent, int factor, int bitWidth, long reference, int exceptionCount)
        {
            super(sizeInBytes);
            this.exponent = exponent;
            this.factor = factor;
            this.bitWidth = bitWidth;
            this.reference = reference;
            this.exceptionCount = exceptionCount;
        }

        @Override
        byte subFormat()
        {
            return SUBFORMAT_ALP;
        }

        @Override
        byte[] encode(long[] rawBits, int count)
        {
            ByteArrayOutputStream head = new ByteArrayOutputStream();
            head.write(SUBFORMAT_ALP);
            head.write(exponent);
            head.write(factor);
            head.write(bitWidth);
            ColumnarChunkCodec.writeVarLong(head, ColumnarChunkCodec.zigzagEncode(reference));
            ColumnarChunkCodec.writeVarLong(head, exceptionCount);

            int previousPosition = -1;
            for (int i = 0; i < count; i++)
            {
                if (tryEncode(rawBits[i], exponent, factor) != NOT_ENCODABLE)
                    continue;
                ColumnarChunkCodec.writeVarLong(head, i - previousPosition - 1);
                writeLongBigEndian(head, rawBits[i]);
                previousPosition = i;
            }

            BitWriter body = new BitWriter();
            if (bitWidth > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    long encoded = tryEncode(rawBits[i], exponent, factor);
                    if (encoded != NOT_ENCODABLE)
                        body.writeBits(encoded - reference, bitWidth);
                }
            }
            return assemble(head, body);
        }
    }

    // ---- ALP-RD ----------------------------------------------------------------------------

    static RdPlan planRd(long[] rawBits, int count)
    {
        // Scratch shared by every candidate width: the largest left part is 16 bits, and only the
        // first (1 << leftBits) entries are ever touched for a given width, so one pair of arrays
        // cleared per width beats sixteen allocations.
        int[] histogram = new int[1 << RD_MAX_LEFT_BITS];
        int[] codeOf = new int[1 << RD_MAX_LEFT_BITS];

        RdPlan best = null;
        // leftBits ascending, and the winner is taken on a strict `<`, so ties go to the narrowest
        // cut. leftBits >= 1 keeps rightBitWidth <= 63, so the low-part mask never shifts by 64.
        for (int leftBits = 1; leftBits <= RD_MAX_LEFT_BITS; leftBits++)
        {
            RdPlan plan = measureRd(rawBits, count, leftBits, histogram, codeOf);
            if (best == null || plan.sizeInBytes < best.sizeInBytes)
                best = plan;
        }
        return best;
    }

    private static RdPlan measureRd(long[] rawBits, int count, int leftBits, int[] histogram, int[] codeOf)
    {
        int rightBitWidth = Long.SIZE - leftBits;
        int slots = 1 << leftBits;
        Arrays.fill(histogram, 0, slots, 0);
        Arrays.fill(codeOf, 0, slots, -1);
        for (int i = 0; i < count; i++)
            histogram[(int) (rawBits[i] >>> rightBitWidth)]++;

        // The dictionary is the top RD_MAX_DICTIONARY left parts under (frequency desc, value asc).
        // Selection scans the histogram array in ascending value order and only displaces an
        // incumbent on a STRICTLY greater frequency, which realises that order exactly -- and, being
        // an array scan, cannot inherit a hash table's unspecified iteration order.
        int[] dictionary = new int[RD_MAX_DICTIONARY];
        int[] frequencies = new int[RD_MAX_DICTIONARY];
        int dictionarySize = 0;
        for (int value = 0; value < slots; value++)
        {
            int frequency = histogram[value];
            if (frequency == 0)
                continue;
            int position = dictionarySize;
            while (position > 0 && frequency > frequencies[position - 1])
                position--;
            if (position >= RD_MAX_DICTIONARY)
                continue;
            for (int i = Math.min(dictionarySize, RD_MAX_DICTIONARY - 1); i > position; i--)
            {
                dictionary[i] = dictionary[i - 1];
                frequencies[i] = frequencies[i - 1];
            }
            dictionary[position] = value;
            frequencies[position] = frequency;
            dictionarySize = Math.min(dictionarySize + 1, RD_MAX_DICTIONARY);
        }
        // count >= 1 guarantees at least one non-zero bucket, so dictionarySize >= 1 and codeBits is
        // well defined (0 when the whole block shares one left part -- then no code is stored at all).
        for (int code = 0; code < dictionarySize; code++)
            codeOf[dictionary[code]] = code;
        int codeBits = codeBitsFor(dictionarySize);

        int exceptions = 0;
        int exceptionBytes = 0;
        int previousPosition = -1;
        for (int i = 0; i < count; i++)
        {
            if (codeOf[(int) (rawBits[i] >>> rightBitWidth)] >= 0)
                continue;
            exceptions++;
            exceptionBytes += varLen(i - previousPosition - 1) + Short.BYTES;
            previousPosition = i;
        }

        int size = 3                                                    // subFormat, leftBits, dictionarySize
                   + dictionarySize * Short.BYTES
                   + varLen(exceptions)
                   + exceptionBytes
                   + packedBytes((long) count * (codeBits + rightBitWidth));
        return new RdPlan(size, leftBits, Arrays.copyOf(dictionary, dictionarySize), exceptions);
    }

    /** Bits needed to address {@code dictionarySize} codes; 0 for a single-entry dictionary. */
    static int codeBitsFor(int dictionarySize)
    {
        return dictionarySize <= 1 ? 0 : Integer.SIZE - Integer.numberOfLeadingZeros(dictionarySize - 1);
    }

    static final class RdPlan extends Plan
    {
        final int leftBits;
        final int[] dictionary;
        final int exceptionCount;

        RdPlan(int sizeInBytes, int leftBits, int[] dictionary, int exceptionCount)
        {
            super(sizeInBytes);
            this.leftBits = leftBits;
            this.dictionary = dictionary;
            this.exceptionCount = exceptionCount;
        }

        @Override
        byte subFormat()
        {
            return SUBFORMAT_ALP_RD;
        }

        @Override
        byte[] encode(long[] rawBits, int count)
        {
            int rightBitWidth = Long.SIZE - leftBits;
            long rightMask = (1L << rightBitWidth) - 1;   // rightBitWidth <= 63, so this cannot shift by 64
            int codeBits = codeBitsFor(dictionary.length);

            ByteArrayOutputStream head = new ByteArrayOutputStream();
            head.write(SUBFORMAT_ALP_RD);
            head.write(leftBits);
            head.write(dictionary.length);
            for (int entry : dictionary)
            {
                head.write((entry >>> 8) & 0xFF);
                head.write(entry & 0xFF);
            }
            ColumnarChunkCodec.writeVarLong(head, exceptionCount);

            int previousPosition = -1;
            for (int i = 0; i < count; i++)
            {
                int left = (int) (rawBits[i] >>> rightBitWidth);
                if (codeFor(left) >= 0)
                    continue;
                ColumnarChunkCodec.writeVarLong(head, i - previousPosition - 1);
                head.write((left >>> 8) & 0xFF);
                head.write(left & 0xFF);
                previousPosition = i;
            }

            BitWriter body = new BitWriter();
            for (int i = 0; i < count; i++)
            {
                if (codeBits > 0)
                {
                    // An exception row still spends its code slot, written as a fixed 0 and ignored
                    // on read. Uniform stride costs at most 3 bits per exception and keeps both
                    // loops position-independent -- and a FIXED filler is what makes it deterministic.
                    int code = codeFor((int) (rawBits[i] >>> rightBitWidth));
                    body.writeBits(code < 0 ? 0 : code, codeBits);
                }
                body.writeBits(rawBits[i] & rightMask, rightBitWidth);
            }
            return assemble(head, body);
        }

        private int codeFor(int left)
        {
            for (int code = 0; code < dictionary.length; code++)
                if (dictionary[code] == left)
                    return code;
            return -1;
        }
    }

    // ---------------------------------------------------------------------------------------
    // Decoding
    // ---------------------------------------------------------------------------------------

    /**
     * Decodes {@code count} values out of one section, as raw IEEE-754 bit patterns.
     *
     * @param section positioned at the section's first byte and limited to its last, exactly as
     *                {@link ColumnarChunkCodec} slices a column's data section
     */
    static long[] decode(ByteBuffer section, int count)
    {
        try
        {
            byte subFormat = section.get();
            switch (subFormat)
            {
                case SUBFORMAT_ALP:
                    return decodeAlp(section, count);
                case SUBFORMAT_ALP_RD:
                    return decodeRd(section, count);
                default:
                    throw new IllegalArgumentException("Corrupt columnar chunk: unknown ALP sub-format " + subFormat);
            }
        }
        catch (IndexOutOfBoundsException | BufferUnderflowException e)
        {
            // Every bit-level read goes through BitReader's absolute gets, which report a section
            // that stops short as an index error. Rewrapped so the read path's single "corruption is
            // an IllegalArgumentException, skip this chunk" rule covers a truncated body too.
            throw new IllegalArgumentException("Corrupt columnar chunk: truncated ALP double section", e);
        }
    }

    private static long[] decodeAlp(ByteBuffer section, int count)
    {
        int exponent = section.get() & 0xFF;
        int factor = section.get() & 0xFF;
        int bitWidth = section.get() & 0xFF;
        if (exponent > MAX_EXPONENT || factor > exponent)
            throw new IllegalArgumentException("Corrupt columnar chunk: ALP exponent pair (" + exponent + ", " +
                                               factor + ") outside 0 <= f <= e <= " + MAX_EXPONENT);
        // MAX_BIT_WIDTH is a property of the format, not of this implementation: a frame of
        // reference over integers bounded by ENCODED_LIMIT spans at most 2^54.
        if (bitWidth > MAX_BIT_WIDTH)
            throw new IllegalArgumentException("Corrupt columnar chunk: ALP bit width " + bitWidth + " exceeds " +
                                               MAX_BIT_WIDTH);
        long reference = ColumnarChunkCodec.zigzagDecode(ColumnarChunkCodec.readVarLong(section));
        if (reference < -ENCODED_LIMIT || reference > ENCODED_LIMIT)
            throw new IllegalArgumentException("Corrupt columnar chunk: ALP frame of reference " + reference +
                                               " outside +/-2^53");

        int[] positions = new int[0];
        long[] values = new long[0];
        int exceptionCount = readExceptionCount(section, count);
        if (exceptionCount > 0)
        {
            positions = new int[exceptionCount];
            values = new long[exceptionCount];
            int previousPosition = -1;
            for (int k = 0; k < exceptionCount; k++)
            {
                previousPosition = readExceptionPosition(section, previousPosition, count);
                positions[k] = previousPosition;
                values[k] = readLongBigEndian(section);
            }
        }

        long[] out = new long[count];
        BitReader bits = new BitReader(section);
        int next = 0;
        for (int i = 0; i < count; i++)
        {
            if (next < exceptionCount && positions[next] == i)
            {
                out[i] = values[next++];
            }
            else
            {
                long encoded = bitWidth == 0 ? reference : reference + bits.readBits(bitWidth);
                out[i] = Double.doubleToRawLongBits(decodeOne(encoded, exponent, factor));
            }
        }
        return out;
    }

    private static long[] decodeRd(ByteBuffer section, int count)
    {
        int leftBits = section.get() & 0xFF;
        int dictionarySize = section.get() & 0xFF;
        if (leftBits < 1 || leftBits > RD_MAX_LEFT_BITS)
            throw new IllegalArgumentException("Corrupt columnar chunk: ALP-RD left bit width " + leftBits +
                                               " outside [1, " + RD_MAX_LEFT_BITS + ']');
        if (dictionarySize < 1 || dictionarySize > RD_MAX_DICTIONARY)
            throw new IllegalArgumentException("Corrupt columnar chunk: ALP-RD dictionary size " + dictionarySize +
                                               " outside [1, " + RD_MAX_DICTIONARY + ']');
        int rightBitWidth = Long.SIZE - leftBits;
        int codeBits = codeBitsFor(dictionarySize);

        long[] dictionary = new long[dictionarySize];
        for (int i = 0; i < dictionarySize; i++)
            dictionary[i] = ((section.get() & 0xFFL) << 8) | (section.get() & 0xFFL);

        int exceptionCount = readExceptionCount(section, count);
        int[] positions = new int[exceptionCount];
        long[] lefts = new long[exceptionCount];
        int previousPosition = -1;
        for (int k = 0; k < exceptionCount; k++)
        {
            previousPosition = readExceptionPosition(section, previousPosition, count);
            positions[k] = previousPosition;
            lefts[k] = ((section.get() & 0xFFL) << 8) | (section.get() & 0xFFL);
        }

        long[] out = new long[count];
        BitReader bits = new BitReader(section);
        int next = 0;
        for (int i = 0; i < count; i++)
        {
            long left;
            if (next < exceptionCount && positions[next] == i)
            {
                // The code slot is still present in the stream (the encoder writes a fixed 0 there),
                // so it must be consumed even though the dictionary is not consulted.
                if (codeBits > 0)
                    bits.readBits(codeBits);
                left = lefts[next++];
            }
            else if (codeBits == 0)
            {
                left = dictionary[0];
            }
            else
            {
                int code = (int) bits.readBits(codeBits);
                // codeBits rounds up to a power of two, so a dictionary of, say, 5 entries leaves
                // codes 5..7 expressible: a corrupt body could name one, and must not index past.
                if (code >= dictionarySize)
                    throw new IllegalArgumentException("Corrupt columnar chunk: ALP-RD code " + code +
                                                       " >= dictionary size " + dictionarySize);
                left = dictionary[code];
            }
            out[i] = (left << rightBitWidth) | bits.readBits(rightBitWidth);
        }
        return out;
    }

    /**
     * Reads the exception count and rejects anything a section of {@code count} rows could not
     * legitimately carry -- before it sizes the position/value arrays, so a corrupt varint is a
     * clean reject rather than an {@link OutOfMemoryError}.
     */
    private static int readExceptionCount(ByteBuffer section, int count)
    {
        long exceptionCount = ColumnarChunkCodec.readVarLong(section);
        if (exceptionCount < 0 || exceptionCount > count)
            throw new IllegalArgumentException("Corrupt columnar chunk: ALP exception count " + exceptionCount +
                                               " outside [0, " + count + ']');
        return (int) exceptionCount;
    }

    /** Reads one gap-coded exception position, enforcing strict ascent and the row bound. */
    private static int readExceptionPosition(ByteBuffer section, int previousPosition, int count)
    {
        long gap = ColumnarChunkCodec.readVarLong(section);
        long position = previousPosition + 1 + gap;
        if (gap < 0 || position >= count)
            throw new IllegalArgumentException("Corrupt columnar chunk: ALP exception position " + position +
                                               " outside [0, " + count + ')');
        return (int) position;
    }

    // ---------------------------------------------------------------------------------------
    // Small shared helpers
    // ---------------------------------------------------------------------------------------

    private static byte[] assemble(ByteArrayOutputStream head, BitWriter body)
    {
        byte[] header = head.toByteArray();
        ByteBuffer out = ByteBuffer.allocate(header.length + body.sizeInBytes());
        out.put(header);
        body.writeTo(out);
        return out.array();
    }

    private static void writeLongBigEndian(ByteArrayOutputStream out, long value)
    {
        for (int shift = Long.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE)
            out.write((int) (value >>> shift) & 0xFF);
    }

    private static long readLongBigEndian(ByteBuffer buffer)
    {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++)
            value = (value << 8) | (buffer.get() & 0xFFL);
        return value;
    }

    /** Bits needed to hold {@code span} as an unsigned value; 0 when the span is 0. */
    private static int bitWidthOf(long span)
    {
        return Long.SIZE - Long.numberOfLeadingZeros(span);
    }

    private static int packedBytes(long bits)
    {
        return (int) ((bits + 7) >>> 3);
    }

    /** Bytes an unsigned LEB128 varint of {@code value} occupies -- see ColumnarChunkCodec.writeVarLong. */
    private static int varLen(long value)
    {
        int length = 1;
        while ((value >>>= 7) != 0)
            length++;
        return length;
    }
}
