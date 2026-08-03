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

import java.util.Comparator;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * The comparison order a chunk-format-v4 statistic was computed under (§4), and the comparator that
 * order denotes.
 *
 * <p><b>This is the field that makes pruning sound by construction.</b> §4: <i>a reader may use a
 * statistic only when its {@code statOrder} matches the comparator it intends to prune with.</i> A
 * min/max pair is not a number, it is a pair of extrema <em>under some order</em>, and the orders a
 * time-series column can plausibly be stored in disagree in ways that are invisible in a round-trip
 * test: {@code date} is an unsigned day count with the epoch at {@code 0x80000000}, so unsigned and
 * signed comparison of the very same four bytes put 1969 on opposite sides of 1971;
 * {@code double}'s serialized bits are not byte-comparable across the sign; {@code decimal} and
 * {@code timeuuid} have no byte order at all. Recording the order beside the stat turns "these
 * extrema may not mean what you think" from a comment into a check the reader can make.
 *
 * <p><b>The failure this prevents is a wrong answer, not a slow one.</b> §1: a pruning statistic can
 * only be wrong in the direction of opening a block that did not need opening. That is true only
 * while the order matches. Under a mismatched order the extrema are not bounds at all, the reader
 * skips a block that does contain matching rows, and the query returns fewer rows than it should --
 * silently, for one column, on one shape of data.
 *
 * <p><b>Every comparator here was checked against the type it claims to describe</b>, by reading
 * {@code AbstractType.compare} and the {@code compareCustom}/{@code ComparisonType.BYTE_ORDER}
 * implementations rather than by assuming. Three findings shaped the mapping:
 *
 * <ul>
 * <li>{@code time} ({@link TimeType}) is {@code ComparisonType.BYTE_ORDER}, i.e. <b>unsigned</b>
 *     bytes, not signed like {@code bigint}/{@code timestamp}. Over its legal domain
 *     (0 .. 86399999999999 nanoseconds of day) signed and unsigned agree, so a signed declaration
 *     would pass every test built from legal values and disagree with Cassandra only on a value
 *     with bit 63 set. It is mapped {@link #UNSIGNED_INT}, which is what the comparator actually
 *     does, on the principle that the declared order must match the real comparator and not the
 *     domain we expect to see.</li>
 * <li>{@code date} ({@link SimpleDateType}) is likewise {@code BYTE_ORDER}, and its serializer
 *     shifts by 2^31 precisely so unsigned byte order is chronological order -- confirmed at
 *     {@code SimpleDateSerializer.timeInMillisToDay}, where the epoch maps to {@code 0x80000000}.
 *     {@code int} of the same width is {@code CUSTOM} and signed. The two four-byte types therefore
 *     need different orders, which is the case §4 names first.</li>
 * <li>Every one of these types sorts an <b>empty value first</b> -- the {@code CUSTOM} ones via an
 *     explicit {@code Boolean.compare(right.isEmpty, left.isEmpty)} guard, the {@code BYTE_ORDER}
 *     ones via the {@code length1 - length2} tie-break at the bottom of {@code FastByteOperations}.
 *     The comparators below reproduce that rather than rejecting a zero-length value, because a
 *     zero-length value in a fixed-width column is exactly the input §12 names as the trigger for
 *     the {@code OPAQUE} downgrade, and a comparator that throws on it would make the soundness
 *     test unable to cover the case it exists for.</li>
 * </ul>
 *
 * <p>{@link #UNSIGNED_INT} and {@link #BYTES_UNSIGNED} are the same function today: unsigned
 * big-endian comparison of two equal-width values <em>is</em> unsigned lexicographic comparison of
 * their bytes. They stay distinct code points because §4 requires the reader to match on the
 * declared <em>meaning</em> -- a reader pruning a {@code date} range and a reader pruning a
 * {@code blob} prefix are making different claims about the values, and a v5 that adds a fixed-width
 * order which is not lexicographic (or a variable-width one which is not numeric) must not find the
 * two already conflated.
 *
 * <p>Anything not listed maps to {@link #NONE}, and {@code NONE} is not a fallback comparator -- it
 * has none. {@code FloatType} is the deliberate example: {@code float} orders by
 * {@code Float.compare}, which {@link #IEEE754_TOTAL} (defined by §4 as {@code Double.compare} over
 * eight bytes) does not describe, so a float column carries no usable stats rather than
 * approximately usable ones. {@code BooleanType} is the second: it normalises any non-zero byte to
 * true before comparing, so no byte order describes it -- which is why §5 gives boolean blocks the
 * {@code HAS_FALSE}/{@code HAS_TRUE} flags instead of extrema. Unknown codes are corruption, not a
 * future format (§11).
 */
public enum StatOrder implements Comparator<byte[]>
{
    /**
     * No order: the statistic, if any, may not be used to prune. §4 forces this on any column
     * downgraded to {@code OPAQUE}, and {@link ChunkV4Directory} additionally refuses to carry
     * statistics at all under it, so {@code NONE} means "no stats" everywhere in a v4 payload.
     */
    NONE(0x00)
    {
        public int compare(byte[] left, byte[] right)
        {
            // Not an IllegalArgumentException: a corrupt payload cannot reach here, because the
            // directory rejects stats under NONE. Reaching here means a reader ignored §4's
            // matching rule, which is a bug in the reader and not a property of the bytes.
            throw new UnsupportedOperationException("statOrder NONE declares no comparison order; a statistic " +
                                                    "recorded under it may never be used to prune (v4 §4)");
        }
    },

    /**
     * Big-endian two's complement, as {@link Int32Type}, {@link LongType} and {@link TimestampType}
     * compare. The algorithm below is theirs verbatim -- signed comparison of byte 0, then unsigned
     * comparison of the rest -- rather than "sign-extend to a long and compare", so that it agrees
     * with them on the inputs where the two formulations could differ: an empty value, and two
     * values of unequal length.
     */
    SIGNED_INT(0x01)
    {
        public int compare(byte[] left, byte[] right)
        {
            if (left.length == 0 || right.length == 0)
                return emptyFirst(left, right);
            // Int32Type.compareCustom / LongType.compareLongs: the leading byte carries the sign, so
            // it is compared as a signed byte and every byte after it as unsigned.
            int diff = left[0] - right[0];
            return diff != 0 ? diff : compareUnsignedBytes(left, right);
        }
    },

    /**
     * Big-endian unsigned, as {@link SimpleDateType} and {@link TimeType} compare -- both are
     * {@code ComparisonType.BYTE_ORDER}, so this is byte-for-byte their comparator.
     */
    UNSIGNED_INT(0x02)
    {
        public int compare(byte[] left, byte[] right)
        {
            return compareUnsignedBytes(left, right);
        }
    },

    /**
     * {@code Double.compare} over the eight IEEE-754 bytes, as {@link DoubleType} compares: -0.0
     * sorts below 0.0, NaN sorts above every finite value and above +Infinity, and all NaN payloads
     * compare equal because {@code Double.compare} goes through {@code doubleToLongBits}.
     *
     * <p>This is emphatically <b>not</b> a comparison of the raw bits: the raw bits of a negative
     * double ascend as the value descends. §4 lists {@code float}/{@code double} among the types
     * where byte order and type order diverge, and this is that divergence.
     */
    IEEE754_TOTAL(0x03)
    {
        public int compare(byte[] left, byte[] right)
        {
            if (left.length == 0 || right.length == 0)
                return emptyFirst(left, right);
            return Double.compare(toDouble(left), toDouble(right));
        }
    },

    /**
     * Unsigned lexicographic bytes, shorter-is-smaller on a prefix tie, as {@link UTF8Type},
     * {@link AsciiType} and {@link BytesType} compare. All three are {@code BYTE_ORDER} and none
     * adds any comparison logic of its own, so one comparator covers the three.
     */
    BYTES_UNSIGNED(0x04)
    {
        public int compare(byte[] left, byte[] right)
        {
            return compareUnsignedBytes(left, right);
        }
    };

    /** The {@code statOrder} byte written to the directory entry (§4). */
    public final int code;

    StatOrder(int code)
    {
        this.code = code;
    }

    /**
     * Compares two serialized values under this order. Declared abstract so that every code point
     * is forced to name its comparator at the point it is added: a new {@code statOrder} code (§9
     * permits them without a version bump) cannot be introduced as a bare constant that silently
     * inherits somebody else's ordering.
     */
    public abstract int compare(byte[] left, byte[] right);

    /** False only for {@link #NONE}; callers must check before comparing. */
    public boolean hasOrder()
    {
        return this != NONE;
    }

    /**
     * The order for {@code code}, rejecting anything else as corruption.
     *
     * <p>§9 allows a future build to add {@code statOrder} codes without a version bump, but a code
     * this build does not know, arriving inside a payload whose version byte says 4, is a corrupt
     * byte and not a message from the future -- §11: if it were a future format the version byte
     * would differ. Accepting it would mean carrying extrema under an order we cannot evaluate,
     * which is the one mistake that costs rows rather than time.
     */
    public static StatOrder forCode(int code)
    {
        switch (code)
        {
            case 0x00: return NONE;
            case 0x01: return SIGNED_INT;
            case 0x02: return UNSIGNED_INT;
            case 0x03: return IEEE754_TOTAL;
            case 0x04: return BYTES_UNSIGNED;
            default:
                throw new IllegalArgumentException("Corrupt v4 chunk: unknown statOrder code " + code);
        }
    }

    /**
     * The order a column of {@code type} may declare, or {@link #NONE} when no code describes the
     * type's comparator.
     *
     * <p><b>Matched by identity against the singleton instances, not by {@code instanceof}.</b> A
     * subclass may override {@code compareCustom} or flip {@code ComparisonType}, and this method's
     * whole job is to promise that the returned order <em>is</em> the type's order; a subclass that
     * quietly changes it would break that promise while passing an {@code instanceof} test. Identity
     * degrades to {@link #NONE}, which costs a little pruning and cannot cost a row.
     *
     * <p>{@code ReversedType} is not unwrapped, and so lands on {@link #NONE}: min and max exchange
     * roles under a reversed comparator, nothing in the format records which direction was used, and
     * a reader that matched on the wrapped type's order would prune from the wrong end.
     */
    public static StatOrder forType(AbstractType<?> type)
    {
        if (type == null)
            throw new IllegalArgumentException("null column type has no statOrder");
        if (type == Int32Type.instance || type == LongType.instance || type == TimestampType.instance)
            return SIGNED_INT;
        // date and time are BYTE_ORDER types: date because its serializer shifts by 2^31 to make
        // unsigned bytes chronological, time because its domain is non-negative nanoseconds of day.
        if (type == SimpleDateType.instance || type == TimeType.instance)
            return UNSIGNED_INT;
        if (type == DoubleType.instance)
            return IEEE754_TOTAL;
        if (type == UTF8Type.instance || type == AsciiType.instance || type == BytesType.instance)
            return BYTES_UNSIGNED;
        return NONE;
    }

    // -----------------------------------------------------------------------------------------
    // shared comparison primitives
    // -----------------------------------------------------------------------------------------

    /**
     * Unsigned lexicographic with a {@code length1 - length2} tie-break: the exact contract of
     * {@code FastByteOperations.compareUnsigned}, which is where every {@code BYTE_ORDER} type in
     * {@code db.marshal} ends up. Reimplemented rather than delegated so that this class depends on
     * the marshal package for the mapping only, and so the agreement test compares two independent
     * expressions of the rule instead of one expression with itself.
     */
    private static int compareUnsignedBytes(byte[] left, byte[] right)
    {
        int shared = Math.min(left.length, right.length);
        for (int i = 0; i < shared; i++)
        {
            int diff = (left[i] & 0xFF) - (right[i] & 0xFF);
            if (diff != 0)
                return diff;
        }
        return left.length - right.length;
    }

    /**
     * The {@code AbstractType.compareComposed} guard: an empty value sorts before every non-empty
     * one and equals another empty. The argument order looks inverted and is not -- it is copied
     * from the marshal package, where {@code Boolean.compare(right.isEmpty, left.isEmpty)} yields
     * -1 when the left value is the empty one.
     */
    private static int emptyFirst(byte[] left, byte[] right)
    {
        return Boolean.compare(right.length == 0, left.length == 0);
    }

    private static double toDouble(byte[] value)
    {
        // A double that is neither empty nor eight bytes is not a double. Cassandra's own path
        // raises a buffer exception here; v4 reports corruption as IllegalArgumentException
        // uniformly, so a stat of the wrong width fails as corruption rather than as a JDK error
        // escaping the codec.
        if (value.length != 8)
            throw new IllegalArgumentException("IEEE754_TOTAL needs an 8-byte value, got " + value.length);
        long bits = 0;
        for (int i = 0; i < 8; i++)
            bits = (bits << 8) | (value[i] & 0xFFL);
        return Double.longBitsToDouble(bits);
    }
}
