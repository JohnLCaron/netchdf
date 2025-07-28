package com.sunya.cdm.util

import kotlin.math.abs
import kotlin.math.max

// Note that this method is closely related to the logarithm base 2. For all positive int values x:
//  floor(log2(x)) = 31 - numberOfLeadingZeros(x)
//  ceil(log2(x)) = 32 - numberOfLeadingZeros(x - 1)
// LOOK maybe want ceil ??
internal fun log2(n: Int): Int {
    require(n > 0)
    return 31 - n.countLeadingZeroBits()
}

// Note that this method is closely related to the logarithm base 2. For all positive long values x:
//  floor(log2(x)) = 63 - numberOfLeadingZeros(x)
//  ceil(log2(x)) = 64 - numberOfLeadingZeros(x - 1)
// LOOK maybe want ceil ??
internal fun log2(n: Long): Int {
    require(n > 0)
    return 63 - n.countLeadingZeroBits()
}

/*
fun widenNumber(number: Number): Number {
    when (number) {
        is Byte -> {
            return unsignedByteToShort(number.toByte())
        }

        is Short -> {
            return unsignedShortToInt(number.toShort())
        }

        is Int -> {
            return unsignedIntToLong(number.toInt())
        }

        is Long -> {
            return unsignedLongToBigInt(number.toLong())
        }

        else -> {
            return number
        }
    }
}

 * This method is similar to [.widenNumber], but only integral types *that are negative* are widened.
 *
 * @param number an integral number to treat as unsigned.
 * @return an equivalent value that Java will interpret as non-negative.
 *
fun widenNumberIfNegative(number: Number): Number {
    if (number is Byte && number.toByte() < 0) {
        return unsignedByteToShort(number.toByte())
    } else if (number is Short && number.toShort() < 0) {
        return unsignedShortToInt(number.toShort())
    } else if (number is Int && number.toInt() < 0) {
        return unsignedIntToLong(number.toInt())
    } else if (number is Long && number.toLong() < 0) {
        return unsignedLongToBigInt(number.toLong())
    }
    return number
}

 * Converts the argument to a [BigInteger] by an unsigned conversion. In an unsigned conversion to a
 * [BigInteger], zero and positive `long` values are mapped to a numerically equal [BigInteger]
 * value and negative `long` values are mapped to a [BigInteger] value equal to the input plus
 * 2<sup>64</sup>.
 *
 * @param l a `long` to treat as unsigned.
 * @return the equivalent [BigInteger] value.
 *
fun unsignedLongToBigInt(l: Long): BigInteger {
    // This is a copy of the implementation of Long.toUnsignedBigInteger(), which is private for some reason.
    if (l >= 0L) return BigInteger.valueOf(l) else {
        val upper: Int = (l ushr 32).toInt()
        val lower: Int = l.toInt()

        // return (upper << 32) + lower
        return (BigInteger.valueOf(Integer.toUnsignedLong(upper))).shiftLeft(32)
            .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)))
    }
}

 */

/*
 * Converts the argument to a `long` by an unsigned conversion. In an unsigned conversion to a `long`,
 * the high-order 32 bits of the `long` are zero and the low-order 32 bits are equal to the bits of the integer
 * argument.
 *
 * Consequently, zero and positive `int` values are mapped to a numerically equal `long` value and
 * negative `int` values are mapped to a `long` value equal to the input plus 2<sup>32</sup>.
 *
 * @param i an `int` to treat as unsigned.
 * @return the equivalent `long` value.

fun unsignedIntToLong(i: Int): Long {
    return Integer.toUnsignedLong(i)
}

 * Converts the argument to an `int` by an unsigned conversion. In an unsigned conversion to an `int`,
 * the high-order 16 bits of the `int` are zero and the low-order 16 bits are equal to the bits of the `short` argument.
 *
 * Consequently, zero and positive `short` values are mapped to a numerically equal `int` value and
 * negative `short` values are mapped to an `int` value equal to the input plus 2<sup>16</sup>.
 *
 * @param s a `short` to treat as unsigned.
 * @return the equivalent `int` value.

fun unsignedShortToInt(s: Short): Int {
    return java.lang.Short.toUnsignedInt(s)
}

 * Converts the argument to a `short` by an unsigned conversion. In an unsigned conversion to a `short`, the high-order 8 bits of the `short` are zero and the low-order 8 bits are equal to the bits of
 * the `byte` argument.
 *
 * Consequently, zero and positive `byte` values are mapped to a numerically equal `short` value and
 * negative `byte` values are mapped to a `short` value equal to the input plus 2<sup>8</sup>.
 *
 * @param b a `byte` to treat as unsigned.
 * @return the equivalent `short` value.

fun unsignedByteToShort(b: Byte): Short {
    return java.lang.Byte.toUnsignedInt(b).toShort()
}
*/

////////////////////////////////////////////////////////////////////////
const val defaultMaxRelativeDiffFloat = 1.0e-5f

/** The default maximum relative difference for floats, when comparing as doubles.  */
const val defaultDiffFloat = 1.0e-5

/**
 * The default maximum [relative difference][.relativeDifference] that two doubles can have in
 * order to be deemed [nearly equal][.nearlyEquals].
 */
const val defaultMaxRelativeDiffDouble = 1.0e-8

/** The absolute difference between two floats, i.e. `|a - b|`.  */
fun absoluteDifference(a: Float, b: Float): Float {
    return if (a.compareTo(b) == 0) {
        0f
    } else {
        abs(a - b)
    }
}

/** The absolute difference between two doubles, i.e. `|a - b|`.  */
fun absoluteDifference(a: Double, b: Double): Double {
    return if (a.compareTo(b) == 0) { // Shortcut: handles infinities and NaNs.
        0.0
    } else {
        abs(a - b)
    }
}

/**
 * Returns the relative difference between two numbers, i.e. `|a - b| / max(|a|, |b|)`.
 *
 * For cases where `a == 0`, `b == 0`, or `a` and `b` are extremely close, traditional
 * relative difference calculation breaks down. So, in those instances, we compute the difference relative to
 * Float.MIN_NORMAL, i.e. `|a - b| / Float.MIN_NORMAL`.
 *
 * @param a first number.
 * @param b second number.
 * @return the relative difference.
 * @see [The Floating-Point Guide](http://floating-point-gui.de/errors/comparison/)
 *
 * @see [
 * Comparing Floating Point Numbers, 2012 Edition](https://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/)
 */
fun relativeDifference(a: Float, b: Float): Float {
    val absDiff: Float = absoluteDifference(a, b)
    return if (a == b) {
        0f
    } else {
        val maxAbsValue = max(abs(a), abs(b))
        if (maxAbsValue < defaultMaxRelativeDiffFloat) absDiff else absDiff / maxAbsValue
    }
}

fun relativeDifference(a: Double, b: Double): Double {
    val absDiff: Double = absoluteDifference(a, b)
    return if (a == b) { // Shortcut: handles infinities and NaNs.
        0.0
    } else {
        val maxAbsValue = max(abs(a), abs(b))
        if (maxAbsValue < defaultMaxRelativeDiffDouble) absDiff else absDiff / maxAbsValue
    }
}

/** RelativeDifference is less than maxRelDiff.  */
fun nearlyEquals(a: Float, b: Float, maxRelDiff: Float = defaultMaxRelativeDiffFloat): Boolean {
    return relativeDifference(a, b) < maxRelDiff
}

/** RelativeDifference is less than maxRelDiff.  */
fun nearlyEquals(a: Double, b: Double, maxRelDiff: Double = defaultMaxRelativeDiffDouble): Boolean {
    return relativeDifference(a, b) < maxRelDiff
}
