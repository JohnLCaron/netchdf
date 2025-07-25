@file:OptIn(ExperimentalUnsignedTypes::class)

package com.sunya.cdm.array

import com.fleeksoft.charset.Charset
import com.fleeksoft.charset.Charsets
import com.fleeksoft.charset.decodeToString
import com.sunya.cdm.api.*
import com.sunya.cdm.layout.IndexND
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk

// fake ByteBuffer
class ArrayString(shape : IntArray, val values : List<String>) : ArrayTyped<String>(Datatype.STRING, shape) {

    constructor(shape : IntArray, valueArray : Array<String>) : this (shape, valueArray.toList())

    override fun iterator(): Iterator<String> = StringIterator()
    private inner class StringIterator : AbstractIterator<String>() {
        private var idx = 0
        override fun computeNext() = if (idx >= values.size) done() else setNext(values[idx++])
    }

    override fun showValues(): String {
        return buildString {
            val iter = this@ArrayString.iterator()
            for (value in iter) {
                append("'$value',")
            }
        }
    }

    override fun section(section : Section) : ArrayString {
        val odo = IndexND(IndexSpace(section), this.shape.toLongArray())
        val sectionList = mutableListOf<String>()
        for (index in odo) {
            sectionList.add( values[odo.element().toInt()])
        }
        return ArrayString(section.shape.toIntArray(), sectionList)
    }

    override fun transfer(dst: Any, tc: TransferChunk) {
        val src = this.values
        val dest = dst as MutableList<String>
        repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
    }
}

/**
 * Create a String out of this ByteArray, collapsing all dimensions into one.
 * If there is a null (zero) value in the array, the String will end there.
 * The null is not returned as part of the String.
 *
fun ByteArray.makeStringFromBytes(): String {
    /* var count = 0
    for (c in this) {
        if (c.toInt() == 0) {
            break
        }
        count++
    }
    return String(this, 0, count, Charsets.UTF8) */
    return makeStringZ(this)
} */

/**
 * Create a String out of this ArrayByte, collapsing all dimensions into one.
 * If there is a null (zero) value in the array, the String will end there.
 * The null is not returned as part of the String.
 */
fun ArrayByte.makeStringFromBytes(charset : Charset = Charsets.UTF8): String {
    var count = 0
    for (c in this) {
        if (c.toInt() == 0) {
            break
        }
        count++
    }
    // ByteArray.decodeToString(charset: Charset, off: Int = 0, len: Int = this.size): String {
    return this.values.decodeToString(charset, 0, count)
}

fun ArrayUByte.makeStringFromBytes(charset : Charset = Charsets.UTF8): String {
    var count = 0
    for (c in this) {
        if (c.toInt() == 0) {
            break
        }
        count++
    }
    val ba = this.map { it.toByte()  }.toByteArray()
    return ba.decodeToString(charset, 0, count)
}

/**
 * Create an ArrayString out of this ArrayUByte of any rank.
 * If there is a null (zero) value in the array, the String will end there.
 * The null is not returned as part of the String.
 *
 * @return Array of Strings of rank - 1.
 */
fun ArrayUByte.makeStringsFromBytes(charset : Charset = Charsets.UTF8): ArrayString {
    val rank = shape.size
    if (rank < 2) {
        return ArrayString(intArrayOf(), listOf(makeStringFromBytes()))
    }
    val (outerShape, innerLength) = shape.breakoutInner()
    val outerLength = outerShape.computeSize()

    val result = arrayOfNulls<String>(outerLength)
    val carr = ByteArray(innerLength)
    var cidx = 0
    var sidx = 0
    while (sidx < outerLength) {
        val idx = sidx * innerLength + cidx
        val c: Byte = values[idx].toByte()
        if (c.toInt() == 0) {
            result[sidx++] = makeStringZ(carr, 0, maxBytes = cidx, charset = charset) // String(carr, 0, cidx, Charsets.UTF_8)
            cidx = 0
            continue
        }
        carr[cidx++] = c
        if (cidx == innerLength) {
            result[sidx++] = makeStringZ(carr, charset = charset) // String(carr, Charsets.UTF_8)
            cidx = 0
        }
    }
    return ArrayString(outerShape, Array(outerLength) { result[it]!!} )
}