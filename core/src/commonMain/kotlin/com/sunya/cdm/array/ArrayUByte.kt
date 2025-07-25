package com.sunya.cdm.array

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Section
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk

@OptIn(ExperimentalUnsignedTypes::class)
class ArrayUByte(shape: IntArray, datatype: Datatype<*>, val values: UByteArray) :
    ArrayTyped<UByte>(datatype, shape) {

    constructor(shape: IntArray, values: UByteArray) : this(shape, Datatype.UBYTE, values)

    override fun iterator(): Iterator<UByte> = BufferIterator()
    private inner class BufferIterator : AbstractIterator<UByte>() {
        private var idx = 0
        override fun computeNext() = if (idx >= nelems) done() else setNext(values.get(idx++))
    }

    override fun section(section: Section): ArrayUByte {
        return ArrayUByte(section.shape.toIntArray(), this.datatype, sectionOf(section))
    }

    private fun sectionOf(section: Section): UByteArray {
        require(IndexSpace(shape).contains(IndexSpace(section))) { "Variable does not contain requested section" }
        val sectionNelems = section.totalElements.toInt()
        if (sectionNelems == nelems)
            return values

        val dst = UByteArray(sectionNelems)
        val chunker = Chunker(IndexSpace(this.shape), IndexSpace(section))
        for (chunk: TransferChunk in chunker) {
            val dstIdx = chunk.destElem.toInt()
            val srcIdx = chunk.srcElem.toInt()
            repeat(chunk.nelems) {
                dst[dstIdx + it] = values[srcIdx + it]
            }
        }
        return dst
    }

    override fun transfer(dst: Any, tc: TransferChunk) {
        val src = this.values
        val dest = dst as UByteArray
        repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
    }

    companion object {
        fun fromByteArray(shape: IntArray, values: ByteArray): ArrayUByte =
            ArrayUByte(shape, UByteArray(values.size) { values[it].toUByte() })
        fun fromByteArray(shape: IntArray, datatype: Datatype<*>, values: ByteArray): ArrayUByte =
            ArrayUByte(shape, datatype, UByteArray(values.size) { values[it].toUByte() })
    }
}