package com.sunya.cdm.array

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Section
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk

@OptIn(ExperimentalUnsignedTypes::class)
class ArrayUShort(shape : IntArray, datatype : Datatype<*>, val values: UShortArray) : ArrayTyped<UShort>(datatype, shape) {

    constructor(shape : IntArray, values: UShortArray) : this(shape, Datatype.USHORT, values)

    override fun iterator(): Iterator<UShort> = BufferIterator()
    private inner class BufferIterator : AbstractIterator<UShort>() {
        private var idx = 0
        override fun computeNext() = if (idx >= nelems) done() else setNext(values.get(idx++))
    }

    override fun section(section: Section): ArrayUShort {
        return ArrayUShort(section.shape.toIntArray(), this.datatype, sectionOf(section))
    }

    private fun sectionOf(section: Section): UShortArray {
        require(IndexSpace(shape).contains(IndexSpace(section))) { "Variable does not contain requested section" }
        val sectionNelems = section.totalElements.toInt()
        if (sectionNelems == nelems)
            return values

        val dst = UShortArray(sectionNelems)
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
        val dest = dst as UShortArray
        repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
    }

    companion object {
        fun fromShortArray(shape : IntArray, values : ShortArray): ArrayUShort =
            ArrayUShort(shape, UShortArray(values.size) { values[it].toUShort() } )
    }

}