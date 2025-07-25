package com.sunya.cdm.array

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Section
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk

@OptIn(ExperimentalUnsignedTypes::class)
class ArrayUInt(shape : IntArray, datatype : Datatype<*>, val values: UIntArray) : ArrayTyped<UInt>(datatype, shape) {

    constructor(shape : IntArray, values: UIntArray) : this(shape, Datatype.UINT, values)

    override fun iterator(): Iterator<UInt> = BufferIterator()
    private inner class BufferIterator : AbstractIterator<UInt>() {
        private var idx = 0
        override fun computeNext() = if (idx >= nelems) done() else setNext(values.get(idx++))
    }

    override fun section(section: Section): ArrayUInt {
        return ArrayUInt(section.shape.toIntArray(), this.datatype, sectionOf(section))
    }

    private fun sectionOf(section: Section): UIntArray {
        require(IndexSpace(shape).contains(IndexSpace(section))) { "Variable does not contain requested section" }
        val sectionNelems = section.totalElements.toInt()
        if (sectionNelems == nelems)
            return values

        val dst = UIntArray(sectionNelems)
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
        val dest = dst as UIntArray
        repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
    }

    companion object {
        fun fromIntArray(shape : IntArray, values : IntArray): ArrayUInt =
            ArrayUInt(shape, UIntArray(values.size) { values[it].toUInt() } )
    }

}