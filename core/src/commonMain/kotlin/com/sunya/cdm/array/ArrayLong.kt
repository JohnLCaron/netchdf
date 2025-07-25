package com.sunya.cdm.array

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Section
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk

class ArrayLong(shape : IntArray, val values: LongArray) : ArrayTyped<Long>(Datatype.LONG, shape) {

    override fun iterator(): Iterator<Long> = BufferIterator()
    private inner class BufferIterator : AbstractIterator<Long>() {
        private var idx = 0
        override fun computeNext() = if (idx >= nelems) done() else setNext(values.get(idx++))
    }

    override fun section(section : Section) : ArrayLong {
        return ArrayLong(section.shape.toIntArray(), sectionOf(section))
    }

    private fun sectionOf(section: Section): LongArray {
        require(IndexSpace(shape).contains(IndexSpace(section))) { "Variable does not contain requested section" }
        val sectionNelems = section.totalElements.toInt()
        if (sectionNelems == nelems)
            return values

        val dst = LongArray(sectionNelems)
        val chunker = Chunker(IndexSpace(this.shape), IndexSpace(section))
        for (chunk : TransferChunk in chunker) {
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
        val dest = dst as LongArray
        repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
    }
}