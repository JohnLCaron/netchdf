package com.sunya.cdm.array

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Section
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk

class ArrayInt(shape : IntArray, val values: IntArray) : ArrayTyped<Int>(Datatype.INT, shape) {

    override fun iterator(): Iterator<Int> = BufferIterator()
    private inner class BufferIterator : AbstractIterator<Int>() {
        private var idx = 0
        override fun computeNext() = if (idx >= nelems) done() else setNext(values.get(idx++))
    }

    override fun section(section: Section): ArrayInt {
        return ArrayInt(section.shape.toIntArray(), sectionOf(section))
    }

    private fun sectionOf(section: Section): IntArray {
        require(IndexSpace(shape).contains(IndexSpace(section))) { "Variable does not contain requested section" }
        val sectionNelems = section.totalElements.toInt()
        if (sectionNelems == nelems)
            return values

        val dst = IntArray(sectionNelems)
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
        val dest = dst as IntArray
        repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
    }
}
