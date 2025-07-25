package com.sunya.cdm.array

import com.sunya.cdm.api.*
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk

// TODO I dont think you need size anymore.
class ArrayOpaque(shape : IntArray, val values : List<ByteArray>, val size : Int) : ArrayTyped<ByteArray>(Datatype.OPAQUE, shape) {

    override fun iterator(): Iterator<ByteArray> = BufferIterator()
    private inner class BufferIterator : AbstractIterator<ByteArray>() {
        private var idx = 0
        override fun computeNext() = if (idx >= nelems) done() else {
            setNext(values.get(idx++))
        }
    }

    /*
    fun getElement(srcElem : Int) : ByteArray {
        val elem = ByteArray(size)
        copyElem(srcElem, elem, 0)
        return elem
    }

    // copy the src[srcIdx] element the dstIdx element in dest[dstIdx]
    private fun copyElem(srcIdx : Int, dest : ByteArray, dstIdx : Int) {
        repeat(size) { dest.set(dstIdx * size + it, values.get(srcIdx * size + it)) }
    }
     */

    override fun showValues(): String {
        return buildString {
            val iter = this@ArrayOpaque.iterator()
            for (ba in iter) {
                append("'${ba.contentToString()}',")
            }
        }
    }

    override fun section(section : Section) : ArrayOpaque {
        return ArrayOpaque(section.shape.toIntArray(), sectionOf(section), this.size)
    }

    private fun sectionOf(section: Section):  List<ByteArray> {
        require(IndexSpace(shape).contains(IndexSpace(section))) { "Variable does not contain requested section" }
        val sectionNelems = section.totalElements.toInt()
        if (sectionNelems == nelems)
            return values

        val holder = ByteArray(0)
        val dst = MutableList<ByteArray>(sectionNelems) { holder }
        val chunker = Chunker(IndexSpace(this.shape), IndexSpace(section))
        for (chunk : TransferChunk in chunker) {
            val dstIdx = chunk.destElem.toInt()
            val srcIdx = chunk.srcElem.toInt()
            repeat(chunk.nelems) {
                dst[dstIdx + it] = values.get(srcIdx + it)
            }
        }
        return dst
    }

    companion object {
        fun fromByteArray(shape : IntArray, ba : ByteArray, elemSize: Int): ArrayOpaque {
            val nelems = shape.computeSize()
            require(nelems * elemSize == ba.size)
            val values = mutableListOf<ByteArray>()
            var start = 0
            repeat(nelems) {
                values.add( ByteArray(elemSize) { ba[start + it] } )
                start += elemSize
            }
            return ArrayOpaque(shape, values, elemSize)
        }
    }

    override fun transfer(dst: Any, tc: TransferChunk) {
        val src = this.values
        val dest = dst as MutableList<ByteArray>
        repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
    }

}