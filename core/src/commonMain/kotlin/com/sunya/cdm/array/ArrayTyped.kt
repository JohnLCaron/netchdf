package com.sunya.cdm.array

import com.sunya.cdm.api.*
import com.sunya.cdm.layout.TransferChunk

// here, shape must be integers, since size cant exceed 32 bits
// TODO ArrayTyped<T> is Iterable<T>, but Datatype<T> doesnt have to be T
// TODO all the value arrays are mutable. So all the ArrayTyped are mutable. not good.
abstract class ArrayTyped<T>(val datatype: Datatype<*>, val shape: IntArray) : Iterable<T> {
    val nelems = shape.computeSize()

    override fun toString(): String {
        return buildString {
            append("${this@ArrayTyped::class} shape=${shape.contentToString()} data=")
            append(showValues())
            append("\n")
        }
    }

    open fun showValues(): String {
        return buildString {
            val iter = this@ArrayTyped.iterator()
            var idx = 0
            for (value in iter) {
                if (idx > 0) append(",")
                append(value)
                idx++
            }
        }
    }

    // create a section of this Array. LOOK not checking section against array shape.
    abstract fun section(section : Section) : ArrayTyped<T>

    // TODO
    /* This makes a copy. might do logical sections in the future.
    protected fun sectionFrom(section : Section) : ByteArray {
        require( IndexSpace(shape).contains(IndexSpace(section))) {"Variable does not contain requested section"}
        val sectionNelems = section.totalElements.toInt()
        if (sectionNelems == nelems)
            return ba.ba

        val dst = ByteArray(sectionNelems)
        val chunker = Chunker(IndexSpace(this.shape), IndexSpace(section))
        chunker.transferBA(ba.ba, datatype.size, dst)

        return dst
    } */

    abstract fun transfer(dst: Any, tc: TransferChunk)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ArrayTyped<*>) return false

        if (datatype != other.datatype) return false
        if (!shape.equivalent(other.shape)) return false
        if (nelems != other.nelems) return false
        // cant compare bb because byte-order might be different
        return valuesEqual(this, other)
    }

    // TODO problem with not using values()
    override fun hashCode(): Int {
        var result = datatype.hashCode()
        result = 31 * result + shape.contentHashCode()
        result = 31 * result + nelems
        return result
    }

    companion object {
        fun valuesEqual(array1 : ArrayTyped<*>, array2 : ArrayTyped<*>) : Boolean {
            val iter1 = array1.iterator()
            val iter2 = array2.iterator()
            while (iter1.hasNext() && iter2.hasNext()) {
                val v1 = iter1.next()
                val v2 = iter2.next()
                if ((v1 is ByteArray) && (v2 is ByteArray)) {
                    if (!v1.contentEquals(v2)) {
                        return false
                    }
                } else if (v1 != v2) {
                    return false
                }
            }
            return true
        }

        fun countDiff(array1 : ArrayTyped<*>, array2 : ArrayTyped<*>) : Int {
            val iter1 = array1.iterator()
            val iter2 = array2.iterator()
            var idx = 0
            var count = 0
            while (iter1.hasNext() && iter2.hasNext()) {
                val v1 = iter1.next()
                val v2 = iter2.next()
                if (v1 != v2) {
                    println("idx=$idx $v1 != $v2")
                    count++
                }
                idx++
            }
            return count
        }
    }
}

// An array of any shape that has a single value for all elements, usually the fill value
class ArraySingle<T>(shape : IntArray, datatype : Datatype<*>, fillValueAny : Any) :
        ArrayTyped<T>(datatype, shape) {

    val fillValue = fillValueAny as T
    override fun iterator(): Iterator<T> = SingleIterator()
    private inner class SingleIterator : AbstractIterator<T>() {
        private var idx = 0
        override fun computeNext() = if (idx++ >= nelems) done() else setNext(fillValue)
    }

    override fun toString(): String {
        return buildString {
            append("ArraySingle datatype=$datatype shape=${shape.contentToString()} data= $fillValue (${(fillValue as Any)::class}) \n")
        }
    }

    override fun section(section : Section) : ArrayTyped<T> {
        return ArraySingle(section.shape.toIntArray(), datatype, fillValue as Any)
    }

    override fun transfer(dst: Any, tc: TransferChunk) {
        // hmmmm could be trouble
    }
}

// An empty array of any shape that has no values
class ArrayEmpty<T>(shape : IntArray, datatype : Datatype<*>) : ArrayTyped<T>(datatype, shape) {
    override fun iterator(): Iterator<T> = listOf<T>().iterator()
    override fun section(section : Section) : ArrayTyped<T> {
        return ArrayEmpty(section.shape.toIntArray(), datatype)
    }

    override fun transfer(dst: Any, tc: TransferChunk) {}
}


