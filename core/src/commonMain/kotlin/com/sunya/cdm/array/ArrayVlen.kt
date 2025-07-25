package com.sunya.cdm.array

import com.sunya.cdm.api.*
import com.sunya.cdm.layout.IndexND
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk

// maybe should just return primitive array if only one ??
class ArrayVlen<T>(shape : IntArray, val values : List<Array<T>>, val baseType : Datatype<T>)
    : ArrayTyped<Array<T>>(Datatype.VLEN as Datatype<Array<T>>, shape) {

    override fun iterator(): Iterator<Array<T>> = ArrayIterator()
    private inner class ArrayIterator : AbstractIterator<Array<T>>() {
        private var idx = 0
        override fun computeNext() = if (idx >= values.size) done() else setNext(values[idx++])
    }

    override fun showValues(): String {
        return buildString {
            val iter = this@ArrayVlen.iterator()
            var count = 0
            for (value : Array<T> in iter) {
                append("${value.contentToString()},")
                count++
            }
        }
    }

    override fun section(section: Section): ArrayVlen<T> {
        val odo = IndexND(IndexSpace(section), this.shape.toLongArray())
        val sectionList = mutableListOf<Array<T>>()
        for (index in odo) {
            sectionList.add(values[odo.element().toInt()])
        }
        return ArrayVlen(section.shape.toIntArray(), sectionList, baseType)
    }

    override fun transfer(dst: Any, tc: TransferChunk) {
        val src = this.values
        val dest = dst as MutableList<Array<T>>
        repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ArrayVlen<*>) return false

        if (datatype != other.datatype) return false
        if (!shape.equivalent(other.shape)) return false
        if (nelems != other.nelems) return false
        if (baseType != other.baseType) return false

        return vlenEqual(this, other)
    }

    private fun vlenEqual(array1 : ArrayVlen<*>, array2 : ArrayVlen<*>) : Boolean {
        val iter1 = array1.iterator()
        val iter2 = array2.iterator()
        while (iter1.hasNext() && iter2.hasNext()) {
            val v1 : Array<*> = iter1.next()
            val v2 : Array<*> = iter2.next()
            if (!v1.contentEquals(v2)) {
                return false
            }
        }
        return true
    }

    // TODO problem with not using values()
    override fun hashCode(): Int {
        var result = datatype.hashCode()
        result = 31 * result + shape.contentHashCode()
        result = 31 * result + nelems
        result = 31 * result + baseType.hashCode()
        result = 31 * result + values.hashCode()
        return result
    }

    companion object {
        fun fromArray(shape : IntArray, arrays: List<Array<*>>, baseType : Datatype<*>) : ArrayVlen<*> {
            return when (baseType) {
                Datatype.BYTE -> ArrayVlen(shape, arrays.map { it as Array<Byte> }, baseType as Datatype<Byte>)
                Datatype.UBYTE, Datatype.ENUM1 -> ArrayVlen(shape, arrays.map { it as Array<UByte> }, baseType as Datatype<UByte>)
                Datatype.SHORT -> ArrayVlen(shape, arrays.map { it as Array<Short> }, baseType as Datatype<Short>)
                Datatype.USHORT, Datatype.ENUM2 -> ArrayVlen(shape, arrays.map { it as Array<UShort> }, baseType as Datatype<UShort>)
                Datatype.INT -> ArrayVlen(shape, arrays.map { it as Array<Int> }, baseType as Datatype<Int>)
                Datatype.UINT, Datatype.ENUM4 -> ArrayVlen(shape, arrays.map { it as Array<UInt> }, baseType as Datatype<UInt>)
                Datatype.LONG -> ArrayVlen(shape, arrays.map { it as Array<Long> }, baseType as Datatype<Long>)
                Datatype.ULONG, Datatype.ENUM8 -> ArrayVlen(shape, arrays.map { it as Array<ULong> }, baseType as Datatype<ULong>)
                Datatype.FLOAT -> ArrayVlen(shape, arrays.map { it as Array<Float> }, baseType as Datatype<Float>)
                Datatype.DOUBLE -> ArrayVlen(shape, arrays.map { it as Array<Double> }, baseType as Datatype<Double>)
                Datatype.STRING -> ArrayVlen(shape, arrays.map { it as Array<String> }, baseType as Datatype<String>)
                Datatype.OPAQUE -> ArrayVlen(shape, arrays.map { it as Array<ByteArray> }, baseType as Datatype<ByteArray>)
                Datatype.COMPOUND -> ArrayVlen(shape, arrays.map { it as Array<ArrayStructureData.StructureData> }, baseType as Datatype<ArrayStructureData.StructureData>)
                else -> throw IllegalArgumentException("unsupported datatype $baseType")
            }
        }
    }
}