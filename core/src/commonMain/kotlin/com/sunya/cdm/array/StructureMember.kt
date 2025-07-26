@file:OptIn(ExperimentalUnsignedTypes::class)

package com.sunya.cdm.array

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.EnumTypedef
import com.sunya.cdm.api.computeSize
import com.sunya.cdm.array.ArrayStructureData.StructureData
import com.sunya.cdm.util.makeValidCdmObjectName

// dim lengths here are ints; Hdf4,5 only supports ints.
/**
 * @param offset byte offset into the StructureData.ByteArray
 * @param isBE is BIG_ENDIAN
 */
class StructureMember<T>(orgName: String, val datatype : Datatype<T>, val offset: Int, val shape : IntArray, val isBE : Boolean) {
    val name = makeValidCdmObjectName(orgName)
    val nelems = shape.computeSize()

    /**
     * Get the value of this member from the given StructureData.
     * return T for nelems = 1, ArrayTyped<T> for nelems > 1
     */
    fun value(sdata: StructureData): Any {
        val offset = sdata.offset + this.offset

        if (nelems > 1 && !datatype.isVlenString && (datatype != Datatype.VLEN)) {
            val tba = TypedByteArray(this.datatype, sdata.ba, offset, this.isBE)
            return tba.convertToArrayTyped(shape) // TODO charToString = ??
        }

        val enumTypedef = if (!datatype.isEnum || this.datatype.typedef == null) null
            else this.datatype.typedef as EnumTypedef

        return when (datatype) {
            Datatype.BYTE -> sdata.ba.get(offset)
            Datatype.SHORT -> convertToShort(sdata.ba, offset, this.isBE)
            Datatype.INT -> convertToInt(sdata.ba, offset, this.isBE)
            Datatype.LONG -> convertToLong(sdata.ba, offset, this.isBE)
            Datatype.UBYTE, Datatype.CHAR -> sdata.ba.get(offset).toUByte()
            Datatype.ENUM1 -> {
                val evalue = sdata.ba.get(offset).toInt()
                enumTypedef?.convertEnum(evalue) ?: evalue
            }
            Datatype.USHORT -> convertToShort(sdata.ba, offset, this.isBE).toUShort()
            Datatype.ENUM2 -> {
                val evalue = convertToShort(sdata.ba, offset, this.isBE).toInt()
                enumTypedef?.convertEnum(evalue) ?: evalue
            }
            Datatype.UINT -> convertToInt(sdata.ba, offset, this.isBE).toUInt()
            Datatype.ENUM4 -> {
                val evalue = convertToInt(sdata.ba, offset, this.isBE)
                enumTypedef?.convertEnum(evalue) ?: evalue
            }
            Datatype.ULONG -> convertToLong(sdata.ba, offset, this.isBE).toULong()
            Datatype.ENUM8 -> {
                val evalue = convertToLong(sdata.ba, offset, this.isBE).toInt()
                enumTypedef?.convertEnum(evalue) ?: evalue
            }
            Datatype.FLOAT -> convertToFloat(sdata.ba, offset, this.isBE)
            Datatype.DOUBLE -> convertToDouble(sdata.ba, offset, this.isBE)
            Datatype.STRING -> {
                if (datatype.isVlenString) {
                    val ret = sdata.getFromHeap(offset)
                    if (ret is List<*>) {
                        if (ret.size == 1) ret[0]!! else ArrayString(intArrayOf(ret.size), ret as List<String>)
                    } else if (ret is String) {
                        ret
                    } else {
                        "unknown $ret"
                    }
                } else {
                    makeStringZ(sdata.ba, offset, nelems * datatype.size) // TODO what about non-hdf5 ?
                }
            }
            Datatype.VLEN -> {
                val ret = sdata.getFromHeap(offset)
                if (ret != null) (ret as ArrayVlen<*>) else {
                    throw RuntimeException("cant find ArrayVlen on heap at $offset")
                }
            }
            else -> makeStringZ(sdata.ba, offset, nelems) // // wtf?
        }
    }

    /** Same as value(sdata: ArrayStructureData.StructureData), except wrap scalars in ArrayTyped. */
    fun values(sdata: StructureData): ArrayTyped<*> {
        val value = value(sdata)
        if (value is ArrayTyped<*>) return value

        if (value is String) return ArrayString(intArrayOf(1), listOf(value))

        return when (datatype) {
            Datatype.BYTE -> ArrayByte(intArrayOf(1), ByteArray(1) { value as Byte })
            Datatype.SHORT -> ArrayShort(intArrayOf(1), ShortArray(1) { value as Short })
            Datatype.INT -> ArrayInt(intArrayOf(1), IntArray(1) { value as Int })
            Datatype.LONG -> ArrayLong(intArrayOf(1), LongArray(1) { value as Long })
            Datatype.UBYTE, Datatype.CHAR -> ArrayUByte(intArrayOf(1), UByteArray(1) { value as UByte })
            Datatype.ENUM1, Datatype.ENUM2, Datatype.ENUM4, Datatype.ENUM8, -> ArrayInt(intArrayOf(1), IntArray(1) { value as Int })
            Datatype.USHORT -> ArrayUShort(intArrayOf(1), UShortArray(1) { value as UShort })
            Datatype.UINT -> ArrayUInt(intArrayOf(1), UIntArray(1) { value as UInt })
            Datatype.ULONG -> ArrayULong(intArrayOf(1), datatype, ULongArray(1) { value as ULong })
            Datatype.FLOAT -> ArrayFloat(intArrayOf(1), FloatArray(1) { value as Float })
            Datatype.DOUBLE -> ArrayDouble(intArrayOf(1), DoubleArray(1) { value as Double })
            Datatype.STRING -> ArrayString(intArrayOf(1), listOf(value as String))
            else -> throw RuntimeException("StructureMember.values datatype $datatype")
        }
    }

    // iterator over all the member values
    fun values(arraysd: ArrayStructureData): Iterator<T> = DoubleIterator(arraysd.iterator(), this)

    override fun toString(): String {
        return "\nStructureMember(name='$name', datatype=$datatype, offset=$offset, dims=${shape.contentToString()}, nelems=$nelems)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is StructureMember<*>) return false

        if (name != other.name) return false
        if (datatype != other.datatype) return false
        if (!shape.contentEquals(other.shape)) return false
        return nelems == other.nelems
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + datatype.hashCode()
        result = 31 * result + shape.contentHashCode()
        result = 31 * result + nelems
        return result
    }
}

// double iterator (iterator of iterator)
class DoubleIterator<T>(val sdataIter: Iterator<StructureData>, val member: StructureMember<T>) : AbstractIterator<T>() {
    var valueIterator : Iterator<T>

    init {
        val sdata = sdataIter.next()
        val sda = member.values(sdata)
        valueIterator = sda.iterator() as Iterator<T>
    }

    override fun computeNext() {
        if (valueIterator.hasNext()) {
            setNext(valueIterator.next())

        } else if (sdataIter.hasNext()) {
            val sdata = sdataIter.next()
            val sda = member.values(sdata)
            valueIterator = sda.iterator() as Iterator<T>
            setNext(valueIterator.next())

        } else {
            done()
        }
    }
}