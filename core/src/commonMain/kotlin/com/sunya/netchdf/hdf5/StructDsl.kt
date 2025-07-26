package com.sunya.netchdf.hdf5

import com.sunya.cdm.array.convertToShort
import com.sunya.cdm.array.convertToInt
import com.sunya.cdm.array.convertToLong
import com.sunya.cdm.array.makeStringZ
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState

/** Experimental DSL for reading structure data from file */
internal class StructDsl(val name : String, val ba : ByteArray, val isBE: Boolean, val flds : List<StructFld>, val startPos : Long) {
    val fldm = flds.associateBy { it.fldName }

    fun dataSize() = ba.size

    fun show() {
        println("$name startPos = $startPos")
        flds.filter{ it.fldName.isNotEmpty() }.forEach {
            println("  $it = ${show(it)}")
        }
    }

    fun show(fld : StructFld) : Any {
        if (fld.nelems == 0) {
            return "empty"
        }
        // hmmmm
        if (fld.nelems > 1 && fld.elemSize != 1) {
            return when (fld.elemSize) {
                2 -> Array<Short>(fld.nelems) { idx -> convertToShort(ba, fld.pos + 2 * idx, isBE) }
                4 -> Array<Int>(fld.nelems) { idx -> convertToInt(ba, fld.pos + 4 * idx, isBE) }
                8 -> Array<Long>(fld.nelems) { idx -> convertToLong(ba, fld.pos + 8 * idx, isBE) }
                else -> getString(fld.fldName)
            }
        }

        return when(fld.elemSize) {
            1 -> ba.get(fld.pos)
            2 -> convertToShort(ba, fld.pos, isBE)
            4 -> convertToInt(ba, fld.pos, isBE)
            8 -> convertToLong(ba, fld.pos, isBE)
            else -> getString(fld.fldName)
        }
    }

    fun getLong(fldName : String) : Long {
        val fld = fldm[fldName] ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        return if (fld.elemSize == 4) convertToInt(ba, fld.pos, isBE).toLong()
        else convertToLong(ba, fld.pos, isBE)
    }
    fun getInt(fldName : String) : Int {
        val fld = fldm[fldName] ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        return if (fld.elemSize == 8) convertToLong(ba, fld.pos, isBE).toInt()
        else convertToInt(ba, fld.pos, isBE)
    }
    fun getByte(fldName : String) : Byte {
        val fld = fldm[fldName] ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        require(fld.elemSize == 1) { fldName }
        return ba.get(fld.pos)
    }
    fun getShort(fldName : String) : Short {
        val fld = fldm[fldName] ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        require(fld.elemSize == 2) { fldName }
        return convertToShort(ba, fld.pos, isBE)
    }
    fun getIntArray(fldName : String) : IntArray {
        val fld = fldm[fldName] ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        return when (fld.elemSize) {
            4 -> IntArray(fld.nelems) { idx -> convertToInt(ba, fld.pos + fld.elemSize * idx, isBE) }
            8 -> IntArray(fld.nelems) { idx -> convertToLong(ba, fld.pos + fld.elemSize * idx, isBE).toInt() }
            else -> throw RuntimeException("$fld must be 4 or 8")
        }
    }
    fun getLongArray(fldName : String) : LongArray {
        val fld = fldm[fldName] ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        return when (fld.elemSize) {
            4 -> LongArray(fld.nelems) { idx -> convertToInt(ba,fld.pos + fld.elemSize * idx, isBE).toLong() }
            8 -> LongArray(fld.nelems) { idx -> convertToLong(ba,fld.pos + fld.elemSize * idx, isBE) }
            else -> throw RuntimeException("$fld must be 4 or 8")
        }
    }
    fun getByteArray(fldName : String) : ByteArray {
        val fld = fldm[fldName] ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        return ByteArray(fld.nelems * fld.elemSize) { ba[fld.pos + it] } // LOOK
    }
    fun getString(fldName : String) : String {
        val fld = fldm[fldName] ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        return makeStringZ(ba, fld.pos, fld.nelems * fld.elemSize)
    }
}

internal class StructDslBuilder(val name : String, val raf: OpenFileIF, val state: OpenFileState) {
    val flds = mutableListOf<StructFld>()
    val startPos = state.pos
    var pos = 0

    fun fld(fldName: String, length: Int): StructFld {
        val fld = StructFld(fldName, pos, length)
        flds.add(fld)
        pos += length
        return fld
    }

    fun fld(fldName: String, lambda: StructDslBuilder.() -> Int): StructFld {
        val length = lambda()
        val fld = StructFld(fldName, pos, length)
        flds.add(fld)
        pos += length
        return fld
    }

    fun overlay(fldName: String, offset: Int, overlayName: String) {
        val fld = flds.find { it.fldName == fldName }
            ?: throw IllegalArgumentException("StructDsl $name has no fld '$fldName'")
        val overlay = StructFld(overlayName, fld.pos + offset, 0)
        flds.add(overlay)
    }

    fun array(fldName: String, length: Int, nelemsFld: String) {
        val fld = flds.find { it.fldName == nelemsFld }
            ?: throw IllegalArgumentException("StructDsl $name has no nelemsFld '$fldName'")
        val nelems: Int = eagerRead(fld)
        val overlay = StructFld(fldName, pos, length, nelems)
        pos += length * nelems
        flds.add(overlay)
    }

    fun eagerRead(from: StructFld) : Int {
        val tstate = state.copy(pos = startPos + from.pos)
        val ba = raf.readByteArray(tstate, from.elemSize)
        return when (from.elemSize) {
            1 -> ba.get(0).toInt()
            2 -> convertToShort(ba, 0, state.isBE).toInt()
            4 -> convertToInt(ba, 0, state.isBE)
            8 -> convertToInt(ba, 0, state.isBE) // ignore extra bytes
            else -> throw IllegalArgumentException("StructDsl $name illegal eager read length=${from.elemSize}")
        }
    }

    fun skip(nbytes: Int) {
        fld("", nbytes)
    }

    fun build(): StructDsl {
        val total = flds.sumOf { it.elemSize * it.nelems }
        val bb = raf.readByteArray(state, total)
        return StructDsl(name, bb, state.isBE, flds, startPos)
    }
}

internal data class StructFld(val fldName: String, val pos: Int, val elemSize: Int, val nelems : Int) {
    constructor(fldName: String, pos: Int, length: Int) : this(fldName, pos, length, 1)

    override fun toString(): String {
        return "$fldName, $pos, $elemSize"
    }
}

internal fun structdsl(name : String, raf : OpenFileIF, state : OpenFileState, lambda: StructDslBuilder.() -> Unit): StructDsl {
    val builder = StructDslBuilder(name, raf, state)
    builder.lambda()
    return builder.build()
}