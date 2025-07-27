package com.sunya.netchdf.hdf5

import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState

// for H5readConcurrent
class OpenFileExtended(val delegate: OpenFileIF,
                       val isLengthLong: Boolean,
                       val isOffsetLong: Boolean,
                       val startingOffset: Long, ) : OpenFileIF by delegate {

    fun readLength(state : OpenFileState): Long {
        return if (isLengthLong) delegate.readLong(state) else delegate.readInt(state).toLong()
    }

    fun readOffset(state : OpenFileState): Long {
        return if (isOffsetLong) delegate.readLong(state) else delegate.readInt(state).toLong()
    }

    fun sizeOffsets(): Int {
        return if (isOffsetLong) 8 else 4
    }

    fun getFileOffset(address: Long): Long {
        return startingOffset + address
    }

    fun readAddress(state : OpenFileState): Long {
        return getFileOffset(readOffset(state))
    }

    fun readVariableSizeUnsigned(state : OpenFileState, size: Int): Long {
        val vv: Long
        when (size) {
            1 -> vv = delegate.readByte(state).toUByte().toLong()
            2 -> vv = delegate.readShort(state).toUShort().toLong()
            4 -> vv = delegate.readInt(state).toUInt().toLong()
            8 -> vv = delegate.readLong(state)
            else -> vv = readVariableSizeN(state, size)
        }
        return vv
    }

    fun readVariableSizeDimension(state : OpenFileState, size: Byte): Int {
        val vv: Int
        val sizeInt = size.toInt()
        when (sizeInt) {
            1 -> vv = delegate.readByte(state).toUByte().toInt()
            2 -> vv = delegate.readShort(state).toUShort().toInt()
            4 -> vv = delegate.readInt(state).toUInt().toInt()
            else -> {
                val vs = readVariableSizeN(state, sizeInt)
                vv = vs.toInt()
            }
        }
        return vv
    }

    private fun readVariableSizeN(state : OpenFileState, nbytes : Int): Long {
        val ch = IntArray(nbytes)
        for (i in 0 until nbytes) ch[i] = delegate.readByte(state).toInt()
        var result = ch[nbytes - 1].toLong()
        for (i in nbytes - 2 downTo 0) {
            result = result shl 8
            result += ch[i].toLong()
        }
        return result
    }


    override fun close() = delegate.close()
}