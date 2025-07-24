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

    fun getFileOffset(address: Long): Long {
        return startingOffset + address
    }

    fun readAddress(state : OpenFileState): Long {
        return getFileOffset(readOffset(state))
    }

    override fun close() = delegate.close()
}