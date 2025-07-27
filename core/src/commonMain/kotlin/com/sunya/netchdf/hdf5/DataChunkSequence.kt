package com.sunya.netchdf.hdf5

interface DataChunkSequence {
    fun asSequence(): Sequence<DataChunkIF>
}

interface DataChunkIF {
    fun childAddress(): Long
    fun offsets(): LongArray
    fun isMissing(): Boolean
    fun chunkSize(): Int
    fun filterMask(): Int

    fun show(): String
}