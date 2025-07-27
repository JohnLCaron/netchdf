package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.toLongArray
import com.sunya.cdm.layout.Tiling

interface DataChunkSequence {
    fun asSequence(): Sequence<DataChunk>
}

data class DataChunk(val address: Long, val size: Int, val offsets: IntArray, val filterMask: Int?, val order: Int, val tiling: Tiling?) {
    fun isMissing() = (address <= 0)
    fun show() : String = "order=$order, chunkSize=${size}, chunkStart=${offsets.contentToString()}" +
            ", tile= ${tiling?.tile(offsets.toLongArray()).contentToString()}"
}

fun missingDataChunk(order: Int, tiling: Tiling) : DataChunk {
    return DataChunk(-1, 0, tiling.orderToIndex(order), 0, order, tiling)
}