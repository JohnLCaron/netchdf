@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.computeSize
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.util.InternalLibraryApi
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.math.ceil

// DataLayoutMessage version 4, layout class 2 (chunked), chunkIndexingType 1-5
// jhdf

@OptIn(InternalLibraryApi::class)
internal fun readChunkedDataLayoutMessageV4(builder: H5builder, raf: OpenFileIF, state : OpenFileState) : DataLayoutMessage {
    val version = raf.readByte(state)
    val layoutClass = raf.readByte(state)
    val flags = raf.readByte(state)
    val rank = raf.readByte(state).toInt()
    val dimSizeLength = raf.readByte(state)
    val chunkDimensions = IntArray(rank) { builder.readVariableSizeDimension(state, dimSizeLength) }  // TODO is dimSizeLength correct ??
    val chunkIndexingType = raf.readByte(state).toInt()

    var chunkSize = chunkDimensions.computeSize()

    return when (chunkIndexingType) {
        1 -> {
                var filterMask = 0
                if (isBitSet(flags.toInt(), 1)) {
                    chunkSize = builder.readLength(state).toInt() // Utils.readBytesAsUnsignedInt(bb, sb.getSizeOfLengths())
                    filterMask = raf.readInt(state) // java.util.BitSet.valueOf(byteArrayOf(bb.get(), bb.get(), bb.get(), bb.get()))
                }
                // Address of the single chunk. size specified in “Size of Lengths” field in the superblock.
                // The address may be undefined if the chunk or index storage is not allocated yet.
                val address = builder.readLength(state) // Utils.readBytesAsUnsignedLong(bb, sb.getSizeOfOffsets())
                DataLayoutSingleChunk4(flags, chunkDimensions, chunkSize = chunkSize, address, filterMask)
            }
        2 -> {
                val address =  builder.readLength(state) // possibly wrong
                DataLayoutImplicit4(flags, chunkDimensions, address)
            }
        3 -> {
                val pageBits = raf.readByte(state) // bb.get()
                val address =  builder.readLength(state) // possibly wrong
                DataLayoutFixedArray4(flags, chunkDimensions, pageBits, address)
            }
        4 -> {
                val maxBits = raf.readByte(state) // bb.get()
                val indexElements = raf.readByte(state) // bb.get()
                val minPointers = raf.readByte(state) // bb.get()
                val minElements = raf.readByte(state) // bb.get()
                val pageBits = raf.readByte(state) // bb.get() // This is wrong in the spec says 2 bytes its actually 1
                val address =  builder.readLength(state) // possibly wrong
                DataLayoutExtensibleArray4(flags, chunkDimensions, maxBits, indexElements, minPointers, minElements, pageBits, address)
            }
        5 -> {
                val nodeSize = raf.readInt(state) // bb.getInt()
                val splitPercent = raf.readByte(state) // bb.get()
                val mergePercent = raf.readByte(state) // bb.get()
                val address =  builder.readLength(state) // possibly wrong
                DataLayoutBtreeVer2(flags, chunkDimensions, nodeSize, splitPercent, mergePercent, address)
            }

        else -> throw RuntimeException("Unrecognized chunk indexing type. type= $chunkIndexingType" )
    }
}

//////////////////////////////////////////////////////////////////
internal class FixedArrayIndex(val h5: H5builder, val varShape: IntArray, val mdl: DataLayoutFixedArray4) {
    val chunkDimensions = IntArray(mdl.chunkDimensions.size - 1) { mdl.chunkDimensions[it] } // remove the element "dimension"

    val clientId: Int
    val entrySize: Int
    val pageBits: Int // This field contains the number of bits needed to store the maximum number of elements in a data block page. bits ??
    val maxNumberOfEntries : Int
    val pages: Int
    val pageSize: Int
    val dataAddress: Long

    val state = OpenFileState(h5.getFileOffset(mdl.indexAddress), false)
    val chunks = mutableListOf<ChunkImpl>()

    init {
        val raf = h5.raf

        // FixedArray Header
        val magic = raf.readString(state, 4)
        check(magic == "FAHD") { "$magic should equal FAHD" }
        val version0: Byte = raf.readByte(state)
        clientId = raf.readByte(state).toInt()
        entrySize = raf.readByte(state).toInt()
        pageBits = raf.readByte(state).toInt() // presumably this replicates the MessageDataLayout
        maxNumberOfEntries = h5.readLength(state).toInt()
        pageSize = 1 shl pageBits
        pages = (maxNumberOfEntries + pageSize - 1) / pageSize

        dataAddress = h5.readOffset(state)
        state.pos = dataAddress

        val pageBitmapBytes: Int = (pages + 7) / 8
        // dont actually need this
        var headerSize: Int = 6 + h5.sizeOffsets + entrySize * maxNumberOfEntries + 4
        if (pages > 1) {
            headerSize += pageBitmapBytes + (4 * pages)
        }

        // FixedArrayDataBlock
        val magic2 = raf.readString(state, 4)
        check(magic2 == "FADB") { "$magic2 should equal FADB" }

        val version = raf.readByte(state).toInt()
        if (version != 0) {
            throw RuntimeException("Unsupported fixed array data block version detected. Version: $version")
        }

        val dataBlockclientId = raf.readByte(state).toInt()
        if (dataBlockclientId != clientId) {
            throw RuntimeException("Fixed array client ID mismatch")
        }

        val headerAddress = raf.readLong(state)
        if (headerAddress != mdl.indexAddress) {
            throw RuntimeException("Fixed array data block header address mismatch")
        }

        if (pages > 1) {
            readPaged(pageBitmapBytes, dataBlockclientId)
        } else {
            // Unpaged
            logger.info { "Reading unpaged" }
            if (dataBlockclientId == 0) { // Not filtered
                for (i in 0..< maxNumberOfEntries) {
                    readUnfiltered(h5.raf, state, i)
                }
            } else if (dataBlockclientId == 1) { // Filtered
                for (i in 0..< maxNumberOfEntries) {
                    readFiltered(h5.raf, state,i)
                }
            } else {
                throw RuntimeException("Unrecognized client ID  = $dataBlockclientId")
            }
        }
    }

    fun readPaged(
        pageBitmapBytes: Int,
        dataBlockclientId: Int
    ) {
        logger.info {"Reading paged"}
        /*
        val pageBitmap = ByteArray(pageBitmapBytes)
        bb.get(pageBitmap)
        ChecksumUtils.validateChecksumFromMark(bb)
         */
        val pageBitmap = h5.raf.readByteArray(state, pageBitmapBytes)
        val checksum = h5.raf.readInt(state) // confusing

        var chunkIndex = 0
        for (page in 0..< pages) {
            val currentPageSize = getCurrentPageSize(page)

            if (dataBlockclientId == 0) { // Not filtered
                for (i in 0..< currentPageSize) {
                    readUnfiltered(h5.raf, state,chunkIndex++)
                }
            } else if (dataBlockclientId == 1) { // Filtered
                for (i in 0..<currentPageSize) {
                    readFiltered(h5.raf, state, chunkIndex++)
                }
            } else {
                throw RuntimeException("Unrecognized client ID  = $dataBlockclientId")
            }
            val checksum = h5.raf.readInt(state) // confusing
        }
    }

    fun getCurrentPageSize(page: Int): Int {
        val currentPageSize: Int
        if (page == pages - 1) {
            // last page so maybe not a full page
            val lastPageSize: Int = maxNumberOfEntries % pageSize
            currentPageSize = if (lastPageSize == 0) pageSize else lastPageSize
        } else {
            currentPageSize = pageSize
        }
        return currentPageSize
    }

    // unfilteredChunkSize = Arrays.stream(getChunkDimensions()).reduce(1, Math::multiplyExact) * getDataType().getSize();
    // chunkDimensions = int[] chunkDimensions = layoutMessage.getChunkDimensions(); ArrayUtils.subarray(chunkDimensions, 0, chunkDimensions.length - 1);
    // datasetDimensions = dataSpace.getDimensions();

    fun readFiltered(raf: OpenFileIF, state : OpenFileState, chunkIndex: Int) {
        val chunkAddress = h5.readOffset(state)
        // yikes: Utils.readBytesAsUnsignedInt(bb, this@FixedArrayIndex.entrySize - h5.sizeOffsets - 4)
        val chunkSizeInBytes: Int = h5.readVariableSizeUnsigned(state, entrySize - h5.sizeOffsets - 4).toInt()
        val filterMask = raf.readInt(state) // java.util.BitSet = java.util.BitSet.valueOf(byteArrayOf(bb.get(), bb.get(), bb.get(), bb.get()))
        val chunkOffset: IntArray = chunkIndexToChunkOffset(chunkIndex, chunkDimensions, varShape)

        chunks.add(ChunkImpl(chunkAddress, chunkSizeInBytes, chunkOffset, filterMask))
    }

    fun readUnfiltered(raf: OpenFileIF, state : OpenFileState, chunkIndex: Int) {
        val chunkAddress = h5.readOffset(state) // val chunkAddress: Long = Utils.readBytesAsUnsignedLong(bb, sizeOfOffsets)
        val chunkOffset: IntArray = chunkIndexToChunkOffset(chunkIndex, chunkDimensions, varShape)
        val unfilteredChunkSize = mdl.chunkDimensions.computeSize()

        chunks.add(ChunkImpl(chunkAddress, unfilteredChunkSize, chunkOffset, null))
    }

    fun chunkIterator() : Iterator<ChunkImpl> = chunks.iterator()

    companion object {
        val logger = KotlinLogging.logger("ChunkedDataLayoutMessageV4")
    }
}

/////////////////////////////////////////////////
// internal data class DataLayoutImplicit4(val flags: Byte, val chunkDimensions: IntArray, val address: Long) : DataLayoutMessage() {
internal class ImplicitChunkIndex(val h5: H5builder, val varShape: IntArray, val mdl: DataLayoutImplicit4) {
    val chunkDimensions = IntArray(mdl.chunkDimensions.size - 1) { mdl.chunkDimensions[it] } // remove the element "dimension"
    var chunkSize = mdl.chunkDimensions.computeSize()

    fun getAllChunks(): List<ChunkImpl> {
        val totalChunks: Int = totalChunks(varShape, chunkDimensions)
        val chunks = mutableListOf<ChunkImpl>()
        for (i in 0..< totalChunks) {
            chunks.add(
                ChunkImpl(
                    mdl.address + i * chunkSize,
                    chunkSize,
                    chunkIndexToChunkOffset(i, chunkDimensions, varShape),
                    null)
                )
        }
        return chunks
    }

    fun totalChunks(datasetDimensions: IntArray, chunkDimensions: IntArray): Int {
        var chunks = 1
        for (i in datasetDimensions.indices) {
            var chunksInDim = datasetDimensions[i] / chunkDimensions[i]
            // If there is a partial chunk then we need to add one chunk in this dim
            if (datasetDimensions[i] % chunkDimensions[i] != 0) chunksInDim++
            chunks *= chunksInDim
        }
        return chunks
    }

    fun chunkIterator() : Iterator<ChunkImpl> = getAllChunks().iterator()

}

fun chunkIndexToChunkOffset(chunkIndex: Int, chunkDimensions: IntArray, datasetDimensions: IntArray): IntArray {
    var chunkIndex = chunkIndex
    val chunkOffset = IntArray(chunkDimensions.size)

    // Start from the slowest dim
    for (i in chunkOffset.indices) {
        // Find out how many chunks make one chunk in this dim
        var chunksBelowThisDim = 1
        // Start one dim faster
        for (j in i + 1..<chunkOffset.size) {
            chunksBelowThisDim *= ceil(datasetDimensions[j].toDouble() / chunkDimensions[j]).toInt()
        }

        chunkOffset[i] = (chunkIndex / chunksBelowThisDim) * chunkDimensions[i]
        chunkIndex -= chunkOffset[i] / chunkDimensions[i] * chunksBelowThisDim
    }

    return chunkOffset
}

////////////////////////////////////////////////////
data class ChunkImpl(val address: Long, val size: Int, val chunkOffset: IntArray, val filterMask: Int?) {
    override fun toString(): String {
        return "ChunkImpl(address=$address, size=$size, chunkOffset=${chunkOffset.contentToString()}, filterMask=$filterMask)"
    }
}