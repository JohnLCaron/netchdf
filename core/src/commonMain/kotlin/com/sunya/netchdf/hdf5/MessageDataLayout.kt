@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.util.InternalLibraryApi

// Message Type 8 "Data Layout" : regular (contiguous), chunked, or compact (stored with the message)
// The dimensions were specified in version 1 and 2. In version 3 and 4, dimensions are in the Dataspace message.
// When chunked, the last dimension is the chunk size. LOOK version 4 not done.

// The Data Layout message describes how the elements of a multi-dimensional array are stored in the HDF5 file.
// Four types of data layout are supported:
//  Contiguous: The array is stored in one contiguous area of the file. This layout requires that the size of the array
//     be constant: data manipulations such as chunking, compression, checksums, or encryption are not permitted.
//     The message stores the total storage size of the array. The offset of an element from the beginning of the
//     storage area is computed as in a C array.
//  Chunked: The array domain is regularly decomposed into chunks, and each chunk is allocated and stored separately.
//     This layout supports arbitrary element traversals, compression, encryption, and checksums (these features are
//     described in other messages). The message stores the size of a chunk instead of the size of the entire array;
//     the storage size of the entire array can be calculated by traversing the chunk index that stores the chunk addresses.
//  Compact: The array is stored in one contiguous block as part of this object header message.
//  Virtual: This is only supported for version 4 of the Data Layout message. The message stores information that is
//     used to locate the global heap collection containing the Virtual Dataset (VDS) mapping information. The mapping
//     associates the VDS to the source dataset elements that are stored across a collection of HDF5 files.


internal fun H5builder.readDataLayoutMessage(state : OpenFileState) : DataLayoutMessage {
    val tstate = state.copy()
    val version = raf.readByte(tstate).toInt()
    val layoutClass = if (version > 2) raf.readByte(tstate).toInt() else raf.readByte(tstate.incr(1)).toInt()

    if (version < 3) {
        val rawdata =
            structdsl("MessageLayout", raf, state) {
                fld("version", 1)
                fld("rank", 1)
                fld("layoutClass", 1)
                skip(5)
                if (layoutClass != 0) { // not compact
                    fld("dataAddress", sizeOffsets)
                }
                array("dims", 4, "rank")
                if (layoutClass == 2) { // chunked
                    fld("chunkedElementSize", 4)
                }
                if (layoutClass == 0) { // compact
                    fld("compactDataSize", 4)
                    array("compactData", 1, "compactDataSize")
                }
            }
        if (debugMessage) rawdata.show()

        return when (layoutClass) {
            0 -> DataLayoutCompact(rawdata.getIntArray("dims"), rawdata.getByteArray("compactData"))
            1 -> DataLayoutContiguous(rawdata.getIntArray("dims"), rawdata.getLong("dataAddress"))
            2 -> DataLayoutBTreeVer1(version, rawdata.getIntArray("dims"), rawdata.getLong("dataAddress"), rawdata.getInt("chunkedElementSize"))
            else -> throw RuntimeException()
        }

    } else if (version == 3) {
        val rawdata =
            structdsl("MessageLayout3", raf, state) {
                fld("version", 1)
                fld("layoutClass", 1)
                when (layoutClass) {
                    0 -> {
                        fld("compactDataSize", 2)
                        array("compactData", 1, "compactDataSize")
                    }

                    1 -> {
                        fld("dataAddress", sizeOffsets)
                        fld("dataSize", sizeLengths)
                    }

                    2 -> { // chunked
                        fld("rank", 1) // aka dimensionality
                        fld("btreeAddress", sizeOffsets)
                        array("dims", 4,"rank")
                        fld("chunkedElementSize", 4)
                    }

                    else -> throw RuntimeException()
                }
            }
        if (debugMessage) rawdata.show()

        return when (layoutClass) {
            0 -> DataLayoutCompact3(rawdata.getByteArray("compactData"))
            1 -> DataLayoutContiguous3(rawdata.getLong("dataAddress"), rawdata.getLong("dataSize"))
            2 -> DataLayoutBTreeVer1(version, rawdata.getIntArray("dims"), rawdata.getLong("btreeAddress"), rawdata.getInt("chunkedElementSize"))
            else -> throw RuntimeException()
        }

    } else if (version == 4) {

        if (layoutClass != 2) { // layoutClass = 0 and 1 are the same as version 3
            val rawdata =
                structdsl("MessageLayout4", raf, state) {
                    fld("version", 1)
                    fld("layoutClass", 1)
                    when (layoutClass) {
                        0 -> {
                            fld("compactDataSize", 2)
                            array("compactData", 1, "compactDataSize")
                        }

                        1 -> {
                            fld("dataAddress", sizeOffsets)
                            fld("dataSize", sizeLengths)
                        }

                        3 -> {
                            // throw new UnsupportedHdfException("Virtual storage is not supported") TODO
                            // This is the address of the global heap collection where the VDS mapping entries are stored.
                            fld("heapAddress", sizeOffsets)
                            fld("index", sizeLengths)
                        }
                    }
                }
            if (debugMessage) rawdata.show()

            return when (layoutClass) {
                0 -> DataLayoutCompact3(rawdata.getByteArray("compactData"))
                1 -> DataLayoutContiguous3(rawdata.getLong("dataAddress"), rawdata.getLong("dataSize"))
                3 -> DataLayoutVirtual4(rawdata.getLong("heapAddress"), rawdata.getInt("index"))
                else -> throw RuntimeException()
            }
        }

        // version 4, layoutClass = 2 is too complex for structdls
        return readChunkedDataLayoutMessageV4(this, raf, state)
    }
    throw RuntimeException()
}

//////////////////////////////////////////////////////////////////////////////////////////
internal val dataLayoutNames = listOf(
        DataLayoutCompact::class.simpleName,
        DataLayoutContiguous::class.simpleName,
        DataLayoutBTreeVer1::class.simpleName,
        DataLayoutCompact3::class.simpleName,
        DataLayoutContiguous3::class.simpleName,
        DataLayoutVirtual4::class.simpleName,
        DataLayoutSingleChunk4::class.simpleName,
        DataLayoutImplicit4::class.simpleName,
        DataLayoutFixedArray4::class.simpleName,
        DataLayoutExtensibleArray4::class.simpleName,
        DataLayoutBtreeVer2::class.simpleName,
    )

// debugging
internal fun getDataLayoutCounts() : MutableMap<String, Int> {
    return dataLayoutNames.associate { Pair(it!!, 0) }.toMutableMap()
}

////////////////////////////////////////////////////////////////////////////////////////

internal open class DataLayoutMessage() : MessageHeader(MessageType.Layout) {
    val isCompact = (this is DataLayoutCompact) || (this is DataLayoutCompact3)
    val isContiguous = (this is DataLayoutContiguous) || (this is DataLayoutContiguous3)
    override fun show() : String = "${this::class.simpleName}"
}

// 1 & 2
internal data class DataLayoutCompact(val dims : IntArray, val compactData: ByteArray) : DataLayoutMessage()

internal data class DataLayoutContiguous(val dims : IntArray, val dataAddress: Long) : DataLayoutMessage() {
    override fun show() : String = "${super.show()} dims=${dims.contentToString()} dataAddress=$dataAddress"
}
internal data class DataLayoutBTreeVer1(val version : Int, val chunkDims : IntArray, val btreeAddress: Long, val chunkedElementSize : Int)
    : DataLayoutMessage() {
    override fun show(): String = "${super.show()} dims=${chunkDims.contentToString()} btreeAddress=$btreeAddress chunkedElementSize=$chunkedElementSize"
}

// 3
internal data class DataLayoutCompact3(val compactData: ByteArray) : DataLayoutMessage()

internal data class DataLayoutContiguous3(val dataAddress: Long, val dataSize: Long) : DataLayoutMessage() {
    override fun show(): String = "${super.show()} dataAddress=$dataAddress dataSize=$dataSize"
}

// 4
internal data class DataLayoutSingleChunk4(val flags: Byte, val chunkDimensions: IntArray, val chunkSize: Int, val heapAddress: Long, val filterMask: Int?) : DataLayoutMessage() {
    val isFiltered = isBitSet(flags.toInt(), 1)
    override fun show(): String = "${super.show()} flags=$flags chunkDimensions=${chunkDimensions.contentToString()} heapAddress=$heapAddress chunkSize=$chunkSize"
}
internal data class DataLayoutImplicit4(val flags: Byte, val chunkDimensions: IntArray, val address: Long) : DataLayoutMessage() {
    override fun show(): String = "${super.show()} flags=$flags chunkDimensions=${chunkDimensions.contentToString()} address=$address"
}
internal data class DataLayoutFixedArray4(val flags: Byte, val chunkDimensions: IntArray, val pageBits: Byte, val indexAddress: Long) : DataLayoutMessage() {
    override fun show(): String = "${super.show()} flags=$flags chunkDimensions=${chunkDimensions.contentToString()} pageBits=$pageBits indexAddress=$indexAddress"
}
internal data class DataLayoutExtensibleArray4(val flags: Byte, val chunkDimensions: IntArray, val maxBits: Byte, val indexElements: Byte,
        val minPointers: Byte, val minElements: Byte, val pageBits: Byte, val indexAddress: Long) : DataLayoutMessage() {
    override fun show(): String = "${super.show()} flags=$flags chunkDimensions=${chunkDimensions.contentToString()} maxBits=$maxBits indexElements=$indexElements " +
            "minPointers=$minPointers minElements=$minPointers pageBits=$pageBits indexAddress=$indexAddress"
}
internal data class DataLayoutBtreeVer2(val flags: Byte, val chunkDimensions: IntArray, val nodeSize: Int, val splitPercent: Byte, val mergePercent: Byte, val heapAddress: Long)
    : DataLayoutMessage() {
    override fun show(): String = "${super.show()} flags=$flags chunkDimensions=${chunkDimensions.contentToString()} nodeSize=$nodeSize splitPercent=$splitPercent nodeSize=$nodeSize " +
            "mergePercent = $mergePercent heapAddress=$heapAddress"
}
internal data class DataLayoutVirtual4(val heapAddress: Long, val index: Int) : DataLayoutMessage() {
    override fun show(): String = "${super.show()} heapAddress=$heapAddress index=$index"
}