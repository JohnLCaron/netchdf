package com.sunya.netchdf.hdf5


import com.sunya.cdm.api.computeSize
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState

import com.sunya.cdm.layout.Tiling
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.cdm.util.log2
import kotlin.math.ceil
import kotlin.math.pow

@OptIn(InternalLibraryApi::class)

/*  Btree version 2, for data. From jhdf. */
internal class BTree2j(private val h5: H5builder, owner: String, address: Long, storageDims: LongArray? = null) { // BTree2
    val btreeType: Int
    private val nodeSize: Int // size in bytes of btree nodes
    private val recordSize: Int // size in bytes of btree records
    private val owner: String
    private val raf: OpenFileIF
    val rootNodeAddress: Long
    val treeDepth : Int
    val numberOfRecordsInRoot : Int
    val totalNumberOfRecordsInTree: Long

    val chunkSize: Long
    val chunkDims: LongArray

    /** The records in this b-tree */
    val records = mutableListOf<Any>()

    init {
        if (storageDims != null) {
            chunkSize = storageDims.computeSize()
            chunkDims = LongArray(storageDims.size - 1) { storageDims[it] }
        } else {
            chunkSize = 0
            chunkDims = longArrayOf()
        }

        raf = h5.raf
        this.owner = owner
        val state = OpenFileState(h5.getFileOffset(address), false)

        // header
        val magic = raf.readString(state, 4)
        check(magic == "BTHD") { "$magic should equal BTHD" }
        val version: Byte = raf.readByte(state)
        btreeType = raf.readByte(state).toInt()
        nodeSize = raf.readInt(state) // This is the size in bytes of all B-tree nodes.
        recordSize = raf.readShort(state).toUShort().toInt() // This field is the size in bytes of the B-tree record.
        treeDepth = raf.readShort(state).toUShort().toInt()

        val splitPct = raf.readByte(state)
        val mergePct = raf.readByte(state)
        rootNodeAddress = h5.readOffset(state)
        numberOfRecordsInRoot = raf.readShort(state).toUShort().toInt()
        totalNumberOfRecordsInTree = h5.readLength(state) // total in entire btree
        val checksum: Int = raf.readInt(state)

        readRecords(rootNodeAddress, treeDepth, numberOfRecordsInRoot, totalNumberOfRecordsInTree)
    }

    fun readRecords(address: Long, depth: Int, numberOfRecords: Int, totalRecords: Long) {
        val state = OpenFileState(h5.getFileOffset(address), false)

        val magic = raf.readString(state, 4)
        val leafNode = if (magic == "BTIN") {
            false
        } else if (magic == "BTLF") {
            true
        } else {
            throw RuntimeException("$magic unknown tag")
        }

        val version: Byte = raf.readByte(state)
        val nodeType = raf.readByte(state).toInt() // same as the B-tree type in the header
        check(nodeType == btreeType)

        repeat(numberOfRecords) {
            records.add( readRecord(state, nodeType))
        }

        if (!leafNode) {
            repeat(numberOfRecords + 1) {
                val childAddress = h5.readOffset(state) // Child Node Pointer
                val sizeOfNumberOfRecords = getSizeOfNumberOfRecords(nodeSize, depth, totalRecords.toInt(), recordSize, h5.sizeOffsets)
                val numberOfChildRecords: Int = h5.readVariableSizeUnsigned(state, sizeOfNumberOfRecords).toInt() // readBytesAsUnsignedInt(bb, sizeOfNumberOfRecords)
                val sizeNumberOfChildRecords = getSizeOfTotalNumberOfChildRecords(nodeSize, depth, recordSize)
                val totalNumberOfChildRecords = if (depth > 1) {
                    h5.readVariableSizeUnsigned(state, sizeNumberOfChildRecords)
                } else {
                    -1
                }
                readRecords(childAddress, depth - 1, numberOfChildRecords, totalNumberOfChildRecords)
            }
        }
    }

    // heroic jhdf
    private fun getSizeOfNumberOfRecords(
        nodeSize: Int,
        depth: Int,
        totalRecords: Int,
        recordSize: Int,
        sizeOfOffsets: Int
    ): Int {
        val NODE_OVERHEAD_BYTES = 10
        var size: Int = nodeSize - NODE_OVERHEAD_BYTES

        // If the child is not a leaf
        if (depth > 1) {
            // Need to subtract the pointers as well
            val pointerTripletBytes = bytesNeededToHoldNumber(totalRecords) * 2 + sizeOfOffsets
            size -= pointerTripletBytes

            return bytesNeededToHoldNumber(size / recordSize)
        } else {
            // Its a leaf
            return bytesNeededToHoldNumber(size / recordSize)
        }
    }

    // jhdf
    private fun bytesNeededToHoldNumber(number: Int): Int {
        return (Integer.numberOfTrailingZeros(Integer.highestOneBit(number)) + 8) / 8
    }

    /* private fun getSizeOfTotalNumberOfChildRecords(nodeSize: Int, depth: Int, recordSize: Int): Int {
        require (nodeSize % recordSize == 0)
        val recordsInLeafNode = (nodeSize / recordSize).toDouble()
        val totalRecords = recordsInLeafNode.pow(depth)
        val totalBits = log2(totalRecords)
        val totalBitsInt = totalBits.toInt()
        return (totalBitsInt + 8) / 8
    } */

    // no BigInteger, max depth 6
    private fun getSizeOfTotalNumberOfChildRecords(nodeSize: Int, depth: Int, recordSize: Int): Int {
        require(depth < 7 ) { "no BigInteger, max depth 6 "}
        val recordsInLeafNode = (nodeSize/ recordSize).toDouble()
        val totalRecords = recordsInLeafNode.pow(depth)
        val totalRecordsL = ceil(totalRecords).toLong()
        val alt = log2(totalRecordsL) + 1
        val alt1 =  (alt + 8) / 8
        return alt1
    }

    // jhdf
    //private fun getSizeOfTotalNumberOfChildRecordsOrg(nodeSize: Int, depth: Int, recordSize: Int): Int {
    //    val recordsInLeafNode = nodeSize / recordSize
    //    return (BigInteger.valueOf(recordsInLeafNode.toLong()).pow(depth).bitLength() + 8) / 8
    //}

    fun readRecord(state: OpenFileState, type: Int): Any {
        return when (type) {
            1 -> Record1(state)
            2 -> Record2(state)
            3 -> Record3(state)
            4 -> Record4(state)
            5 -> Record5(state)
            6 -> Record6(state)
            7 -> Record70(state) // TODO wrong
            8 -> Record8(state)
            9 -> Record9(state)
            10 -> Record10(state, chunkDims.toIntArray(), chunkSize.toInt())
            11 -> Record11(state, chunkDims.toIntArray() )
            else -> throw IllegalStateException()
        }
    }

    // Type 1 Record Layout - Indirectly Accessed, Non-filtered, ‘Huge’ Fractal Heap Objects
    internal inner class Record1(state: OpenFileState) {
        val hugeObjectAddress = h5.readOffset(state)
        val hugeObjectLength = h5.readLength(state)
        val hugeObjectID = h5.readLength(state)
    }

    // Type 2 Record Layout - Indirectly Accessed, Filtered, ‘Huge’ Fractal Heap Objects
    internal inner class Record2(state: OpenFileState) {
        val hugeObjectAddress = h5.readOffset(state)
        val hugeObjectLength = h5.readLength(state)
        val filterMask = raf.readInt(state)
        val hugeObjectSize = h5.readLength(state)
        val hugeObjectID = h5.readLength(state)
    }

    // Type 3 Record Layout - Directly Accessed, Non-filtered, ‘Huge’ Fractal Heap Objects
    internal inner class Record3(state: OpenFileState) {
        val hugeObjectAddress = h5.readOffset(state)
        val hugeObjectLength = h5.readLength(state)
    }

    // Type 4 Record Layout - Directly Accessed, Filtered, ‘Huge’ Fractal Heap Objects
    internal inner class Record4(state: OpenFileState) {
        val hugeObjectAddress = h5.readOffset(state)
        val hugeObjectLength = h5.readLength(state)
        val filterMask = raf.readInt(state)
        val hugeObjectSize = h5.readLength(state)
    }

    // Type 5 Record Layout - Link Name for Indexed Group
    inner class Record5(state: OpenFileState) {
        val nameHash = raf.readInt(state)
        val heapId: ByteArray = raf.readByteArray(state, 7)
    }

    // Type 6 Record Layout - Creation Order for Indexed Group
    inner class Record6(state: OpenFileState) {
        val creationOrder = raf.readLong(state)
        val heapId: ByteArray = raf.readByteArray(state, 7)
    }

    // Type 7 Record Layout - Shared Object Header Messages (Sub-type 0 - Message in Heap)
    internal inner class Record70(state: OpenFileState) {
        val location = raf.readByte(state)
        val hash = raf.readInt(state)
        val refCount = raf.readInt(state)
        val id: ByteArray = raf.readByteArray(state, 8)
    }

    // Type 7 Record Layout - Shared Object Header Messages (Sub-type 1 - Message in Object Header)
    internal inner class Record71(state: OpenFileState) {
        val location = raf.readByte(state)
        val hash = raf.readInt(state)
        val skip = raf.readByte(state)
        val messtype = raf.readByte(state)
        val index = raf.readShort(state)
        val address = h5.readOffset(state)
    }

    // Type 8 Record Layout - Attribute Name for Indexed Attributes
    inner class Record8(state: OpenFileState) {
        val heapId: ByteArray = raf.readByteArray(state, 8)
        val flags = raf.readByte(state)
        val creationOrder = raf.readInt(state)
        val nameHash = raf.readInt(state)
    }

    // Type 9 Record Layout - Creation Order for Indexed Attributes
    inner class Record9(state: OpenFileState) {
        val heapId: ByteArray = raf.readByteArray(state, 8)
        val flags = raf.readByte(state)
        val creationOrder = raf.readInt(state)
    }

    // Type 10 Record Layout - Non-filtered Dataset Chunks
    inner class Record10(state: OpenFileState, dims : IntArray, chunkSize: Int) {
        val chunk : ChunkImpl

        init {
            val address = h5.readOffset(state)

            // This field is the scaled offset of the chunk within the dataset. n is the number of dimensions for the dataset.
            val scaledOffset = LongArray(dims.size) { raf.readLong(state) }

            // Scaled offset is calculated by dividing the chunk dimension sizes into the chunk offsets.
            // so to get the chunk offset:
            // jhdf
            // 		int[] chunkOffset = new int[datasetInfo.getDatasetDimensions().length];
            //		for (int i = 0; i < chunkOffset.length; i++) {
            //			chunkOffset[i] = Utils.readBytesAsUnsignedInt(buffer, 8) * datasetInfo.getChunkDimensions()[i];
            //		}
            val chunkOffset = scaledOffset.mapIndexed { idx, scaledOffset -> (scaledOffset * dims[idx]).toInt() }

            // ChunkImpl(val address: Long, val size: Int, val chunkOffset: IntArray, val filterMask: Int?)
            chunk = ChunkImpl(address, chunkSize, chunkOffset.toIntArray(), null)
        }
    }

    // Type 11 Record Layout - Filtered Dataset Chunks
    inner class Record11(state: OpenFileState, dims : IntArray) {
        val chunk : ChunkImpl

        init {
            val address = h5.readOffset(state)

            // LOOK variable size based on what? "Chunk Size (variable size; at most 8 bytes)"
            // jhdf
            // 		final int chunkSizeBytes = buffer.limit()
            //			- 8 // size of offsets
            //			- 4 // filter mask
            //			- datasetInfo.getDatasetDimensions().length * 8; // dimension offsets
            val rank = dims.size
            val chunkSizeBytes = recordSize - 8 - 4 - rank * 8
            val chunkSize = h5.readVariableSizeUnsigned(state, chunkSizeBytes).toInt()

            val filterMask = raf.readInt(state)

            // This field is the scaled offset of the chunk within the dataset. n is the number of dimensions for the dataset.
            val scaledOffset = LongArray(rank) { raf.readLong(state) }

            // Scaled offset is calculated by dividing the chunk dimension sizes into the chunk offsets.
            // so to get the chunk offset:
            // jhdf
            // 		int[] chunkOffset = new int[datasetInfo.getDatasetDimensions().length];
            //		for (int i = 0; i < chunkOffset.length; i++) {
            //			chunkOffset[i] = Utils.readBytesAsUnsignedInt(buffer, 8) * datasetInfo.getChunkDimensions()[i];
            //		}
            val chunkOffset = scaledOffset.mapIndexed { idx, scaledOffset -> (scaledOffset * dims[idx]).toInt() }

            // ChunkImpl(val address: Long, val size: Int, val chunkOffset: IntArray, val filterMask: Int?)
            chunk = ChunkImpl(address, chunkSize, chunkOffset.toIntArray(), filterMask)
        }
    }

    fun chunkIterator() : Iterator<ChunkImpl> = ChunkIterator()

    private inner class ChunkIterator : AbstractIterator<ChunkImpl>() {
        var count = 0
        var first = true

        override fun computeNext() {
            if (count >= records.size) {
                return done()
            }
            val chunk = when (btreeType) {
                10 -> (records[count] as Record10).chunk
                11 -> (records[count] as Record11).chunk
                else -> throw RuntimeException()
            }
            setNext(chunk)
            count++
        }
    }

    fun makeMissingDataChunkEntry(rootNode: BTree1.Node, wantKey: LongArray): DataChunkIF {
        return MissingDataChunk()
    }

    class MissingDataChunk() : DataChunkIF {
        override fun childAddress() = -1L
        override fun offsets() = longArrayOf()
        override fun isMissing() = true
        override fun chunkSize() = 0
        override fun filterMask() = 0

        override fun show(tiling : Tiling) : String = "missing"
    }

    companion object {
        internal fun findRecord1byId(records: List<Any>, hugeObjectID: Int): Record1? {
            for (record in records) {
                if (record is Record1 && record.hugeObjectID == hugeObjectID.toLong()) return record
            }
            return null
        }
    }
}
