package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.computeSize
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.iosp.OpenFileState

import com.sunya.cdm.layout.Tiling
import com.sunya.cdm.util.InternalLibraryApi

@OptIn(InternalLibraryApi::class)

/*  Btree version 2, for data. From jhdf. */
internal class BTree2data(
    val raf: OpenFileExtended,
    val owner: String,
    address: Long,
    varShape: LongArray,
    storageDims: LongArray,
)  : DataChunkSequence { // BTree2

    val chunkSize = storageDims.computeSize()
    val chunkShape = LongArray(storageDims.size - 1) { storageDims[it] }
    val tiling = Tiling(varShape, chunkShape)

    val btreeType: Int
    private val nodeSize: Int // size in bytes of btree nodes
    private val recordSize: Int // size in bytes of btree records
    val treeDepth : Int
    val rootNodeAddress: Long
    val numberOfRecordsInRoot : Int
    val totalNumberOfRecordsInTree: Int

    val rootNode: BTreeNode

    init {
        val state = OpenFileState(raf.getFileOffset(address), false)

        // header
        val magic = raf.readString(state, 4)
        check(magic == "BTHD") { "$magic should equal BTHD" }
        val version: Byte = raf.readByte(state)
        btreeType = raf.readByte(state).toInt()
        require(btreeType == 10 || btreeType == 11)

        nodeSize = raf.readInt(state) // This is the size in bytes of all B-tree nodes.
        recordSize = raf.readShort(state).toUShort().toInt() // This field is the size in bytes of the B-tree record.
        treeDepth = raf.readShort(state).toUShort().toInt()

        val splitPct = raf.readByte(state)
        val mergePct = raf.readByte(state)
        rootNodeAddress = raf.readOffset(state)
        numberOfRecordsInRoot = raf.readShort(state).toUShort().toInt()
        totalNumberOfRecordsInTree = raf.readLength(state).toInt() // total in entire btree
        val checksum: Int = raf.readInt(state)

        rootNode = BTreeNode(rootNodeAddress, treeDepth, numberOfRecordsInRoot, totalNumberOfRecordsInTree, null)
    }

    override fun asSequence(): Sequence<DataChunk> = sequence {
        repeat( tiling.nelems) {
            val result = findDataChunk(it) ?: missingDataChunk(it, tiling)
            yield(result)
        }
    }

    fun countChunks() = asSequence().count()

    fun chunkIterator(): Iterator<DataChunk> = asSequence().iterator()

    internal fun findDataChunk(order: Int): DataChunk? {
        return rootNode.findDataChunk(order)
    }

    inner class BTreeNode(val address: Long, depth: Int, numberOfRecords: Int, totalRecords: Int, val parent: BTreeNode?)  {
        var level: Int = 0
        var nentries: Int = 0

        val dataChunks = mutableListOf<DataChunk>() // tile order to DataChunk
        val children = mutableListOf<BTreeNode>()

        var lastOrder : Int = 0

        init {
            if (address > 0) {
                val state = OpenFileState(raf.getFileOffset(address), false)

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

                // dataChunks
                repeat(numberOfRecords) {
                    val dataChunk = readRecord(state, nodeType)
                    dataChunks.add(dataChunk)
                    lastOrder = dataChunk.order
                }

                // children
                if (!leafNode) {
                    repeat(numberOfRecords + 1) {
                        val childAddress = raf.readOffset(state) // Child Node Pointer
                        val sizeOfNumberOfRecords = getSizeOfNumberOfRecords(nodeSize, depth, totalRecords.toInt(), recordSize, raf.sizeOffsets())
                        val numberOfChildRecords: Int = raf.readVariableSizeUnsigned(state, sizeOfNumberOfRecords).toInt() // readBytesAsUnsignedInt(bb, sizeOfNumberOfRecords)
                        val sizeNumberOfChildRecords = getSizeOfTotalNumberOfChildRecords(nodeSize, depth, recordSize)
                        val totalNumberOfChildRecords = if (depth > 1) {
                            raf.readVariableSizeUnsigned(state, sizeNumberOfChildRecords).toInt()
                        } else {
                            -1
                        }
                        children.add( BTreeNode(childAddress, depth - 1, numberOfChildRecords, totalNumberOfChildRecords, this))
                    }
                }

                if (children.isNotEmpty()) {
                    lastOrder = children.last().lastOrder
                }
            }
        }

        // uses a tree search = O(log n)
        // this algo assume you dont have xised noted, not true
        fun findDataChunk(wantOrder: Int): DataChunk? {
            if (dataChunks.isNotEmpty()) {
                val result = dataChunks.find { it.order == wantOrder }
                if (result != null) return result
            }
            if (children.isNotEmpty()) { // search tree; assumes that chunks are ordered
                children.forEach { childNode ->
                    if (wantOrder <= childNode.lastOrder)
                        return childNode.findDataChunk(wantOrder)
                }
            } //else {  // If it's a leaf node (no children)
              //  return dataChunks.find { it.order == wantOrder }
            //}
            return null
        }

        override fun toString(): String {
            return "BTreeNode(address=$address, level=$level, nentries=$nentries, lastOrder=$lastOrder)"
        }

    } // BTreeNode

    fun readRecord(state: OpenFileState, type: Int): DataChunk {
        return when (type) {
            10 -> readRecord10(state, chunkShape.toIntArray(), chunkSize.toInt())
            11 -> readRecord11(state, chunkShape.toIntArray() )
            else -> throw IllegalStateException()
        }
    }

    // Type 10 Record Layout - Non-filtered Dataset Chunks
    fun readRecord10(state: OpenFileState, dims : IntArray, chunkSize: Int): DataChunk {
        val address = raf.readOffset(state)

        // This field is the scaled offset of the chunk within the dataset. n is the number of dimensions for the dataset.
        val scaledOffset = LongArray(dims.size) { raf.readLong(state) }

        // Scaled offset is calculated by dividing the chunk dimension sizes into the chunk offsets.
        // so to get the chunk offset:
        // jhdf
        // 		int[] chunkOffset = new int[datasetInfo.getDatasetDimensions().length];
        //		for (int i = 0; i < chunkOffset.length; i++) {
        //			chunkOffset[i] = Utils.readBytesAsUnsignedInt(buffer, 8) * datasetInfo.getChunkDimensions()[i];
        //		}
        val chunkOffset = scaledOffset.mapIndexed { idx, scaledOffset -> (scaledOffset * dims[idx]).toInt() }.toIntArray()

        return DataChunk(address, chunkSize, chunkOffset, null, tiling.order(chunkOffset), tiling)
    }

    // Type 11 Record Layout - Filtered Dataset Chunks
    fun readRecord11(state: OpenFileState, dims : IntArray): DataChunk {
        val address = raf.readOffset(state)

        // LOOK variable size based on what? "Chunk Size (variable size; at most 8 bytes)"
        // jhdf
        // 		final int chunkSizeBytes = buffer.limit()
        //			- 8 // size of offsets
        //			- 4 // filter mask
        //			- datasetInfo.getDatasetDimensions().length * 8; // dimension offsets
        val rank = dims.size
        val chunkSizeBytes = recordSize - 8 - 4 - rank * 8
        val chunkSize = raf.readVariableSizeUnsigned(state, chunkSizeBytes).toInt()

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
        val chunkOffset = scaledOffset.mapIndexed { idx, scaledOffset -> (scaledOffset * dims[idx]).toInt() }.toIntArray()

        // ChunkImpl(val address: Long, val size: Int, val chunkOffset: IntArray, val filterMask: Int?)
        return DataChunk(address, chunkSize, chunkOffset, filterMask, tiling.order(chunkOffset), tiling)
    }

}
