@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.layout.Tiling
import com.sunya.cdm.util.InternalLibraryApi
import kotlin.collections.mutableListOf

/** a BTree1 that uses OpenFileExtended and tracks its own tiling. */
internal class BTree1data(
    val raf: OpenFileExtended,
    rootNodeAddress: Long,
    varShape: LongArray,
    chunkShape: LongArray,
) : DataChunkSequence {

    val tiling = Tiling(varShape, chunkShape)
    val ndimStorage = chunkShape.size
    val rootNode: BTreeNode

    init {
        rootNode = BTreeNode(rootNodeAddress, null)
    }

    override fun asSequence(): Sequence<DataChunk> = sequence {
        repeat( tiling.nelems) {
            yield(findDataChunk(it) ?: missingDataChunk(it, tiling))
        }
    }

    fun chunkIterator(): Iterator<DataChunk> = asSequence().iterator()

    fun countChunks() = asSequence().count()

    internal fun findDataChunk(order: Int): DataChunk? {
        return rootNode.findDataChunk(order)
    }

    // Btree nodes Level 1A1 - Version 1 B-trees
    inner class BTreeNode(val address: Long, val parent: BTreeNode?)  {
        var level: Int = 0
        var nentries: Int = 0

        val dataChunks = mutableListOf<DataChunk>() // tile order to DataChunk
        val children = mutableListOf<BTreeNode>()

        var lastOrder : Int = 0

        init {
            if (address > 0) {
                val state = OpenFileState(raf.getFileOffset(address), false)
                val magic: String = raf.readString(state, 4)
                check(magic == "TREE") { "DataBTree doesnt start with TREE" }

                val type: Int = raf.readByte(state).toInt()
                check(type == 1) { "DataBTree must be type 1" }

                level = raf.readByte(state).toInt() // leaf nodes are level 0
                nentries = raf.readShort(state).toInt() // number of children to which this node points
                val leftAddress = raf.readOffset(state)
                val rightAddress = raf.readOffset(state)

                repeat(nentries) {
                    val chunkSize = raf.readInt(state)
                    val filterMask = raf.readInt(state)
                    val chunkOffset = LongArray(ndimStorage) { j -> raf.readLong(state) }
                    val order = tiling.order(chunkOffset).toInt()
                    val childPointer = raf.readAddress(state) // 4 or 8 bytes, then add fileOffset
                    if (level == 0) {
                        // data class DataChunk(val address: Long, val size: Int, val chunkOffset: IntArray, val filterMask: Int?, val order: Int, val tiling: Tiling?=null) {
                        val dataChunk = DataChunk(childPointer, chunkSize, chunkOffset.toIntArray(), filterMask, order, tiling)
                        dataChunks.add(dataChunk)
                        lastOrder = order
                    } else {
                        children.add( BTreeNode(childPointer, this) )
                    }
                }
                if (children.isNotEmpty()) {
                    lastOrder = children.last().lastOrder
                }
            }

            // note there may be unused entries, "All nodes of a particular type of tree have the same maximum degree,
            // but most nodes will point to less than that number of children""
        }

        // uses a tree search = O(log n)
        fun findDataChunk(wantOrder: Int): DataChunk? {
            if (children.isNotEmpty()) { // search tree; assumes that chunks are ordered
                children.forEach { childNode ->
                    if (wantOrder <= childNode.lastOrder)
                        return childNode.findDataChunk(wantOrder)
                }
            } else {  // If it's a leaf node (no children)
                return dataChunks.find { it.order == wantOrder }
            }
            return null
        }

        override fun toString(): String {
            return "BTreeNode(address=$address, level=$level, nentries=$nentries, lastOrder=$lastOrder)"
        }

    }
}

