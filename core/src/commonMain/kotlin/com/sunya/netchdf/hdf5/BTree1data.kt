@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

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
) {
    val tiling = Tiling(varShape, chunkShape)
    val ndimStorage = chunkShape.size
    val rootNode: BTreeNode

    init {
        rootNode = BTreeNode(rootNodeAddress, null)
    }

    // if other layouts like BTree2data had this interface we could use in chunkConcurrent
    fun asSequence(): Sequence<Pair<Long, DataChunk>> = sequence {
        repeat( tiling.nelems) {
            //val startingIndex = tiling.orderToIndex(it.toLong())
            //val indexSpace = IndexSpace(startingIndex, tiling.chunk)
            yield(Pair(it.toLong(), findDataChunk(it) ?: missingDataChunk(it)))
        }
    }

    internal fun findDataChunk(order: Int): DataChunk? {
        return rootNode.findDataChunk(order)
    }

    // here both internal and leaf are the same structure
    // Btree nodes Level 1A1 - Version 1 B-trees
    inner class BTreeNode(val address: Long, val parent: BTreeNode?)  {
        var level: Int = 0
        var nentries: Int = 0

        val keyValues = mutableListOf<Pair<Int, DataChunk>>() // tile order to DataChunk
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
                    val inner = LongArray(ndimStorage) { j -> raf.readLong(state) }
                    val order = tiling.order(inner).toInt()
                    val key = DataChunkKey(order, chunkSize, filterMask)
                    val childPointer = raf.readAddress(state) // 4 or 8 bytes, then add fileOffset
                    if (level == 0) {
                        keyValues.add(Pair(order, DataChunk(key, childPointer)))
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
                val kv = keyValues.find { it.first == wantOrder }
                return kv?.second
            }
            return null
        }

        override fun toString(): String {
            return "BTreeNode(address=$address, level=$level, nentries=$nentries, lastOrder=$lastOrder)"
        }

    }

    data class DataChunkKey(val order: Int, val chunkSize: Int, val filterMask : Int)

    //  childAddress = data chunk (level 1) else a child node
    inner class DataChunk(val key : DataChunkKey, val childAddress : Long) : DataChunkIF {
        override fun childAddress() = childAddress
        override fun offsets() = tiling.orderToIndex(key.order.toLong())
        override fun isMissing() = (childAddress <= 0L) // may be 0 or -1
        override fun chunkSize() = key.chunkSize
        override fun filterMask() = key.filterMask

        override fun show(tiling : Tiling) : String = "order=$key, chunkSize=${key.chunkSize}, chunkStart=${offsets().contentToString()}" +
                ", tile= ${tiling.tile(offsets() ).contentToString()}"

        fun show() = show(tiling)
    }

    fun missingDataChunk(order: Int) : DataChunk {
        return DataChunk(DataChunkKey(order, 0, 0), -1L)
    }
}

interface DataChunkIF {
    fun childAddress(): Long
    fun offsets(): LongArray
    fun isMissing(): Boolean
    fun chunkSize(): Int
    fun filterMask(): Int?

    fun show(tiling : Tiling): String
}

