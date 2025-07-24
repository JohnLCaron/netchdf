@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.layout.Tiling
import com.sunya.cdm.util.InternalLibraryApi
import kotlin.collections.mutableListOf

/** a BTree1 that uses OpenFileExtended and tracks its own tiling. */
internal class BTree1data(
    val raf: OpenFileExtended,
    val rootNodeAddress: Long,
    varShape: LongArray,
    chunkShape: LongArray,
) {
    val tiling = Tiling(varShape, chunkShape)
    val ndimStorage = chunkShape.size

    fun rootNode(): BTreeNode = BTreeNode(rootNodeAddress, null)

    // here both internal and leaf are the same structure
    // Btree nodes Level 1A1 - Version 1 B-trees
    inner class BTreeNode(val address: Long, val parent: BTreeNode?)  {
        val level: Int
        val nentries: Int
        private val leftAddress: Long
        private val rightAddress: Long

        val keys = mutableListOf<LongArray>()
        val values = mutableListOf<DataChunkIF>()
        val children = mutableListOf<BTreeNode>()

        init {
            val state = OpenFileState(raf.getFileOffset(address), false)
            val magic: String = raf.readString(state, 4)
            check(magic == "TREE") { "DataBTree doesnt start with TREE" }

            val type: Int = raf.readByte(state).toInt()
            check(type == 1) { "DataBTree must be type 1" }

            level = raf.readByte(state).toInt() // leaf nodes are level 0
            nentries = raf.readShort(state).toInt() // number of children to which this node points
            leftAddress = raf.readOffset(state)
            rightAddress = raf.readOffset(state)

            if (nentries == 0) {
                val chunkSize = raf.readInt(state)
                val filterMask = raf.readInt(state)
                val inner = LongArray(ndimStorage) { j -> raf.readLong(state) }
                val key = DataChunkKey(chunkSize, filterMask, inner)
                val childPointer = raf.readAddress(state)
                keys.add(inner)
                values.add(DataChunkEntry1(this, key, childPointer))
            } else {
                repeat(nentries) {
                    val chunkSize = raf.readInt(state)
                    val filterMask = raf.readInt(state)
                    val inner = LongArray(ndimStorage) { j -> raf.readLong(state) }
                    val key = DataChunkKey(chunkSize, filterMask, inner)
                    val childPointer = raf.readAddress(state) // 4 or 8 bytes, then add fileOffset
                    if (level == 0) {
                        keys.add(inner)
                        values.add(DataChunkEntry1( this,  key, childPointer))
                    } else {
                        children.add(BTreeNode(childPointer, this))
                    }
                }
            }

            // note there may be unused entries, "All nodes of a particular type of tree have the same maximum degree,
            // but most nodes will point to less than that number of children""
        }

        //  return only the leaf nodes, in any order
        fun asSequence(): Sequence<Pair<Long, DataChunkIF>> = sequence {
            // Handle child nodes recursively (in-order traversal)
            if (children.isNotEmpty()) {
                children.forEachIndexed { index, childNode ->
                    yieldAll(childNode.asSequence()) // Yield all elements from the child
                }
            } else {  // If it's a leaf node (no children)
                keys.forEachIndexed { index, key ->
                    yield(tiling.order(key) to values[index]) // Yield all key-value pairs
                }
            }
        }
    }

    data class DataChunkKey(val chunkSize: Int, val filterMask : Int, val offsets: LongArray) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is DataChunkKey) return false
            if (!offsets.contentEquals(other.offsets)) return false
            return true
        }

        override fun hashCode(): Int {
            return offsets.contentHashCode()
        }
    }

    //  childAddress = data chunk (level 1) else a child node
    data class DataChunkEntry1(val parent : BTreeNode, val key : DataChunkKey, val childAddress : Long) : DataChunkIF {
        override fun childAddress() = childAddress
        override fun offsets() = key.offsets
        override fun isMissing() = (childAddress <= 0L) // may be 0 or -1
        override fun chunkSize() = key.chunkSize
        override fun filterMask() = key.filterMask

        override fun show(tiling : Tiling) : String = "chunkSize=${key.chunkSize}, chunkStart=${key.offsets.contentToString()}" +
                ", tile= ${tiling.tile(key.offsets).contentToString()}"
    }
}

