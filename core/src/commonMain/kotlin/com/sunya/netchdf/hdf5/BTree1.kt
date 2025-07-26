@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.layout.Tiling
import com.sunya.cdm.util.InternalLibraryApi

/** B-tree, version 1, used for data (node type 1)
internal class BTree1(
    val h5: H5builder,
    val rootNodeAddress: Long,
    val nodeType : Int,  // 0 = group/symbol table, 1 = raw data chunks
    val ndimStorage: Int? = null // TODO allowed to be null ??
) {

    fun rootNodeAddress() = rootNodeAddress

    fun readNode(address: Long, parent: BTree1.Node?): Node =
        Node(address, parent as Node?)

    fun makeMissingDataChunkEntry(rootNode: Node, wantKey: LongArray) : DataChunkIF =
        DataChunkEntry1(0, rootNode, -1, DataChunkKey(-1, 0, wantKey), -1L)

    fun readGroupEntries() : Iterator<GroupEntry> {
        require(nodeType == 0)
        val root = Node(rootNodeAddress, null)
        return if (root.level == 0) {
            root.groupEntries.iterator()
        } else {
            val result = mutableListOf<GroupEntry>()
            for (entry in root.groupEntries) {
                readAllEntries(entry, root, result)
            }
            result.iterator()
        }
    }

    private fun readAllEntries(entry : GroupEntry, parent : Node, list : MutableList<GroupEntry>) {
        val node = Node(entry.childAddress, parent)
        if (node.level == 0) {
            list.addAll(node.groupEntries)
        } else {
            for (nested in node.groupEntries) {
                readAllEntries(nested, node, list)
            }
        }
    }

    // here both internal and leaf are the same structure
    // Btree nodes Level 1A1 - Version 1 B-trees
    inner class Node(val address: Long, val parent: Node?) {
        val level: Int
        val nentries: Int
        private val leftAddress: Long
        private val rightAddress: Long

        // type 0
        val groupEntries = mutableListOf<GroupEntry>()

        // type 1
        val dataChunkEntries = mutableListOf<DataChunkEntry1>()

        init {
            val state = OpenFileState(h5.getFileOffset(address), false)
            val magic: String = h5.raf.readString(state, 4)
            check(magic == "TREE") { "DataBTree doesnt start with TREE" }

            val type: Int = h5.raf.readByte(state).toInt()
            check(type == nodeType) { "DataBTree must be type $nodeType" }

            level = h5.raf.readByte(state).toInt() // leaf nodes are level 0
            nentries = h5.raf.readShort(state).toInt() // number of children to which this node points
            leftAddress = h5.readOffset(state)
            rightAddress = h5.readOffset(state)

            for (idx in 0 until nentries) {
                if (type == 0) {
                    val key = h5.readLength(state) // 4 or 8 bytes
                    val address = h5.readOffset(state) // 4 or 8 bytes
                    if (address > 0) groupEntries.add(GroupEntry(key, address))
                } else {
                    val chunkSize = h5.raf.readInt(state)
                    val filterMask = h5.raf.readInt(state)
                    val inner = LongArray(ndimStorage!!) { j -> h5.raf.readLong(state) }
                    val key = DataChunkKey(chunkSize, filterMask, inner)
                    val childPointer = h5.readAddress(state) // 4 or 8 bytes, then add fileOffset
                    dataChunkEntries.add(DataChunkEntry1(level, this, idx, key, childPointer))
                }
            }

            // note there may be unused entries, "All nodes of a particular type of tree have the same maximum degree,
            // but most nodes will point to less than that number of children""
        }

        fun isLeaf() = (level == 0)

        fun nentries() = nentries

        fun dataChunkEntryAt(idx: Int) = dataChunkEntries[idx]
    }

    /** @param key the byte offset into the local heap for the first object name in the subtree which that key describes. */
    data class GroupEntry(val key : Long, val childAddress : Long)

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
    data class DataChunkEntry1(val level : Int, val parent : Node, val idx : Int, val key : DataChunkKey, val childAddress : Long) : DataChunkIF {
        override fun childAddress() = childAddress
        override fun offsets() = key.offsets
        override fun isMissing() = (childAddress == -1L)
        override fun chunkSize() = key.chunkSize
        override fun filterMask() = key.filterMask

        override fun show(tiling : Tiling) : String = "chunkSize=${key.chunkSize}, chunkStart=${key.offsets.contentToString()}" +
                ", tile= ${tiling.tile(key.offsets).contentToString()}  idx=$idx"
    }

} */


interface DataChunkIF {
    fun childAddress(): Long
    fun offsets(): LongArray
    fun isMissing(): Boolean
    fun chunkSize(): Int
    fun filterMask(): Int?

    fun show(tiling : Tiling): String
}
