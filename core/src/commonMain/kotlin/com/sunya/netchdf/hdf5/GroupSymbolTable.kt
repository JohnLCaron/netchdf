@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.util.InternalLibraryApi
import io.github.oshai.kotlinlogging.KotlinLogging

/** Wraps a BTree1, when its used to store symbol table nodes for GroupOld. */
@OptIn(InternalLibraryApi::class)
internal class GroupSymbolTable(val btreeAddress : Long) {

    fun symbolTableEntries(h5: H5builder): Iterable<SymbolTableEntry> {
        val btree = BTreeSymbolTable(h5, btreeAddress)
        val symbols = mutableListOf<SymbolTableEntry>()
        btree.readGroupEntries().forEach {
            readSymbolTableNode(h5, it.childAddress, symbols)
        }
        return symbols
    }

    // level 1B Group Symbol Table Nodes
    internal fun readSymbolTableNode(h5: H5builder, address: Long, symbols: MutableList<SymbolTableEntry>) {
        val state = OpenFileState(h5.getFileOffset(address), false)
        val magic: String = h5.raf.readString(state, 4)
        check(magic == "SNOD") { "$magic should equal SNOD" }
        state.pos += 2
        val nentries = h5.raf.readShort(state)

        var posEntry = state.pos
        for (i in 0 until nentries) {
            val entry = h5.readSymbolTable(state)
            posEntry += entry.dataSize
            if (entry.objectHeaderAddress != 0L) { // skip zeroes, probably a bug in HDF5 file format or docs, or me
                symbols.add(entry)
            } else {
                logger.warn{"   BAD objectHeaderAddress==0 !! $entry"}
            }
        }
    }

    companion object {
        private val logger = KotlinLogging.logger("GroupSymbolTable")
    }
}

// Level 1C - Symbol Table Entry
internal fun H5builder.readSymbolTable(state : OpenFileState) : SymbolTableEntry {
    val rootEntry =
        structdsl("SymbolTableEntry", raf, state) {
            fld("linkNameOffset", sizeOffsets)
            fld("objectHeaderAddress", sizeOffsets)
            fld("cacheType", 4)
            skip(4)
            fld("scratchPad", 16)
            overlay("scratchPad", 0, "btreeAddress")
            overlay("scratchPad", sizeOffsets, "nameHeapAddress")
        }
    if (debugGroup) rootEntry.show()

    // may be btree or symbolic link
    var btreeAddress : Long? = null
    var nameHeapAddress : Long? = null
    var linkOffset : Int? = null
    var isSymbolicLink = false
    when (val cacheType = rootEntry.getInt("cacheType")) {
        0 -> {
            // no-op
        }
        1 -> {
            btreeAddress = rootEntry.getLong("btreeAddress")
            nameHeapAddress = rootEntry.getLong("nameHeapAddress")
        }
        2 -> {
            linkOffset = rootEntry.getInt("scratchPad")
            isSymbolicLink = true
        }
        else -> {
            throw IllegalArgumentException("SymbolTableEntry has unknown cacheType=$cacheType")
        }
    }

    return SymbolTableEntry(
        rootEntry.getLong("linkNameOffset"), // LOOK what about rootEntry.linkNameOffset.getLong()) sizeOffsets = Int ???
        rootEntry.getLong("objectHeaderAddress"),
        rootEntry.getInt("cacheType"),
        btreeAddress,
        nameHeapAddress,
        linkOffset,
        isSymbolicLink,
        rootEntry.dataSize(),
    )
}

// III.C. Disk Format: Level 1C - Symbol Table Entry (aka Group Entry)
internal data class SymbolTableEntry(
    val nameOffset: Long,
    val objectHeaderAddress: Long,
    val cacheType : Int,
    val btreeAddress: Long?,
    val nameHeapAddress: Long?,
    val linkOffset: Int?,
    val isSymbolicLink: Boolean,
    val dataSize : Int, // nbytes on disk
) {
    init {
        require(dataSize == 32 || dataSize == 40) // sanity check
    }
}

internal class BTreeSymbolTable(
    val h5: H5builder,
    val rootNodeAddress: Long,
) {
    fun readGroupEntries(): Iterator<GroupEntry> {
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

    private fun readAllEntries(entry: GroupEntry, parent: Node, list: MutableList<GroupEntry>) {
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

        init {
            val state = OpenFileState(h5.getFileOffset(address), false)
            val magic: String = h5.raf.readString(state, 4)
            check(magic == "TREE") { "BTreeSymbolTable doesnt start with TREE" }

            val type: Int = h5.raf.readByte(state).toInt()
            check(type == 0) { "BTreeSymbolTable must be node type 0" }

            level = h5.raf.readByte(state).toInt() // leaf nodes are level 0
            nentries = h5.raf.readShort(state).toInt() // number of children to which this node points
            leftAddress = h5.readOffset(state)
            rightAddress = h5.readOffset(state)

            repeat (nentries) {
                    val key = h5.readLength(state) // 4 or 8 bytes
                    val address = h5.readOffset(state) // 4 or 8 bytes
                    if (address > 0) groupEntries.add(GroupEntry(key, address))
                }
            }
        }

    /** @param key the byte offset into the local heap for the first object name in the subtree which that key describes. */
    data class GroupEntry(val key: Long, val childAddress: Long)
}
