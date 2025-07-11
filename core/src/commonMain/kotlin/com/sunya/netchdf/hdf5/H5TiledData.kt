package com.sunya.netchdf.hdf5

import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.IndexND
import com.sunya.cdm.layout.Tiling

/** wraps BTree1 to handle iterating through tiled data (aka chunked data) */
internal class H5TiledData(val btree1 : BTree1, val varShape: LongArray,  val chunkShape: LongArray) {
    private val check = true
    private val debug = false
    private val debugMissing = false

    val tiling = Tiling(varShape, chunkShape)
    val rootNode : BTree1.Node

    // keep track of nodes so we only read once
    private val nodeCache = mutableMapOf<Long, BTree1.Node>()
    private var readHit = 0
    private var readMiss = 0

    init {
        rootNode = readNode(btree1.rootNodeAddress, null)
    }

    // node reading goes through here for caching
    private fun readNode(address : Long, parent : BTree1.Node?) : BTree1.Node {
        if (nodeCache[address] != null) {
            readHit++
            return nodeCache[address]!!
        }
        readMiss++
        val node = btree1.Node(address, parent)
        nodeCache[address] = node

        if (check) {
            if (debug) println("node = $address, level = ${node.level} nentries = ${node.nentries}")
            for (idx in 0 until node.nentries) {
                val thisEntry = node.dataChunkEntries[idx]
                val key = thisEntry.key.offsets
                if (debug) println(" $idx = ${key.contentToString()} tile = ${tiling.tile(key).contentToString()}")
                if (idx < node.nentries - 1) {
                    val nextEntry = node.dataChunkEntries[idx + 1]
                    if (tiling.compare(key, nextEntry.key.offsets) >= 0)
                        println("hey")
                    require(tiling.compare(key, nextEntry.key.offsets) < 0)
                }
            }
        }
        return node
    }

    // optimize later
    private fun findEntryContainingKey(parent : BTree1.Node, key : LongArray) : BTree1.DataChunkEntry1? {
        var foundEntry : BTree1.DataChunkEntry1? = null
        for (idx in 0 until parent.nentries) {
            foundEntry = parent.dataChunkEntries[idx]
            if (idx < parent.nentries - 1) {
                val nextEntry = parent.dataChunkEntries[idx + 1] // look at the next one
                if (tiling.compare(key, nextEntry.key.offsets) < 0) {
                    break
                }
            }
        }
        if (foundEntry == null) {
            if (parent.level == 0) {
                if (debugMissing) println("H5TiledData findEntryContainingKey missing key ${key.contentToString()}")
                return null
            }
            throw RuntimeException("H5TiledData findEntryContainingKey cant find key ${key.contentToString()}")
        }
        if (parent.level == 0) {
            return if (tiling.compare(key, foundEntry.key.offsets) == 0L) foundEntry else null
        }
        val node= readNode(foundEntry.childAddress, parent)
        return findEntryContainingKey(node, key)
    }

    fun dataChunks(wantSpace : IndexSpace) = Iterable { DataChunkIterator(wantSpace) }

    private inner class DataChunkIterator(wantSpace : IndexSpace) : AbstractIterator<BTree1.DataChunkEntry1>() {
        val tileIterator : Iterator<LongArray>

        init {
            val tileSection = tiling.section(wantSpace) // section in tiles that we want
            tileIterator = IndexND(tileSection, tiling.tileShape).iterator() // iterate over tiles we want
        }

        override fun computeNext() {
            if (!tileIterator.hasNext()) {
                return done()
            } else {
                val wantTile = tileIterator.next()
                val wantKey = tiling.index(wantTile) // convert to index "keys"
                val haveEntry = findEntryContainingKey(rootNode, wantKey)
                val useEntry = haveEntry ?:
                    // missing
                    BTree1.DataChunkEntry1(0, rootNode, -1, BTree1.DataChunkKey(-1, 0, wantKey), -1L)
                setNext(useEntry)
            }
        }
    }

    override fun toString(): String {
        return "TiledData(chunk=${chunkShape.contentToString()}, readHit=$readHit, readMiss=$readMiss)"
    }

}