@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import io.github.oshai.kotlinlogging.KotlinLogging
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.util.*

/** Level 1G - Fractal Heap  */
// TODO rewrite
internal class FractalHeap(private val h5: H5builder, forWho: String, address: Long) {
    private val raf: OpenFileIF
    val version: Int
    val heapIdLen: Short
    val flags: Byte
    val maxSizeOfObjects: Int
    val nextHugeObjectId: Long
    val freeSpace: Long
    val managedSpace: Long
    val allocatedManagedSpace: Long
    val offsetDirectBlock: Long
    val nManagedObjects: Long
    val sizeHugeObjects: Long
    val nHugeObjects: Long
    val sizeTinyObjects: Long
    val nTinyObjects: Long
    val btreeAddressHugeObjects: Long
    val freeSpaceTrackerAddress: Long
    val maxHeapSize: Short

    val startingRowsInRootIndirectBlock: Short
    val currentRowsInRootIndirectBlock: Int

    val maxDirectBlockSize: Long
    val tableWidth: Short
    val startingBlockSize: Long
    val rootBlockAddress: Long
    val rootBlock: IndirectBlock

    // filters
    val ioFilterLen: Short
    val sizeFilteredRootDirectBlock: Long
    val ioFilterMask : Int
    val doublingTable: DoublingTable

    var btreeHugeObjects: List<Any>? = null

    init {
        raf = h5.raf
        val state = OpenFileState(h5.getFileOffset(address), false)

        // header
        val magic: String = raf.readString(state,4)
        if (magic != "FRHP") throw IllegalStateException("$magic should equal FRHP")
        version = raf.readByte(state).toInt()

        heapIdLen = raf.readShort(state) // bytes
        ioFilterLen = raf.readShort(state) // bytes
        flags = raf.readByte(state)

        maxSizeOfObjects = raf.readInt(state) // greater than this are huge objects
        nextHugeObjectId = h5.readLength(state) // next id to use for a huge object
        btreeAddressHugeObjects = h5.readOffset(state) // v2 btree to track huge objects
        freeSpace = h5.readLength(state) // total free space in managed direct blocks
        freeSpaceTrackerAddress = h5.readOffset(state)

        managedSpace = h5.readLength(state) // total amount of managed space in the heap
        allocatedManagedSpace = h5.readLength(state) // total amount of managed space in the heap actually allocated
        offsetDirectBlock = h5.readLength(state) // linear heap offset where next direct block should be allocated
        nManagedObjects = h5.readLength(state) // number of managed objects in the heap

        sizeHugeObjects = h5.readLength(state) // total size of huge objects in the heap (in bytes)
        nHugeObjects = h5.readLength(state) // number huge objects in the heap
        sizeTinyObjects = h5.readLength(state) // total size of tiny objects packed in heap Ids (in bytes)
        nTinyObjects = h5.readLength(state) // number of tiny objects packed in heap Ids

        tableWidth = raf.readShort(state) // number of columns in the doubling table for managed blocks, must be power of 2
        startingBlockSize = h5.readLength(state) // starting direct block size in bytes, must be power of 2
        maxDirectBlockSize = h5.readLength(state) // maximum direct block size in bytes, must be power of 2

        maxHeapSize = raf.readShort(state) // log2 of the maximum size of heap's linear address space, in bytes

        startingRowsInRootIndirectBlock = raf.readShort(state) // starting number of rows of the root indirect block, 0 = maximum needed
        rootBlockAddress = h5.readOffset(state) // This is the address of the root block for the heap.
        // It can be the undefined address if there is no data in the heap.
        // It either points to a direct block (if the Current # of Rows in the Root
        // Indirect Block value is 0), or an indirect block.
        currentRowsInRootIndirectBlock = raf.readShort(state).toInt() // current number of rows of the root indirect block, 0 = direct block

        val hasFilters = ioFilterLen > 0
        sizeFilteredRootDirectBlock = if (hasFilters) h5.readLength(state) else -1
        ioFilterMask = if (hasFilters) raf.readInt(state) else -1
        val ioFilterInfo = if (hasFilters) raf.readByteArray(state, ioFilterLen.toInt()) else null

        val checksum: Int = raf.readInt(state)
        val hsize: Int = 8 + (2 * h5.sizeLengths) + h5.sizeOffsets

        doublingTable = DoublingTable(tableWidth.toInt(), startingBlockSize, allocatedManagedSpace, maxDirectBlockSize)

        // data
        rootBlock = IndirectBlock(currentRowsInRootIndirectBlock.toInt(), startingBlockSize)
        val cstate = state.copy(pos = h5.getFileOffset(rootBlockAddress))
        if (currentRowsInRootIndirectBlock == 0) {
            // Read direct block
            val dblock = DirectBlock()
            doublingTable.blockList.add(dblock)
            readDirectBlock(cstate, address, dblock)
            dblock.size = startingBlockSize // - dblock.extraBytes; // removed 10/1/2013
            rootBlock.add(dblock)
        } else {
            readIndirectBlock(rootBlock, cstate, address, hasFilters)

            // read in the direct blocks
            for (dblock: DirectBlock in doublingTable.blockList) {
                if (dblock.address > 0) {
                    val cstate2 = state.copy(pos = h5.getFileOffset(dblock.address))
                    readDirectBlock(cstate2, address, dblock)
                    // dblock.size -= dblock.extraBytes; // removed 10/1/2013
                }
            }
        }
    }

    fun getFractalHeapId(heapId: ByteArray): DHeapId {
        return DHeapId(heapId)
    }

    inner class DHeapId internal constructor(heapId: ByteArray) {
        val type: Int = (heapId[0].toInt() and 0x30) shr 4
        var subtype = 0 // 1 = indirect no filter, 2 = indirect, filter 3 = direct, no filter, 4 = direct, filter
        var n = 0 // the offset field size
        var m = 0
        var offset = 0 // This field is the offset of the object in the heap.
        var size = 0 // This field is the length of the object in the heap

        init {
            when (type) {
                0 -> {
                    n = maxHeapSize / 8
                    // The minimum number of bytes necessary to encode the Maximum Heap Size value
                    m = h5.getNumBytesFromMax(maxDirectBlockSize - 1)
                    // The length of the object in the heap, determined by taking the minimum value of
                    // Maximum Direct Block Size and Maximum Size of Managed Objects in the Fractal Heap Header.
                    // Again, the minimum number of bytes needed to encode that value is used for the size of this field.
                    offset = makeIntFromLEBytes(heapId, 1, n)
                    size = makeIntFromLEBytes(heapId, 1 + n, m)
                }
                1 -> {
                    // how fun to guess the subtype
                    val hasBtree = (btreeAddressHugeObjects > 0)
                    val hasFilters = (ioFilterLen > 0)
                    subtype = if (hasBtree) if (hasFilters) 2 else 1 else if (hasFilters) 4 else 3
                    when (subtype) {
                        1, 2 -> offset = makeIntFromLEBytes(heapId, 1, (heapId.size - 1))
                    }
                }
                2 -> {
                    // The sub-type for tiny heap IDs depends on whether the heap ID is large enough to store objects greater
                    // than T16 bytes or not. If the heap ID length is 18 bytes or smaller, the "normal" tiny heap ID form
                    // is used. If the heap ID length is greater than 18 bytes in length, the "extented" form is used.
                    subtype = if ((heapId.size <= 18)) 1 else 2 // 0 == normal, 1 = extended
                }
                else -> {
                    throw UnsupportedOperationException() // "DHeapId subtype ="+subtype);
                }
            }
        }

        fun computePosition(): Long {
            when (type) {
                0 -> return doublingTable.computePos(offset.toLong())
                1 -> {
                    when (subtype) {
                        1, 2 -> {
                            if (btreeHugeObjects == null) { // lazy
                                val local = BTree2data(h5, "FractalHeap btreeHugeObjects", btreeAddressHugeObjects)
                                require(local.btreeType == subtype)
                                btreeHugeObjects = local.records
                            }

                            val record1: BTree2data.Record1? = BTree2data.findRecord1byId(btreeHugeObjects!!, offset)
                            if (record1 == null) {
                                throw RuntimeException("Cant find DHeapId=$offset")
                            }
                            return record1.hugeObjectAddress
                        }

                        // 3, 4 -> return offset.toLong() // TODO only a guess
                        else -> throw RuntimeException("Unknown DHeapId subtype =$subtype")
                    }
                }

                else -> throw RuntimeException("Unknown DHeapId type =$type")
            }
        }

        override fun toString(): String {
            return "$type,$n,$m,$offset,$size"
        }

        fun show() = buildString {
            appendLine("   $type, $n, $m, $offset, $size, ${computePosition()}")
        }
    }

    inner class DoublingTable internal constructor(
        val tableWidth: Int,
        val startingBlockSize: Long,
        val managedSpace: Long,
        val maxDirectBlockSize: Long
    ) {
        val blockList = mutableListOf<DirectBlock>()

        private fun calcNrows(max: Long): Int {
            var n = 0
            var sizeInBytes: Long = 0
            var blockSize = startingBlockSize
            while (sizeInBytes < max) {
                sizeInBytes += blockSize * tableWidth
                n++
                if (n > 1) blockSize *= 2
            }
            return n
        }

        private fun assignSizes() {
            var block = 0
            var blockSize = startingBlockSize
            for (db: DirectBlock in blockList) {
                db.size = blockSize
                block++
                if ((block % tableWidth == 0) && (block / tableWidth > 1)) blockSize *= 2
            }
        }

        fun computePos(offset: Long): Long {
            var block = 0
            for (db: DirectBlock in blockList) {
                if (db.address < 0) continue
                if ((offset >= db.offset) && (offset <= db.offset + db.size)) {
                    val localOffset = offset - db.offset
                    return db.dataPos + localOffset
                }
                block++
            }
            logger.error { "DoublingTable: illegal offset=$offset" }
            throw IllegalStateException("offset=$offset")
        }

        fun showDetails() = buildString {
            appendLine(" DoublingTable: tableWidth= $tableWidth startingBlockSize = $startingBlockSize managedSpace=$managedSpace maxDirectBlockSize=$maxDirectBlockSize")
            appendLine(" DataBlocks:")
            appendLine("  address            dataPos            offset size")
            for (dblock: DirectBlock in blockList) {
                appendLine("  ${dblock.address}, ${dblock.dataPos}, ${dblock.offset}, ${dblock.size}")
            }
        }
    }

    inner class IndirectBlock(var nrows: Int, val size: Long) {
        var directRows = 0
        var indirectRows = 0
        val directBlocks = mutableListOf<DirectBlock>()
        var indirectBlocks = mutableListOf<IndirectBlock>()

        init {
            if (nrows < 0) {
                nrows = log2(size) - log2(startingBlockSize * tableWidth) + 1 // LOOK
            }
            val maxrows_directBlocks = (log2(maxDirectBlockSize) - log2(startingBlockSize)) + 2 // LOOK
            if (nrows < maxrows_directBlocks) {
                directRows = nrows
                indirectRows = 0
            } else {
                directRows = maxrows_directBlocks
                indirectRows = (nrows - maxrows_directBlocks)
            }
        }

        fun add(dblock: DirectBlock) {
            directBlocks.add(dblock)
        }

        fun add(iblock: IndirectBlock) {
            indirectBlocks.add(iblock)
        }
    }

    fun readIndirectBlock(iblock: IndirectBlock, state: OpenFileState, heapAddress: Long, hasFilter: Boolean) {
        // header
        val magic: String = raf.readString(state,4)
        if (magic != "FHIB") throw IllegalStateException("$magic should equal FHIB")
        val version: Byte = raf.readByte(state)
        val heapHeaderAddress: Long = h5.readOffset(state)
        if (heapAddress != heapHeaderAddress) throw IllegalStateException()
        var nbytes = maxHeapSize / 8
        if (maxHeapSize % 8 != 0) nbytes++
        val blockOffset: Long = h5.readVariableSizeUnsigned(state, nbytes)
        val npos: Long = state.pos

        // child direct blocks
        var blockSize = startingBlockSize
        for (row in 0 until iblock.directRows) {
            if (row > 1) blockSize *= 2
            for (i in 0 until doublingTable.tableWidth) {
                val directBlock = DirectBlock()
                iblock.add(directBlock)
                directBlock.address = h5.readOffset(state) // This field is the address of the child direct block. The size of the
                // [uncompressed] direct block can be computed by its offset in the
                // heap's linear address space.
                if (hasFilter) {
                    directBlock.sizeFilteredDirectBlock = h5.readLength(state)
                    directBlock.filterMask = raf.readInt(state)
                }
                if (debugDetail || debugFractalHeap) println("  DirectChild " + i + " address= " + directBlock.address)
                directBlock.size = blockSize

                // if (directChild.address >= 0)
                doublingTable.blockList.add(directBlock)
            }
        }

        // child indirect blocks
        for (row in 0 until iblock.indirectRows) {
            blockSize *= 2
            for (i in 0 until doublingTable.tableWidth) {
                val iblock2 = IndirectBlock(-1, blockSize)
                iblock.add(iblock2)
                val childIndirectAddress: Long = h5.readOffset(state)
                val cstate = state.copy(pos = childIndirectAddress)
                if (debugDetail || debugFractalHeap) println("  InDirectChild $row address= $childIndirectAddress")
                if (childIndirectAddress >= 0) readIndirectBlock(iblock2, cstate, heapAddress, hasFilter)
            }
        }
    }

    class DirectBlock {
        var address: Long = 0
        var sizeFilteredDirectBlock: Long = 0
        var filterMask = 0
        var dataPos: Long = 0
        var offset: Long = 0
        var size: Long = 0
        var extraBytes = 0
        var wasRead = false // when empty, object exists, but fields are not init. not yet sure where to use.
        override fun toString(): String {
            return "DataBlock{offset=$offset, size=$size, dataPos=$dataPos}"
        }
    }

    fun readDirectBlock(state: OpenFileState, heapAddress: Long, dblock: DirectBlock) {
        if (state.pos < 0) return  // means its empty
        val startPos = state.pos

        val magic: String = raf.readString(state,4)
        if (magic != "FHDB") throw IllegalStateException("$magic should equal FHDB")
        val version = raf.readByte(state)

        val heapHeaderAddress = h5.readOffset(state) // This is the address for the fractal heap header that this block belongs
        // to. This field is principally used for file integrity checking.
        if (heapAddress != heapHeaderAddress) throw IllegalStateException()

        // This is the offset of the block within the fractal heap’s address space (in bytes).
        // The number of bytes used to encode this field is the Maximum Heap Size (in the heap’s header) divided by 8
        // and rounded up to the next highest integer, for values that are not a multiple of 8.
        // This value is principally used for file integrity checking.
        var nbytes = maxHeapSize / 8
        if (maxHeapSize % 8 != 0) nbytes++
        dblock.offset = h5.readVariableSizeUnsigned(state, nbytes)

        dblock.dataPos = startPos  // offsets are from the start of the block

        // keep track of how much room is taken out of block size, that is, how much is left for the object
        // probably dont meed this since we dont use ByteBuffers
        dblock.extraBytes = 5
        dblock.extraBytes += if (h5.isOffsetLong) 8 else 4
        dblock.extraBytes += nbytes
        if ((flags.toInt() and 2) != 0) dblock.extraBytes += 4 // ?? size of checksum

        dblock.wasRead = true
        if (debugDetail || debugFractalHeap) {
            println("  DirectBlock offset= " + dblock.offset + " dataPos = " + dblock.dataPos)
        }
    }

    companion object {
        private val logger = KotlinLogging.logger("FractalHeap")
        var debugDetail = false
        var debugFractalHeap = false
        var debugPos = false
    }
} // FractalHeap
