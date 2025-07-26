@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.util.InternalLibraryApi
import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigInteger
import kotlin.math.ceil
import kotlin.math.min

val UNDEFINED_ADDRESS = -1L

// the start of a port of jfhf FractalHeap. May not need it.
class FractalHeapJ(val h5: H5builder, forWho: String, val fractalHeapAddress: Long) {

    val raf = h5.raf
    val state = OpenFileState(fractalHeapAddress, false)

    val maxDirectBlockSize : Long
    val maxSizeOfManagedObjects: UInt
    val idLength : UInt
    val ioFiltersLength : Int
    val currentRowsInRootIndirectBlock : Int
    val startingRowsInRootIndirectBlock : UShort
    val startingBlockSize : Long
    val maxHeapSize : UShort
    val tableWidth: Int

    val numberOfTinyObjectsInHeap: Long
    val sizeOfTinyObjectsInHeap: Long
    val numberOfHugeObjectsInHeap: Long
    val sizeOfHugeObjectsInHeap: Long
    val numberOfManagedObjectsInHeap: Long
    val offsetOfDirectBlockAllocationIteratorInManagedSpace: Long
    val amountOfAllocatedManagedSpaceInHeap: Long
    val amountOfManagedSpaceInHeap: Long
    val addressOfManagedBlocksFreeSpaceManager: Long
    val freeSpaceInManagedBlocks: Long
    val bTreeAddressOfHugeObjects: Long
    val nextHugeObjectId: Long
    private val flags : Int

    private var blockIndex = 0

    /** This map holds all the direct blocks keyed by their offset in the heap address space. */
    private val directBlocks = mutableMapOf<Long, DirectBlock>() // Sorted map

    val bytesToStoreOffset : Int
    val bytesToStoreLength : Int

    init {
        val headerSize: Int = (4 + 1 + 2 + 2 + 1 + 4 + 12 * h5.sizeLengths + 3 * h5.sizeOffsets + 2 + 2 + 2 + 2 + 4)

        val magic = raf.readString(state, 4)
        check(magic == "FRHP") { "Fractal heap signature 'FRHP' not matched, at address " + fractalHeapAddress }

        // Version Number
        val version = raf.readByte(state).toInt()
        if (version != 0) {
            throw RuntimeException("Unsupported fractal heap version detected. Version: " + version)
        }

        idLength = raf.readShort(state).toUInt() // readBytesAsUnsignedInt(bb, 2)
        ioFiltersLength = raf.readShort(state).toInt() //readBytesAsUnsignedInt(bb, 2)
        flags = raf.readByte(state).toInt() // java.util.BitSet.valueOf(byteArrayOf(bb.get()))

        maxSizeOfManagedObjects = raf.readInt(state).toUInt() // readBytesAsUnsignedLong(bb, 4)
        nextHugeObjectId = h5.readLength(state) // readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())
        bTreeAddressOfHugeObjects = h5.readOffset(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfOffsets())
        freeSpaceInManagedBlocks = h5.readLength(state) // readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())
        addressOfManagedBlocksFreeSpaceManager = h5.readOffset(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfOffsets())

        amountOfManagedSpaceInHeap = h5.readLength(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())
        amountOfAllocatedManagedSpaceInHeap = h5.readLength(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())
        offsetOfDirectBlockAllocationIteratorInManagedSpace = h5.readLength(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())
        numberOfManagedObjectsInHeap = h5.readLength(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())

        sizeOfHugeObjectsInHeap = h5.readLength(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())
        numberOfHugeObjectsInHeap = h5.readLength(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())
        sizeOfTinyObjectsInHeap = h5.readLength(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())
        numberOfTinyObjectsInHeap = h5.readLength(state) //readBytesAsUnsignedLong(bb, sb.getSizeOfLengths())

        tableWidth = raf.readShort(state).toInt() //readBytesAsUnsignedInt(bb, 2)
        startingBlockSize = h5.readLength(state) //readBytesAsUnsignedInt(bb, sb.getSizeOfLengths())
        maxDirectBlockSize = h5.readLength(state) //readBytesAsUnsignedInt(bb, sb.getSizeOfLengths())

        // Value stored in bits
        maxHeapSize = raf.readShort(state).toUShort() // readBytesAsUnsignedInt(bb, 2)

        // Calculate byte sizes needed later
        bytesToStoreOffset = ceil(maxHeapSize.toDouble() / 8.0).toInt()
        bytesToStoreLength = bytesNeededToHoldNumber(min(maxDirectBlockSize, maxSizeOfManagedObjects.toLong()))

        startingRowsInRootIndirectBlock = raf.readShort(state).toUShort() // readBytesAsUnsignedInt(bb, 2);
        val addressOfRootBlock = h5.readOffset(state)//  readBytesAsUnsignedLong(bb, sb.getSizeOfOffsets())
        currentRowsInRootIndirectBlock = raf.readShort(state).toInt() // readBytesAsUnsignedInt(bb, 2)

        //TODO FractalHeap with filters, see existing
        if (ioFiltersLength > 0) {
            throw RuntimeException("IO filters are currently not supported")
        }

        // Read the root block. TODO compare to existing
        if (addressOfRootBlock != UNDEFINED_ADDRESS) {
            if (currentRowsInRootIndirectBlock == 0) {
                // Read direct block
                val db = DirectBlock(addressOfRootBlock)
                directBlocks.put(db.blockOffset, db)
            } else {
                // Read indirect block
                val indirectBlock = IndirectBlock(addressOfRootBlock)
                for (directBlockAddress in indirectBlock.childBlockAddresses) {
                    val blockSize = getSizeOfDirectBlock(blockIndex++)
                    if (blockSize != -1) {
                        val db = DirectBlock(directBlockAddress)
                        directBlocks.put(db.blockOffset, db)
                    } //else {
                    //    IndirectBlock(fractalHeapAddress) // TODO
                    //}
                }
            }
        }
        logger.debug{"Read fractal heap at address $fractalHeapAddress, loaded ${directBlocks.size} direct blocks"}
    }

/* // TODO
    fun getFractalHeapId(heapId: ByteArray): ByteArray {
        /* if (buffer.remaining() != idLength) {
            throw RuntimeException(
                ("ID length is incorrect accessing fractal heap at address " + address
                        + ". IDs should be " + idLength + " bytes but was " + buffer.capacity() + " bytes.")
            )
        } */

        val idFlags: java.util.BitSet = java.util.BitSet.valueOf(byteArrayOf(buffer.get()))

        val version: Int = bitsToInt(idFlags, 6, 2)
        if (version != 0) {
            throw RuntimeException("Unsupported btree v2 ID version detected. Version: " + version)
        }

        // do these agree ?
        val type: Int = bitsToInt(idFlags, 4, 2)
        val myType: Int = (heapId[0].toInt() and 0x30) shr 4


        when (type) {
            0 -> {
                val offset: Long = readBytesAsUnsignedLong(heapId, bytesToStoreOffset)
                val length: Int = readBytesAsUnsignedInt(heapId, bytesToStoreLength)

                logger.debug{"Getting ID at offset=$offset length=$length") }

                // Figure out which direct block holds the offset
                val entry: MutableMap.MutableEntry<Long?, DirectBlock?> = directBlocks.floorEntry(offset)

                val bb: java.nio.ByteBuffer = entry.value!!.getData()
                bb.order(ByteOrder.LITTLE_ENDIAN)
                bb.position(java.lang.Math.toIntExact(offset - entry.key!!))
                return createSubBuffer(bb, length)
            }

            1 -> {
                if (this.bTreeAddressOfHugeObjects <= 0) {
                    throw RuntimeException("Huge objects without BTreev2 are currently not supported")
                }

                val hugeObjectBTree: BTreeV2<HugeFractalHeapObjectUnfilteredRecord?> =
                    BTreeV2(this.hdfBackingStorage, this.bTreeAddressOfHugeObjects)

                if (hugeObjectBTree.getRecords().size() !== 1) {
                    throw RuntimeException("Only Huge objects BTrees with 1 record are currently supported")
                }

                val ho: HugeFractalHeapObjectUnfilteredRecord = hugeObjectBTree.getRecords().get(0)

                return this.hdfBackingStorage.readBufferFromAddress(ho.getAddress(), ho.getLength() as Int)
            }

            2 -> throw RuntimeException("Tiny objects are currently not supported")
            else -> throw RuntimeException("Unrecognized ID type, type=" + type)
        }
    }

 */

    inner class IndirectBlock(address: Long) {
        val state2 = OpenFileState(address, false)
        val childBlockAddresses = mutableListOf<Long>()

        init {
           // val headerSize: Int = (4 + 1 + h5.sizeOffsets + bytesToStoreOffset
           //         + currentRowsInRootIndirectBlock * tableWidth * this.rowSize + 4)
           // val bb: java.nio.ByteBuffer = hdfBackingStorage.readBufferFromAddress(address, headerSize)

            val magic = raf.readString(state2, 4)
            check(magic == "FHIB") { "Fractal heap signature 'FHIB' not matched, at address " + address }

            val directBlockVersion = raf.readByte(state2).toInt()
            if (directBlockVersion != 0) {
                throw RuntimeException("Unsupported direct block version detected. Version: " + directBlockVersion)
            }

            val heapAddress= h5.readOffset(state2) //readBytesAsUnsignedLong(bb, sb.getSizeOfOffsets())
            if (heapAddress != fractalHeapAddress) {
                throw RuntimeException("Indirect block does not match fractalHeapAddress")
            }

            val blockOffset = h5.readOffset(state2) //readBytesAsUnsignedLong(bb, bytesToStoreOffset)

            for (i in 0..<currentRowsInRootIndirectBlock * tableWidth) {
                // TODO only works for unfiltered
                val childAddress = h5.readOffset(state2) // readBytesAsUnsignedLong(bb, this.rowSize)
                if (childAddress == UNDEFINED_ADDRESS) {
                    break
                } else {
                    childBlockAddresses.add(childAddress)
                }
            }

            // Validate checksum
            // bb.rewind()
            // ChecksumUtils.validateChecksum(bb)
        }

        val isIoFilters: Boolean = ioFiltersLength > 0

        fun rowSize(): Int {
            var size: Int = h5.sizeOffsets
            if (this.isIoFilters) {
                size += h5.sizeLengths
                size += 4 // filter mask
            }
            return size
        }
    }

    inner class DirectBlock (val address: Long) {
        val state2 = OpenFileState(address, false)
        val blockOffset: Long

        init {
            // val headerSize: Int = 4 + 1 + h5.sizeOffsets + bytesToStoreOffset + 4
            // ByteBuffer bb = hdfBackingStorage.readBufferFromAddress(address, headerSize);

            val magic = raf.readString(state2, 4)
            check(magic == "FHDB") { "Fractal heap signature 'FHDB' not matched, at address " + address }

            val directBlockVersion = raf.readByte(state2).toInt()
            if (directBlockVersion != 0) {
                throw RuntimeException("Unsupported direct block version detected. Version: " + directBlockVersion)
            }

            val heapAddress= h5.readOffset(state2) //readBytesAsUnsignedLong(bb, sb.getSizeOfOffsets())
            if (heapAddress != fractalHeapAddress) {
                throw RuntimeException("Indirect block read from invalid fractal heap")
            }

            blockOffset = h5.readVariableSizeUnsigned(state2, bytesToStoreOffset)
            val sizeOfDirectBlock = getSizeOfDirectBlock(blockIndex)

            //data = hdfBackingStorage.map(address, sizeOfDirectBlock)

            //if (checksumPresent()) {
                // val storedChecksum: Int = bb.getInt()
                // TODO Validate checksum
            //}
        }

        fun checksumPresent(): Boolean {
            return isBitSet(flags, CHECKSUM_PRESENT_BIT)
        }

        /*
        fun getData(): java.nio.ByteBuffer {
            return data.order(ByteOrder.LITTLE_ENDIAN)
        }

         */

        override fun toString(): String {
            return "DirectBlock [address= $address blockOffset= $blockOffset data here]"
        }
    }

    private fun getSizeOfDirectBlock(blockIndex: Int): Int {
        val row = blockIndex / tableWidth // int division
        if (row < 2) {
            return startingBlockSize.toInt()
        } else {
            val big = TWO.pow(row - 1)
            val size: Int = startingBlockSize.toInt() * big.intValueExact()
            if (size < maxDirectBlockSize) {
                return size
            } else {
                return -1 // Indicates the block is an indirect block
            }
        }
    }

    companion object {
        val CHECKSUM_PRESENT_BIT = 1
        val logger = KotlinLogging.logger("FractalHeapJ")
        val TWO = BigInteger.valueOf(2L)
    }

}

fun bytesNeededToHoldNumber(number: Long): Int {
    require(number >= 0) { "Only for unsigned numbers" }
    if (number == 0L) {
        return 1
    }
    return ceil(java.math.BigInteger.valueOf(number).bitLength() / 8.0).toInt()
}