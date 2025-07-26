@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.computeSize
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.util.InternalLibraryApi
import java.util.*

// DataLayoutExtensibleArray4
class ExtensibleArrayIndex(val h5: H5builder, address: Long, datasetDimensions: IntArray, chunkDimensions: IntArray) {
    val raf = h5.raf

    private val headerAddress: Long
    private val clientId : Int
    private val filtered: Boolean // If the chunks have filters applied
    private val numberOfElementsInIndexBlock: Int
    private val numberOfElements: Int
    private val numberOfSecondaryBlocks: Int
    private val blockOffsetSize: Int
    private val dataBlockSize: Int
    private val secondaryBlockSize: Int

    private val unfilteredChunkSize: Int
    private val datasetDimensions: IntArray
    private val chunkDimensions: IntArray

    private val minNumberOfElementsInDataBlock: Int
    private val dataBlockElementCounter: ExtensibleArrayCounter
    private val minNumberOfDataBlockPointers: Int
    private val secondaryBlockPointerCounter: ExtensibleArraySecondaryBlockPointerCounter
    private val maxNumberOfElementsInDataBlockPageBits: Int
    private val extensibleArrayElementSize: Int

    private var elementCounter = 0

    val chunks: MutableList<ChunkImpl>

    init {
        this.headerAddress = address
         // DataLayoutExtensibleArray4(flags, chunkDimensions, maxBits, indexElements, minPointers, minElements, pageBits, address)
        this.unfilteredChunkSize = chunkDimensions.computeSize()
        this.datasetDimensions = datasetDimensions
        this.chunkDimensions = IntArray(chunkDimensions.size - 1 ) { chunkDimensions[it] } // include element dimension ??

        val headerSize: Int = 16 + h5.sizeOffsets + 6 * h5.sizeLengths

        val state = OpenFileState(address, false)
        val magic = raf.readString(state, 4)
        check(magic == "EAHD") { "$magic should equal EAHD" }
        
        // Version Number
        val version = raf.readByte(state).toInt()
        if (version!= 0) {
            throw RuntimeException("Unsupported extensible array index version detected. Version: " + version)
        }

        clientId = raf.readByte(state).toInt()
        if (clientId == 0) {
            filtered = false
        } else if (clientId == 1) {
            filtered = true
        } else {
            throw RuntimeException("Extensible array unsupported client ID: " + clientId)
        }

        extensibleArrayElementSize = raf.readByte(state).toInt()

        val maxNumberOfElementsBits = raf.readByte(state).toInt()
        blockOffsetSize = maxNumberOfElementsBits / 8 // TODO round up?
        numberOfElementsInIndexBlock = raf.readByte(state).toInt()
        minNumberOfElementsInDataBlock = raf.readByte(state).toInt()
        dataBlockElementCounter = ExtensibleArrayCounter(minNumberOfElementsInDataBlock)
        minNumberOfDataBlockPointers = raf.readByte(state).toInt()
        secondaryBlockPointerCounter = ExtensibleArraySecondaryBlockPointerCounter(minNumberOfDataBlockPointers)
        maxNumberOfElementsInDataBlockPageBits = raf.readByte(state).toInt()

        numberOfSecondaryBlocks = h5.readLength(state).toInt()
        secondaryBlockSize = h5.readLength(state).toInt()
        val numberOfDataBlocks: Int = h5.readLength(state).toInt()
        dataBlockSize = h5.readLength(state).toInt()

        val maxIndexSet: Int = h5.readLength(state).toInt()
        chunks = ArrayList<ChunkImpl>(maxIndexSet)

        numberOfElements = h5.readLength(state).toInt()

        val indexBlockAddress: Int = h5.readLength(state).toInt()

        ExtensibleArrayIndexBlock(indexBlockAddress.toLong())

        // Checksum
        //bb.rewind()
        //ChecksumUtils.validateChecksum(bb)
    }

    inner class ExtensibleArrayIndexBlock(address: Long) {

        init {
            // Figure out the size of the index block

            val headerSize: Int = (6 + h5.sizeOffsets // TODO need to handle filtered elements
                    + h5.sizeOffsets * numberOfElementsInIndexBlock // direct chunk pointers
                    + 6 * extensibleArrayElementSize // Always up to 6 data block pointers are in the index block
                    + numberOfSecondaryBlocks * h5.sizeOffsets // Secondary block addresses.
                    + 4) // checksum
            val state2 = OpenFileState(address, false)

            val magic = raf.readString(state2, 4)
            check(magic == "EAIB") { "$magic should equal EAIB" }

            // Version Number
            val version = raf.readByte(state2).toInt()
            if (version != 0) {
                throw RuntimeException("Unsupported fixed array data block version detected. Version: " + version)
            }

            val clientId = raf.readByte(state2).toInt()
            if (clientId != this@ExtensibleArrayIndex.clientId) {
                throw RuntimeException("Extensible array client ID mismatch. Possible file corruption detected")
            }

            val headerAddress: Long = h5.readOffset(state2)
            if (headerAddress != this@ExtensibleArrayIndex.headerAddress) {
                throw RuntimeException("Extensible array data block header address mismatch")
            }

            // Elements in Index block
            var readElement = true
            run {
                var i = 0
                while (readElement && i < numberOfElementsInIndexBlock) {
                    readElement = readElement(state2)
                    i++
                }
            }

            // Guard against all the elements having already been read
            if (readElement && numberOfElements > numberOfElementsInIndexBlock) {
                // Up to 6 data block pointers directly in the index block
                for (i in 0..5) {
                    val dataBlockAddress: Long = h5.readOffset(state2)
                    if (dataBlockAddress == UNDEFINED_ADDRESS) {
                        break // There was less than 6 data blocks for the full dataset
                    }
                    ExtensibleArrayDataBlock(dataBlockAddress)
                }
            }

            // Now read secondary blocks
            for (i in 0..< numberOfSecondaryBlocks) {
                val secondaryBlockAddress: Long = h5.readOffset(state2)
                // println(" 1. ExtensibleArraySecondaryBlock $secondaryBlockAddress pointer $i")
                ExtensibleArraySecondaryBlock(secondaryBlockAddress)
            }

            // Checksum
            // val checksum = bb.getInt()
            // TODO checksums always seem to be 0 or -1?
        }

        private inner class ExtensibleArrayDataBlock(address: Long) {
            init {
                val numberOfElementsInDataBlock: Int = dataBlockElementCounter.getNextNumberOfChunks()
                val headerSize: Int = (6 + h5.sizeOffsets + blockOffsetSize
                        + numberOfElementsInDataBlock * extensibleArrayElementSize // elements (chunks)
                        + 4) // checksum
                val state2 = OpenFileState(address, false)

                val magic = raf.readString(state2, 4)
                check(magic == "EADB") { "$magic should equal EADB" }
                
                // Version Number
                val version = raf.readByte(state2).toInt()
                if (version.toInt() != 0) {
                    throw RuntimeException("Unsupported extensible array data block version detected. Version: " + version)
                }

                val clientId = raf.readByte(state2).toInt()
                if (clientId != this@ExtensibleArrayIndex.clientId) {
                    throw RuntimeException("Extensible array client ID mismatch. Possible file corruption detected")
                }

                val headerAddress: Long = h5.readOffset(state2)
                if (headerAddress != this@ExtensibleArrayIndex.headerAddress) {
                    throw RuntimeException("Extensible array data block header address mismatch")
                }

                val blockOffset: Long = h5.readVariableSizeUnsigned(state2, blockOffsetSize)

                // Page bitmap

                // Data block addresses
                var readElement = true
                var i = 0
                while (readElement && i < numberOfElementsInDataBlock) {
                    readElement = readElement(state2)
                    i++
                }

                // Checksum
                //bb.rewind()
                //ChecksumUtils.validateChecksum(bb)
            }
        }

        private inner class ExtensibleArraySecondaryBlock(address: Long) {

            init {
                val numberOfPointers: Int = secondaryBlockPointerCounter.getNextNumberOfPointers()
                val secondaryBlockSize: Int = 6 + h5.sizeOffsets +
                        blockOffsetSize +  // Page Bitmap ?
                        numberOfPointers * extensibleArrayElementSize +
                        4 // checksum
                val state2 = OpenFileState(address, false)

                val magic = raf.readString(state2, 4)
                check(magic == "EASB") { "$magic should equal EASB" }
                
                // Version Number
                val version = raf.readByte(state2).toInt()
                if (version.toInt() != 0) {
                    throw RuntimeException("Unsupported fixed array data block version detected. Version: " + version)
                }
                val clientId = raf.readByte(state2).toInt()
                if (clientId != this@ExtensibleArrayIndex.clientId) {
                    throw RuntimeException("Extensible array client ID mismatch. Possible file corruption detected")
                }

                val headerAddress: Long = h5.readOffset(state2)
                if (headerAddress != this@ExtensibleArrayIndex.headerAddress) {
                    throw RuntimeException("Extensible array secondary block header address mismatch")
                }

                val blockOffset: Long = h5.readVariableSizeUnsigned(state2, blockOffsetSize)

                // TODO page bitmap

                // Data block addresses
                for (i in 0..<numberOfPointers) {
                    val dataBlockAddress: Long = h5.readOffset(state2)
                    if (dataBlockAddress == UNDEFINED_ADDRESS) {
                        break // This is the last secondary block and not full.
                    }
                    ExtensibleArrayDataBlock(dataBlockAddress)
                }

                /* Checksum
                val checksum = bb.getInt()
                if (checksum != UNDEFINED_ADDRESS) {
                    bb.limit(bb.position())
                    bb.rewind()
                    ChecksumUtils.validateChecksum(bb)
                } */
            }
        }

        fun readElement(state: OpenFileState): Boolean {
            val chunkAddress: Long = h5.readOffset(state)
            if (chunkAddress != UNDEFINED_ADDRESS) {
                val chunkOffset: IntArray = chunkIndexToChunkOffset(elementCounter, chunkDimensions, datasetDimensions)
                if (filtered) {
                    val chunkSizeInBytes: Int = h5.readVariableSizeUnsigned(state, extensibleArrayElementSize - h5.sizeOffsets - 4).toInt()
                    val filterMask = raf.readInt(state)
                    chunks.add(ChunkImpl(chunkAddress, chunkSizeInBytes, chunkOffset, filterMask))
                } else {
                    chunks.add( ChunkImpl(chunkAddress, unfilteredChunkSize, chunkOffset, null))
                }
                elementCounter++
                return true
            } else {
                return false
            }
        }
    }

    /**
     * This counts the number of elements (chunks) in a data block. The scheme used to assign blocks is described here
     * https://doi.org/10.1007/3-540-48447-7_4
     */
    class ExtensibleArrayCounter(val minNumberOfElementsInDataBlock: Int) {
        private var blockSizeMultiplier = 1
        private var numberOfBlocks = 1
        private var blockCounter = 0
        private var increaseNumberOfBlocksNext = false

        fun getNextNumberOfChunks(): Int {
                if (blockCounter < numberOfBlocks) {
                    blockCounter++
                } else if (increaseNumberOfBlocksNext) {
                    increaseNumberOfBlocksNext = false
                    numberOfBlocks *= 2
                    blockCounter = 1
                } else {
                    increaseNumberOfBlocksNext = true
                    blockSizeMultiplier *= 2
                    blockCounter = 1
                }
                return blockSizeMultiplier * minNumberOfElementsInDataBlock
            }

        override fun toString(): String {
            return "ExtensibleArrayCounter{" +
                    "minNumberOfElementsInDataBlock=" + minNumberOfElementsInDataBlock +
                    ", blockSizeMultiplier=" + blockSizeMultiplier +
                    ", numberOfBlocks=" + numberOfBlocks +
                    ", blockCounter=" + blockCounter +
                    ", increaseNumberOfBlocksNext=" + increaseNumberOfBlocksNext +
                    '}'
        }
    }

    class ExtensibleArraySecondaryBlockPointerCounter(var numberOfPointers: Int) {
        private var counter = 0

        fun getNextNumberOfPointers(): Int {
            if (counter < REPEATS) {
                counter++
            } else {
                numberOfPointers *= 2
                counter = 1
            }
            return numberOfPointers
        }

        companion object {
            private const val REPEATS = 2
        }
    }

    fun chunkIterator() : Iterator<ChunkImpl> = chunks.iterator()

}