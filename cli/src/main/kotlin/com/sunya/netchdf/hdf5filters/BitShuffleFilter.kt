package com.sunya.netchdf.hdf5filters

import com.sunya.netchdf.hdf5.H5filterIF
import com.sunya.netchdf.hdf5.makeIntFromBEBytes
import com.sunya.netchdf.hdf5.makeLongFromBEBytes
import net.jpountz.lz4.LZ4Factory

// seems to handle LZ4_COMPRESSION combined with bit shuffle
class BitShuffleFilter : H5filterIF {
    override fun id() = 32008
    override fun name() = "bitshuffle"
    val lz4Decompressor = LZ4Factory.fastestJavaInstance().safeDecompressor()

    override fun apply(encodedData: ByteArray, clientValues: IntArray): ByteArray {
        val blockSize = if (clientValues[3] == 0) getDefaultBlockSize(clientValues[2]) else clientValues[3]
        val blockSizeBytes = blockSize * clientValues[2]

        when (clientValues[4]) {
            NO_COMPRESSION -> return noCompression(encodedData, clientValues, blockSizeBytes)
            LZ4_COMPRESSION -> return lz4Compression(encodedData, clientValues)
            ZSTD_COMPRESSION -> throw RuntimeException("Bitshuffle zstd not implemented")
            else -> throw RuntimeException("Unknown compression type: " + clientValues[4])
        }
    }

    private fun noCompression(encodedData: ByteArray, clientValues: IntArray, blockSizeBytes: Int): ByteArray {
        val nblocks = encodedData.size / blockSizeBytes
        val unshuffled = ByteArray(encodedData.size)
        for (i in 0..< nblocks) {
            val blockData = ByteArray(blockSizeBytes)
            System.arraycopy(encodedData, i * blockSizeBytes, blockData, 0, blockSizeBytes)
            val unshuffledBlock = ByteArray(blockSizeBytes)
            unshuffle(blockData, clientValues[2], unshuffledBlock)
            System.arraycopy(unshuffledBlock, 0, unshuffled, i * blockSizeBytes, blockSizeBytes)
        }
        if (nblocks * blockSizeBytes < encodedData.size) {
            val finalBlockSize = encodedData.size - nblocks * blockSizeBytes
            val blockData = ByteArray(finalBlockSize)
            System.arraycopy(encodedData, nblocks * blockSizeBytes, blockData, 0, finalBlockSize)
            val unshuffledBlock = ByteArray(finalBlockSize)
            unshuffle(blockData, clientValues[2], unshuffledBlock)
            System.arraycopy(unshuffledBlock, 0, unshuffled, nblocks * blockSizeBytes, finalBlockSize)
        }
        return unshuffled
    }

    private fun lz4Compression(encodedData: ByteArray, clientValues: IntArray): ByteArray {
        val totalDecompressedSize = Math.toIntExact(makeLongFromBEBytes(encodedData, 0, 8))
        val decompressed = ByteArray(totalDecompressedSize)

        val decompressedBlockSize: Int = makeIntFromBEBytes(encodedData, 8, 4)
        val nblocks = if (decompressedBlockSize > totalDecompressedSize) 1 else totalDecompressedSize / decompressedBlockSize
        val decompressedBuffer = ByteArray(decompressedBlockSize)

        var srcOffset = 12
        var dstOffset = 0

        repeat (nblocks) {
            val compressedBlockLength = makeIntFromBEBytes(encodedData, srcOffset, 4)
            srcOffset += 4

            val decompressedBytes = lz4Decompressor.decompress(encodedData, srcOffset, compressedBlockLength, decompressedBuffer, 0)
            unshuffle(decompressedBuffer, decompressedBytes, decompressed, dstOffset, clientValues[2])

            srcOffset += compressedBlockLength
            dstOffset += decompressedBytes
        }

        if (dstOffset < totalDecompressedSize) { // copy remaining into destination
            encodedData.copyInto(decompressed, destinationOffset = dstOffset, startIndex = srcOffset)
        }

        return decompressed
    }

    protected fun unshuffle(shuffledBuffer: ByteArray, elementSize: Int, unshuffledBuffer: ByteArray) {
        unshuffle(shuffledBuffer, shuffledBuffer.size, unshuffledBuffer, 0, elementSize)
    }

    protected fun unshuffle(
        shuffledBuffer: ByteArray,
        shuffledLength: Int,
        unshuffledBuffer: ByteArray,
        unshuffledOffset: Int,
        elementSize: Int
    ) {
        val elements = shuffledLength / elementSize
        val elementSizeBits = elementSize * 8
        val unshuffledOffsetBits = unshuffledOffset * 8

        if (elements < 8) {
            // https://github.com/xerial/snappy-java/issues/296#issuecomment-964469607
            System.arraycopy(shuffledBuffer, 0, unshuffledBuffer, 0, shuffledLength)
            return
        }

        val elementsToShuffle = elements - elements % 8
        val elementsToCopy = elements - elementsToShuffle

        var pos = 0
        for (i in 0..<elementSizeBits) {
            for (j in 0..<elementsToShuffle) {
                val bit: Boolean = getBit(shuffledBuffer, pos)
                if (bit) setBit(unshuffledBuffer, unshuffledOffsetBits + j * elementSizeBits + i, true)
                pos++ // step through the input array
            }
        }

        System.arraycopy(
            shuffledBuffer,
            elementsToShuffle * elementSize,
            unshuffledBuffer,
            elementsToShuffle * elementSize,
            elementsToCopy * elementSize
        )
    }

    // https://github.com/kiyo-masui/bitshuffle/blob/master/src/bitshuffle_core.c#L1830
    // See method bshuf_default_block_size
    private fun getDefaultBlockSize(elementSize: Int): Int {
        var defaultBlockSize = BSHUF_TARGET_BLOCK_SIZE_B / elementSize
        // Ensure it is a required multiple.
        defaultBlockSize = (defaultBlockSize / BSHUF_BLOCKED_MULT) * BSHUF_BLOCKED_MULT
        return Integer.max(defaultBlockSize, BSHUF_MIN_RECOMMEND_BLOCK)
    }

    companion object {
        // Constants see https://github.com/kiyo-masui/bitshuffle/blob/master/src/bitshuffle_internals.h#L32
        private const val BSHUF_MIN_RECOMMEND_BLOCK = 128
        private const val BSHUF_BLOCKED_MULT = 8 // Block sizes must be multiple of this.
        private const val BSHUF_TARGET_BLOCK_SIZE_B = 8192

        const val NO_COMPRESSION: Int = 0

        // https://github.com/kiyo-masui/bitshuffle/blob/master/src/bshuf_h5filter.h#L46
        const val LZ4_COMPRESSION: Int = 2
        const val ZSTD_COMPRESSION: Int = 3
    }
}

fun getBit(bytes: ByteArray, bit: Int): Boolean {
    val byteIndex = bit / 8
    val bitInByte = bit % 8
    return ((bytes[byteIndex].toInt() shr bitInByte) and 1) == 1
}

// See https://stackoverflow.com/a/4674035
fun setBit(bytes: ByteArray, bit: Int, value: Boolean) {
    require(!(bit < 0 || bit >= bytes.size * 8)) { "bit index out of range. index=" + bit }
    val byteIndex = bit / 8
    val bitInByte = bit % 8

    // could pregenerate = 1, 2, 4, .. 128
    val bitset = 1 shl bitInByte
    val bitclear = (1 shl bitInByte).inv()

    if (value) {
        bytes[byteIndex] = (bytes[byteIndex].toInt() or bitset).toByte()
    } else {
        bytes[byteIndex] =  (bytes[byteIndex].toInt() and bitclear).toByte()
    }
}
