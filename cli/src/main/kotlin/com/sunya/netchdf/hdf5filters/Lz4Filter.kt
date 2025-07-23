package com.sunya.netchdf.hdf5filters

import com.sunya.netchdf.hdf5.H5filterIF
import com.sunya.netchdf.hdf5.makeIntFromBEBytes
import com.sunya.netchdf.hdf5.makeLongFromBEBytes
import net.jpountz.lz4.LZ4Factory
import kotlin.math.min

class Lz4Filter : H5filterIF {
    override fun id() = 32004
    override fun name() = "lz4"

    override fun apply(encodedData: ByteArray, clientValues: IntArray): ByteArray {
        val lz4Decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor()

        // https://docs.hdfgroup.org/archive/support/services/filters/HDF5_LZ4.pdf
        // bytearray cant be larger than 2^32
        // TODO Big Endian ?
        val totalDecompressedSizeLong = makeLongFromBEBytes(encodedData, 0, 8)
        val totalDecompressedSize = Math.toIntExact(totalDecompressedSizeLong)
        val decompressed = ByteArray(totalDecompressedSize)

        val decompressedBlockSize: Int = makeIntFromBEBytes(encodedData, 8, 4)
        val nblocks = (totalDecompressedSize + decompressedBlockSize - 1) / decompressedBlockSize

        var srcOffset = 12
        var dstOffset = 0

        repeat (nblocks) {
            val compressedBlockSize = makeIntFromBEBytes(encodedData, srcOffset, 4)
            srcOffset += 4
            val destBlockSize = min(decompressed.size - dstOffset, decompressedBlockSize)

            if (compressedBlockSize == destBlockSize) {
                encodedData.copyInto(decompressed, destinationOffset = dstOffset, startIndex = srcOffset, endIndex = srcOffset + destBlockSize)
            } else {
                //   public abstract int decompress(byte[] src, int srcOff, byte[] dest, int destOff, int destLen);
                lz4Decompressor.decompress(encodedData, srcOffset, decompressed, dstOffset, destBlockSize)
            }
            srcOffset += compressedBlockSize
            dstOffset += decompressedBlockSize
        }

        return decompressed
    }

}