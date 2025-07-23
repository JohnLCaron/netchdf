package com.sunya.netchdf.hdf5filters

import com.ning.compress.lzf.LZFException
import com.ning.compress.lzf.util.ChunkDecoderFactory
import com.sunya.netchdf.hdf5.H5filterIF

class LzfFilter() : H5filterIF {
    override fun id() = 32000
    override fun name() = "lzf"

    override fun apply(encodedData: ByteArray, clientValues: IntArray): ByteArray {
        val compressedLength = encodedData.size
        val uncompressedLength = clientValues[2]

        if (compressedLength == uncompressedLength) {
            return encodedData
        }

        val output = ByteArray(uncompressedLength)

        try {
            ChunkDecoderFactory.safeInstance().decodeChunk(encodedData, 0, output, 0, uncompressedLength)
        } catch (e: LZFException) {
            throw RuntimeException("Inflating failed", e)
        }
        return output
    }

}