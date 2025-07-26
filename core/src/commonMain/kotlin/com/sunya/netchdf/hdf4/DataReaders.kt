@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4

import com.sunya.cdm.iosp.ByteSource
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.iosp.ReaderIntoByteArray
import com.sunya.cdm.iosp.decode
import com.sunya.cdm.util.InternalLibraryApi

internal class ByteSourceReader(byteSource: ByteSource, isCompressed: Boolean): ReaderIntoByteArray {
    val bytes : ByteArray
    init {
        val allBytes = ByteArray(byteSource.totalSize())
        var pos = 0
        while (byteSource.hasNext()) {
            val ba = byteSource.next()
            ba.forEach { allBytes[pos++] = it }
        }
        bytes = if (isCompressed) decode(allBytes) else allBytes
    }

    override fun readIntoByteArray(state: OpenFileState, dest: ByteArray, destPos: Int, nbytes: Int): Int {
        val pos = state.pos.toInt()
        repeat(nbytes) { idx ->
            dest[destPos + idx] = bytes[pos + idx]
        }
        return nbytes
    }
}

internal class ByteArrayReader(val bytes : ByteArray): ReaderIntoByteArray {
     override fun readIntoByteArray(state: OpenFileState, dest: ByteArray, destPos: Int, nbytes: Int): Int {
        val pos = state.pos.toInt()
        repeat(nbytes) { idx ->
            dest[destPos + idx] = bytes[pos + idx]
        }
        return nbytes
    }
}

internal fun getCompressedReader(h4: H4builder, vinfo: Vinfo) : ReaderIntoByteArray {
    val compressedBytes = ByteArray(vinfo.length)
    val state = OpenFileState(vinfo.start, vinfo.isBE)
    h4.raf.readIntoByteArray(state, compressedBytes, 0, vinfo.length)
    return ByteArrayReader(decode(compressedBytes))
}

internal fun getLinkedCompressedReader(h4: H4builder, vinfo: Vinfo) : ReaderIntoByteArray {
    val byteSource = LinkedByteSource(h4, vinfo)
    return ByteSourceReader(byteSource, true)
}

internal fun getLinkedReader(h4: H4builder, vinfo: Vinfo) : ReaderIntoByteArray {
    val byteSource = LinkedByteSource(h4, vinfo)
    return ByteSourceReader(byteSource, false)
}

/* hmmmm this assumes each block is decompresses separately, probably not true
internal class LinkedCompressedSource(val linkedSource: LinkedByteSource) : ByteSource {

    override fun hasNext(): Boolean = linkedSource.hasNext()

    override fun next() : ByteArray {
        val compressedData = linkedSource.next()
        if (compressedData.size == 0) return ByteArray(0)
        return decode(compressedData)
    }
} */

/** Make a linked list of data segments as a ByteSource. */
internal class LinkedByteSource(val h4 : H4builder,
                                 val nsegs: Int,
                                 val segPosA: LongArray,
                                 val segSizeA: IntArray) : ByteSource {

    constructor(h4 : H4builder, vinfo: Vinfo) : this(h4, vinfo.segSize.size, vinfo.segPos, vinfo.segSize)

    private val state : OpenFileState
    private var segno = -1
    private var segSize = 0
    private var exhausted = false
    val totalSize: Int

    init {
        totalSize = segSizeA.sum()
        state = OpenFileState(0, true)
    }

    override fun totalSize() = totalSize

    override fun hasNext(): Boolean = (segno < nsegs)

    override fun next() : ByteArray {
        segno++
        if (segno == nsegs) {
            exhausted = true
            return ByteArray(0)
        }
        segSize = segSizeA[segno]
        while (segSize == 0) { // for some reason may have a 0 length segment
            segno++
            if (segno == nsegs) return ByteArray(0)
            segSize = segSizeA[segno]
        }

        state.pos = segPosA[segno]
        return h4.raf.readByteArray(state, segSize)
    }
}

// just read all the bytes in from SpecialLinked
internal fun readSpecialLinkedInputSource(h4: H4builder, linked: SpecialLinked): ByteArray {
    val linkedBlocks = linked.getLinkedDataBlocks(h4)
    val nsegs = linkedBlocks.size
    val segPos = LongArray(nsegs) { idx -> linkedBlocks[idx].offset }
    val segSize = IntArray(nsegs) { idx -> linkedBlocks[idx].length }

    val byteSource = LinkedByteSource(h4, nsegs, segPos, segSize)

    val result = ByteArray(byteSource.totalSize)
    var pos = 0
    while (byteSource.hasNext()) {
        val ba = byteSource.next()
        ba.forEach { result[pos++] = it }
    }
    return result
}
