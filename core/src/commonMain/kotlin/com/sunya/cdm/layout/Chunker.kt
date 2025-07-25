package com.sunya.cdm.layout

import com.sunya.cdm.api.Section

internal enum class Merge { all, none, notFirst }

/**
 * Copies the intersection of a dataChunk and wantSpace to the destination buffer.
 * The dataChunk, wantSpace values are relative to verShape.
 * Each dataChunk gets its own Chunker, which can only be used once.
 * The wantSpace is a (possibly improper) subset of the variable's IndexSpace.
 * Finds contiguous chunks of data to copy from the dataChunk to the destination.
 * The iteration is monotonic in both src and dest positions.

 * @param dataChunk the dataChunk, may have a trailing dimension that is ignored.
 * @param wantSpace the requested section of data.
 * @param merge merge strategy for dimensions that can be merged and still keep contiguous transfer
 */
internal class Chunker(val dataChunk: IndexSpace, val wantSpace: IndexSpace, merge : Merge = Merge.all) : AbstractIterator<TransferChunk>() {

    val nelems: Int // number of elements to read at one time
    val totalNelems: Long // total number of elements in wantSection

    private val srcOdometer: IndexND
    private val dstOdometer: IndexND
    private val incrDigit: Int
    var transferChunks = 0

    init {
        val intersectSpace = wantSpace.intersect(dataChunk)

        // shift intersect to dataChunk and wantSection origins
        val dataChunkShifted = intersectSpace.shift(dataChunk.start) // dataChunk origin
        val wantSectionShifted = intersectSpace.shift(wantSpace.start) // wantSection origin

        // construct odometers over source and destination index spaces
        this.srcOdometer = IndexND(dataChunkShifted, dataChunk.shape)
        this.dstOdometer = IndexND(wantSectionShifted, wantSpace.shape)
        this.totalNelems = intersectSpace.totalElements

        val rank = intersectSpace.rank
        val mergeNDims = countMergeDims(intersectSpace, dataChunk.shape, wantSpace.shape, merge)
        // the first dimension to merge
        val firstDim = if (rank == mergeNDims) 0 else rank - mergeNDims - 1

        var product = 1L
        for (idx in rank - 1 downTo firstDim) {
            product *= intersectSpace.shape[idx]
        }
        this.nelems = if ((rank == 1) && (merge == Merge.notFirst)) 1 else product.toInt()

        // the digit to increment when iterating
        this.incrDigit = if (firstDim == 0) 0 else firstDim - 1
    }

    constructor(section : Section, merge : Merge = Merge.all) : this(IndexSpace(section.varShape), IndexSpace(section), merge)

    private fun countMergeDims(
        intersect: IndexSpace,
        dataChunkShape: LongArray,
        dataSubsetShape: LongArray,
        merge: Merge
    ): Int {
        if (merge == Merge.none) return 0
        val mergeDownto = if (merge == Merge.all) 0 else 2

        var mergeDims = 0 // how many dimensions can be merged?
        for (idx in intersect.rank - 1 downTo mergeDownto) {
            if ((intersect.shape[idx] == dataChunkShape[idx]) && (intersect.shape[idx] == dataSubsetShape[idx])) {
                mergeDims++
            } else {
                break
            }
        }
        return mergeDims
    }

    //// iterator
    private var done: Long = 0 // done so far
    private var first = true

    override fun computeNext() {
        if (done >= totalNelems) {
            return done()
        }
        if (!first) {
            srcOdometer.incr(incrDigit)
            dstOdometer.incr(incrDigit)
        }
        val srcElem = srcOdometer.element()
        val dstElem = dstOdometer.element()
        //println(" srcElem = ${srcOdometer.current.contentToString()} = ${srcElem}")
        //println(" dstElem = ${dstOdometer.current.contentToString()} = ${dstElem}")
        setNext(TransferChunk(srcElem, nelems, dstElem))

        done += nelems.toLong()
        first = false
        transferChunks++
    }

    override fun toString(): String {
        return "Chunker(nelems=$nelems, totalNelems=$totalNelems, dataChunk=$dataChunk, wantSpace=$wantSpace)"
    }

    // the chunker tracks the dst offset
    internal fun transferBA(src: ByteArray, srcOffset: Int, elemSize : Int, dst: ByteArray, dstOffset: Int) {
        for (chunk in this) {
            // println(" chunker copy $elemSize elements from src ${srcOffset + elemSize * chunk.srcElem.toInt()} to dst ${dstOffset + elemSize * chunk.destElem.toInt()}")
            src.copyInto(dst,
                dstOffset + elemSize * chunk.destElem.toInt(),
                srcOffset + elemSize * chunk.srcElem.toInt(),
                srcOffset + elemSize * (chunk.srcElem.toInt() + chunk.nelems))
        }
    }

    internal fun copyOut(src: ByteArray, srcOffset: Int, elemSize : Int, totalElems: Int) : ByteArray {
        val dst = ByteArray(elemSize * totalElems)
        var dstElem = 0 // ignore chunker dstPosition
        for (chunk in this) {
            // public expect fun ByteArray.copyInto(destination: ByteArray, destinationOffset: Int = 0,
            // startIndex: Int = 0, endIndex: Int = size): ByteArray
            src.copyInto(dst,
                elemSize * dstElem,
                srcOffset + elemSize * chunk.srcElem.toInt(),
                srcOffset + elemSize * (chunk.srcElem.toInt() + chunk.nelems))

            /* System.arraycopy(
                src, srcOffset + elemSize * chunk.srcElem.toInt(),
                dst, elemSize * dstElem,
                elemSize * chunk.nelems,
            ) */
            dstElem += chunk.nelems
        }
        return dst
    }

    // transfer fillValue to dst buffer, using computed chunks
    internal fun transferMissing(fillValue: ByteArray, elemSize: Int, dst: ByteArray) {
        for (chunk in this) {
            if (debugChunking) println("  missing chunk $chunk fillValue=$fillValue")
            // TODO why multiply by elemSize?? seems wrong
            transferMissingNelems(fillValue, chunk.nelems, dst, elemSize * chunk.destElem.toInt())
        }
    }
}

private const val debugChunking = false

internal fun transferMissingNelems(fillValue: ByteArray, nelems : Int, dst: ByteArray, dstOffset: Int) {
    var pos = dstOffset
    repeat(nelems) {
        fillValue.forEachIndexed {idx, fillByte -> dst[pos + idx] = fillByte }
        pos += fillValue.size
    }
}