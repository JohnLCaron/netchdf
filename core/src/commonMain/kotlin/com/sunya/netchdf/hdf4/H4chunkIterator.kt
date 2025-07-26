@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.transferMissingNelems
import com.sunya.cdm.util.InternalLibraryApi

internal class H4chunkIterator<T>(h4 : H4builder, val v2: Variable<*>, val wantSection : Section) : AbstractIterator<ArraySection<T>>() {
    private val debugChunking = false

    private val vinfo = v2.spObject as Vinfo
    private val elemSize = vinfo.elemSize
    private val wantSpace = IndexSpace(wantSection)

    private val tiledData : H4tiledData = H4tiledData(h4, v2.shape, vinfo.chunkLengths, vinfo.chunks!!)
    private val chunkIterator : Iterator<H4CompressedDataChunk>

    init {
        if (debugChunking) println(" ${tiledData.tiling}")
        chunkIterator = tiledData.findDataChunks(wantSpace).iterator()
    }

    override fun computeNext() {
        if (chunkIterator.hasNext()) {
            setNext(getaPair(chunkIterator.next()))
        } else {
            done()
        }
    }

    private fun getaPair(dataChunk : H4CompressedDataChunk) : ArraySection<T> {
        val dataSpace = IndexSpace(v2.rank, dataChunk.offsets.toLongArray(), vinfo.chunkLengths.toLongArray())
        val useEntireChunk = wantSpace.contains(dataSpace)
        val intersectSpace = if (useEntireChunk) dataSpace else wantSpace.intersect(dataSpace)

        val ba: ByteArray = if (dataChunk.isMissing()) {
            if (debugChunking) println("   missing ${dataChunk.show(tiledData.tiling)}")
            val sizeBytes = intersectSpace.totalElements * elemSize
            val bbmissing = ByteArray(sizeBytes.toInt())
            val fillValue = vinfo.fillValue ?: ByteArray(vinfo.elemSize)
            transferMissingNelems(fillValue, intersectSpace.totalElements.toInt(), bbmissing, 0)
            if (debugChunking) println("   transferMissingNelems ${intersectSpace.totalElements} fillValue=${fillValue}")
            bbmissing
        } else {
            if (debugChunking) println("  chunkIterator=${dataChunk.show(tiledData.tiling)}")
            val filteredData = dataChunk.getByteArray() // filter already applied
            if (useEntireChunk) {
                filteredData
            } else {
                val chunker = Chunker(dataSpace, wantSpace) // each DataChunkEntry has its own Chunker iteration
                chunker.copyOut(filteredData, 0, elemSize, intersectSpace.totalElements.toInt())
            }
        }

        val shape = wantSpace.shape.toIntArray()

        // val datatype: Datatype<T>, val ba: ByteArray, val offset: Int, val isBE: Boolean
        val tba = TypedByteArray(v2.datatype, ba, 0, isBE = vinfo.isBE)
        val array = tba.convertToArrayTyped(shape)

        /*
        bb.position(0)
        bb.limit(bb.capacity())
        bb.order(vinfo.endian)

        val array = when (datatype) {
            Datatype.BYTE -> ArrayByte(shape, bb)
            Datatype.STRING, Datatype.CHAR, Datatype.UBYTE -> ArrayUByte(shape, datatype as Datatype<UByte>, bb)
            Datatype.SHORT -> ArrayShort(shape, bb)
            Datatype.USHORT -> ArrayUShort(shape, bb)
            Datatype.INT -> ArrayInt(shape, bb)
            Datatype.UINT -> ArrayUInt(shape, bb)
            Datatype.FLOAT -> ArrayFloat(shape, bb)
            Datatype.DOUBLE -> ArrayDouble(shape, bb)
            Datatype.LONG -> ArrayLong(shape, bb)
            Datatype.ULONG -> ArrayULong(shape, bb)
            else -> throw IllegalStateException("unimplemented type= $datatype")
        }

         */

        return ArraySection(array as ArrayTyped<T>, intersectSpace.section(v2.shape)) // LOOK use space instead of Section ??
    }

}