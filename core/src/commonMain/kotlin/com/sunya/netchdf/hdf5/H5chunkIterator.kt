package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.*
import com.sunya.cdm.array.ArrayTyped
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.transferMissingNelems
import com.sunya.cdm.util.InternalLibraryApi

/*  (to be removed)
@OptIn(InternalLibraryApi::class)
internal class H5chunkIterator<T>(val h5 : H5builder, val v2: Variable<T>, val wantSection : Section) : AbstractIterator<ArraySection<T>>() {
    private val debugChunking = false

    val vinfo : DataContainerVariable = v2.spObject as DataContainerVariable
    val h5type : H5TypeInfo
    val elemSize : Int
    val datatype : Datatype<*>
    val tiledData : H5TiledData1
    val filters : FilterPipeline
    val state : OpenFileState

    private val wantSpace : IndexSpace
    private val chunkIterator : Iterator<DataChunkIF>

    init {
        h5type = vinfo.h5type
        elemSize = vinfo.storageDims[vinfo.storageDims.size - 1].toInt() // last one is always the elements size
        datatype = h5type.datatype()

        val btree1 = BTree1(h5, vinfo.dataPos, 1, vinfo.storageDims.size)
        tiledData = H5TiledData1(btree1, v2.shape, vinfo.storageDims)
        filters = FilterPipeline(v2.name, vinfo.mfp, h5type.isBE)
        if (debugChunking) println(" H5chunkIterator tiles=${tiledData.tiling}")

        state = OpenFileState(0L, h5type.isBE)
        wantSpace = IndexSpace(wantSection)
        chunkIterator = tiledData.dataChunks(wantSpace).iterator()
    }

    override fun computeNext() {
        if (chunkIterator.hasNext()) {
            setNext( getaPair(chunkIterator.next()) )
        } else {
            done()
        }
    }

    private fun getaPair(dataChunk : DataChunkIF) : ArraySection<T> {
        val dataSpace = IndexSpace(v2.rank, dataChunk.offsets(), vinfo.storageDims)

        // TODO we need to intersect the dataChunk with the wanted section.
        // optionally, we could make a view of the array, rather than copying the data.
        val useEntireChunk = wantSpace.contains(dataSpace)
        val intersectSpace = if (useEntireChunk) dataSpace else wantSpace.intersect(dataSpace)

        val ba = if (dataChunk.isMissing()) {
            if (debugChunking) println("   missing ${dataChunk.show(tiledData.tiling)}")
            val sizeBytes = intersectSpace.totalElements * elemSize
            val bbmissing = ByteArray(sizeBytes.toInt())
            transferMissingNelems(vinfo.fillValue, intersectSpace.totalElements.toInt(), bbmissing, 0)
            if (debugChunking) println("   missing transfer ${intersectSpace.totalElements} fillValue=${vinfo.fillValue}")
            bbmissing
        } else {
            if (debugChunking) println("  chunkIterator=${dataChunk.show(tiledData.tiling)}")
            state.pos = dataChunk.childAddress()
            val rawdata = h5.raf.readByteArray(state, dataChunk.chunkSize())
            val filteredData = if (dataChunk.filterMask() == null) rawdata else filters.apply(rawdata, dataChunk.filterMask()!!)
            if (useEntireChunk) {
                filteredData
            } else {
                val chunker = Chunker(dataSpace, wantSpace) // each DataChunkEntry has its own Chunker iteration
                chunker.copyOut(filteredData, 0, elemSize, intersectSpace.totalElements.toInt())
            }
        }

        val array = if (h5type.datatype5 == Datatype5.Vlen) {
            // internal fun <T> H5builder.processVlenIntoArray(h5type: H5TypeInfo, shape: IntArray, ba: ByteArray, nelems: Int, elemSize : Int): ArrayTyped<T> {
            h5.processVlenIntoArray(h5type, intersectSpace.shape.toIntArray(), ba, intersectSpace.totalElements.toInt(), elemSize)
        } else {
            h5.processDataIntoArray(ba, h5type.isBE, datatype, intersectSpace.shape.toIntArray(), h5type, elemSize) as ArrayTyped<T>
        }

        return ArraySection(array, intersectSpace.section(v2.shape)) // LOOK use space instead of Section ??
    }
} */

