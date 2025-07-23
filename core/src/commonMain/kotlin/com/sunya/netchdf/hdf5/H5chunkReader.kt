@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.util.InternalLibraryApi

internal class H5chunkReader(val h5 : H5builder) {
    private val debugChunking = false

    internal fun <T> readChunkedData(v2: Variable<T>, wantSection: Section, index: Iterator<ChunkImpl>): ArrayTyped<T> {
        val vinfo = v2.spObject as DataContainerVariable
        val h5type = vinfo.h5type

        val elemSize = vinfo.storageDims[vinfo.storageDims.size - 1].toInt() // last one is always the elements size
        val datatype = vinfo.h5type.datatype()

        val wantSpace = IndexSpace(wantSection)
        val sizeBytes = wantSpace.totalElements * elemSize
        if (sizeBytes <= 0 || sizeBytes >= Int.MAX_VALUE) {
            throw RuntimeException("Illegal nbytes to read = $sizeBytes")
        }
        val ba = ByteArray(sizeBytes.toInt())

        // just reading into memory the entire index for now
        // val index =  BTree2j(h5, v2.name, vinfo.dataPos, vinfo.storageDims)

        val filters = FilterPipeline(v2.name, vinfo.mfp, vinfo.h5type.isBE)
        val state = OpenFileState(0L, vinfo.h5type.isBE)

        // just run through all the chunks, we wont read any that we dont want
        for (dataChunk : ChunkImpl in index) {
            val dataSection = IndexSpace(v2.rank, dataChunk.chunkOffset.toLongArray(), vinfo.storageDims)
            val chunker = Chunker(dataSection, wantSpace) // each DataChunkEntry has its own Chunker iteration
            if (chunker.nelems > 0) { // TODO efficient enough ??
                /* if (dataChunk.isMissing()) {
                if (debugChunking) println("   missing ${dataChunk.show(tiledData.tiling)}")
                chunker.transferMissing(vinfo.fillValue, elemSize, ba)
            } else { */
                state.pos = dataChunk.address
                val rawdata = h5.raf.readByteArray(state, dataChunk.size)
                val filteredData = if (vinfo.mfp == null || dataChunk.filterMask == null) rawdata
                else filters.apply(rawdata, dataChunk.filterMask)
                chunker.transferBA(filteredData, 0, elemSize, ba, 0)
            }
            // }
        }

        val shape = wantSpace.shape.toIntArray()

        return if (h5type.datatype5 == Datatype5.Vlen) {
            h5.processVlenIntoArray(h5type, shape, ba, wantSpace.totalElements.toInt(), elemSize)
        } else {
            h5.processDataIntoArray(ba, vinfo.h5type.isBE, datatype, shape, h5type, elemSize) as ArrayTyped<T>
        }
    }

    internal fun <T> readBtreeVer1(v2: Variable<T>, wantSection: Section): ArrayTyped<T> {
        val vinfo = v2.spObject as DataContainerVariable
        val h5type = vinfo.h5type

        val elemSize = vinfo.storageDims[vinfo.storageDims.size - 1].toInt() // last one is always the elements size
        val datatype = vinfo.h5type.datatype()

        val wantSpace = IndexSpace(wantSection)
        val sizeBytes = wantSpace.totalElements * elemSize
        if (sizeBytes <= 0 || sizeBytes >= Int.MAX_VALUE) {
            throw RuntimeException("Illegal nbytes to read = $sizeBytes")
        }
        val ba = ByteArray(sizeBytes.toInt())

        val btree1 = if (vinfo.mdl is DataLayoutBTreeVer1)
            BTree1(h5, vinfo.dataPos, 1, vinfo.storageDims.size)
        else
            throw RuntimeException("Unsupprted mdl ${vinfo.mdl}")

        val tiledData = H5TiledData1(btree1, v2.shape, vinfo.storageDims)
        val filters = FilterPipeline(v2.name, vinfo.mfp, vinfo.h5type.isBE)
        if (debugChunking) println(" readChunkedData tiles=${tiledData.tiling}")

        var transferChunks = 0
        val state = OpenFileState(0L, vinfo.h5type.isBE)
        for (dataChunk: DataChunkIF in tiledData.dataChunks(wantSpace)) { // : Iterable<BTree1New.DataChunkEntry>
            val dataSection = IndexSpace(v2.rank, dataChunk.offsets(), vinfo.storageDims)
            val chunker = Chunker(dataSection, wantSpace) // each DataChunkEntry has its own Chunker iteration
            if (dataChunk.isMissing()) {
                if (debugChunking) println("   missing ${dataChunk.show(tiledData.tiling)}")
                chunker.transferMissing(vinfo.fillValue, elemSize, ba)
            } else {
                if (debugChunking) println("   chunk=${dataChunk.show(tiledData.tiling)}")
                state.pos = dataChunk.childAddress()
                val chunkData = h5.raf.readByteArray(state, dataChunk.chunkSize())
                val filteredData = if (dataChunk.filterMask() == null) chunkData
                else filters.apply(chunkData, dataChunk.filterMask()!!)
                chunker.transferBA(filteredData, 0, elemSize, ba, 0)
                transferChunks += chunker.transferChunks
            }
        }

        val shape = wantSpace.shape.toIntArray()

        return if (h5type.datatype5 == Datatype5.Vlen) {
            h5.processVlenIntoArray(h5type, shape, ba, wantSpace.totalElements.toInt(), elemSize)
        } else {
            h5.processDataIntoArray(ba, vinfo.h5type.isBE, datatype, shape, h5type, elemSize) as ArrayTyped<T>
        }
    }
}

// Chunked data apparently has heapIds directly, not addresses of heapIds. Go figure.
internal fun <T> H5builder.processVlenIntoArray(h5type: H5TypeInfo, shape: IntArray, ba: ByteArray, nelems: Int, elemSize : Int): ArrayTyped<T> {
    val h5heap = H5heap(this)

    if (h5type.isVlenString) {
        val sarray = mutableListOf<String>()
        for (i in 0 until nelems) {
            val sval = h5heap.readHeapString(ba, i * elemSize)
            sarray.add(sval ?: "")
        }
        return ArrayString(shape, sarray) as ArrayTyped<T>

    } else {
        val base = h5type.base!!
        if (base.datatype5 == Datatype5.Reference) {
            val refsList = mutableListOf<String>()
            for (i in 0 until nelems) {
                val heapId = h5heap.readHeapIdentifier(ba, i * elemSize)
                val vlenArray = h5heap.getHeapDataArray(heapId, Datatype.LONG, base.isBE) as Array<Long>
                // LOOK require vlenArray is Array<Long>
                // TODO val refsArray = this.convertReferencesToDataObjectName(vlenArray.asIterable())
                val refsArray = this.convertReferencesToDataObjectName(vlenArray)
                for (s in refsArray) {
                    refsList.add(s)
                }
            }
            return ArrayString(shape, refsList) as ArrayTyped<T>
        }

        // general case is to read an array of vlen objects
        // each vlen generates an Array of type baseType
        val listOfArrays = mutableListOf<Array<*>>()
        val readDatatype = base.datatype()
        for (i in 0 until nelems) {
            val heapId = h5heap.readHeapIdentifier(ba, i * elemSize)
            val vlenArray = h5heap.getHeapDataArray(heapId, readDatatype, base.isBE)
            // LOOK require vlenArray is Array<T>
            listOfArrays.add(vlenArray)
        }
        return ArrayVlen.fromArray(shape, listOfArrays, readDatatype) as ArrayTyped<T>
    }
}