@file:OptIn(InternalLibraryApi::class, ExperimentalUnsignedTypes::class)

package com.sunya.netchdf.hdf5

import com.fleeksoft.charset.decodeToString
import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.TransferChunk
import com.sunya.cdm.util.InternalLibraryApi
import kotlin.collections.iterator

private val debugChunking = false

// DataLayoutSingleChunk4, DataLayoutImplicit4, DataLayoutFixedArray4, DataLayoutExtensibleArray4, DataLayoutBtreeVer2
internal fun <T> H5builder.readChunkedData(v2: Variable<T>, wantSection: Section, index: Iterator<ChunkImpl>): ArrayTyped<T> {
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
            val rawdata = this.raf.readByteArray(state, dataChunk.size)
            val filteredData = if (vinfo.mfp == null || dataChunk.filterMask == null) rawdata
            else filters.apply(rawdata, dataChunk.filterMask)
            chunker.transferBA(filteredData, 0, elemSize, ba, 0)
        }
        // }
    }

    val shape = wantSpace.shape.toIntArray()

    return if (h5type.datatype5 == Datatype5.Vlen) {
        this.processVlenIntoArray(h5type, shape, ba, wantSpace.totalElements.toInt(), elemSize)
    } else {
        this.processDataIntoArray(ba, vinfo.h5type.isBE, datatype, shape, h5type, elemSize) as ArrayTyped<T>
    }
}

/* TODO can we use concurrent reading ??
internal fun <T> readBtree1data(v2: Variable<T>, wantSection: Section): ArrayTyped<T> {
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

    val reader = H5chunkConcurrent(h5, v2, wantSection)
    val availableProcessors = com.sunya.netchdf.util.getAvailableProcessors()
    reader.readChunks(availableProcessors, lamda = { asection: ArraySection<*> ->
        val (array, section) = asection
        println(" section = ${section}")
        val dataSpace = IndexSpace(section)

        val useEntireChunk = wantSpace.contains(dataSpace)
        val intersectSpace = if (useEntireChunk) dataSpace else wantSpace.intersect(dataSpace)

        val chunker = Chunker(dataSpace, wantSpace) // each DataChunkEntry has its own Chunker iteration
        chunker.transferBA(array, 0, elemSize, ba, 0)

        if (h5type.datatype5 == Datatype5.Vlen) {
            // internal fun <T> H5builder.processVlenIntoArray(h5type: H5TypeInfo, shape: IntArray, ba: ByteArray, nelems: Int, elemSize : Int): ArrayTyped<T> {
            this.processVlenIntoArray(h5type, intersectSpace.shape.toIntArray(), ba, intersectSpace.totalElements.toInt(), elemSize)
        } else {
            this.processDataIntoArray(ba, h5type.isBE, datatype, intersectSpace.shape.toIntArray(), h5type, elemSize)
        }

    }, done = { })

    return ArraySection(array, intersectSpace.section(v2.shape))
} */

// DataLayoutBTreeVer1
internal fun <T> H5builder.readBtreeVer1(v2: Variable<T>, wantSection: Section): ArrayTyped<T> {
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
        BTree1(this, vinfo.dataPos, 1, vinfo.storageDims.size)
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
            val chunkData = this.raf.readByteArray(state, dataChunk.chunkSize())
            val filteredData = if (dataChunk.filterMask() == null) chunkData
            else filters.apply(chunkData, dataChunk.filterMask()!!)
            chunker.transferBA(filteredData, 0, elemSize, ba, 0)
            transferChunks += chunker.transferChunks
        }
    }

    val shape = wantSpace.shape.toIntArray()

    return if (h5type.datatype5 == Datatype5.Vlen) {
        this.processVlenIntoArray(h5type, shape, ba, wantSpace.totalElements.toInt(), elemSize)
    } else {
        this.processDataIntoArray(ba, vinfo.h5type.isBE, datatype, shape, h5type, elemSize) as ArrayTyped<T>
    }
}

// DataLayoutBTreeVer1
internal fun <T> readBtree1data(hdf5: Hdf5File, v2: Variable<T>, wantSection: SectionPartial?): ArrayTyped<T> {
    val vinfo = v2.spObject as DataContainerVariable
    val h5type = vinfo.h5type
    val datatype = vinfo.h5type.datatype()
    val elemSize = vinfo.storageDims[vinfo.storageDims.size - 1].toInt() // last one is always the elements size

    val useSection = SectionPartial.fill(wantSection, v2.shape)
    val wantSpace = IndexSpace(useSection)
    val nelems = wantSpace.totalElements.toInt()

    val values = when (datatype) {
        Datatype.BYTE -> ByteArray(nelems)
        Datatype.CHAR, Datatype.UBYTE, Datatype.ENUM1 -> UByteArray(nelems)
        Datatype.SHORT -> ShortArray(nelems)
        Datatype.USHORT, Datatype.ENUM2  -> UShortArray(nelems)
        Datatype.INT -> IntArray(nelems)
        Datatype.UINT, Datatype.ENUM4 -> UIntArray(nelems)
        Datatype.LONG -> LongArray(nelems)
        Datatype.ULONG, Datatype.ENUM8 -> ULongArray(nelems)
        Datatype.DOUBLE -> DoubleArray(nelems)
        Datatype.FLOAT -> FloatArray(nelems)
        Datatype.STRING -> MutableList<String>(nelems) {""}
        else -> throw IllegalArgumentException("datatype ${datatype}")
    }

    // so instead of type byte array we have the actual ArrayTyped
    // we have to transfer the chunk into the approprate places in the array
    val chunkIter = hdf5.chunkIterator(v2, wantSection)
    chunkIter.forEach { dataChunk : ArraySection<T> ->
        val dataSection = IndexSpace(dataChunk.chunkSection)
        val chunker = Chunker(dataSection, wantSpace) // each DataChunkEntry has its own Chunker iteration
        chunker.forEach {
            // println(it)
            dataChunk.array.transfer(values, it)
        }
    }

    val shape = wantSpace.shape.toIntArray()
    val result = when (datatype) {
        Datatype.BYTE -> ArrayByte(shape, values as ByteArray)
        Datatype.CHAR, Datatype.UBYTE, Datatype.ENUM1 -> ArrayUByte(shape, values as UByteArray)
        Datatype.SHORT -> ArrayShort(shape, values as ShortArray)
        Datatype.USHORT, Datatype.ENUM2  -> ArrayUShort(shape, values as UShortArray)
        Datatype.INT -> ArrayInt(shape, values as IntArray)
        Datatype.UINT, Datatype.ENUM4 -> ArrayUInt(shape, values as UIntArray)
        Datatype.LONG -> ArrayLong(shape, values as LongArray)
        Datatype.ULONG, Datatype.ENUM8 -> ArrayULong(shape, values as ULongArray)
        Datatype.DOUBLE -> ArrayDouble(shape, values as DoubleArray)
        Datatype.FLOAT -> ArrayFloat(shape, values as FloatArray)
        Datatype.STRING -> ArrayString(shape, values as List<String>)
        else -> throw IllegalArgumentException("datatype ${datatype}")
    }
    return result as ArrayTyped<T>
}

internal fun transfer(src: FloatArray, dest: FloatArray, tc: TransferChunk) {
    repeat(tc.nelems) { dest[tc.destElem.toInt()+it] = src[tc.srcElem.toInt() + it] }
}