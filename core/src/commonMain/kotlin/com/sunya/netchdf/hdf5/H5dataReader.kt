@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.*
import com.sunya.cdm.layout.Layout
import com.sunya.cdm.layout.LayoutRegular
import com.sunya.cdm.util.InternalLibraryApi

private const val debugLayout = false

// Handles reading attributes and non-chunked Variables (DataLayoutContiguous, DataLayoutContiguous3)
internal fun <T> H5builder.readRegularData(dc: DataContainer, datatype: Datatype<T>, section : Section?): ArrayTyped<T> {
    if (dc.mds.type == DataspaceType.Null) {
        return ArrayString(intArrayOf(), listOf()) as ArrayTyped<T>
    }
    val h5type = dc.h5type
    val elemSize = h5type.elemSize

    val wantSection = section ?: Section(dc.storageDims)
    val layout: Layout = LayoutRegular(dc.dataPos, elemSize, wantSection)

    if (h5type.datatype5 == Datatype5.Vlen) {
        return readVlenDataWithLayout(dc, layout, wantSection) as ArrayTyped<T>
    }

    val state = OpenFileState(0, h5type.isBE)
    return readDataWithLayout(state, layout, datatype, wantSection.shape, h5type)
}

// TODO: not subsetting
// DataLayoutCompact, DataLayoutCompact3
internal fun <T> H5builder.readCompactData(v2 : Variable<T>, shape : IntArray): ArrayTyped<T> {
    val vinfo = v2.spObject as DataContainerVariable
    val ba = when (vinfo.mdl) {
        is DataLayoutCompact -> vinfo.mdl.compactData
        is DataLayoutCompact3 -> vinfo.mdl.compactData
        else -> throw RuntimeException("CompactData must be DataLayoutCompact or DataLayoutCompact3")
    }

    return if (vinfo.h5type.datatype5 == Datatype5.Vlen) {
        this.processVlenIntoArray(vinfo.h5type, shape, ba, shape.computeSize(), vinfo.elementSize)
    } else {
        this.processDataIntoArray(ba, vinfo.h5type.isBE, vinfo.h5type.datatype(), shape, vinfo.h5type, vinfo.elementSize) as ArrayTyped<T>
    }
}

// handles reading data with a Layout. TODO: are we using Fill Value ??
internal fun <T> H5builder.readDataWithLayout(state: OpenFileState, layout: Layout, datatype: Datatype<T>, shape : LongArray, h5type : H5TypeInfo): ArrayTyped<T> {
    val sizeBytes = layout.totalNelems * layout.elemSize
    if (sizeBytes <= 0 || sizeBytes >= Int.MAX_VALUE) {
        throw RuntimeException("Illegal nbytes to read = $sizeBytes")
    }
    val ba = ByteArray(sizeBytes.toInt())
    state.isBE = h5type.base?.isBE ?: h5type.isBE

    var count = 0
    while (layout.hasNext()) {
        val chunk = layout.next()
        state.pos = chunk.srcPos()
        raf.readIntoByteArray(
            state,
            ba,
            layout.elemSize * chunk.destElem().toInt(),
            layout.elemSize * chunk.nelems()
        )
        count++
        if (debugLayout and (count < 20)) println("oldchunk = $chunk")
    }

    return this.processDataIntoArray(ba, state.isBE, datatype, shape.toIntArray(), h5type, layout.elemSize)
}

internal fun <T> H5builder.processDataIntoArray(ba: ByteArray, isBE: Boolean, datatype: Datatype<T>, shape : IntArray, h5type : H5TypeInfo, elemSize : Int): ArrayTyped<T> {

    if (h5type.datatype5 == Datatype5.Compound) {
        val members = (datatype.typedef as CompoundTypedef).members
        // class ArrayStructureData(shape : IntArray, val ba : ByteArray, val isBE: Boolean, val recsize : Int, val members : List<StructureMember<*>>)
        val sdataArray =  ArrayStructureData(shape, ba, isBE, elemSize, members)
        return processCompoundData(sdataArray, isBE) as ArrayTyped<T>
    }

    // convert to array of Strings by reducing rank by 1, tricky shape shifting for non-scalars
    if (datatype == Datatype.STRING) {
        val extshape = IntArray(shape.size + 1) { if (it == shape.size) elemSize else shape[it] }
        val result = ArrayUByte.fromByteArray(extshape, ba)
        return result.makeStringsFromBytes() as ArrayTyped<T>
    }

    if (datatype == Datatype.OPAQUE) {
        return ArrayOpaque.fromByteArray(shape, ba, h5type.elemSize) as ArrayTyped<T>
    }

    val tba = TypedByteArray(datatype, ba, 0, isBE = isBE)
    return tba.convertToArrayTyped(shape, charToString = true)
}

// Put the variable length members (vlen, string) on the heap
internal fun H5builder.processCompoundData(sdataArray : ArrayStructureData, isBE : Boolean) : ArrayStructureData {
    val h5heap = H5heap(this)
    sdataArray.putVlenStringsOnHeap { member, offset ->
        val result = mutableListOf<String>()
        repeat(member.nelems) {
            val sval = h5heap.readHeapString(sdataArray.ba, offset + it * 16) // 16 byte "heap ids" are in the ByteBuffer
            result.add(sval!!)
        }
        result
    }

    sdataArray.putVlensOnHeap { member, offset ->
        val listOfArrays = mutableListOf<Array<*>>()
        for (i in 0 until member.nelems) {
            val heapId = h5heap.readHeapIdentifier(sdataArray.ba, offset)
            val vlenArray = h5heap.getHeapDataArray(heapId, member.datatype, isBE)
            listOfArrays.add(vlenArray)
        }
        val basetype = member.datatype.typedef!!.baseType
        ArrayVlen.fromArray(member.shape, listOfArrays, basetype)
    }

    return sdataArray
}

// this apparently has heapId addresses
internal fun H5builder.readVlenDataWithLayout(dc: DataContainer, layout : Layout, wantSection : Section) : ArrayTyped<*> {
    val h5heap = H5heap(this)
    val shape = wantSection.shape.toIntArray()

    if (dc.h5type.isVlenString) {
        val sarray = mutableListOf<String>()
        while (layout.hasNext()) {
            val chunk: Layout.Chunk = layout.next()
            for (i in 0 until chunk.nelems()) {
                val address: Long = chunk.srcPos() + layout.elemSize * i
                val sval = h5heap.readHeapString(address)
                sarray.add(sval ?: "")
            }
        }
        return ArrayString(shape, sarray)

    } else {
        val base = dc.h5type.base!!
        if (base.datatype5 == Datatype5.Reference) {
            val refsList = mutableListOf<String>()
            while (layout.hasNext()) {
                val chunk: Layout.Chunk = layout.next()
                for (i in 0 until chunk.nelems()) {
                    val address: Long = chunk.srcPos() + layout.elemSize * i  // address of the heapId vs the heap id ??
                    val vlenArray = h5heap.getHeapDataArray(address, Datatype.LONG, base.isBE) as Array<Long>
                    // so references are addresses; then use address to point to String
                    // TODO val refsArray = this.convertReferencesToDataObjectName(vlenArray.asIterable())
                    val refsArray = this.convertReferencesToDataObjectName(vlenArray)
                    for (s in refsArray) {
                        refsList.add(s)
                    }
                }
            }
            return ArrayString(shape, refsList)
        }

        // general case is to read an array of vlen objects
        // each vlen generates an Array of type baseType
        val listOfArrays = mutableListOf<Array<*>>()
        val readDatatype = base.datatype()
        var count = 0
        while (layout.hasNext()) {
            val chunk: Layout.Chunk = layout.next()
            for (i in 0 until chunk.nelems()) {
                val address: Long = chunk.srcPos() + layout.elemSize * i
                val vlenArray = h5heap.getHeapDataArray(address, readDatatype, base.isBE)
                // LOOK require vlenArray is Array<T>
                listOfArrays.add(vlenArray)
                count++
            }
        }
        return ArrayVlen.fromArray(shape, listOfArrays, readDatatype)
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

