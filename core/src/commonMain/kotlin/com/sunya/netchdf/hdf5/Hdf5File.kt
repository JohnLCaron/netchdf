@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.*
import com.sunya.cdm.api.Datatype.Companion.STRING
import com.sunya.cdm.api.Datatype.Companion.CHAR
import com.sunya.cdm.array.ArrayEmpty
import com.sunya.cdm.array.ArraySingle
import com.sunya.cdm.array.ArrayString
import com.sunya.cdm.array.ArrayTyped
import com.sunya.cdm.array.TypedByteArray
import com.sunya.cdm.iosp.*
import com.sunya.cdm.util.InternalLibraryApi
import kotlin.String

/**
 * @param strict true = make it agree with nclib if possible
 */
class Hdf5File(val filename : String, strict : Boolean = false) : Netchdf {
    private val raf : OpenFileIF = OkioFile(filename)
    val header : H5builder = H5builder(raf, strict)

    override fun close() {
        raf.close()
    }

    override fun rootGroup() = header.cdmRoot
    override fun location() = filename
    override fun cdl() = cdl(this)
    override fun type() = header.formatType()
    override val size : Long get() = raf.size()

    fun layoutName(v: Variable<*>): String {
        if (v.spObject is DataContainerAttribute) {
            return("DataContainerAttribute")
        }
        val vinfo = (v.spObject as DataContainerVariable)
        return vinfo.mdl.javaClass.simpleName
    }

    override fun <T> readArrayData(v2: Variable<T>, section: SectionPartial?): ArrayTyped<T> {
        if (v2.nelems == 0L) {
            return ArrayEmpty(v2.shape.toIntArray(), v2.datatype)
        }
        val wantSection = SectionPartial.fill(section, v2.shape)

        // promoted attributes
        if (v2.spObject is DataContainerAttribute) {
            return header.readRegularData(v2.spObject, v2.datatype, wantSection)
        }

        val vinfo = v2.spObject as DataContainerVariable
        if (vinfo.onlyFillValue) { // fill value only, no data
            if (v2.datatype == STRING) return ArrayString(v2.shape.toIntArray(), List(v2.nelems.toInt()) {""} ) as ArrayTyped<T>
            if (v2.datatype == CHAR) {
                val shapeMinus1 = if (v2.rank == 0) intArrayOf(1) else IntArray(v2.rank - 1) { v2.shape[it].toInt()  }
                return ArrayString(shapeMinus1, List(shapeMinus1.computeSize()) {""} ) as ArrayTyped<T>
            }
            val tba = TypedByteArray(v2.datatype, vinfo.fillValue, 0, isBE = vinfo.h5type.isBE)
            return ArraySingle(wantSection.shape.toIntArray(), v2.datatype, tba.get(0))
        }

        return try {
            if (vinfo.mdl.isCompact) {
                val alldata = header.readCompactData(v2, v2.shape.toIntArray())
                alldata.section(wantSection)

            } else if (vinfo.mdl.isContiguous) {
                header.readRegularData(vinfo, v2.datatype, wantSection)

            } else if (vinfo.mdl is DataLayoutBTreeVer1) {
                H5chunkReader(header).readBtreeVer1(v2, wantSection)

            } else if (vinfo.mdl is DataLayoutSingleChunk4) {
                // H5chunkReader(header).readSingleChunk(v2, wantSection)
                // internal data class DataLayoutSingleChunk4(val flags: Byte, val chunkDimensions: IntArray, val chunkSize: Int, val heapAddress: Long, val filterMask: Int?) : DataLayoutMessage() {
                val offset = IntArray(v2.rank)
                val chunk = ChunkImpl(vinfo.mdl.heapAddress, vinfo.mdl.chunkSize, offset, vinfo.mdl.filterMask)

                H5chunkReader(header).readChunkedData(v2, wantSection, listOf(chunk).iterator())

            } else if (vinfo.mdl is DataLayoutImplicit4) {
                // H5chunkReader(header).readImplicit4(v2, wantSection)
                val index = ImplicitChunkIndex(header, varShape=v2.shape.toIntArray(), vinfo.mdl)
                H5chunkReader(header).readChunkedData(v2, wantSection, index.chunkIterator())

            } else if (vinfo.mdl is DataLayoutFixedArray4) {
                // H5chunkReader(header).readFixedArray4(v2, wantSection)
                val index = FixedArrayIndex(header, varShape=v2.shape.toIntArray(), vinfo.mdl) // mdl.fixedArrayIndex
                H5chunkReader(header).readChunkedData(v2, wantSection, index.chunkIterator())

            } else if (vinfo.mdl is DataLayoutExtensibleArray4) {
                val index = ExtensibleArrayIndex(header, vinfo.mdl.indexAddress,
                    v2.shape.toIntArray(), vinfo.mdl.chunkDimensions)
                H5chunkReader(header).readChunkedData(v2, wantSection, index.chunkIterator())

            } else if (vinfo.mdl is DataLayoutBtreeVer2) {
                // H5chunkReader(header).readBtreeVer2j(v2, wantSection)
                val index =  BTree2data(header, v2.name, vinfo.dataPos, vinfo.storageDims)
                H5chunkReader(header).readChunkedData(v2, wantSection, index.chunkIterator())

            } else {
                throw RuntimeException("Unsupported data layer type ${vinfo.mdl}")
            }
        } catch (ex: Exception) {
            println("failed to read ${v2.fullname()}, $ex")
            throw ex
        }
    }

    override fun <T> chunkIterator(v2: Variable<T>, section: SectionPartial?, maxElements : Int?) : Iterator<ArraySection<T>> {
        if (v2.nelems == 0L) {
            return listOf<ArraySection<T>>().iterator()
        }
        val wantSection = SectionPartial.fill(section, v2.shape)
        if (v2.spObject is DataContainerVariable) {
            val vinfo = v2.spObject
            if (vinfo.onlyFillValue) { // fill value only, no data
                val tba = TypedByteArray(v2.datatype, vinfo.fillValue, 0, isBE = vinfo.h5type.isBE)
                val single =
                    ArraySection<T>(ArraySingle(wantSection.shape.toIntArray(), v2.datatype, tba.get(0)), wantSection)
                return listOf(single).iterator()
            }
        }

        // TODO can we use concurrent reading ??
        return if (this.layoutName(v2) == "DataLayoutBTreeVer1") {
            H5chunkIterator(header, v2, wantSection)
        } else {
            H5maxIterator(this, v2, wantSection, maxElements ?: 100_000)
        }
    }

    override fun <T> readChunksConcurrent(v2: Variable<T>, lamda : (ArraySection<*>) -> Unit, done : () -> Unit,
                                          wantSection: SectionPartial?, nthreads: Int?) {
        val reader = H5chunkConcurrent(this, v2, wantSection)
        // TODO default nthreads ??
        reader.readChunks(nthreads ?: 20, lamda, done = { done() })
    }

    /*
    class Btree1chunkIterator(val hdfFile: Hdf5File, val v2: Variable<*>, val wantSection: SectionPartial?): AbstractIterator<ArraySection<*>>() {
        val reader = H5readConcurrent(hdfFile, v2)
        val nthreads = 20
        var currElement: ArraySection<*>? = null // could be a queue ? or a stack with a limit
        var done: Boolean = false

        init {
            reader.readChunks(nthreads,
                lamda = { it ->
                    if (currElement == null) currElement = it
                },
                done = { done = true }
            )
        }

        override fun computeNext() {
            if (currElement != null) {
                setNext( currElement!! )
                currElement = null
            } else {
                wait()
            }
            if (done) done()
        }
    } */
}