@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4Clib

/*
hdf4 library src:
/home/stormy/dev/github/hdf4
install:
/home/stormy/install/hdf4

cd /home/stormy/install/jextract-19/bin

./jextract --source \
    --header-class-name mfhdf_h \
    --target-package com.sunya.netchdf.mfhdfClib.ffm \
    -I /home/stormy/install/hdf4/include/mfhdf.h \
    -l /home/stormy/install/hdf4/lib/libmfhdf.so \
    --output /home/stormy/dev/github/netcdf/netchdf/testclibs/src/main/java \
    /home/stormy/install/hdf4/include/mfhdf.h
 */

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.OpenFileIF.Companion.nativeByteOrder
import com.sunya.cdm.layout.MaxChunker
import com.sunya.cdm.util.InternalLibraryApi

import com.sunya.netchdf.mfhdfClib.ffm.mfhdf_h.*
import java.lang.foreign.MemoryLayout
import java.lang.foreign.Arena
import java.lang.foreign.ValueLayout
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import kotlin.math.min

class Hdf4ClibFile(val filename: String) : Netchdf {
    private val header: HCheader = HCheader(filename)

    override fun rootGroup() = header.rootGroup

    override fun location() = filename
    override fun cdl() = cdl(this)
    override fun type() = if (header.isEos()) "hdf-eos2" else "hdf4"

    override fun close() {
        header.close()
    }

    // LOOK SDreadchunk ??
    override fun <T> readArrayData(v2: Variable<T>, wantSection: SectionPartial?): ArrayTyped<T> {
        return readArrayData(v2, SectionPartial.fill(wantSection, v2.shape))
    }

    internal fun <T> readArrayData(v2: Variable<T>, filled: Section): ArrayTyped<T> {
        if (v2.nelems == 0L) {
            return ArrayEmpty(v2.shape.toIntArray(), v2.datatype)
        }

        val vinfo = v2.spObject as Vinfo4
        val datatype = v2.datatype
        val nbytes = filled.totalElements * datatype.size
        if (nbytes == 0L) {
            return ArraySingle(filled.shape.toIntArray(), v2.datatype, 0)
        }

        if (vinfo.value != null) {
            return vinfo.value!!.section(filled) as ArrayTyped<T>

        } else if (vinfo.sdsIndex != null) {
            return readSDdata(header.sdsStartId, vinfo.sdsIndex!!, datatype, filled, nbytes)

        } else if (vinfo.vsInfo != null) {
            val startRecord = if (filled.rank == 0) 0 else filled.ranges[0].first.toInt()
            val numRecords = if (filled.rank == 0) 1 else filled.shape[0].toInt()
            return readVSdata(header.fileOpenId, vinfo.vsInfo!!, datatype, startRecord, numRecords)

        } else if (vinfo.grIndex != null) {
            return readGRdata(header.grStartId, vinfo.grIndex!!, datatype, filled, nbytes)
        }

        throw RuntimeException("cant read ${v2.name}")
    }

    override fun <T> chunkIterator(v2: Variable<T>, wantSection: SectionPartial?, maxElements : Int?): Iterator<ArraySection<T>> {
        if (v2.nelems == 0L) {
            return listOf<ArraySection<T>>().iterator()
        }

        val filled = SectionPartial.fill(wantSection, v2.shape)
        return HCmaxIterator(v2, filled, maxElements ?: 100_000)
    }

    private inner class HCmaxIterator<T>(val v2: Variable<T>, wantSection : Section, maxElems: Int) : AbstractIterator<ArraySection<T>>() {
        private val debugChunking = false
        private val maxIterator  = MaxChunker(maxElems,  wantSection)

        override fun computeNext() {
            if (maxIterator.hasNext()) {
                val indexSection = maxIterator.next()
                if (debugChunking) println("  chunk=${indexSection}")

                val section = indexSection.section(v2.shape)
                val array = readArrayData(v2, section)
                setNext(ArraySection(array, section))
            } else {
                done()
            }
        }
    }

    companion object {
        var valueCharset: Charset = StandardCharsets.UTF_8
    }
}

fun <T> readSDdata(sdsStartId: Int, sdindex: Int, datatype: Datatype<T>, wantSection: Section, nbytes: Long): ArrayTyped<T> {
    val rank = wantSection.rank

    Arena.ofConfined().use { session ->
        val intArray = MemoryLayout.sequenceLayout(rank.toLong(), C_INT)
        val origin_p = session.allocateArray(intArray, rank.toLong())
        val shape_p = session.allocateArray(intArray, rank.toLong())
        val stride_p = session.allocateArray(intArray, rank.toLong())
        for (idx in 0 until rank) {
            val range = wantSection.ranges[idx]
            origin_p.setAtIndex(C_INT, idx.toLong(), range.first.toInt())
            shape_p.setAtIndex(C_INT, idx.toLong(), wantSection.shape[idx].toInt())
            stride_p.setAtIndex(C_INT, idx.toLong(), range.step.toInt())
        }
        val data_p = session.allocate(nbytes)

        val sds_id = SDselect(sdsStartId, sdindex)
        try {
            checkErr("SDreaddata", SDreaddata(sds_id, origin_p, stride_p, shape_p, data_p))
            val raw = data_p.toArray(ValueLayout.JAVA_BYTE)!!
            val tba = TypedByteArray(datatype, raw, 0, isBE = nativeByteOrder)
            return tba.convertToArrayTyped(wantSection.shape.toIntArray())

        } finally {
            SDendaccess(sds_id)
        }
    }
}

fun <T> readVSdata(fileOpenId: Int, vsInfo: VSInfo, datatype : Datatype<T>, startRecnum: Int, wantRecords: Int): ArrayTyped<T> {
    val startRecord = if (vsInfo.nrecords == 1) 0 else startRecnum // trick because we promote single field structures
    val numRecords = min(vsInfo.nrecords, wantRecords) // trick because we promote single field structures
    val nelt = numRecords * vsInfo.recsize
    val shape = if (datatype.size == 1) intArrayOf(nelt) else intArrayOf(numRecords) // TODO seems wrong

    Arena.ofConfined().use { session ->
        val read_access_mode = session.allocateUtf8String("r")
        val fldnames_p = session.allocateUtf8String(vsInfo.fldNames)
        val data_p = session.allocate(nelt.toLong()) // LOOK memory clobber?
        val vdata_id = VSattach(fileOpenId, vsInfo.vs_ref, read_access_mode)
        try {
            checkErrNeg("VSsetfields", VSsetfields(vdata_id, fldnames_p))
            checkErrNeg("VSseek", VSseek(vdata_id, startRecord))
            // int32 VSread(int32 vdata_id, uint8 *databuf, int32 n_records, int32 interlace_mode)
            val nread = VSread(vdata_id, data_p, numRecords, FULL_INTERLACE())
            checkErrNeg("VSread", nread)
            require(nread == numRecords)

            // As the data is stored contiguously in the vdata, VSfpack should be used to
            // unpack the fields after reading.

            val raw = data_p.toArray(ValueLayout.JAVA_BYTE)!!

            if (datatype.typedef is CompoundTypedef) {
                val members = (datatype.typedef as CompoundTypedef).members
                return ArrayStructureData(shape, raw, isBE = nativeByteOrder, vsInfo.recsize, members) as ArrayTyped<T>
            } else {
                // a single field is made into a regular variable
                val tba = TypedByteArray(datatype, raw, 0, isBE = nativeByteOrder)
                return tba.convertToArrayTyped(shape, vsInfo.recsize)
            }
        } finally {
            VSdetach(vdata_id)
        }
    }
}

fun <T> readGRdata(grStartId: Int, grIdx: Int, datatype: Datatype<T>, wantSection: Section, nbytes: Long): ArrayTyped<T> {

    Arena.ofConfined().use { session ->
        // flip the shape
        val rank = wantSection.rank
        val flipShape = IntArray(rank) { wantSection.shape[rank - it - 1].toInt() }

        val intArray = MemoryLayout.sequenceLayout(rank.toLong(), C_INT)
        val origin_p = session.allocateArray(intArray, rank.toLong())
        val shape_p = session.allocateArray(intArray, rank.toLong())
        val stride_p = session.allocateArray(intArray, rank.toLong())
        for (idx in 0 until rank) {
            val range = wantSection.ranges[idx]
            origin_p.setAtIndex(C_INT, idx.toLong(), range.first.toInt())
            shape_p.setAtIndex(C_INT, idx.toLong(), flipShape[idx])
            stride_p.setAtIndex(C_INT, idx.toLong(), range.step.toInt())
        }
        val data_p = session.allocate(nbytes)

        val grId = GRselect(grStartId, grIdx)
        try {
            // intn GRreadimage(int32 ri_id, int32 start[2], int32 stride[2], int32 edge[2], VOIDP data)
            checkErr("GRreadimage", GRreadimage(grId, origin_p, stride_p, shape_p, data_p))
            val raw = data_p.toArray(ValueLayout.JAVA_BYTE)!!
            val tba = TypedByteArray(datatype, raw, 0, isBE = nativeByteOrder)
            return tba.convertToArrayTyped(wantSection.shape.toIntArray())
        } finally {
            GRendaccess(grId)
        }
    }
}