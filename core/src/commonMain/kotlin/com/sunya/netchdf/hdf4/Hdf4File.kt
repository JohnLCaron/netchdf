@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4

import com.fleeksoft.charset.Charset
import com.fleeksoft.charset.Charsets
import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.*
import com.sunya.cdm.layout.*
import com.sunya.cdm.util.InternalLibraryApi

@InternalLibraryApi
class Hdf4File(val filename : String) : Netchdf {
    private val raf : OpenFileIF = OkioFile(filename)
    val header: H4builder
    private val rootGroup: Group

    var valueCharset: Charset = Charsets.UTF8

    init {
        header = H4builder(raf, valueCharset)
        rootGroup = header.rootBuilder.build(null)
    }

    override fun close() {
        raf.close()
    }

    override fun rootGroup() = rootGroup
    override fun location() = filename
    override fun cdl() = cdl(this)

    override fun type() = header.type()
    override val size : Long get() = raf.size()

    override fun <T> readArrayData(v2: Variable<T>, wantSection: SectionPartial?): ArrayTyped<T> {
        if (v2.nelems == 0L) {
            return ArrayEmpty(v2.shape.toIntArray(), v2.datatype)
        }
        val filledSection = SectionPartial.fill(wantSection, v2.shape)
        return if (v2.datatype == Datatype.COMPOUND) {
            readStructureDataArray(v2, filledSection) as ArrayTyped<T>
        } else {
            readRegularDataArray(v2, filledSection)
        }
    }

    override fun <T> chunkIterator(v2: Variable<T>, section: SectionPartial?, maxElements : Int?): Iterator<ArraySection<T>> {
        if (v2.nelems == 0L) {
            return listOf<ArraySection<T>>().iterator()
        }
        val wantSection = SectionPartial.fill(section, v2.shape)
        val vinfo = v2.spObject as Vinfo

        return if (vinfo.isChunked) {  // LOOK isLinked?
            H4chunkIterator(header, v2, wantSection)
        } else {
            H4maxIterator(v2, wantSection, maxElements ?: 100_000)
        }
    }

    private inner class H4maxIterator<T>(val v2: Variable<T>, wantSection : Section, maxElems: Int) : AbstractIterator<ArraySection<T>>() {
        private val debugChunking = false
        private val maxIterator  = MaxChunker(maxElems,  wantSection)

        override fun computeNext() {
            if (maxIterator.hasNext()) {
                val indexSection = maxIterator.next()
                if (debugChunking) println("  chunk=${indexSection}")

                val section = indexSection.section(v2.shape)
                val array = if (v2.datatype == Datatype.COMPOUND) {
                    readStructureDataArray(v2, section)
                } else {
                    readRegularDataArray(v2, section)
                }
                setNext(ArraySection(array as ArrayTyped<T>, section))
            } else {
                done()
            }
        }
    }

    private fun <T> readRegularDataArray(v: Variable<T>, section: Section): ArrayTyped<T> {
        requireNotNull(v.spObject) { "Variable ${v.name}"}
        val vinfo = v.spObject as Vinfo

        if (vinfo.tagData != null) {
            vinfo.setLayoutInfo(header) // make sure needed info is present LOOK why wait until now ??
        }

        if (vinfo.hasNoData) {
            return ArraySingle(section.shape.toIntArray(), v.datatype, convertFromBytes(v.datatype, vinfo.fillValue!!, vinfo.isBE))
        }

        if (vinfo.svalue != null) {
            return ArrayString(intArrayOf(), listOf(vinfo.svalue!!)) as ArrayTyped<T>
        }

        if (!vinfo.isCompressed) {
            if (!vinfo.isLinked && !vinfo.isChunked) {
                val layout = LayoutRegular(vinfo.start, vinfo.elemSize, section)
                return readDataWithFill(raf, layout, v, section)

            } else if (vinfo.isLinked) {
                val layout = LayoutSegmented(vinfo.segPos, vinfo.segSize, vinfo.elemSize, section)
                return readDataWithFill(raf, layout, v, section)

            } else if (vinfo.isChunked) {
                return H4chunkReader(header).readChunkedData(v, section)
            }
        } else {
            if (!vinfo.isLinked && !vinfo.isChunked) {
                val layout = LayoutRegular(0, vinfo.elemSize, section)
                val reader = getCompressedReader(header, vinfo) // was getCompressedInputStream
                return readDataWithFill(reader, layout, v, section)

            } else if (vinfo.isLinked) {
                val layout = LayoutRegular(0, vinfo.elemSize, section)
                val reader = getLinkedCompressedReader(header, vinfo) // was getLinkedCompressedInputStream
                return readDataWithFill(reader, layout, v, section)

            } else if (vinfo.isChunked) {
                return H4chunkReader(header).readChunkedData(v, section)
            }
        }
        throw IllegalStateException()
    }

    private fun <T> readDataWithFill(reader: ReaderIntoByteArray, layout: Layout, v2: Variable<T>, wantSection: Section)
            : ArrayTyped<T> {
        require(wantSection.totalElements == layout.totalNelems)
        val vinfo = v2.spObject as Vinfo
        val totalNbytes = (vinfo.elemSize * layout.totalNelems)
        require(totalNbytes < Int.MAX_VALUE)
        val ba = ByteArray(totalNbytes.toInt())

        // prefill with fill value
        if (vinfo.fillValue != null) {
            val fillValue = vinfo.fillValue!!
            val nz = fillValue.map { it != 0.toByte() }.count() > 0
            if (nz) {
                val size = fillValue.size
                ba.forEachIndexed { idx, b -> ba[idx] = fillValue[idx % size] }
            }
        }

        var bytesRead = 0
        val filePos = OpenFileState(vinfo.start, vinfo.isBE)
        while (layout.hasNext()) {
            val chunk = layout.next()
            filePos.pos = chunk.srcPos()
            val dstPos = (vinfo.elemSize * chunk.destElem()).toInt()
            val chunkBytes = vinfo.elemSize * chunk.nelems()
            bytesRead += reader.readIntoByteArray(filePos, ba, dstPos, chunkBytes)
        }

        val shape = wantSection.shape.toIntArray()
        val tba = TypedByteArray(v2.datatype, ba, 0, isBE = true)
        return tba.convertToArrayTyped(shape) // , vinfo.elemSize)
    }

    private fun <T> readStructureDataArray(v2: Variable<T>, section: Section): ArrayStructureData {
        val vinfo = v2.spObject as Vinfo

        if (vinfo.tagData != null) {
            vinfo.setLayoutInfo(header) // make sure needed info is present LOOK why wait until now ??
        } else {
            vinfo.hasNoData = true
        }

        requireNotNull(v2.datatype.typedef)
        require(v2.datatype.typedef is CompoundTypedef)
        val recsize: Int = vinfo.elemSize
        val members = v2.datatype.typedef.members
        val shape = section.shape.toIntArray()

        val array = if (vinfo.hasNoData) {
            val nbytes = (recsize * section.totalElements).toInt()
            ArrayStructureData(shape, ByteArray(nbytes), isBE = true, recsize, members)

        } else if (!vinfo.isLinked && !vinfo.isCompressed) {
            val layout = LayoutRegular(vinfo.start, recsize, section)
            header.raf.readArrayStructureData(layout, shape, members)

        } else if (vinfo.isLinked && !vinfo.isCompressed) {
            val reader = getLinkedReader(header, vinfo)
            // val dataSource = PositioningDataInputSource(source)
            val layout = LayoutRegular(0, recsize, section)
            reader.readArrayStructureData(layout, shape, members)

        } else if (!vinfo.isLinked && vinfo.isCompressed) {
            val reader = getCompressedReader(header, vinfo)
            // val dataSource = PositioningDataInputSource(source)
            val layout: Layout = LayoutRegular(0, recsize, section)
            reader.readArrayStructureData(layout, shape, members)

        } else  { // if (vinfo.isLinked && vinfo.isCompressed)
            val reader = getLinkedCompressedReader(header, vinfo)
            // val dataSource = PositioningDataInputSource(source)
            val layout: Layout = LayoutRegular(0, recsize, section)
            reader.readArrayStructureData(layout, shape, members)
        }

        return array
    }
}
/*
internal fun getCompressedSource(h4: H4builder, vinfo: Vinfo): Source {
    val ba = h4.raf.readByteArray(OpenFileState(vinfo.start, true), vinfo.length)
    val okioBuffer = Buffer()
    okioBuffer.write(ba)
    return okioBuffer.inflate()
}

internal fun getLinkedCompressedSource(h4: H4builder, vinfo: Vinfo): Source {
    return LinkedCompressedSource(h4, vinfo)
} */

// called from special.getDataChunks()
internal fun readStructureDataArray(h4: H4builder, vinfo: Vinfo, section: Section, members: List<StructureMember<*>>): ArrayStructureData {
    val shape = section.shape.toIntArray()
    val recsize: Int = vinfo.elemSize

    if (!vinfo.isLinked && !vinfo.isCompressed) {
        val layout = LayoutRegular(vinfo.start, recsize, section)
        return h4.raf.readArrayStructureData(layout, shape, members)

    } else if (vinfo.isLinked && !vinfo.isCompressed) {
        val reader = getLinkedReader(h4, vinfo)
        // val dataSource = PositioningDataInputSource(source)
        val layout = LayoutRegular(0, recsize, section)
        return reader.readArrayStructureData(layout, shape, members)

    } else if (!vinfo.isLinked && vinfo.isCompressed) {
        val reader  = getCompressedReader(h4, vinfo)
        //val dataSource = PositioningDataInputSource(source)
        val layout: Layout = LayoutRegular(0, recsize, section)
        return reader.readArrayStructureData(layout, shape, members)

    } else { // if (vinfo.isLinked && vinfo.isCompressed) {
        val reader = getLinkedCompressedReader(h4, vinfo)
        // val dataSource = PositioningDataInputSource(source)
        val layout: Layout = LayoutRegular(0, recsize, section)
        return reader.readArrayStructureData(layout, shape, members)
    }
}


internal fun ReaderIntoByteArray.readArrayStructureData(layout: Layout, shape : IntArray, members : List<StructureMember<*>>): ArrayStructureData {
    val state = OpenFileState(0, true)
    val sizeBytes = shape.computeSize() * layout.elemSize
    val ba = ByteArray(sizeBytes)

    while (layout.hasNext()) {
        val chunk: Layout.Chunk = layout.next()
        state.pos = chunk.srcPos()
        this.readIntoByteArray(state, ba, layout.elemSize * chunk.destElem().toInt(), layout.elemSize * chunk.nelems())
    }

    return ArrayStructureData(shape, ba, isBE = true, layout.elemSize, members)
}
