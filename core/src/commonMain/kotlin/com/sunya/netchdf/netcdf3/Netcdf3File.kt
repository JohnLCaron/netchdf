@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.netcdf3

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.*
import com.sunya.cdm.layout.*
import com.sunya.cdm.util.InternalLibraryApi

internal class Netcdf3File(val filename : String) : Netchdf {
    private val raf : OpenFileIF = OkioFile(filename)
    private val header : N3header
    private val rootGroup : Group

    init {
        val rootBuilder = Group.Builder("")
        header = N3header(raf, rootBuilder)
        rootGroup = rootBuilder.build(null)
    }

    override fun close() {
        raf.close()
    }

    override fun rootGroup() = rootGroup
    override fun location() = filename
    override fun cdl() = cdl(this)
    override fun type() = header.formatType()
    override val size : Long get() = raf.size()

    override fun <T> readArrayData(v2: Variable<T>, wantSection: SectionPartial?): ArrayTyped<T> {
        if (v2.nelems == 0L) {
            return ArrayEmpty(v2.shape.toIntArray(), v2.datatype)
        }
        val section : Section = SectionPartial.fill(wantSection, v2.shape)
        val vinfo = v2.spObject as VinfoN3
        val layout = if (!v2.hasUnlimited()) {
            LayoutRegular(vinfo.begin, vinfo.elemSize, section)
        } else {
            LayoutRegularSegmented(vinfo.begin, vinfo.elemSize, header.recsize, section)
        }
        return readDataWithLayout(layout, v2, section)
    }

    private fun Variable<*>.hasUnlimited() : Boolean {
        return this.dimensions.find { it == header.unlimitedDimension } != null
    }

    override fun <T> chunkIterator(v2: Variable<T>, wantSection: SectionPartial?, maxElements : Int?): Iterator<ArraySection<T>> {
        if (v2.nelems == 0L) {
            return listOf<ArraySection<T>>().iterator()
        }
        val section = SectionPartial.fill(wantSection, v2.shape)
        return NCmaxIterator(v2, section, maxElements ?: 100_000)
    }

    private inner class NCmaxIterator<T>(val v2: Variable<T>, wantSection : Section, maxElems: Int) : AbstractIterator<ArraySection<T>>() {
        private val debugChunking = false
        val vinfo = v2.spObject as VinfoN3
        private val maxIterator  = MaxChunker(maxElems,  wantSection)

        override fun computeNext() {
            if (maxIterator.hasNext()) {
                val indexSection = maxIterator.next()
                if (debugChunking) println("  chunk=${indexSection}")
                val section = indexSection.section(v2.shape)

                val layout = if (!v2.hasUnlimited()) {
                    LayoutRegular(vinfo.begin, vinfo.elemSize, section)
                } else {
                    // I think this will segment on the record dimension
                    LayoutRegularSegmented(vinfo.begin, vinfo.elemSize, header.recsize, section)
                }

                val array = readDataWithLayout(layout, v2, section)
                setNext(ArraySection(array, section))
            } else {
                done()
            }
        }
    }

    private fun <T> readDataWithLayout(layout: Layout, v2: Variable<T>, wantSection : Section): ArrayTyped<T> {
        require(wantSection.totalElements == layout.totalNelems)
        val vinfo = v2.spObject as VinfoN3
        val totalNbytes = (vinfo.elemSize * layout.totalNelems)
        require(totalNbytes < Int.MAX_VALUE)
        val ba = ByteArray(totalNbytes.toInt())

        var bytesRead = 0
        val filePos = OpenFileState(vinfo.begin, true)
        while (layout.hasNext()) {
            val chunk = layout.next()
            filePos.pos = chunk.srcPos()
            val dstPos = (vinfo.elemSize * chunk.destElem()).toInt()
            val chunkBytes = vinfo.elemSize * chunk.nelems()
            bytesRead += raf.readIntoByteArray(filePos, ba, dstPos, chunkBytes)
        }
        require(bytesRead == totalNbytes.toInt())

        val shape = wantSection.shape.toIntArray()
        val tba = TypedByteArray(v2.datatype, ba, 0, isBE = true)
        return tba.convertToArrayTyped(shape)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Netchdf) return false

        if (filename != other.location()) return false
        if (rootGroup != other.rootGroup()) return false

        return true
    }

    override fun hashCode(): Int {
        var result = filename.hashCode()
        result = 31 * result + rootGroup.hashCode()
        return result
    }

}