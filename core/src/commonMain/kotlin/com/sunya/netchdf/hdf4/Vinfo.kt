@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4

import com.sunya.cdm.api.*
import com.sunya.cdm.array.convertToBytes
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_BYTE
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_DOUBLE
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_FLOAT
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_INT
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_SHORT
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_UBYTE
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_UINT
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_USHORT
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_INT64
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_STRING
import com.sunya.netchdf.netcdf4.Netcdf4.NC_FILL_UINT64

internal class Vinfo(val refno: Int) : Comparable<Vinfo?> {
    var vb: Variable.Builder<*>? = null
    val tags = mutableListOf<Tag>()

    // info about reading the data
    var tagDataRI: TagRasterImage? = null
    var tagData: TagData? = null
    var elemSize = 0 // for Structures, this is recsize
    var fillValue: ByteArray? = null

    // below is not set until setLayoutInfo() is called
    var isLinked = false
    var isCompressed = false
    var isChunked = false
    var hasNoData = false

    // regular
    var start = -1L
    var length = 0

    // linked
    var segPos = LongArray(0)
    var segSize = IntArray(0)

    // chunked
    var chunks: List<SpecialDataChunk>? = null
    var chunkLengths = IntArray(0)

    // internal string valued
    var svalue : String? = null

    // LOOK "always big-endian on disk"
    var isBE: Boolean = true// LOOK TABLE 2H Little-Endian Format Data Type Definitions

    fun setVariable(v: Variable.Builder<*>) {
        vb = v
        v.spObject = this
    }

    override fun compareTo(other: Vinfo?): Int {
        return refno.compareTo(other!!.refno)
    }

    fun setData(data: TagData?, elemSize: Int) {
        this.tagData = data
        this.elemSize = elemSize
        hasNoData = (data == null) || (data.offset < 0)
    }

    fun setFillValue(att: Attribute<*>) {
        fillValue = convertToBytes(att.datatype, att.values[0], isBE)
    }

    fun setSValue(svalue : String) : Vinfo {
        this.svalue = svalue
        return this
    }

    // make sure that the needed info is present: call this when variable needs to be read
    // this allows us to defer getting layout info until then
    fun setLayoutInfo(header: H4builder) {
        if (tagData == null) return
        val useData = tagData!!
        if (null != useData.linked) {
            isLinked = true
            setDataBlocks(useData.linked!!.getLinkedDataBlocks(header))

        } else if (null != useData.compress) {
            isCompressed = true
            val compData: TagData = useData.compress!!.getDataTag(header)
            tags.add(compData)
            isLinked = (compData.linked != null)
            if (isLinked) {
                setDataBlocks(compData.linked!!.getLinkedDataBlocks(header))
            } else {
                start = compData.offset
                length = compData.length
                hasNoData = (start < 0) || (length < 0)
            }
        } else if (null != useData.chunked) {
            isChunked = true
            chunks = useData.chunked!!.getDataChunks(header)
            chunkLengths = useData.chunked!!.chunkLength
            isCompressed = useData.chunked!!.isCompressed
        } else {
            start = useData.offset
            hasNoData = (start < 0)
        }
    }

    private fun setDataBlocks(linkedBlocks: List<TagLinkedBlock>) {
        val nsegs = linkedBlocks.size
        segPos = LongArray(nsegs)
        segSize = IntArray(nsegs)
        for ((count, tag: TagLinkedBlock) in linkedBlocks.withIndex()) {
            segPos[count] = tag.offset
            segSize[count] = tag.length
        }
    }

    override fun toString(): String {
        return buildString {
            vb?.let { append("refno=$refno name=${it.name} fillValue=$fillValue") }
            append(" isChunked=$isChunked isCompressed=$isCompressed isLinked=$isLinked hasNoData=$hasNoData")
            append(" elemSize=$elemSize data start=$start length=$length tags=${tags.map{ it.refno }}")
        }
    }
}

// the netcdf default fill values
internal fun getNcDefaultFillValue(datatype: Datatype<*>): Any {
    return when (datatype) {
        Datatype.BYTE -> NC_FILL_BYTE
        Datatype.CHAR,
        Datatype.UBYTE -> NC_FILL_UBYTE
        Datatype.SHORT -> NC_FILL_SHORT
        Datatype.USHORT -> NC_FILL_USHORT
        Datatype.INT -> NC_FILL_INT
        Datatype.UINT -> NC_FILL_UINT
        Datatype.FLOAT -> NC_FILL_FLOAT
        Datatype.DOUBLE -> NC_FILL_DOUBLE
        Datatype.LONG -> NC_FILL_INT64
        Datatype.ULONG -> NC_FILL_UINT64
        Datatype.STRING -> NC_FILL_STRING
        else -> 0
    }
}

// #define FILL_BYTE    ((char)-127)        /* Largest Negative value */
//#define FILL_CHAR    ((char)0)
//#define FILL_SHORT    ((short)-32767)
//#define FILL_LONG    ((long)-2147483647)

// it would be convenient to turn into bytes
// the Hdf4 SD default fill values, uses different values for the unsigned types.
internal fun getSDefaultFillValue(datatype: Datatype<*>, isBE: Boolean): ByteArray {
    val fillValue = when (datatype) {
        Datatype.BYTE -> NC_FILL_BYTE
        Datatype.CHAR,
        Datatype.UBYTE -> NC_FILL_BYTE.toUByte()
        Datatype.SHORT -> NC_FILL_SHORT
        Datatype.USHORT -> NC_FILL_SHORT.toUShort()
        Datatype.INT -> NC_FILL_INT
        Datatype.UINT -> NC_FILL_INT.toUInt()
        Datatype.FLOAT -> NC_FILL_FLOAT
        Datatype.DOUBLE -> NC_FILL_DOUBLE
        Datatype.LONG -> NC_FILL_INT64
        Datatype.ULONG -> NC_FILL_INT64.toULong()
        Datatype.STRING -> NC_FILL_STRING
        else -> 0
    }
    return convertToBytes(datatype, fillValue, isBE) // TODO
}