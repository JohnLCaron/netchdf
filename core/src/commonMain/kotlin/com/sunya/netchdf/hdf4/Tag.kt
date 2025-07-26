@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Group
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.hdf4.H4builder.Companion.tagid
import com.sunya.netchdf.hdf4.TagEnum.Companion.obsolete

internal fun readTag(raf : OpenFileIF, state: OpenFileState): Tag {
    // read just the DD part of the tag. see p 11
    val xtag = raf.readShort(state).toUShort().toInt()
    val btag = xtag and 0x3FFF // 14 bits // basic tags are numbered 0x0001 through 0x3FFF,
    val refno = raf.readShort(state).toUShort().toInt()
    val offset = raf.readInt(state).toLong() // LOOK only 32 bit (!)
    val length = raf.readInt(state)

    when (val tagEnum = TagEnum.byCode(btag)) {
        TagEnum.LINKED -> return TagLinkedBlock(xtag, refno, offset, length)
        TagEnum.VERSION -> return TagVersion(xtag, refno, offset, length)
        TagEnum.COMPRESSED, TagEnum.CHUNK, TagEnum.SD, TagEnum.VS -> return TagData(xtag, refno, offset, length)
        TagEnum.FID, TagEnum.FD, TagEnum.SDC -> return TagText(xtag, refno, offset, length)
        TagEnum.DIL, TagEnum.DIA -> return TagAnnotate(xtag, refno, offset, length)
        TagEnum.NT -> return TagNT(xtag, refno, offset, length)
        TagEnum.ID, TagEnum.LD, TagEnum.MD -> return TagImageDim(xtag, refno, offset, length)
        TagEnum.ID8 -> return TagRI8Dimension(xtag, refno, offset, length)
        TagEnum.LUT -> return TagLookupTable(xtag, refno, offset, length)
        TagEnum.IP8 -> return TagIP8(xtag, refno, offset, length)
        TagEnum.RI -> return TagRasterImage(xtag, refno, offset, length)
        TagEnum.RIG, TagEnum.NDG -> return TagDataGroup(xtag, refno, offset, length)
        TagEnum.SDD -> return TagSDD(xtag, refno, offset, length)
        // TagEnum.SDS -> return TagSDS(xtag, refno, offset, length)
        TagEnum.SDL, TagEnum.SDU, TagEnum.SDF -> return TagTextN(xtag, refno, offset, length)
        TagEnum.SDM -> return TagSDminmax(xtag, refno, offset, length)
        TagEnum.FV -> return TagFV(xtag, refno, offset, length)
        TagEnum.VH -> return TagVH(xtag, refno, offset, length)
        TagEnum.VG -> return TagVGroup(xtag, refno, offset, length)
        // wtf? 17086 -> return TagVGroup(icode, refno, offset, length)
        else -> {
            if ((xtag > 1) and !obsolete.contains(tagEnum)) {
                println(" Unknown xtag=$xtag btag=$btag refno=$refno")
            }
            return Tag(xtag, refno, offset, length)
        }
    }
}

// Tag == "Data Descriptor" (DD) and (usually) a "Data Element" that the offset/length points to
internal open class Tag(xtag: Int, val refno : Int, val offset : Long, val length : Int) {
    val isExtended: Boolean = (xtag and 0x4000) != 0
    val code = (xtag and 0x3FFF) // basic tag

    internal var isUsed = false
    internal var usedBy : Tag? = null
    internal var vinfo: Vinfo? = null

    // read the offset/length part of the tag. overridden by subclasses
    open fun readTag(h4 : H4builder) {
    }

    open fun detail(): String {
        return toString()
    }

    override fun toString(): String {
        return buildString {
            append(if (isUsed) " " else "*")
            // append(" ${"%-17s".format("${refCode()} ${vClass()}")}")
            append(" ${"${refCode()} ${vClass()}".padStart(17)}")
            append(" usedBy=${usedBy?.refCode()?:isUsed}")
            append(" pos=$offset/$length")
            append(if (isExtended) " isExtended" else "")
        }
    }

    fun tagid(): Int {
        return tagid(refno, code)
    }

    fun tagEnum(): TagEnum {
        return TagEnum.byCode(this.code)
    }

    fun refCode(): String {
        return refCode(refno, code)
    }

    private fun vClass() : String {
        return when (this) {
            is TagVGroup -> this.className
            is TagVH -> this.className
            else -> ""
        }
    }

    companion object {
        fun refCode(refno : Int, code : Int): String {
            return "${TagEnum.byCode(code).name}/$refno".padStart(7)
        }
    }
}

// TagEnum.COMPRESSED (40), TagEnum.CHUNK (61), TagEnum.SD (702), TagEnum.VS (1963)
// Combining so we just have one data object
internal class TagData(icode: Int, refno : Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    private var extendedTag: Int = 0
    internal var linked: SpecialLinked? = null
    internal var compress: SpecialComp? = null
    internal var chunked: SpecialChunked? = null
    var tag_len = 0 // needed?

    override fun readTag(h4 : H4builder) {
        if (isExtended) {
            val state = OpenFileState(offset, true)
            extendedTag = h4.raf.readShort(state).toInt()
            when (extendedTag) {
                TagEnum.SPECIAL_LINKED -> linked = SpecialLinked(h4.raf, state, this) // TagEnum.VS
                TagEnum.SPECIAL_COMP -> compress = SpecialComp(h4.raf, state, this) // TagEnum.COMPRESSED
                TagEnum.SPECIAL_CHUNKED -> chunked = SpecialChunked(h4.raf, state, this) // TagEnum.CHUNK
            }
            tag_len = (state.pos - offset).toInt()
        }
    }

    override fun detail(): String {
        return if (linked != null) {
            super.detail() + linked!!.detail()
        } else if (compress != null) {
            super.detail() + compress!!.detail()
        } else if (chunked != null) {
            super.detail() + chunked!!.detail()
        } else super.detail()
    }

    fun markDataTags(h4: H4builder) {
        linked?.getLinkedDataBlocks(h4)
        compress?.getDataTag(h4)
        chunked?.getDataChunks(h4, false)
    }
}

// 20 p 146 Also used for data blocks, which has no next_ref! (!)
internal class TagLinkedBlock(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var next_ref: Int = 0
    var block_ref = IntArray(0)
    var n = 0

    fun read2(h4 : H4builder, nb: Int, dataBlocks: MutableList<TagLinkedBlock>, owner : Tag? = null) {
        val state = OpenFileState(offset, true)
        next_ref = h4.raf.readShort(state).toUShort().toInt()
        block_ref = IntArray(nb)
        for (i in 0 until nb) {
            block_ref[i] = h4.raf.readShort(state).toUShort().toInt()
            if (block_ref[i] == 0) break
            n++
        }
        for (i in 0 until n) {
            val tagid = tagid(block_ref[i], TagEnum.LINKED.code)
            val tag = h4.tagidMap[tagid] as TagLinkedBlock?
            if (tag != null) {
                tag.isUsed = true
                tag.usedBy = owner
                dataBlocks.add(tag)
            }
        }
    }

    override fun detail(): String {
        return buildString {
            append(super.detail())
            append(" next_ref= ").append(next_ref)
            append(" dataBlks= ")
            for (i in 0 until n) {
                val ref = block_ref[i]
                append(ref).append(" ")
            }
        }
    }
}

// 30
internal class TagVersion(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var major = 0
    var minor = 0
    var release = 0
    var name: String? = null

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        major = h4.raf.readInt(state)
        minor = h4.raf.readInt(state)
        release = h4.raf.readInt(state)
        name = h4.raf.readString(state, length - 12)
    }

    fun value(): String {
        return "$major.$minor.$release ($name)"
    }

    override fun detail(): String {
        return "${super.detail()} ${value()} ($name)"
    }
}

// 100, 101
internal class TagText(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var text: String = "null"

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        text = h4.raf.readString(state, length, h4.valueCharset).trim()
    }

    override fun detail(): String {
        val t = if ((text.length < 60)) text else text.substring(0, 59)
        return super.detail() + " text= '$t'"
    }
}

// 104, 105
internal class TagAnnotate(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var text: String = "null"
    var obj_tagno: Int = 0
    var obj_refno: Int = 0

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        obj_tagno = h4.raf.readShort(state).toUShort().toInt()
        obj_refno = h4.raf.readShort(state).toUShort().toInt()
        text = h4.raf.readString(state, length - 4, h4.valueCharset).trim()
    }

    override fun detail(): String {
        val t = if ((text.length < 60)) text else text.substring(0, 59)
        return super.detail() + " for= $obj_refno/$obj_tagno text= '$t'"
    }
}

// 106 p.114 DFTAG_NT
internal class TagNT(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var version: Byte = 0
    var numberType: Int = 0 // see H4type.getDataType()
    var nbits: Byte = 0 // Number of bits, all of which are assumed to be significant
    var type_class: Byte = 0 // meaning depends on type: floating point, integer, or character

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        version = h4.raf.readByte(state)
        numberType = h4.raf.readByte(state).toInt()
        nbits = h4.raf.readByte(state)
        type_class = h4.raf.readByte(state)
    }

    override fun detail(): String {
        return super.detail() + " version=" + version + " type=" + numberType + " nbits=" + nbits + " type_class=" + type_class
    }

    override fun toString(): String {
        return super.toString() + " type= ${H4type.getDataType(numberType)} nbits= $nbits"
    }
}

// Image palette-8 (200) p.144 DS
internal class TagRI8Dimension(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var xdim = 0
    var ydim = 0

    // this i think can be read as data(256, 3) - so make into a variable?
    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        xdim = h4.raf.readShort(state).toUShort().toInt()
        ydim = h4.raf.readShort(state).toUShort().toInt()
    }

    override fun detail(): String {
        return super.detail() + " xdim=$xdim ydim=$ydim"
    }
}

// DFTAG_IP8 Image dimension-8 (201) p.144
internal class TagIP8(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var rgb : ArrayByte? = null

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        val raw = h4.raf.readByteArray(state, 3 * 256)
        rgb =  ArrayByte(intArrayOf(256, 3), raw )
    }

    override fun detail(): String {
        return super.detail() + " rgb=${rgb?.showValues()}"
    }
}

// DFTAG_ID 300, DFTAG_LD 307, DFTAG_MD 308, p.123
internal class TagImageDim(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var xdim = 0 // Length of x (horizontal) dimension
    var ydim = 0 // Length of y (vertical) dimension
    var nt_ref: Int = 0 // Reference number for number type information
    var nelems: Int = 0 // Number of elements that make up one entry. Is this only used for LUT ??
    var interlace: Short = 0 // 0 = The components of each pixel are together.
                            // 1 = Color elements are grouped by scan lines.
                            // 2 = Color elements are grouped by planes.
    var compress_type: Short = 0 // Tag which tells the type of compression used and any associated parameters
    var compress_ref: Short = 0 // Reference number of compression tag

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        // For GRreadimage, those parameters are expressed in (x,y) or [column,row] order. p 321
        xdim = h4.raf.readInt(state)
        ydim = h4.raf.readInt(state)
        state.pos += 2 // DFTAG_NT
        nt_ref = h4.raf.readShort(state).toUShort().toInt()
        nelems = h4.raf.readShort(state).toUShort().toInt()
        interlace = h4.raf.readShort(state)
        compress_type = h4.raf.readShort(state)
        compress_ref = h4.raf.readShort(state)
    }

    override fun detail(): String {
        return (super.detail() + " xdim=" + xdim + " ydim=" + ydim + " nelems=" + nelems + " nt_ref=" + nt_ref
                + " interlace=" + interlace + " compress_type=" + compress_type + " compress_ref=" + compress_ref)
    }
}

// DFTAG_LUT 301 p.125
// xdim*ydim*elements*NTsize bytes (xdim, ydim, elements, and NTsize are specified in the corresponding DFTAG_ID)
// LOOK should be DFTAG_LD not DFTAG_ID ?
internal class TagLookupTable(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var table : ArrayTyped<*>? = null

    // not needed - regular data reading will do the right thing.
    fun read(h4 : H4builder, tagID : TagImageDim, tagNT : TagNT) {
        val state = OpenFileState(offset, true)
        val datatype = H4type.getDataType(tagNT.numberType)
        val raw = h4.raf.readByteArray(state, tagID.ydim * tagID.xdim * tagID.nelems)
        val shape = intArrayOf(tagID.ydim, tagID.xdim, tagID.nelems)

        val tba = TypedByteArray(datatype, raw, 0, isBE = true)
        table = tba.convertToArrayTyped(shape)

        /*
        table = when (datatype) {
            Datatype.BYTE -> ArrayByte(shape, raw)
            Datatype.UBYTE, Datatype.CHAR -> ArrayUByte(shape, datatype as Datatype<UByte>, bb)
            Datatype.SHORT -> ArrayShort(shape, bb)
            Datatype.USHORT -> ArrayUShort(shape, bb)
            Datatype.INT -> ArrayInt(shape, bb)
            Datatype.UINT -> ArrayUInt(shape, bb)
            else -> throw RuntimeException("not supporting $datatype for TagRasterImage")
        } */
    }

    override fun detail(): String {
        return buildString {
            append(super.detail())
            append(" nelems=${table?.nelems}")
        }
    }
}

// DFTAG_RI 302 p.124
// xdim*ydim*elements*NTsize bytes (xdim, ydim, elements, and NTsize are specified in the corresponding DFTAG_ID)
// LOOK should be xdim*ydim*NTsize bytes ??
internal class TagRasterImage(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var raster : ArrayTyped<*>? = null

    // not needed - regular data reading will do the right thing.
    fun read(h4 : H4builder, tagID : TagImageDim, tagNT : TagNT) {
        val state = OpenFileState(offset, true)
        val datatype = H4type.getDataType(tagNT.numberType)
        val raw = h4.raf.readByteArray(state, tagID.ydim * tagID.xdim * tagID.nelems)

        val shape = intArrayOf(tagID.ydim, tagID.xdim, tagID.nelems)
        val tba = TypedByteArray(datatype, raw, 0, isBE = true)
        raster = tba.convertToArrayTyped(shape)
    }
}

// DFTAG_RIG (306), DFTAG_NDG (720) lists of other tags
internal class TagDataGroup(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    val nelems = length / 4
    var elem_code = IntArray(0)
    var elem_ref = IntArray(0)

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        elem_code = IntArray(nelems)
        elem_ref = IntArray(nelems)
        for (i in 0 until nelems) {
            elem_code[i] = h4.raf.readShort(state).toUShort().toInt()
            elem_ref[i] = h4.raf.readShort(state).toUShort().toInt()
        }
    }

    override fun detail(): String {
        return buildString {
            append(super.detail())
            append(" nelems= $nelems")
            append(" elems=")
            for (i in 0 until nelems) {
                append(refCode(elem_ref[i], elem_code[i]))
                append(",")
            }
        }
    }
}

// Scientific data dimension record 701 p.133
internal class TagSDD(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var rank: Short = 0
    var shape = IntArray(0)
    var data_nt_ref: Int = 0 // Reference number of DFTAG_NT for data
    var scale_nt_ref = ShortArray(0) // Reference number for DFTAG_NT for the scale for the nth dimension
    // LOOK maybe means DFTAG_SDS ??

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        rank = h4.raf.readShort(state)
        shape = IntArray(rank.toInt()) { h4.raf.readInt(state) }
        h4.raf.readShort(state)
        data_nt_ref = h4.raf.readShort(state).toUShort().toInt()
        scale_nt_ref = ShortArray(rank.toInt()) {
            h4.raf.readShort(state)
            h4.raf.readShort(state)
        }
    }

    override fun toString(): String {
        return buildString {
            append(super.toString())
            append(" shape${shape.contentToString()}")
            append(" data_nt_ref = $data_nt_ref")
            append(" scale_nt_ref${scale_nt_ref.contentToString()}")
        }
    }
}


/* 701 p.135
class TagSDS(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var rank: Short = 0
    var hasScale = ShortArray(0)
    var scale = ShortArray(0) // Reference number for DFTAG_NT for the scale for the nth dimension

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
    }
} */

// SDL, SDU, SDF (704, 705, 706) p 130
internal class TagTextN(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var texts = mutableListOf<String>()
    var wasRead : Boolean = false

    override fun readTag(h4 : H4builder) { // not Idempotent
        if (wasRead) return
        wasRead = true
        val state = OpenFileState(offset, true)
        val ba = h4.raf.readByteArray(state, length)
        texts.add( makeStringZ(ba, charset = h4.valueCharset))
    }

    override fun toString(): String {
        return "${super.toString()} texts='$texts'"
    }
}

// Scientific data max/min 707, p132
internal class TagSDminmax(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var bb: ByteArray? = null

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        bb = h4.raf.readByteArray(state, length)
    }

    fun getMin(dataType: Datatype<*>): Number {
        return get(dataType, 1)
    }

    fun getMax(dataType: Datatype<*>): Number {
        return get(dataType, 0)
    }

    // dont know the datatype until now ??
    operator fun get(dataType: Datatype<*>?, index: Int): Number {
        val useBB = bb!!
        return when (dataType) {
            Datatype.BYTE -> useBB[index]
            Datatype.SHORT -> convertToShort(useBB, index * 2, true)
            Datatype.INT -> convertToInt(useBB, index * 4, true)
            Datatype.LONG -> convertToLong(useBB, index * 8, true)
            Datatype.FLOAT -> convertToFloat(useBB, index * 4, true)
            Datatype.DOUBLE -> convertToDouble(useBB, index * 8, true)
            else -> Double.NaN
        }
    }

    fun toString(dt : Datatype<*>): String {
        return "${super.toString()} min=${getMin(dt)} max=${getMax(dt)}"
    }
}

// should be
internal class TagSDminmaxShouldBe(icode: Int, refno: Int, offset : Long, length : Int, val datatype: Datatype<*>) : Tag(icode, refno, offset, length) {
    private var minmax: Array<*>? = null

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        minmax = when (datatype) {
            Datatype.BYTE -> h4.raf.readArrayOfByte(state, 2)
            Datatype.SHORT -> h4.raf.readArrayOfShort(state, 2)
            Datatype.INT -> h4.raf.readArrayOfInt(state, 2)
            Datatype.FLOAT -> h4.raf.readArrayOfFloat(state, 2)
            Datatype.DOUBLE -> h4.raf.readArrayOfDouble(state, 2)
            Datatype.LONG -> h4.raf.readArrayOfLong(state, 2)
            else -> null
        }
    }

    fun getMin(dataType: Datatype<*>): Number {
        return get(dataType, 1)
    }

    fun getMax(dataType: Datatype<*>): Number {
        return get(dataType, 0)
    }

    // dont know the datatype until now ?? "Cloud of Messages"
    operator fun get(dataType: Datatype<*>?, index: Int): Number {
        require (minmax != null)
        return when (datatype) {
            Datatype.BYTE -> minmax!![index] as Byte
            Datatype.SHORT -> minmax!![index] as Short
            Datatype.INT -> minmax!![index] as Int
            Datatype.LONG -> minmax!![index] as Long
            Datatype.FLOAT -> minmax!![index] as Float
            Datatype.DOUBLE -> minmax!![index] as Double
            else -> Double.NaN
        }
    }

    fun toString(dt : Datatype<*>): String {
        return "${super.toString()} min=${getMin(dt)} max=${getMax(dt)}"
    }
}


// 732 fill value
internal class TagFV(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var fillValue : Any? = null

    fun readFillValue(h4 : H4builder, datatype : Datatype<*>): Any? {
        val state = OpenFileState(offset, true)
        fillValue = when (datatype) {
            Datatype.BYTE -> h4.raf.readByte(state)
            Datatype.CHAR, Datatype.UBYTE -> h4.raf.readByte(state).toUByte()
            Datatype.SHORT -> h4.raf.readShort(state)
            Datatype.USHORT -> h4.raf.readShort(state).toUShort()
            Datatype.INT -> h4.raf.readInt(state)
            Datatype.UINT -> h4.raf.readInt(state).toUInt()
            Datatype.FLOAT -> h4.raf.readFloat(state)
            Datatype.DOUBLE -> h4.raf.readDouble(state)
            Datatype.LONG -> h4.raf.readLong(state)
            Datatype.ULONG -> h4.raf.readLong(state).toULong()
            else -> null
        }
        return fillValue
    }

    override fun detail(): String {
        return if (fillValue != null) "$fillValue" else "null"
    }
}

// DFTAG_VG 1965 p 140
// Group tags together to create "user definded objects" such as Variables.
// A Vset is identified by a Vgroup, an object that contains information about the members of the Vset.
internal class TagVGroup(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var nelems : Int = 0
    var elem_code = IntArray(0)
    var elem_ref = IntArray(0)

    var extag: Short = 0
    var exref: Short = 0
    var version: Short = 0
    var name: String = "null"
    var className: String = "null"
    var group: Group.Builder? = null

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        nelems = h4.raf.readShort(state).toUShort().toInt()
        elem_code = IntArray(nelems) { h4.raf.readShort(state).toUShort().toInt() }
        elem_ref = IntArray(nelems) { h4.raf.readShort(state).toUShort().toInt() }
        val len = h4.raf.readShort(state).toInt()
        name = h4.raf.readString(state, len)
        val len2 = h4.raf.readShort(state).toInt()
        className = h4.raf.readString(state, len2)
        extag = h4.raf.readShort(state)
        exref = h4.raf.readShort(state)
        version = h4.raf.readShort(state)
    }

    override fun toString(): String {
        return "${super.toString()} name= '${name}'"
    }

    override fun detail(): String {
        return buildString {
            append(super.detail())
            append(" var='${vinfo?.vb?.name}'")
            append(" class='$className'")
            append(" extag=$extag")
            append(" exref=$exref")
            append(" version=$version")
            append(" name='$name'")
            append(" nelems=$nelems")
            append(" elems=")
            for (i in 0 until nelems) {
                append(refCode(elem_ref[i], elem_code[i] ))
                append(",")
            }
        }
    }
}

// DFTAG_VH 1962 Vdata header p 141. Describes a Structure.
internal class TagVH(icode: Int, refno: Int, offset : Long, length : Int) : Tag(icode, refno, offset, length) {
    var interlace: Short = 0 // Constant indicating interlace scheme used
    var nelems = 0 // number of entries in Vdata
    var ivsize = 0 // Size of one Vdata record
    var nfields = 0 // Number of fields per entry in the Vdata: so one dimensional Structure(nelems)
    var fld_type = ShortArray(0) // Constant indicating the data type of the nth field of the Vdata
    var fld_isize = IntArray(0) // Size in bytes of the nth field of the Vdata
    var fld_offset = IntArray(0) // Offset of the nth field within the Vdata
    var fld_nelems = IntArray(0) // aka order:  number of elements in the field
    var fld_name = mutableListOf<String>()
    var name: String = "null"
    var className: String = "null"
    var extag: Short = 0 // Extension tag
    var exref: Short = 0 // Extension reference number
    var version: Short = 0 // Version number of DFTAG_VH information

    override fun readTag(h4 : H4builder) {
        val state = OpenFileState(offset, true)
        interlace = h4.raf.readShort(state)
        nelems = h4.raf.readInt(state)
        ivsize = h4.raf.readShort(state).toUShort().toInt()
        nfields = h4.raf.readShort(state).toUShort().toInt()
        fld_type = ShortArray(nfields) { h4.raf.readShort(state) }
        fld_isize = IntArray(nfields) { h4.raf.readShort(state).toUShort().toInt() }
        fld_offset = IntArray(nfields) { h4.raf.readShort(state).toUShort().toInt() }
        fld_nelems = IntArray(nfields) { h4.raf.readShort(state).toUShort().toInt() } // "Order of the nth field of the Vdata (16-bit integer)"
        repeat (nfields) {
            val slen = h4.raf.readShort(state).toInt()
            fld_name.add(h4.raf.readString(state, slen))
        }
        var len = h4.raf.readShort(state).toInt()
        name = h4.raf.readString(state, len)
        len = h4.raf.readShort(state).toInt()
        className = h4.raf.readString(state, len)
        extag = h4.raf.readShort(state)
        exref = h4.raf.readShort(state)
        version = h4.raf.readShort(state)
    }

    override fun toString(): String {
        return super.toString() + " name= '$name'"
    }

    override fun detail(): String {
        return buildString {
            append(super.detail())
            append(" interlace= ").append(interlace.toInt())
            append(" nvert= ").append(nelems)
            append(" ivsize= ").append(ivsize)
            append(" extag= ").append(extag.toInt())
            append(" exref= ").append(exref.toInt())
            append(" version= ").append(version.toInt())
            if (nfields == 1) {
                append(" field='${fld_name[0]}' type=${fld_type[0]}  isize=${fld_isize[0]}  offset=${fld_offset[0]}  nelems=${fld_nelems[0]}")
            } else {
                append("\n   field     type  isize  offset  nelems\n   ")
                for (i in 0 until nfields) {
                    append(fld_name[i]).append(" ")
                    append(fld_type[i]).append(" ")
                    append(fld_isize[i]).append(" ")
                    append(fld_offset[i]).append(" ")
                    append(fld_nelems[i]).append(" ")
                    append("\n")
                }
            }
        }
    }

    // fld_type fld_name(fld_order), so 1 dimensional of length fld_order
    fun readStructureMembers(): List<StructureMember<*>> {
        val members = mutableListOf<StructureMember<*>>()
        for (fld in 0 until this.nfields) {
            val type = this.fld_type[fld].toInt()
            val fdatatype = H4type.getDataType(type)
            val nelems = this.fld_nelems[fld]
            // val name: String, val datatype : Datatype, val offset: Int, val dims : IntArray
            val m = StructureMember(this.fld_name[fld], fdatatype, this.fld_offset[fld], intArrayOf(nelems), true)
            members.add(m)
        }
        return members
    }
}