@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.*
import com.sunya.cdm.iosp.*
import com.sunya.netchdf.hdf4.ODLparser
import com.sunya.netchdf.NetchdfFileFormat

import com.fleeksoft.charset.Charset
import com.fleeksoft.charset.Charsets
import com.sunya.cdm.array.makeString
import com.sunya.cdm.util.InternalLibraryApi
import io.github.oshai.kotlinlogging.KotlinLogging

private const val debugStart = false
private const val debugSuperblock = false
internal const val debugTypedefs = false
internal const val debugFlow = false

/**
 * Build the rootGroup for an HDF5 file.
 * @param strict  true = make it agree with nclib if possible
 * @param valueCharset used when reading HDF5 header. LOOK need example to test
 *
 * @see "https://support.hdfgroup.org/HDF5/doc/Specs.html"
 */
@InternalLibraryApi
class H5builder(
    rafOrg: OpenFileIF,
    val strict: Boolean,
    val valueCharset: Charset = Charsets.UTF8,
) {
    val raf: OpenFileIF

    val superblockStart: Long // may be offset for arbitrary metadata
    var sizeOffsets: Int = 0
    var sizeLengths: Int = 0
    var sizeHeapId = 0
    var isOffsetLong = false
    var isLengthLong = false

    var isNetcdf4 = false

    internal val hashGroups = mutableMapOf<Long, H5GroupBuilder>() // key =  btreeAddress
    internal val symlinkMap = mutableMapOf<String, DataObjectFacade>()
    internal val dataObjectMap = mutableMapOf<Long, DataObject>() // key = DataObject address
    val structMetadata = mutableListOf<String>()
    val datasetMap = mutableMapOf<Long, Pair<Group.Builder, Variable.Builder<*>>>()
    val typedefManager: TypedefManager

    val cdmRoot : Group
    fun formatType() : String {
        return if (isNetcdf4) "netcdf4" else {
            if (structMetadata.isEmpty()) "hdf5" else "hdf-eos5"
        }
    }

    fun getDataObjectMap() = dataObjectMap

    init {
         // search for the superblock
        val state = OpenFileState(0L, false)
        var start = 0L
        while (start < NetchdfFileFormat.MAXHEADERPOS) {
            state.pos = start
            val testForMagic = rafOrg.readByteArray(state, 8)
            if (testForMagic.contentEquals(magicHeader)) {
                break
            }
            start = if (start == 0L) 512 else 2 * start
        }
        if (start > NetchdfFileFormat.MAXHEADERPOS) {
            throw RuntimeException("Not an HDF5 file")
        }
        this.superblockStart = start
        if (debugStart) {
            println("H5builder opened file ${rafOrg.location()} at pos $superblockStart")
        }

        // buffer when reading in metadata. this probably doesnt work because header is not contiguous
        raf = rafOrg // if (useOkio) OpenFileBuffered(rafOrg as com.sunya.cdm.okio.OpenFile, start) else rafOrg

        val superBlockVersion = raf.readByte(state).toInt()
        val rootGroupBuilder = when {
            superBlockVersion < 2 -> { // 0 and 1
                readSuperBlock01(superblockStart, state, superBlockVersion)
            }
            superBlockVersion < 4 -> { // 2 and 3
                readSuperBlock23(superblockStart, state, superBlockVersion)
            }
            else -> {
                throw RuntimeException("Unknown superblock version= $superBlockVersion")
            }
        }

        // now look for symbolic links TODO does this work??
        replaceSymbolicLinks(rootGroupBuilder)

        // build tree of H5groups
        val h5rootGroup = rootGroupBuilder.build()

        typedefManager = TypedefManager(this)

        // convert into CDM
        val rootBuilder = this.buildCdm(h5rootGroup)

        // add Typedefs To Groups, pushing up to common parent
        typedefManager.typedefGroups.forEach { (typedef, groupList) ->
            var topgroup = groupList[0]
            for (idx in 1 until groupList.size) {
                topgroup = topgroup.commonParent(groupList[idx])
            }
            topgroup.addTypedef(typedef)
        }

        convertReferences(rootBuilder) // TODO

        // hdf-eos5
        if (structMetadata.isNotEmpty()) {
            val sm = structMetadata.joinToString("")
            ODLparser(rootBuilder, false).applyStructMetadata(sm)
        }

        this.cdmRoot =  rootBuilder.build(null)

        // if (useOkio) raf.close()
    }

    private fun readSuperBlock01(superblockStart : Long, state : OpenFileState, version : Int) : H5GroupBuilder {
        // have to read ahead a bit
        state.pos = superblockStart + 13
        this.sizeOffsets = raf.readByte(state).toInt()
        this.sizeLengths = raf.readByte(state).toInt()
        this.isOffsetLong = (sizeOffsets == 8)
        this.isLengthLong = (sizeLengths == 8)
        this.sizeHeapId = 8 + sizeOffsets
        state.pos = superblockStart

      val superblock01 =
          structdsl("superblock01", raf, state) {
            fld("format", 8)
            fld("version", 1)
            fld("versionFSS", 1)
            fld("versionGST", 1)
            skip(1)
            fld("versionSHMF", 1)
            fld("sizeOffsets", 1)
            fld("sizeLengths", 1)
            skip(1)
            fld("groupLeafNodeSize", 2)
            fld("groupInternalNodeSize", 2)
            fld("flags", 4)
            if (version == 1) {
                fld("storageInternalNodeSize", 2)
                skip(2)
            }
            fld("baseAddress") { sizeOffsets }
            fld("heapAddress") { sizeOffsets }
            fld("eofAddress") { sizeOffsets }
            fld("driverAddress") { sizeOffsets }
        }
        if (debugSuperblock) superblock01.show()

        // look for file truncation
        val baseAddress = superblock01.getLong("baseAddress")
        var eofAddress = superblock01.getLong("eofAddress")
        if (baseAddress != this.superblockStart) {
            eofAddress += superblockStart
        }
        if (raf.size() < eofAddress) throw RuntimeException(
            "File is truncated should be= $eofAddress actual ${raf.size()} baseAddress= $baseAddress superblockStart= $superblockStart")

        if (debugFlow) {
            println("superBlockVersion $version sizeOffsets = $sizeOffsets sizeLengths = $sizeLengths")
        }

        // extract the root group object, recursively read all objects
        val rootSymbolTableEntry = this.readSymbolTable(state)
        val rootObject = this.getDataObject(rootSymbolTableEntry.objectHeaderAddress, "root")!!

        return this.readH5Group(DataObjectFacade(null, "").setDataObject(rootObject))!!
    }

    @OptIn(ExperimentalStdlibApi::class)
    private fun readSuperBlock23(superblockStart: Long, state : OpenFileState, version: Int) : H5GroupBuilder {
        if (debugStart) {
            println("readSuperBlock version = $version")
        }
        sizeOffsets = raf.readByte(state).toInt()
        isOffsetLong = (sizeOffsets == 8)
        sizeLengths = raf.readByte(state).toInt()
        isLengthLong = (sizeLengths == 8)
        if (debugStart) {
            println(" sizeOffsets= $sizeOffsets sizeLengths= $sizeLengths")
            println(" isLengthLong= $isLengthLong isOffsetLong= $isOffsetLong")
        }
        val fileFlags: Byte = raf.readByte(state)
        if (debugStart) {
            println(" fileFlags= 0x${fileFlags.toInt().toHexString()}")
        }
        val baseAddress = readOffset(state)
        val extensionAddress = readOffset(state)
        var eofAddress = readOffset(state)
        val rootObjectAddress = readOffset(state)
        val checksum: Int = raf.readInt(state)
        if (debugStart) {
            println(" superblockStart= 0x${this.superblockStart.toHexString()}")
            println(" extensionAddress= 0x${extensionAddress.toHexString()}")
            println(" eof Address=$eofAddress")
            println(" raf length= ${raf.size()}")
            println(" rootObjectAddress= 0x${rootObjectAddress.toHexString()}")
            println("")
        }

        // look for file truncation
        if (baseAddress != this.superblockStart) {
            eofAddress += superblockStart
        }
        if (raf.size() < eofAddress) throw RuntimeException(
            "File is truncated should be= $eofAddress actual ${raf.size()} baseAddress= $baseAddress superblockStart= $superblockStart")

        if (debugFlow) {
            println("superBlockVersion $version sizeOffsets = $sizeOffsets sizeLengths = $sizeLengths")
        }

        val rootObject = this.getDataObject(rootObjectAddress, "root")!!
        val facade = DataObjectFacade(null, "").setDataObject( rootObject)
        return this.readH5Group(facade)!!
    }

    //////////////////////////////////////////////////////////////
    // Internal organization of Data Objects

    fun convertReferenceToDataObjectName(reference: Long): String {
        val name = getDataObjectName(reference)
        return name ?: reference.toString() // LOOK
    }

    fun convertReferencesToDataObjectName(refArray: Array<Long>): List<String> {
        return refArray.map { convertReferenceToDataObjectName(it) }
    }

    /**
     * Get a data object's name, using the objectId you get from a reference (aka hard link).
     *
     * @param objId address of the data object
     * @return String the data object's name, or null if not found
     * @throws IOException on read error
     */
    fun getDataObjectName(objId: Long): String {
        return getDataObject(objId, null)?.name ?: "unknown"
    }

    /**
     * Get a data object's name, using the objectId you get from a reference (aka hard link).
     *
     * @param objId address of the data object
     * @return String the data object's name, or null if not found
     * @throws IOException on read error
     *
    @Throws(IOException::class)
    fun getDataObjectName(objId: Long): String {
        return getDataObject(objId, null)?.name ?: "unknown"
    } */

    /**
     * All access to data objects come through here, so we can cache.
     * Look in cache first; read if not in cache.
     * I think this is just for shared objects.
     *
     * @param address object address (aka id)
     * @param name optional name, sometimes isnt known until later, so leave null. Applicable eg for an Attribute
     *   referencing a typedef before the typedef is found through a hardlink, which supplies the name. Because
     *   the DataObject doesnt know its name. Because its name is free to be something else. Cause thats how we roll.
     */
    internal fun getDataObject(address: Long, name: String?): DataObject? {
        // find it
        var dobj = dataObjectMap[address]
        if (dobj != null) {
            if (dobj.name == null && name != null) {
                dobj.name = name
                if (debugFlow) {
                    println("named object@$address as $name")
                }
            }
            return dobj
        }

        // read and cache
        dobj = this.readDataObject(address, name)
        if (dobj != null) {
            dataObjectMap[address] = dobj
        }
        return dobj
    }

    //////////////////////////////////////////////////////////////
    // utilities

    fun getFileOffset(address: Long): Long {
        return this.superblockStart + address
    }

    fun readLength(state : OpenFileState): Long {
        return if (isLengthLong) raf.readLong(state) else raf.readInt(state).toLong()
    }

    fun readOffset(state : OpenFileState): Long {
        return if (isOffsetLong) raf.readLong(state) else raf.readInt(state).toLong()
    }

    // size of data depends on "maximum possible number"
    fun readVariableSizeMax(state : OpenFileState, maxNumber: Long): Long {
        val size: Int = this.getNumBytesFromMax(maxNumber)
        return this.readVariableSizeUnsigned(state, size)
    }

    // always skip 8 bytes
    fun readVariableSizeFactor(state : OpenFileState, sizeFactor: Int): Long {
        val size = variableSizeFactor (sizeFactor)
        return readVariableSizeUnsigned(state, size)
    }

    fun variableSizeFactor(sizeFactor: Int): Int {
        return when (sizeFactor) {
            0 -> 1
            1 -> 2
            2 -> 4
            3 -> 8
            else -> throw RuntimeException("Illegal SizFactor $sizeFactor")
        }
    }

    fun readVariableSizeUnsigned(state : OpenFileState, size: Int): Long {
        val vv: Long
        when (size) {
            1 -> vv = raf.readByte(state).toUByte().toLong()
            2 -> vv = raf.readShort(state).toUShort().toLong()
            4 -> vv = raf.readInt(state).toUInt().toLong()
            8 -> vv = raf.readLong(state)
            else -> vv = readVariableSizeN(state, size)
        }
        return vv
    }

    fun readVariableSizeDimension(state : OpenFileState, size: Byte): Int {
        val vv: Int
        val sizeInt = size.toInt()
        when (sizeInt) {
            1 -> vv = raf.readByte(state).toUByte().toInt()
            2 -> vv = raf.readShort(state).toUShort().toInt()
            4 -> vv = raf.readInt(state).toUInt().toInt()
            else -> {
                val vs = readVariableSizeN(state, sizeInt)
                vv = vs.toInt()
            }
        }
        return vv
    }

    private fun readVariableSizeN(state : OpenFileState, nbytes : Int): Long {
        val ch = IntArray(nbytes)
        for (i in 0 until nbytes) ch[i] = raf.readByte(state).toInt()
        var result = ch[nbytes - 1].toLong()
        for (i in nbytes - 2 downTo 0) {
            result = result shl 8
            result += ch[i].toLong()
        }
        return result
    }

    fun readAddress(state : OpenFileState): Long {
        return getFileOffset(readOffset(state))
    }

    // size of data depends on "maximum possible number"
    fun getNumBytesFromMax(maxNumber: Long): Int {
        var maxn = maxNumber
        var size = 0
        while (maxn != 0L) {
            size++
            maxn = maxn ushr 8 // right shift with zero extension
        }
        return size
    }

    ////////////////////////////////////////////////////////////////////////////////
    fun convertReferences(gb : Group.Builder) {
        val refAtts = gb.attributes.filter{ it.datatype == Datatype.REFERENCE }
        refAtts.forEach { att ->
            val convertAtt = convertReferenceAttribute(att)
            if (convertAtt != null) {
                gb.addAttribute(convertAtt)
            }
            gb.attributes.remove(att)
        }

        gb.variables.forEach{ vb ->
            val refAtts = vb.attributes.filter{ it.datatype == Datatype.REFERENCE}
            refAtts.forEach { att ->
                val convertAtt = convertReferenceAttribute(att)
                if (convertAtt != null) {
                    if (att.name == HDF5_DIMENSION_LIST) {
                        vb.dimNames = convertAtt.values as List<String>
                    } else {
                        vb.addAttribute(convertAtt)
                    }
                }
                vb.attributes.remove(att)
            }
        }
        gb.groups.forEach{ convertReferences(it) }
    }

    fun convertReferenceAttribute(att : Attribute<*>) : Attribute<*>? {
        val svalues = mutableListOf<String>()
        att.values.forEach {
            val dsetId = it as Long
            val pair = datasetMap[dsetId]
            if (pair == null)  {
                logger.warn {"H5 cant find dataset reference for att $att in file ${raf.location()}"}
                return null
            }
            val (gb, vb) = pair
            val name = vb.fullname(gb)
            svalues.add(name)
        }
        return Attribute(att.name, Datatype.STRING, svalues)
    }

    fun openFileExtended(): OpenFileExtended {
        val raf: OpenFileIF = OkioFile(this.raf.location())
        return OpenFileExtended(raf, this.isLengthLong, this.isOffsetLong, this.superblockStart)
    }

    companion object {
        // special attribute names in HDF5
        const val HDF5_CLASS = "CLASS"
        const val HDF5_DIMENSION_LIST = "DIMENSION_LIST"
        const val HDF5_DIMENSION_SCALE = "DIMENSION_SCALE"
        const val HDF5_DIMENSION_LABELS = "DIMENSION_LABELS"
        const val HDF5_DIMENSION_NAME = "NAME"
        const val HDF5_REFERENCE_LIST = "REFERENCE_LIST"

        val HDF5_SPECIAL_ATTS = listOf<String>()
        val HDF5_SKIP_ATTS = listOf(HDF5_DIMENSION_LABELS, HDF5_REFERENCE_LIST)
        val HDF5_IGNORE_ATTS = listOf(HDF5_CLASS, HDF5_DIMENSION_LIST, HDF5_DIMENSION_SCALE, HDF5_DIMENSION_LABELS, HDF5_REFERENCE_LIST, HDF5_DIMENSION_NAME)

        private val magicHeader = byteArrayOf(
            0x89.toByte(),
            'H'.code.toByte(),
            'D'.code.toByte(),
            'F'.code.toByte(),
            '\r'.code.toByte(),
            '\n'.code.toByte(),
            0x1a,
            '\n'.code.toByte()
        )

        private val magicString = makeString(magicHeader)

        private const val transformReference = true

        ////////////////////////////////////////////////////////////////////////////////
          /*
           * Implementation notes
           * any field called address is actually relative to the base address.
           * any field called filePos or dataPos is a byte offset within the file.
           *
           * it appears theres no sure fire way to tell if the file was written by netcdf4 library
           * 1) if one of the the NETCF4-XXX atts are set
           * 2) dimension scales:
           * 1) all dimensions have a dimension scale
           * 2) they all have the same length as the dimension
           * 3) all variables' dimensions have a dimension scale
           */
        private const val KNOWN_FILTERS = 3

        val logger = KotlinLogging.logger("H5builder")
    }
}