@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4Clib

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.OpenFileIF.Companion.nativeByteOrder
import com.sunya.cdm.array.makeStringZ
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.hdf4.*
import com.sunya.netchdf.hdf4.H4builder.Companion.tagid
import com.sunya.netchdf.hdf4.H4builder.Companion.tagidName
import com.sunya.netchdf.hdf4.H4builder.Companion.tagidNameR
import com.sunya.netchdf.mfhdfClib.ffm.mfhdf_h.*
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.lang.foreign.*
import java.lang.foreign.ValueLayout.JAVA_BYTE

private val showLibraryVersion = false
private val debugVGroup = false
private val debugVGroupDetails = false
private val debugVSdata = false
private val debugVSdetails = false
private val debugSD = false
private val debugSDdetails = false
private val debugGR = false
private val debugNestedGroup = false
private val debugAttributeBug = false
private val debugDims = false

private val MAX_NAME = 255L
// The combined width of the fields in a vdata is less than MAX_FIELD_SIZE (or 65535.)
private val MAX_FIELDS_SIZE = MAX_FIELD_SIZE().toLong()

class HCheader(val filename: String) {
    internal val rootBuilder = Group.Builder("")
    val rootGroup: Group
    val read_access_mode: MemorySegment

    internal var sdsStartId = 0
    internal var fileOpenId = 0
    internal var grStartId = 0

    private val completedObjects = mutableSetOf<Int>()
    private val metadata = mutableListOf<Attribute<*>>()
    private val promotedAttributes = mutableListOf<Attribute<*>>()
    private val structMetadata = mutableListOf<String>()

    private val unparentedGroups = mutableMapOf<Int, Group4>() // vg refno, vg group4
    private val parentedGroups = mutableMapOf<Int, Group4>() // vg refno, vg

    init {
        Arena.ofConfined().use { session ->
            this.read_access_mode = session.allocateUtf8String("r")
            val rootBuilder = build(session)
            this.rootGroup = rootBuilder.build(null)
            checkLibraryVersion(session)
        }
    }

    fun checkLibraryVersion(session: Arena) {
        val major_p = session.allocate(C_INT, 0)
        val minor_p = session.allocate(C_INT, 0)
        val release_p = session.allocate(C_INT, 0)
        val info_p: MemorySegment = session.allocate(MAX_NAME) // char string[] ??

        // Hgetlibversion(uint32 *majorv, uint32 *minorv, uint32 *release, char string[])
        Hgetlibversion(major_p, minor_p, release_p, info_p)

        val major = major_p[C_INT, 0]
        val minor = minor_p[C_INT, 0]
        val release = release_p[C_INT, 0]
        val info = info_p.getUtf8String(0)

        if (showLibraryVersion) println("Hgetlibversion major = $major minor = $minor release = $release info = $info")

        val fromHfile = "HDF Version 4.2 Release 17-1, March 8, 2023" // from testclibs/src/main/java/com/sunya/netchdf/mfhdfClib/include/hfile.h
        if (info != fromHfile) {
            logger.warn{"HDF4 library version mismatch.  Expected '$fromHfile' but got '$info'" }
        }
    }

    fun isEos() = (metadata.isNotEmpty())

    @Throws(IOException::class)
    private fun build(session: Arena) : Group.Builder {
        val filenameSeg: MemorySegment = session.allocateUtf8String(filename)

        /* Initialize the SD interface. */
        this.sdsStartId = SDstart(filenameSeg, DFACC_READ())

        /* Initialize the rest of the API. */
        this.fileOpenId = Hopen(filenameSeg, DFACC_READ(), 0)
        checkErr("Vinitialize", Vinitialize(this.fileOpenId))
        this.grStartId = GRstart(this.fileOpenId)

        VgroupIterate("constructNestedGroups") { ref : Int -> constructNestedGroups(session, null, ref) }
        unparentedGroups.forEach { refno, group4 ->  // these are the top level groups - do them and all nested groups first
            rootBuilder.addGroup(group4.gb)
            Vgroup4Read(session, group4, refno)
        }

        // now add all the orphans to the rootGroup
        VgroupIterate("VgroupRead") { ref : Int -> VgroupRead(session, rootBuilder, ref) }
        SDiterate(session, rootBuilder)
        GRiterate(session, rootBuilder)
        VStructureIterate(session, rootBuilder)

        if (metadata.isNotEmpty()) {
            val eos = Group.Builder("EosMetadata")
            rootBuilder.addGroup(eos)
            metadata.forEach { makeVariableFromStringAttribute(eos, it) }
        }
        promotedAttributes.forEach { makeVariableFromStringAttribute(rootBuilder, it) }

        if (structMetadata.isNotEmpty()) {
            val sm = structMetadata.joinToString("")
            ODLparser(rootBuilder, false).applyStructMetadata(sm)
        }

        addFileVersionAttribute(session, rootBuilder)
        addAnnotations(session, rootBuilder)

        rootBuilder.removeEmptyGroups()
        return rootBuilder
    }

    fun close() {
        SDend(this.sdsStartId)
        GRend(this.grStartId)
        Vfinish(this.fileOpenId)
        Hclose(this.fileOpenId)
    }

    fun addFileVersionAttribute(session: Arena, gb: Group.Builder) {
        val major_p = session.allocate(C_INT, 0)
        val minor_p = session.allocate(C_INT, 0)
        val release_p = session.allocate(C_INT, 0)
        val info_p: MemorySegment = session.allocate(MAX_NAME)

        // status = Hgetfileversion(file_id, &major_v, &minor_v, &release, string);
        checkErr("Hgetfileversion", Hgetfileversion(this.fileOpenId, major_p, minor_p, release_p, info_p))

        val major = major_p[C_INT, 0]
        val minor = minor_p[C_INT, 0]
        val release = release_p[C_INT, 0]
        val info = info_p.getUtf8String(0)
        require(info.length < MAX_NAME)

        gb.addAttribute(Attribute.from("HDF4FileVersion", "$major.$minor.$release ($info)"))
    }

    fun addAnnotations(session: Arena, gb: Group.Builder) {
        val an_id = ANstart(this.fileOpenId)

        val n_file_labels_p = session.allocate(C_INT, 0)
        val n_file_descs_p = session.allocate(C_INT, 0)
        val n_data_labels_p = session.allocate(C_INT, 0)
        val n_data_descs_p = session.allocate(C_INT, 0)

        // ANfileinfo (an_id, &n_file_labels, &n_file_descs, &n_data_labels, &n_data_descs)
        checkErr("ANfileinfo", ANfileinfo(an_id, n_file_labels_p, n_file_descs_p, n_data_labels_p, n_data_descs_p))

        val n_file_labels = n_file_labels_p[C_INT, 0]
        val n_file_descs = n_file_descs_p[C_INT, 0]
        val n_data_labels = n_data_labels_p[C_INT, 0]
        val n_data_descs = n_data_descs_p[C_INT, 0]

        repeat(n_data_labels) {index ->
            val ann_id = ANselect (an_id, index, AN_DATA_LABEL())
            val ann_length = ANannlen (ann_id)

            val info_p: MemorySegment = session.allocate(ann_length + 1L)
            checkErr("ANreadann", ANreadann (ann_id, info_p, ann_length+1))
            val info = info_p.getUtf8String(0)
            require(info.length < MAX_NAME)
            gb.addAttribute(Attribute.from("DataLabel.$index", info.trim()))

            checkErr("ANendaccess", ANendaccess (ann_id))
        }

        repeat(n_data_descs) {index ->
            val ann_id = ANselect (an_id, index, AN_DATA_DESC())
            val ann_length = ANannlen (ann_id)

            val info_p: MemorySegment = session.allocate(ann_length + 1L)
            checkErr("ANreadann", ANreadann (ann_id, info_p, ann_length+1))
            val info = info_p.getUtf8String(0)
            gb.addAttribute(Attribute.from("DataDesc.$index", info.trim()))

            checkErr("ANendaccess", ANendaccess (ann_id))
        }

        repeat(n_file_labels) {index ->
            val ann_id = ANselect (an_id, index, AN_FILE_LABEL())
            val ann_length = ANannlen (ann_id)

            val info_p: MemorySegment = session.allocate(ann_length + 1L)
            checkErr("ANreadann", ANreadann (ann_id, info_p, ann_length+1))
            val info = info_p.getUtf8String(0)
            require(info.length < MAX_NAME)
            gb.addAttribute(Attribute.from("FileLabel.$index", info.trim()))

            checkErr("ANendaccess", ANendaccess (ann_id))
        }

        repeat(n_file_descs) {index ->
            val ann_id = ANselect (an_id, index, AN_FILE_DESC())
            val ann_length = ANannlen (ann_id)

            val info_p: MemorySegment = session.allocate(ann_length + 1L)
            checkErr("ANreadann", ANreadann (ann_id, info_p, ann_length+1))
            val info = info_p.getUtf8String(0)
            gb.addAttribute(Attribute.from("FileDesc.$index", info.trim()))

            checkErr("ANendaccess", ANendaccess (ann_id))
        }

        checkErr( "ANend", ANend(an_id))
    }

    ///////////////////////////////////////////////////////////////////////////////////

    // create a Group4 hierarchy, with deferred assignment of parent, allowing groups to be in any order in the file
    private fun constructNestedGroups(session: Arena, parent: Group4?, vgroup_ref: Int) {
        val vgroup_id = Vattach(fileOpenId, vgroup_ref, read_access_mode)
        try {
            val vclass_p: MemorySegment = session.allocate(MAX_NAME)
            checkErr("Vgetclass", Vgetclass(vgroup_id, vclass_p))
            val vclass = vclass_p.getUtf8String(0)
            require(vclass.length < MAX_NAME)

            val nobjects_p = session.allocate(C_INT, 0)
            val name_p: MemorySegment = session.allocate(MAX_NAME)

            checkErr("Vinquire", Vinquire(vgroup_id, nobjects_p, name_p))
            val max_nobjects = nobjects_p[C_INT, 0]
            val groupName = name_p.getUtf8String(0)
            require(groupName.length < MAX_NAME)

            val tag_array_p = session.allocateArray(C_INT as MemoryLayout, max_nobjects.toLong())
            val ref_array_p = session.allocateArray(C_INT as MemoryLayout, max_nobjects.toLong())
            val nobjects = Vgettagrefs(vgroup_id, tag_array_p, ref_array_p, max_nobjects)
            val ref_array = IntArray(nobjects) { ref_array_p.getAtIndex(C_INT, it.toLong()) }
            val tag_array = IntArray(nobjects) { tag_array_p.getAtIndex(C_INT, it.toLong()) }
            if (!isNestedGroup(groupName, vclass, tag_array)) {
                return
            }

            if (parentedGroups[vgroup_ref] != null) {
                if (debugNestedGroup)  println("  already parented $vgroup_ref $groupName")
                return
            }

            val already = unparentedGroups[vgroup_ref]
            if (already != null && parent != null) {
                parent.subgroups[vgroup_ref] = already
                unparentedGroups.remove(vgroup_ref)
                parentedGroups[vgroup_ref] = already
                if (debugNestedGroup) println("  reparent $vgroup_ref ${groupName} to ${parent.name}")
                return
            }
            val group4 = Group4(groupName, parent)
            if (parent == null) {
                unparentedGroups[vgroup_ref] = group4
                if (debugNestedGroup) println("  unparented $vgroup_ref ${groupName}")
            } else {
                parentedGroups[vgroup_ref] = group4
                parent.subgroups[vgroup_ref] = group4
                if (debugNestedGroup) println("  parent $vgroup_ref ${groupName} to ${parent.name}")
            }

            tag_array.forEachIndexed { idx, tag ->
                if (tag == TagEnum.VG.code) {
                    constructNestedGroups(session, group4, ref_array[idx])
                }
            }

        } finally {
            checkErr("Vdetach", Vdetach(vgroup_id))
        }

    }

    ///////////////////////////////////////////////////////////////////////////////
    // The Vgroup API: group related objects. DFTAG_VG.
    // "vgroups can contain any combination of HDF data objects". so that narrows it down.

    // Iterate over Vgroups (1965)
    private fun VgroupIterate(what : String, lamda : (t : Int) -> Unit) {
        if (debugVGroup) println(" VgroupIterate $what")
        var last_ref = -1
        while (true) {
            val vgroup_ref = Vgetid(fileOpenId, last_ref)
            if (vgroup_ref == -1) {
                break
            }
            if (debugNestedGroup) println("VgroupIterate $vgroup_ref")
            lamda.invoke(vgroup_ref)
            last_ref = vgroup_ref
        }
    }

    // read a VGroup wrapped in a Group4, and recurse
    private fun Vgroup4Read(session: Arena, g4: Group4, vgroup_ref : Int) {
        VgroupRead(session, g4.gb, vgroup_ref)

        g4.subgroups.forEach { ref, nested4 ->
            g4.gb.addGroup(nested4.gb)
            Vgroup4Read(session, nested4, ref)
        }
    }

    // read a VGroup message (1965)
    private fun VgroupRead(session: Arena, gb: Group.Builder, vgroup_ref : Int) {
        val tagid = tagid(vgroup_ref, TagEnum.VG.code)
        if (completedObjects.contains(tagid)) {
            if (debugVGroup) println(" VgroupRead skip $vgroup_ref")
            return
        }
        completedObjects.add(tagid)

        val vgroup_id = Vattach(fileOpenId, vgroup_ref, read_access_mode)
        try {
            val vclass_p: MemorySegment = session.allocate(MAX_NAME)
            checkErr("Vgetclass", Vgetclass(vgroup_id, vclass_p))
            val vclass = vclass_p.getUtf8String(0)
            require(vclass.length < MAX_NAME)

            val nobjects_p = session.allocate(C_INT, 0)
            val name_p: MemorySegment = session.allocate(MAX_NAME)

            checkErr("Vinquire", Vinquire(vgroup_id, nobjects_p, name_p))
            val max_nobjects = nobjects_p[C_INT, 0]
            val groupName = name_p.getUtf8String(0)
            require(groupName.length < MAX_NAME)

            val tag_array_p = session.allocateArray(C_INT as MemoryLayout, max_nobjects.toLong())
            val ref_array_p = session.allocateArray(C_INT as MemoryLayout, max_nobjects.toLong())
            val nobjects = Vgettagrefs(vgroup_id, tag_array_p, ref_array_p, max_nobjects)
            val ref_array = IntArray(nobjects) { ref_array_p.getAtIndex(C_INT, it.toLong()) }
            val tag_array = IntArray(nobjects) { tag_array_p.getAtIndex(C_INT, it.toLong()) }

            if (debugVGroup) {
                println("  readVGroup $vgroup_ref vclass '$vclass' name='$groupName' nobjects=$nobjects")
                if (debugVGroupDetails) {
                    repeat(nobjects) {
                        println("    ref=${ref_array[it]} ${TagEnum.byCode(tag_array[it])} ")
                    }
                }
            }

            if (nobjects == 0) {
                return
            }

            if (isDimClass(vclass)) { // ??
                VgroupDim(ref_array, tag_array)
            } else if (vclass == "Var0.0") {
                VgroupVar(session, gb, ref_array, tag_array)
            } else if (vclass == "CDF0.0") {
                // only looking at the attributes on the group
                VgroupCDF(session, gb, vgroup_id)
            } else {
                repeat(ref_array.size) { objIdx ->
                    val tag = tag_array[objIdx]
                    val ref = ref_array[objIdx]
                    val tage = TagEnum.byCode(tag)
                    when (tage) {
                        TagEnum.NDG -> {
                            val sds_index = SDreftoindex(sdsStartId, ref)
                            SDread(session, gb, sds_index)
                        }
                        TagEnum.VH -> {
                            VStructureRead(session, gb, ref, true)
                        }
                        else -> {}
                    }
                }
            }
        } finally {
            checkErr("Vdetach", Vdetach(vgroup_id))
        }
    }

    // Vgroup contains a object identified as a Dimension. LOOK not actually using this
    private fun VgroupDim(ref_array: IntArray, tag_array: IntArray) {
        repeat(ref_array.size) { objIdx ->
            val tag = tag_array[objIdx]
            val ref = ref_array[objIdx]
            val tage = TagEnum.byCode(tag)
            if (tage == TagEnum.VH) {
                // val vh = readVH()
                // break
            }
        }
    }

    // Vgroup contains an object identified as a Variable using the NDG tag.
    private fun VgroupVar(session: Arena, gb: Group.Builder, ref_array: IntArray, tag_array: IntArray) {
        repeat(ref_array.size) { objIdx ->
            val tag = tag_array[objIdx]
            val ref = ref_array[objIdx]
            val tage = TagEnum.byCode(tag)
            if (tage == TagEnum.NDG) {
                val sds_index = SDreftoindex(sdsStartId, ref)
                SDread(session, gb, sds_index)
                // break
            }
        }
    }

    // Vgroup contains an object of CDF class, assume to be container of attributes.
    private fun VgroupCDF(session: Arena, gb: Group.Builder, vgroup_id: Int) {
        val nattrs2 = Vnattrs2(vgroup_id)
        repeat(nattrs2) { idx ->
            // val attr = VgroupReadAttribute(session, vgroup_id, idx)
            val attr2 = VgroupReadAttribute2(session, vgroup_id, idx)
            if (attr2 == null) {
                return
            }
            if (debugVGroupDetails) println("     read attribute ${attr2.name}")
            checkEosOrPromote(attr2, gb, true)
        }
    }

    // read attributes with Vgetattr() - not used
    fun VgroupReadAttribute(session: Arena, vgroup_id: Int, idx: Int): Attribute<*> {
        val name_p: MemorySegment = session.allocate(MAX_NAME)
        val datatype_p = session.allocate(C_INT, 0)
        val count_p = session.allocate(C_INT, 0)
        val size_p = session.allocate(C_INT, 0)
        val flds_p = session.allocate(C_INT, 0)
        val refnum_p = session.allocate(C_SHORT, 0)

        val ret = Vattrinfo(vgroup_id, idx, name_p, datatype_p, count_p, size_p)
        if (ret != 0) {
            println("VgroupReadAttribute ret = $ret bailing out")
            return Attribute.from("what", "the")
        }
        val aname: String = name_p.getUtf8String(0)
        require(aname.length < MAX_NAME)
        val datatype4 = datatype_p[C_INT, 0]
        val datatype = H4type.getDataType(datatype4)
        val nelems = count_p[C_INT, 0]
        val size = size_p[C_INT, 0] // LOOK bug where size returns nelems instead
        if (debugAttributeBug) println("VgroupReadAttribute $aname size = $size nelems = $nelems datatype.size = ${datatype.size}")

        val data_p: MemorySegment = session.allocate(nelems * datatype.size.toLong())
        // checkErr("Vgetattr2", Vgetattr2(vgroup_id, idx, data_p)) // LOOK malloc(): corrupted top size
        checkErr("Vgetattr", Vgetattr(vgroup_id, idx, data_p)) // Vgetattr return -1
        val raw = data_p.toArray(ValueLayout.JAVA_BYTE)!!

        return processAttribute(aname, nelems, datatype, raw, isBE = false)
    }

    // read attributes with Vgetattr2()
    // LOOK a bug where C API has nvalues = fld[0].nelems, instead of vh.count * fld[0].nelems
    // LOOK sometimes a bug where size returns nelems instead
    // workaround by not calling Vgetattr2, but use refno to call VSinquire(). See VStructureReadAsAttribute(), vattr.c in HClib.
    fun VgroupReadAttribute2(session: Arena, vgroup_id: Int, attr_idx: Int): Attribute<*>? {
        val name_p: MemorySegment = session.allocate(MAX_NAME)
        val datatype_p = session.allocate(C_INT, 0)
        val count_p = session.allocate(C_INT, 0)
        val size_p = session.allocate(C_INT, 0)
        val flds_p = session.allocate(C_INT, 0)
        val refnum_p = session.allocate(C_SHORT, 0)

        // intn Vattrinfo2(int32 vgroup_id, intn attr_index, char *attr_name, int32 *data_type, int32 *count, int32 *size, int32 *nfields, uint16 *refnum)
        checkErr("Vattrinfo2", Vattrinfo2(vgroup_id, attr_idx, name_p, datatype_p, count_p, size_p, flds_p, refnum_p))
        val aname: String = name_p.getUtf8String(0) // Name of the attribute
        require(aname.length < MAX_NAME)
        val datatype4 = datatype_p[C_INT, 0] // Data type of the attribute
        val datatype = H4type.getDataType(datatype4)
        val count = count_p[C_INT, 0]    // Number of values in the attribute ? // LOOK a bug where nvalues returns fld[0].nelems instead of vh.count * fld[0].nelems
        val sizeInBytes = size_p[C_INT, 0] // Size, in bytes, of the attribute values // LOOK sometimes a bug where size returns nelems instead
        val nflds = flds_p[C_INT, 0] // Number of fields in the attribute vdata
        val refnum = refnum_p[C_SHORT, 0].toUShort().toInt() // Reference number of the attribute vdata
        if (debugAttributeBug) println("VgroupReadAttribute2 $aname size = $sizeInBytes count = $count datatype.size = ${datatype.size} nflds = $nflds refnum=$refnum")

        // println("  sizeInBytes = $sizeInBytes nvalues = $nvalues total = ${sizeInBytes * nvalues}" )
        // println("  datatype.size = ${datatype.size} nvalues = $nvalues total = ${datatype.size * nvalues}" )

        // only use the VS refno to read the VS "directly"
        return VStructureReadAsAttribute(session, refnum)
    }

    //////////////////////////////////////////////////////////////////////////////

    private fun SDiterate(session: Arena, gb: Group.Builder): Int {
        if (debugSD) println("iterateSDs '${gb.name}'")

        val nvars_p = session.allocate(C_INT, 0)
        val nattrs_p = session.allocate(C_INT, 0)
        checkErr("SDfileinfo", SDfileinfo(sdsStartId, nvars_p, nattrs_p))
        val nvars = nvars_p[C_INT, 0]
        val nattrs = nattrs_p[C_INT, 0] // Number of global attributes in the file

        repeat(nvars) { SDread(session, gb, it) }

        repeat(nattrs) {
            val attr = SDreadAttribute(session, sdsStartId, it)
            checkEosOrPromote(attr, gb, true)
        }

        return nvars
    }

    // LOOK The integration with netCDF has required that a dimension (or coordinate
    //variable) is stored as a data set in the file. Therefore, the value of sds_index
    //may correspond to the coordinate variable instead of the actual data set. Users
    //should use the routine SDiscoordvar to determine whether the given data set is
    //a coordinate variable.

    @Throws(IOException::class)
    private fun SDread(session: Arena, gb: Group.Builder, sdidx: Int) {
        val sd_id = SDselect(sdsStartId, sdidx)
        try {
            val sd_ref = SDidtoref(sd_id)
            val tagid = tagid(sd_ref, TagEnum.VG.code)
            if (completedObjects.contains(tagid)) {
                if (debugSD) println(" SDread skip $sd_ref")
                return
            }
            completedObjects.add(tagid)

            val name_p: MemorySegment = session.allocate(MAX_NAME)
            val rank_p = session.allocate(C_INT, 0)
            val dims_p = session.allocateArray(C_INT as MemoryLayout, 100)
            val datatype_p = session.allocate(C_INT, 0)
            val nattrs_p = session.allocate(C_INT, 0)

            // SDgetinfo(sds_id, sds_name, &rank, dim_sizes, &data_type, &n_attrs)
            // SDgetinfo ( int sdsid,  Addressable name,  Addressable rank,  Addressable dimsizes,  Addressable nt,  Addressable nattr) {
            checkErr("SDgetinfo", SDgetinfo(sd_id, name_p, rank_p, dims_p, datatype_p, nattrs_p))

            val sdName = name_p.getUtf8String(0)
            require(sdName.length < MAX_NAME)
            val rank = rank_p[C_INT, 0]
            val dims = IntArray(rank) { dims_p.getAtIndex(ValueLayout.JAVA_INT, it.toLong()) }
            val datatype4 = datatype_p[C_INT, 0]
            val datatype = H4type.getDataType(datatype4)
            val nattrs = nattrs_p[C_INT, 0]

            if (sdName.startsWith("fakeDim")) {
                return
            }

            // SDgetdatastrs: The predefined attributes are label, unit, format, and coordinate system

            // find available dimensions by using the variables
            val dimList = mutableListOf<String>()
            for (dimidx in 0 until rank) {
                val dimid = SDgetdimid(sd_id, dimidx)
                val dimname_p: MemorySegment = session.allocate(MAX_NAME)
                val dimLength_p = session.allocate(C_INT, 0)
                val dimtype_p = session.allocate(C_INT, 0)
                val dimnattrs_p = session.allocate(C_INT, 0)

                checkErr("SDdiminfo", SDdiminfo(dimid, dimname_p, dimLength_p, dimtype_p, dimnattrs_p))
                if (debugDims) println("   dimid=$dimid == ${tagidName(dimid)} reverse=${tagidNameR(dimid)}")
                val dimName: String = dimname_p.getUtf8String(0)
                require(dimName.length < MAX_NAME)
                var dimLength = dimLength_p[C_INT, 0]
                val datatype = dimtype_p[C_INT, 0] // wtf? "coordinates"?
                val nattrs = dimnattrs_p[C_INT, 0] // wtf?
                if (dimLength == 0) {
                    dimLength = dims[dimidx]
                }
                if (dimName.startsWith("fakeDim")) { // LOOK
                    dimList.add(dimLength.toString())
                } else {
                    val useDim = Dimension(dimName, dimLength)
                    gb.addDimensionIfNotExists(useDim)
                    dimList.add(useDim.name) // name has been cleaned up
                }
            }
            if (debugSD) println("  readSD '$sdName' ref=$sd_ref datatype=${datatype} dims=${dims.contentToString()} dimList=$dimList")

            // create the Variable
            val vb = Variable.Builder(sdName, datatype)
            vb.spObject = Vinfo4().setSDSindex(sdidx)
            vb.dimNames = dimList

            // read Variable attributes
            repeat(nattrs) {
                val att = SDreadAttribute(session, sd_id, it) // LOOK
                if (debugSDdetails) println("    att = ${att.name}")
                vb.addAttribute(att)
            }
            gb.addVariable(vb)

        } finally {
            SDendaccess(sd_id)
        }
    }

    private fun SDreadAttribute(session: Arena, sd_id: Int, idx: Int): Attribute<*> {
        val name_p: MemorySegment = session.allocate(MAX_NAME)
        val datatype_p = session.allocate(C_INT, 0)
        val count_p = session.allocate(C_INT, 0)

        //     public static int SDattrinfo ( int id,  int idx,  Addressable name,  Addressable nt,  Addressable count) {
        checkErr("SDattrinfo", SDattrinfo(sd_id, idx, name_p, datatype_p, count_p))
        val aname: String = name_p.getUtf8String(0)
        require(aname.length < MAX_NAME)
        val datatype4 = datatype_p[C_INT, 0]
        val nelems = count_p[C_INT, 0]
        val datatype = H4type.getDataType(datatype4)

        val data_p: MemorySegment = session.allocate(nelems * datatype.size.toLong())
        checkErr("SDreadattr", SDreadattr(sd_id, idx, data_p)) // MEMORY
        val raw = data_p.toArray(ValueLayout.JAVA_BYTE)!!

        return processAttribute(aname, nelems, datatype, raw, isBE = false)
    }

    //////////////////////////////////////////////////////////////////////

    // the GR API
    private fun GRiterate(session: Arena, gb : Group.Builder) {
        val n_datasets_p = session.allocate(C_INT, 0)
        val n_gattrs_p = session.allocate(C_INT, 0)

        // GRfileinfo ( int grid,  Addressable n_datasets,  Addressable n_attrs)
        val ret = GRfileinfo(grStartId, n_datasets_p, n_gattrs_p)
        if (ret != 0) {
            return
        }
        val n_datasets = n_datasets_p[C_INT, 0]
        val n_gattrs = n_gattrs_p[C_INT, 0]

        if (debugGR) println("GRiterator $n_datasets attrs = $n_gattrs")
        repeat(n_datasets) { idx -> GRread(session, gb, idx) }
    }

    private fun GRread(session: Arena, gb : Group.Builder, gridx: Int) {
        val grId = GRselect(grStartId, gridx) // Raster image identifier
        try {
            val name_p: MemorySegment = session.allocate(MAX_NAME)
            val n_comps_p = session.allocate(C_INT, 0) // Number of pixel components in the pixel
            val data_type_p = session.allocate(C_INT, 0) // Pixel data type
            val interlace_p = session.allocate(C_INT, 0) // Interlace mode of the data in the raster image
            val dim_sizes_p: MemorySegment = session.allocateArray(C_INT as MemoryLayout, 2)
            val n_attrs_p = session.allocate(C_INT, 0)

            checkErr("GRgetiminfo", GRgetiminfo(grId, name_p, n_comps_p, data_type_p, interlace_p, dim_sizes_p, n_attrs_p))

            val name = name_p.getUtf8String(0)
            require(name.length < MAX_NAME)
            val n_comps = n_comps_p[C_INT, 0]
            val datatype = H4type.getDataType(data_type_p[C_INT, 0])
            val interlace = interlace_p[C_INT, 0]
            val dims = IntArray(2) { dim_sizes_p.getAtIndex(C_INT, it.toLong()) }
            val nattrs = n_attrs_p[C_INT, 0]

            // create the Variable
           //  val datatype = if (orgDataType == Datatype.CHAR) Datatype.UBYTE else orgDataType
            val vb = Variable.Builder(name, datatype)
            vb.spObject = Vinfo4().setGRindex(gridx)

            // For GRreadimage, those parameters are expressed in (x,y) or [column,row] order. p 321 TODO
            val rdims = IntArray(2) { if (it == 0) dims[1] else dims[0] }
            vb.setDimensionsAnonymous(rdims)
            gb.addVariable(vb)

            // read Variable attributes // look probably need GRattrinfo
            // For GRreadimage, those parameters are expressed in (x,y) or [column,row] order.
            repeat(nattrs) { vb.addAttribute(GRreadAttribute(session, grId, it)) }
            if (debugGR) println("  GRgetiminfo '$name', n_comps=$n_comps, type=${vb.datatype}, interlace=$interlace, dims=${dims.contentToString()}, attrs=$nattrs")

            // get the pallette if it exists
            // intn GRgetnluts(int32 ri_id)
            val npalettes = GRgetnluts(grId) // Gets the identifier of a palette given its index.

            if (npalettes == 1) {
                // int32 GRgetlutid(int32 ri_id, int32 pal_index)
                val palId = GRgetlutid(grId, 0) // Gets the identifier of a palette given its index.

                // intn GRgetlutinfo(int32 pal_id, int32 *ncomp, int32 *data_type, int32 *interlace_mode, int32 *num_entries)
                val nt_p = session.allocate(C_INT, 0) // Number type of the palette
                val nentries_p = session.allocate(C_INT, 0) // Number of color lookup table entries in the palette
                checkErr("GRgetlutinfo", GRgetlutinfo(palId, n_comps_p, nt_p, interlace_p, nentries_p))

                val ncomps = n_comps_p[C_INT, 0]
                val nt = nt_p[C_INT, 0]
                val lutType = H4type.getDataType(nt)
                val pdatatype = if (lutType == Datatype.CHAR) Datatype.UBYTE else lutType // inconsistent C library

                val interlace = interlace_p[C_INT, 0]
                val nentries = nentries_p[C_INT, 0]
                if (debugGR) println("  GRgetlutinfo '$name', ncomps=$ncomps, type=$nt, datatype = $pdatatype interlace=$interlace, nentries=$nentries")

                // intn GRreadlut(int32 pal_id, VOIDP pal_data)
                val pal_data_p = session.allocate((pdatatype.size * ncomps * nentries).toLong())
                checkErr("GRreadlut", GRreadlut(palId, pal_data_p))
                val palData = pal_data_p.toArray(JAVA_BYTE)!!
                val shape = intArrayOf(nentries, ncomps)

                val tba = TypedByteArray(pdatatype, palData, 0, isBE = true)
                val lutData = tba.convertToArrayTyped(shape)
                /*
                val lutData = when (pdatatype) {
                    Datatype.BYTE -> ArrayByte(shape, palData)
                    Datatype.UBYTE, Datatype.CHAR -> ArrayUByte(shape, palData)
                    Datatype.SHORT -> ArrayShort(shape, palData)
                    Datatype.USHORT -> ArrayUShort(shape, palData)
                    Datatype.INT -> ArrayInt(shape, palData)
                    Datatype.UINT -> ArrayUInt(shape, palData)
                    else -> throw RuntimeException("not supporting $pdatatype for GR lookup table")
                } */
                if (debugGR) println("  lutData=${lutData}")

                val lutv_name = "${name}_lookup"
                val lutvb = Variable.Builder(lutv_name, pdatatype)
                lutvb.setDimensionsAnonymous(shape)
                lutvb.spObject = Vinfo4().setValue(lutData)
                gb.addVariable(lutvb)
            }

        } finally {
            GRendaccess(grId)
        }
    }

    private fun GRreadAttribute(session: Arena, gr_id: Int, idx: Int): Attribute<*> {
        val name_p: MemorySegment = session.allocate(MAX_NAME)
        val datatype_p = session.allocate(C_INT, 0)
        val count_p = session.allocate(C_INT, 0)

        //     public static int SDattrinfo ( int id,  int idx,  Addressable name,  Addressable nt,  Addressable count) {
        checkErr("GRattrinfo", GRattrinfo(gr_id, idx, name_p, datatype_p, count_p))
        val aname: String = name_p.getUtf8String(0)
        require(aname.length < MAX_NAME)
        val datatype4 = datatype_p[C_INT, 0]
        val nelems = count_p[C_INT, 0]
        val datatype = H4type.getDataType(datatype4)

        val data_p: MemorySegment = session.allocate(nelems * datatype.size.toLong())
        checkErr("GRgetattr", GRgetattr(gr_id, idx, data_p)) // MEMORY
        val raw = data_p.toArray(ValueLayout.JAVA_BYTE)!!

        return processAttribute(aname, nelems, datatype, raw, isBE = false)
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    // Vdata interface (also called the VS interface or the VS API). Lets call it VStructure
    // A vdata is a collection of records whose values are stored in fixed-length fields.

    // sequentially iterates through an HDF file to obtain the vdata
    private fun VStructureIterate(session: Arena, gb : Group.Builder) {
        if (debugVSdata) println("VStructureIterate group='${gb.name}'")
        var last_ref = -1
        while (true) {
            val vdata_ref = VSgetid(fileOpenId, last_ref)
            if (vdata_ref == -1) {
                break
            }
            VStructureRead(session, gb, vdata_ref, false)
            last_ref = vdata_ref
        }
        if (debugVSdata) println("VStructureIterate DONE")
    }

    private fun VStructureRead(session: Arena, gb : Group.Builder, vs_ref: Int, addAttributesToGroup: Boolean) {
        val tagid = tagid(vs_ref, TagEnum.VG.code)
        if (completedObjects.contains(tagid)) {
            if (debugVSdata) println(" VStructureRead skip $vs_ref")
            return
        }
        completedObjects.add(tagid)

        val vdata_id = VSattach(fileOpenId, vs_ref, read_access_mode)
        try {
            val vclass_p: MemorySegment = session.allocate(MAX_NAME)
            checkErr("VSgetclass", VSgetclass(vdata_id, vclass_p))
            val vclass = vclass_p.getUtf8String(0)
            require(vclass.length < MAX_NAME)
            if (vclass.startsWith("RIATTR0")) { // VHRR
                return
            }

            val n_records_p = session.allocate(C_INT, 0)
            val interlace_p = session.allocate(C_INT, 0)
            val fieldnames_p = session.allocate(MAX_FIELDS_SIZE)
            val recsize_p = session.allocate(C_INT, 0) // size, in bytes, of the vdata record
            val vsname_p: MemorySegment = session.allocate(MAX_NAME)

            // intn VSinquire(int32 vdata_id, int32 *n_records, int32 *interlace_mode, char *field_name_list,
            // int32 *vdata_size, char *vdata_name)
            checkErr("VSinquire", VSinquire(vdata_id, n_records_p, interlace_p, fieldnames_p, recsize_p, vsname_p))
            val nrecords = n_records_p[C_INT, 0]
            val interlace = interlace_p[C_INT, 0]
            val fieldnames = fieldnames_p.getUtf8String(0)
            require(fieldnames.length < MAX_FIELDS_SIZE)

            val recsize = recsize_p[C_INT, 0]
            val vsname = vsname_p.getUtf8String(0)
            require(vsname.length < MAX_NAME)

            if (debugVSdata) {
                println("  VStructureRead '$vsname' ref=$vs_ref class = '$vclass' nrecords=$nrecords fieldnames='$fieldnames' (${fieldnames.length}) recsize=$recsize")
            }

            // bail out if no data
            if (nrecords == 0) {
                return
            }

            val vhname = if (vsname.equals("Ancillary_Data")) vclass else vsname // Lame

            val index_p = session.allocate(C_INT, 0)
            val names = fieldnames.split(",").map { it.trim() }
            val members = mutableListOf<StructureMember<*>>()
            var offset = 0
            for (name in names) {
                // intn VSfindex(int32 vdata_id, char *fieldname, int32 *field_index)
                checkErr("VSfindex", VSfindex(vdata_id, session.allocateUtf8String(name), index_p))
                val idx = index_p[C_INT, 0]
                val type = VFfieldtype(vdata_id, idx)
                val fdatatype = H4type.getDataType(type)
                val esize = VFfieldesize(vdata_id, idx)
                val isize = VFfieldisize(vdata_id, idx) // native machine size of the field.
                val nelems = VFfieldorder(vdata_id, idx) // field "order" ??
                if (debugVSdetails) println("    VSfield name='$name' fdatatype=$fdatatype offset='$offset' nelems=$nelems esize =$esize isize = $isize")

                // class StructureMember<T>(orgName: String, val datatype : Datatype<T>, val offset: Int, val shape : IntArray, val isBE : Boolean) {
                val m = StructureMember(name, fdatatype, offset, intArrayOf(nelems), isBE = nativeByteOrder)
                members.add(m)
                offset += isize
            }

            val vb = if (members.size == 1) {
                val member = members[0]
                /* kludge
                if (member.datatype == Datatype.CHAR && member.shape.size == 1) {
                    val vb0 = Variable.Builder(vsname, Datatype.STRING)
                    println("Change $vsname to String")
                    vb0
                } else { */
                    val vb1 = Variable.Builder(vhname, member.datatype)
                    val totalNelems = nrecords * member.nelems
                    if (totalNelems > 1) {
                        if (nrecords != 1 && member.nelems != 1)
                            vb1.setDimensionsAnonymous(intArrayOf(nrecords, member.nelems))
                        else
                            vb1.setDimensionsAnonymous(intArrayOf(totalNelems))
                    }
                    vb1
                // }
            } else {
                val typedef = CompoundTypedef(vsname, members)
                val vb2 = Variable.Builder(vhname, Datatype.COMPOUND.withTypedef(typedef))
                if (nrecords > 1) {
                    vb2.setDimensionsAnonymous(intArrayOf(nrecords))
                }
                vb2
            }

            val vinfo = Vinfo4()
            vinfo.vsInfo = VSInfo(vs_ref, nrecords, recsize, fieldnames, interlace)
            vb.spObject = vinfo

            if (vclass.startsWith("Attr")) {
                val attr = VStructureMakeAttribute(session, vb.name, vb.datatype!!, vdata_id, vinfo.vsInfo!!)
                if (attr != null) {
                    checkEosOrPromote(attr, gb, addAttributesToGroup)
                }
            } else if (vclass.startsWith("DimVal") || vclass.startsWith("_HDF_CHK_TBL")) {
                // noop
            } else {
                if (vb.datatype!!.cdlName == "compound") { // LOOK why?
                    rootBuilder.addTypedef(vb.datatype!!.typedef!!)
                }
                gb.addVariable(vb)
            }
        } finally {
            checkErr("VSdetach", VSdetach(vdata_id))
        }
    }

    fun VStructureMakeAttribute(session: Arena, aname : String, datatype: Datatype<*>, vdata_id : Int, vsInfo : VSInfo): Attribute<*>? {
        val tagid = tagid(vsInfo.vs_ref, TagEnum.VS.code)
        if (completedObjects.contains(tagid)) {
            if (debugVSdata) println(" VStructureReadAsAttribute skip ${vsInfo.vs_ref}")
            return null
        }
        completedObjects.add(tagid)

        val fldNames: MemorySegment = session.allocateUtf8String(vsInfo.fldNames)
        val nbytes = vsInfo.nrecords * vsInfo.recsize
        val data_p = session.allocate(nbytes.toLong())

        checkErrNeg("VSsetfields", VSsetfields(vdata_id, fldNames))
        checkErrNeg("VSread", VSread(vdata_id, data_p, vsInfo.nrecords, FULL_INTERLACE())) // MEMORY

        val raw = data_p.toArray(ValueLayout.JAVA_BYTE)!!
        return processAttribute(aname, vsInfo.nrecords, datatype, raw, isBE = nativeByteOrder)
    }

    // LOOK structure members can have attributes (!) with fld_idx
    fun VStructureReadAttribute(session: Arena, vdata_id: Int, fld_idx: Int, idx: Int): Attribute<*> {
        val name_p: MemorySegment = session.allocate(MAX_NAME)
        val datatype_p = session.allocate(C_INT, 0)
        val count_p = session.allocate(C_INT, 0)
        val size_p = session.allocate(C_INT, 0)

        checkErr("VSattrinfo", VSattrinfo(vdata_id, fld_idx, idx, name_p, datatype_p, count_p, size_p))
        val aname: String = name_p.getUtf8String(0)
        require(aname.length < MAX_NAME)
        val datatype4 = datatype_p[C_INT, 0]
        val datatype = H4type.getDataType(datatype4)
        val nelems = count_p[C_INT, 0]
        val size = size_p[C_INT, 0]

        val data_p: MemorySegment = session.allocate(nelems * datatype.size.toLong())
        checkErr("VSgetattr", VSgetattr(vdata_id, fld_idx, idx, data_p)) // MEMORY
        val raw = data_p.toArray(ValueLayout.JAVA_BYTE)!!

        return processAttribute(aname, nelems, datatype, raw, isBE = nativeByteOrder)
    }

    // use VSinquire() to get info for an attribute
    // then use VSfindex() and VFfieldtype() to read rest of info
    private fun VStructureReadAsAttribute(session: Arena, vs_ref: Int) : Attribute<*>? {
        val tagid = tagid(vs_ref, TagEnum.VS.code) //  Reference number of the attribute vdata
        if (completedObjects.contains(tagid)) {
            if (debugVSdata) println(" VStructureReadAsAttribute skip $vs_ref")
            return null
        }
        completedObjects.add(tagid)

        val vdata_id = VSattach(fileOpenId, vs_ref, read_access_mode)
        try {
            val vclass_p: MemorySegment = session.allocate(MAX_NAME)
            checkErr("VSgetclass", VSgetclass(vdata_id, vclass_p))
            val vclass = vclass_p.getUtf8String(0)
            require(vclass.length < MAX_NAME)

            val n_records_p = session.allocate(C_INT, 0)
            val interlace_p = session.allocateArray(C_INT as MemoryLayout, 100) // LOOK 100 ??
            val fieldnames_p: MemorySegment = session.allocate(MAX_FIELDS_SIZE)
            val recsize_p = session.allocate(C_INT, 0) // size, in bytes, of the vdata record
            val vsname_p: MemorySegment = session.allocate(MAX_NAME)

            // VSinquire(vdata_id, &n_records, &interlace_mode, fieldname_list, &vdata_size, vdata_name)
            checkErr("VSinquire", VSinquire(vdata_id, n_records_p, interlace_p, fieldnames_p, recsize_p, vsname_p))
            val nrecords = n_records_p[C_INT, 0]
            val interlace = interlace_p[C_INT, 0]
            val fieldnames = fieldnames_p.getUtf8String(0)
            require(fieldnames.length < MAX_FIELDS_SIZE)
            val recsize = recsize_p[C_INT, 0] // this is correct, bypassing the Vattrinfo2 bug
            val vsname = vsname_p.getUtf8String(0)
            require(vsname.length < MAX_NAME)

            if (debugVSdata) {
                println("  VStructureReadAsAttribute '$vsname' ref=$vs_ref class = '$vclass' nrecords=$nrecords fieldnames='$fieldnames' recsize=$recsize")
            }

            // now we use the fieldnames to get the field index, which lets us call the VF*() methods.
            val index_p = session.allocate(C_INT, 0)
            val names = fieldnames.split(",").map { it.trim() }
            val members = mutableListOf<StructureMember<*>>()
            var offset = 0
            for (name in names) {
                checkErr("VSfindex", VSfindex(vdata_id, session.allocateUtf8String(name), index_p))
                val idx = index_p[C_INT, 0]
                val type = VFfieldtype(vdata_id, idx)
                val fdatatype = H4type.getDataType(type)
                val esize = VFfieldesize(vdata_id, idx)
                val isize = VFfieldisize(vdata_id, idx) // native machine size of the field.
                val nelems = VFfieldorder(vdata_id, idx) // misnamed field "order" ??
                if (debugVSdetails) println("    VSfield name='$name' fdatatype=$fdatatype offset='$offset' nelems=$nelems esize =$esize isize = $isize")
                val m = StructureMember(name, fdatatype, offset, intArrayOf(nelems), isBE = nativeByteOrder) // TODO nativeByteOrder??
                members.add(m)
                offset += isize
            }

            val member : StructureMember<*>
            if (members.size == 1) {
                member = members[0]
                val totalNelems = nrecords * member.nelems
            } else {
                // I dunno
                return null
            }

            val vsInfo = VSInfo(vs_ref, nrecords, recsize, fieldnames, interlace)
            return VStructureMakeAttribute(session, vsname, member.datatype, vdata_id, vsInfo)

        } finally {
            checkErr("VSdetach", VSdetach(vdata_id))
        }
    }

    /////////////////////////////////////////////////////////////////////////////////

    fun isNestedGroup(groupName: String, className : String, tag_array: IntArray) : Boolean {
        if (groupName.contains("RIG0"))
            return false
        if (className.startsWith("Var0") || className.startsWith("Att0") || className.startsWith("CDF0")
            || className.startsWith("RI0")
            || className.startsWith("Dim0") || className.startsWith("DimVal0") || className.startsWith("UDim0")) {
            return false
        }

        var isGroup = false
        repeat(tag_array.size) {
            val tagCode = tag_array[it]
            if (tagCode == TagEnum.VG.code || tagCode == TagEnum.VH.code || tagCode == TagEnum.NDG.code) {
                isGroup = true
            }
        }
        return isGroup
    }

    fun checkEosOrPromote(attr : Attribute<*>, gb : Group.Builder, addAttributesToGroup: Boolean) {
        if (EOS.isMetadata(attr.name)) {
            if (metadata.find { it.name == attr.name } == null) {
                metadata.add(attr)
                if (attr.name.startsWith("StructMetadata")) { // LOOK assume its in order
                    this.structMetadata.add(attr.values[0] as String)
                }
            }
            return
        }
        if (!addAttributesToGroup) {
            return
        }
        if (attr.isString && attr.values.size == 1 && (attr.values[0] as String).length > attLengthMaxPromote) {
            if (promotedAttributes.find { it.name == attr.name } == null) {
                promotedAttributes.add(attr)
            }
        } else {
            gb.addAttributeIfNotExists(attr)
        }
    }

    companion object {
        val logger = KotlinLogging.logger("HCheader")
    }
}

///////////////////////////////////////////////////////////////////////////////////////////


private fun processAttribute(name : String, nelems : Int, datatype : Datatype<*>, ba : ByteArray, isBE: Boolean) : Attribute<*> {
    // problem, eg see testHdf4Attribute(). datatype float has recsize 716, and nrecords = 1. should have 716/4 = 29
    // currently
    //         :_BLKSOM:RedBand = 0.0f ;
    // should be
    //       :_BLKSOM:RedBand = 0.0f, 64.0f, 0.0f, 64.0f, 0.0f, 0.0f, 0.0f, 64.0f, 0.0f, 0.0f, 0.0f, 0.0f, 64.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -64.0f, 0.0f, 0.0f, 0.0f, -64.0f, 0.0f, 0.0f, -64.0f, 0.0f, 0.0f, -64.0f, 0.0f, -64.0f, 0.0f, -64.0f, 0.0f, -64.0f, -64.0f, 0.0f, -64.0f, 0.0f, -64.0f, -64.0f, 0.0f, -64.0f, -64.0f, -64.0f, 0.0f, -64.0f, -64.0f, -64.0f, -64.0f, 0.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -128.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -128.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, 0.0f, -64.0f, -64.0f, -64.0f, -64.0f, -64.0f, 0.0f, -64.0f, -64.0f, -64.0f, 0.0f, -64.0f, -64.0f, 0.0f, -64.0f, 0.0f, -64.0f, -64.0f, 0.0f, -64.0f, 0.0f, -64.0f, 0.0f, 0.0f, -64.0f, 0.0f, -64.0f, 0.0f, 0.0f, -64.0f, 0.0f, 0.0f, 0.0f, 0.0f, -64.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 64.0f, 0.0f, 0.0f, 64.0f, 0.0f, 0.0f, 64.0f, 0.0f ;
    var useNelems = if (datatype == Datatype.CHAR) nelems else (ba.size / datatype.size)
    if (nelems != useNelems) {
        println("processAttribute $name: nelems = $nelems calcNelems = $useNelems")
    }
    val shape = intArrayOf(useNelems)

    if (datatype == Datatype.CHAR) {
        val svalue = makeStringZ(ba, charset = Hdf4ClibFile.valueCharset)
        return Attribute.from(name, svalue)
    }

    val tba = TypedByteArray(datatype, ba, 0, isBE = isBE)
    val values = tba.convertToArrayTyped(shape)
    return Attribute.Builder(name, datatype).setValues(values.toList()).build()
}

private fun makeVariableFromStringAttribute(group : Group.Builder, att : Attribute<*>) {
    require(att.isString)
    val svalue = att.values[0] as String
    // create the Variable
    val vb = Variable.Builder(att.name, Datatype.STRING)
    vb.spObject = Vinfo4().setSValue(svalue)
    group.addVariable(vb)
}

fun checkErr (where : String, ret: Int) {
    if (ret != 0) {
        throw IOException("$where return $ret")
    }
}

fun checkErrNeg (where : String, ret: Int) {
    if (ret < 0) {
        throw IOException("$where return $ret")
    }
}

data class Group4(val name : String, val parent: Group4?) {
    val subgroups = mutableMapOf<Int, Group4>()
    var gb = Group.Builder(name)
}

// info needed to read data with VSread()
data class VSInfo(val vs_ref : Int, val nrecords : Int, val recsize : Int, val fldNames : String, val interlace : Int)

class Vinfo4() {
    var value : ArrayTyped<*>? = null
    var sdsIndex : Int? = null
    var grIndex : Int? = null
    var vsInfo : VSInfo? = null

    fun setSDSindex(sdsIndex : Int) : Vinfo4 {
        this.sdsIndex = sdsIndex
        return this
    }

    fun setGRindex(grIndex : Int) : Vinfo4 {
        this.grIndex = grIndex
        return this
    }

    fun setSValue(svalue : String) : Vinfo4 {
        this.value = ArrayString(intArrayOf(), listOf(svalue))
        return this
    }

    fun setValue(value : ArrayTyped<*>) : Vinfo4 {
        this.value = value
        return this
    }
}

