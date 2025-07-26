@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4

import com.fleeksoft.charset.Charset
import com.fleeksoft.charset.Charsets
import com.fleeksoft.charset.decodeToString

import com.sunya.cdm.api.*
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.util.Indent
import com.sunya.netchdf.netcdf4.NUG
import com.sunya.cdm.array.convertToBytes
import com.sunya.cdm.util.InternalLibraryApi

import io.github.oshai.kotlinlogging.KotlinLogging


const val attLengthMaxPromote = 4000

/**
 * @see "https://support.hdfgroup.org/release4/doc/index.html"
 */
/* Implementation Notes
   1. Early version seem to use the refno as a grouping mechanism. Perhaps before Vgroup existed??
 */
@InternalLibraryApi
class H4builder(val raf: OpenFileIF, val valueCharset: Charset) {
    private val alltags = mutableListOf<Tag>() // in order as they appear in the file

    var rootBuilder: Group.Builder = Group.Builder("")
    val metadata = mutableListOf<Attribute<*>>()
    val promotedAttributes = mutableListOf<Attribute<*>>()
    val structMetadata = mutableListOf<String>()

    private val unparentedGroups = mutableMapOf<Int, Group4>() // vg refno, vg group4
    private val parentedGroups = mutableMapOf<Int, TagVGroup>() // vg refno, vg

    internal val grVGaliasMap = mutableMapOf<Int, TagVGroup>() // ri refno, VG
    internal val grRIGaliasMap = mutableMapOf<Int, TagDataGroup>() // ri refno, RIG

    internal val sdAliasMap = mutableMapOf<Int, TagVGroup>() // sd refno, sd parent group
    internal val vhAliasMap = mutableMapOf<Int, TagVGroup>()    // vh refno, vh group
    internal val tagidMap = mutableMapOf<Int, Tag>()
    private var imageCount = 0

    fun type(): String {
        return if (metadata.isEmpty()) "hdf4" else "hdf-eos2"
    }

    init {
        // header information is in big endian byte order
        val state = OpenFileState(0, true)

        // this positions the file after the header
        if (!isValidFile(raf, state)) {
            throw RuntimeException("Not an HDF4 file ")
        }

        // read the DDH and DD records, and populate the tag list
        var link = state.pos
        while (link > 0) {
            state.pos = link
            val ndd: Int = raf.readShort(state).toUShort().toInt() // number of DD blocks
            link = raf.readInt(state).toUInt().toLong() // point to the next DDH; link == 0 means no more
            var pos = state.pos
            repeat (ndd) {
                val tag: Tag = readTag(raf, state)
                pos += 12
                state.pos = pos // tag usually changed the file pointer
                if (tag.code > 1) alltags.add(tag) // ignore NONE, NULL
            }
        }

        // now read the individual tags' data
        for (tag: Tag in alltags) {
            with(this) { tag.readTag(this) }
            val tagid = tagid(tag.refno, tag.code)
            tagidMap[tagid] = tag // track all tags in a map, key is the "tag id" = code, refno.
            if (debugTag) println(if (debugTagDetail) tag.detail() else tag)
        }

        build()

        showTags(debugTagSummary, debugTagUsed)
    }

    private fun build() {
        // alltags.forEach { findRasterInfo(it) }
        // if (debugGR) checkRasterInfo()

        VgroupIterate("VgroupNesting") { t: Tag -> constructNestedGroups(t as TagVGroup, null, Indent(2)) }
        VgroupIterate("VgroupAliases") { t: Tag -> VgroupAliases(t as TagVGroup, Indent(2)) }
        unparentedGroups.values.forEach {
            val vgroup = it.vgroup
            val group = Group.Builder(vgroup.name)
            rootBuilder.addGroup(group)
            Vgroup4Read(it, group)
        }

        // now add all the orphans to the rootGroup
        VgroupIterate("VgroupRead") { t: Tag -> VgroupRead(t as TagVGroup, rootBuilder) }
        SDiterate(rootBuilder)
        GRiterate(rootBuilder)
        VStructureIterate(rootBuilder)

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

        // the annotations are in reverse order in the C library
        var dataLabel = 0
        var dataDesc = 0
        var fileLabel = 0
        var fileDesc = 0
        alltags.reversed().forEach {
            when (it.code) {
                TagEnum.VERSION.code -> {
                    rootBuilder.addAttribute(Attribute.from("HDF4FileVersion", (it as TagVersion).value()))
                    it.isUsed = true
                }

                TagEnum.DIL.code -> {
                    rootBuilder.addAttribute(Attribute.from("DataLabel.${dataLabel++}", (it as TagAnnotate).text))
                    it.isUsed = true
                }

                TagEnum.DIA.code -> {
                    rootBuilder.addAttribute(Attribute.from("DataDesc.${dataDesc++}", (it as TagAnnotate).text))
                    it.isUsed = true
                }

                TagEnum.FID.code -> {
                    rootBuilder.addAttribute(Attribute.from("FileLabel.${fileLabel++}", (it as TagText).text))
                    it.isUsed = true
                }

                TagEnum.FD.code -> {
                    rootBuilder.addAttribute(Attribute.from("FileDesc.${fileDesc++}", (it as TagText).text))
                    it.isUsed = true
                }
            }
        }
        rootBuilder.removeEmptyGroups()
    }

    fun showTags(showSummary: Boolean, showUnused: Boolean, showAll: Boolean = false): Int {
        if (showSummary) {
            val summ = mutableMapOf<Int, MutableList<Int>>()
            alltags.forEach {
                val tags = summ.getOrPut(it.code) { mutableListOf() }
                tags.add(it.refno)
            }
            println("alltags = ${alltags.size} unique = ${summ.size} ")
            val summSorted = summ.entries.sortedBy { it.key}
            summSorted.forEach { (key, list) ->
                val tagEnum = TagEnum.byCode(key)
                println(" $tagEnum (${list.size} tags)")
                // if (list.size < 100) println("  $list")
            }
        }

        if (showUnused) {
            alltags.filterIsInstance<TagData>().forEach { data ->
                data.markDataTags(this)
            }
        }

        val unused = alltags.count { !it.isUsed }
        if (showUnused) {
            println("unused tags $unused")
            alltags.filter { !it.isUsed }.forEach {
                println(it.detail())
            }
        }

        if (showAll) {
            println("all tags ${alltags.size} sort by refno/code")
            alltags.sortedWith { t1, t2 ->
                when {
                    t1.refno > t2.refno -> 1
                    t1.refno < t2.refno -> -1
                    else -> t1.refno - t2.refno
                }
            }.forEach {
                println(it.detail())
            }
        }
        return unused
    }

    internal fun TagVGroup.nestedTags(): List<Tag> {
        val result = mutableListOf<Tag>()
        elem_code.forEachIndexed { idx, code ->
            val tag = tagidMap[tagid(elem_ref[idx], code)]
            if (tag != null) {
                result.add(tag)
            } else if (code != TagEnum.IGNORE.code && debugTagUsed) {
                println("vGroup ${refCode()} missing tag ${Tag.refCode(elem_ref[idx], code)} ")
            }
        }
        return result
    }

    internal fun TagDataGroup.nestedTags(): List<Tag> {
        val result = mutableListOf<Tag>()
        elem_code.forEachIndexed { idx, code ->
            val tag = tagidMap[tagid(elem_ref[idx], code)]
            if (tag != null) {
                result.add(tag)
            } else if (code != TagEnum.IGNORE.code && debugTagUsed) {
                println("dataGroup ${refCode()} missing tag ${Tag.refCode(elem_ref[idx], code)} ")
            }
        }
        return result
    }

    private fun VgroupIterate(name: String, lamda: (t: Tag) -> Unit) {
        if (debugConstruct) println("--VgroupIterate $name")
        for (t: Tag in alltags) {
            if (t.tagEnum() == TagEnum.VG) {
                lamda.invoke(t)
            }
        }
    }

    // create a Group4 hierarchy, with deferred assignment of parent, allowing groups to be in any order in the file
    private fun constructNestedGroups(vgroup: TagVGroup, parent: Group4?, indent: Indent) {
        if (!isNestedGroup(vgroup)) {
            return
        }
        if (debugVGroup) println("$indent VgroupNesting ${vgroup.refno} '${vgroup.name}' ${vgroup.className} ")
        if (parentedGroups[vgroup.tagid()] != null) {
            if (debugVGroup) println("$indent   already parented ${vgroup.name}")
            return
        }
        val already = unparentedGroups[vgroup.tagid()]
        if (already != null && parent != null) {
            parent.subgroups.add(already)
            unparentedGroups.remove(vgroup.tagid())
            parentedGroups[vgroup.tagid()] = vgroup
            if (debugVGroup) println("$indent   add ${vgroup.name} to ${parent.vgroup.name}")
            return
        }
        val group4 = Group4(vgroup)
        if (parent == null) {
            unparentedGroups[vgroup.tagid()] = group4
            if (debugVGroup) println("$indent   unparented ${vgroup.name}")
        } else {
            parentedGroups[vgroup.tagid()] = vgroup
            parent.subgroups.add(group4)
            if (debugVGroup) println("$indent   add ${vgroup.name} to ${parent.vgroup.name}")
        }

        vgroup.nestedTags().forEach { tag ->
            if (tag.tagEnum() == TagEnum.VG) {
                constructNestedGroups(tag as TagVGroup, group4, indent.incr())
            }
        }
    }

    // look for VG, RIG for a given RI
    private fun findRasterInfo(tag: Tag) {
        if (tag.code == TagEnum.VG.code) {
            val vgroup = tag as TagVGroup
            vgroup.nestedTags().forEach { ntag ->
                if (ntag.tagEnum() == TagEnum.RI) {
                    grVGaliasMap[ntag.refno] = vgroup
                }
            }
        }

        if (tag.code == TagEnum.RIG.code) {
            val dgroup = tag as TagDataGroup
            dgroup.nestedTags().forEach { ntag ->
                if (ntag.tagEnum() == TagEnum.RI) {
                    grRIGaliasMap[ntag.refno] = dgroup
                }
            }
        }
    }

    fun checkRasterInfo() {
        grVGaliasMap.forEach { (ri, vg) ->
            println("RI=$ri VG=${vg.refno} elems=${vg.elem_code.contentToString()} ${vg.elem_ref.contentToString()} ")
            val rig = grRIGaliasMap[ri]
            if (rig != null) {
                println("   RIG=${rig.refno} elems=${rig.elem_code.contentToString()} ${rig.elem_ref.contentToString()} ")
            }
        }
        grRIGaliasMap.forEach { (ri, rig) ->
            val vg = grVGaliasMap[ri]
            if (vg == null) {
                println("RI=$ri  RIG=${rig.refno} elems=${rig.elem_code.contentToString()} ${rig.elem_ref.contentToString()} ")
            }
        }
    }

    // looking for Var0.0 aliases for sd and vh TODO dont need I think?
    private fun VgroupAliases(vgroup: TagVGroup, indent: Indent) {
        if (debugVGroupDetails) print("VgroupAliases ${vgroup.className}")
        if (!vgroup.className.startsWith("Var0.0") && !vgroup.className.startsWith("RI0.0")) {
            return
        }
        if (debugVGroupDetails) println("$indent  VgroupFindParents ${vgroup.refno} ${vgroup.className} ${vgroup.name} ")
        vgroup.nestedTags().forEach { tag ->
            if (debugVGroupDetails) println("$indent    ${tag.refCode()}")
            if (tag.tagEnum() == TagEnum.NDG) {
                sdAliasMap[tag.refno] = vgroup
                tag.usedBy = vgroup
                if (debugVGroup) println("$indent   sdAliasMap add $tag to ${vgroup.name}")
            } else if (tag.tagEnum() == TagEnum.VH) {
                vhAliasMap[tag.refno] = vgroup
                tag.usedBy = vgroup
                if (debugVGroup) println("$indent   vhAliasMap add $tag  to ${vgroup.name}")
            }
        }
    }

    // read a VGroup wrapped in a Group4, and recurse
    private fun Vgroup4Read(group4: Group4, group: Group.Builder, parent: TagVGroup? = null) {
        VgroupRead(group4.vgroup, group)
        group4.vgroup.isUsed = true
        group4.vgroup.usedBy = parent

        group4.subgroups.filter { it.vgroup.nelems > 0 }.forEach {
            val nested = Group.Builder(it.vgroup.name)
            group.addGroup(nested)
            Vgroup4Read(it, nested, group4.vgroup)
        }
    }

    private fun VgroupRead(vgroup: TagVGroup, group: Group.Builder) {
        /*if (vgroup.isUsed) {
            if (debugConstruct) println("VgroupRead skip ${vgroup.refno} '${vgroup.name}'")
            return
        }
        vgroup.isUsed = true */
        if (vgroup.nelems == 0) {
            if (debugConstruct) println("  VgroupRead empty group '${vgroup.name}' ref=${vgroup.refno}")
            vgroup.isUsed = true
            return
        }

        if (isDimClass(vgroup.className)) {
            VgroupDim(vgroup)
        } else if (vgroup.className == "Var0.0") { // LOOK undocumented convention
            VgroupVarSD(vgroup, group)
        } else if ((vgroup.className == "RI0.0") || (vgroup.className == "image")) { // LOOK undocumented convention
            GRVariableFromVGroup(vgroup, group)
        } else if (vgroup.className == "CDF0.0") { // LOOK undocumented convention
            VgroupCDF(vgroup, group)
        } else {
            vgroup.nestedTags().forEach { tag ->
                if (tag.tagEnum() == TagEnum.NDG) {
                    val sdAlias = sdAliasMap[tag.refno] // needed? we know the group
                    if (sdAlias != null) {
                        VgroupVarSD(sdAlias, group)
                    } else {
                        SDread(tag as TagDataGroup, null, group, emptyList())
                    }
                    //tag.isUsed = true
                    //tag.usedBy = vgroup
                } else if (tag.tagEnum() == TagEnum.VH) {
                    val vtag = tag as TagVH
                    if (debugVGroup) println("    ${vtag.refCode()} '${vtag.className}' '${vtag.name}'")
                    val vhAlias = vhAliasMap[tag.refno]
                    VStructureRead(vtag, vhAlias?.name, group, true)
                    //tag.isUsed = true
                    //tag.usedBy = vgroup
                }
            }
        }
    }

    // Add an SD variable from a TagVGroup of class Var0.0
    private fun VgroupVarSD(vgroup: TagVGroup, group: Group.Builder) {
        if (vgroup.isUsed) {
            return
        }
        vgroup.isUsed = true
        if (debugConstruct) println("  VgroupVar '${vgroup.name}' ref=${vgroup.refno}")

        val dims = mutableListOf<String>()
        val tagVHs = mutableListOf<TagVH>()
        var tagNDG: TagDataGroup? = null

        vgroup.nestedTags().forEach { tag ->
            if (tag.tagEnum() == TagEnum.VG) {
                val tagV = tag as TagVGroup
                if (isDimClass(tagV.className)) {
                    val dim = VgroupDim(tagV)
                    if (dim != null && !dim.name.startsWith("fakeDim")) dims.add(dim.name) // length is wrong - haha
                    tag.isUsed = true
                    tag.usedBy = vgroup
                }
            }
            if (tag.tagEnum() == TagEnum.VH) {
                val tagVH = tag as TagVH
                if (debugVGroup) println("    ${tag.refCode()} ${tagVH.className} ${tagVH.name}")
                if (tagVH.className.startsWith("Att")) {
                    tagVHs.add(tagVH)
                }
                tag.isUsed = true
                tag.usedBy = vgroup
            }
            if (tag.tagEnum() == TagEnum.NDG) {
                tagNDG = tag as TagDataGroup
                tag.usedBy = vgroup
            }
        }

        if (tagNDG == null) {
            log.warn { "Var0.0 ${vgroup.refCode()} is missing the NDG message" }
            return
        }

        // compute the vb and add to the group
        val vb = SDread(tagNDG, vgroup.name, group, dims) ?: return

        // tagVH's on the group tag (TagVGroup) might be attributes
        tagVHs.forEach {
            val att = VStructureReadAttribute(it)
            if (att != null) {
                vb.addAttribute(att)
                val vinfo = vb.spObject as Vinfo
                if (att.name == NUG.FILL_VALUE) vinfo.setFillValue(att)
                it.isUsed = true
                it.usedBy = vgroup
            }
        }
    }

    // These are coordinate variables, I think. Always has an associated VS for the data.
    // Unfortunately, they are not always correct.
    private fun VgroupDim(vgroup: TagVGroup): Dimension? {
        /* if (vgroup.isUsed) { LOOK
            return null
        } */
        vgroup.isUsed = true

        if (debugConstruct) println("  VgroupDimOld '${vgroup.name}' ref=${vgroup.refno}")
        val isFake = vgroup.name.startsWith("fakeDim")
        vgroup.nestedTags().forEach { tag ->
            tag.isUsed = true

            if (tag.tagEnum() == TagEnum.VH) {
                val tagVH = tag as TagVH
                val length = tagVH.nelems * tagVH.fld_nelems[0]
                if (debugDims) println("     read dimension ${vgroup.name} length='${length}'")
                val data: TagData? = tagidMap[tagid(tagVH.refno, TagEnum.VS.code)] as TagData?
                if (data != null) {
                    data.isUsed = true
                }
                return Dimension(vgroup.name, length)
            }
        }
        return null
    }

    // This contains VH with class Attr0.0, turn into global attributes. Identify EOS Metadata.
    private fun VgroupCDF(vgroup: TagVGroup, parent: Group.Builder) {
        if (vgroup.isUsed) {
            return
        }
        vgroup.isUsed = true

        vgroup.nestedTags().forEach { tag ->

            if (tag.tagEnum() == TagEnum.VH) {
                val tagVH = tag as TagVH
                if (tagVH.className == "Attr0.0") {
                    val attr = VStructureReadAttribute(tagVH)
                    tag.usedBy = vgroup
                    if (attr != null) {
                        if (debugVGroupDetails) println("     read attribute ${attr.name}")
                        checkEosOrPromote(attr, parent, true)
                    }
                }
            }
        }
    }

    fun checkEosOrPromote(attr: Attribute<*>, gb: Group.Builder, addAttributesToGroup: Boolean) {
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

    private fun SDiterate(rootBuilder: Group.Builder) {
        if (debugConstruct) println("--SDiterate")
        for (t: Tag in alltags) {
            if (t.tagEnum() == TagEnum.NDG) {
                SDread(t as TagDataGroup, null, rootBuilder, emptyList())
            }
        }
    }

    private fun SDread(
        dataGroup: TagDataGroup,
        groupName: String?,
        parent: Group.Builder,
        dimNames: List<String>
    ): Variable.Builder<*>? {
        if (dataGroup.isUsed) {
            if (debugConstruct) println("SDread skip  ${dataGroup.refCode()} $groupName")
            return null
        }
        dataGroup.isUsed = true
        if (debugNG) println(" SDread ${dataGroup.refCode()} name=$groupName")

        val vinfo = Vinfo(dataGroup.refno)
        vinfo.tags.add(dataGroup)
        dataGroup.isUsed = true

        var dimSDDout: TagSDD? = null
        var data: TagData? = null
        dataGroup.nestedTags().forEach { tag ->
            tag.usedBy = dataGroup
            vinfo.tags.add(tag) // is this needed?
            tag.vinfo = vinfo // track which variable this tag belongs to
            tag.isUsed = true
            tag.usedBy = dataGroup

            if (tag.tagEnum() == TagEnum.SDD) dimSDDout = tag as TagSDD
            if (tag.tagEnum() == TagEnum.SD) data = tag as TagData
        }
        // see if there are obsolete tags, set used
        tagidMap[tagid(dataGroup.refno, TagEnum.SDG.code)]?.isUsed = true
        tagidMap[tagid(dataGroup.refno, TagEnum.SDLNK.code)]?.isUsed = true

        if (dimSDDout == null) {
            println("   **** NO dimensions found for SD ${dataGroup.refCode()}")
            return null
        }
        val dimSDD = dimSDDout

        val nt: TagNT = tagidMap[tagid(dimSDD.data_nt_ref, TagEnum.NT.code)] as TagNT?
            ?: throw IllegalStateException("   **** NO nt tag found for SD ${dataGroup.refCode()}")
        nt.isUsed = true
        nt.usedBy = dataGroup
        val dataType = H4type.getDataType(nt.numberType)

        val vb = Variable.Builder(groupName ?: "Data-Set-${dataGroup.refno}", dataType)

        // have to use the variables to figure out the dimensions. barf
        val rank = dimSDD.shape.size
        val ndimNames = dimNames.size
        repeat(ndimNames) {
            val dim = Dimension(dimNames[it], dimSDD.shape[it])
            vb.addDimension(dim)
            parent.addDimensionIfNotExists(dim)
        }
        // sometimes dont have any or enough names, use anon dimensions for remaining
        for (dimidx in ndimNames until rank) {
            val dim = Dimension(dimSDD.shape[dimidx]) // anon
            vb.addDimension(dim)
        }

        vinfo.setVariable(vb)
        vinfo.setData(data, dataType.size)

        // Apparently SD uses defaults (but not VS). They are sort-of NC, except for unsigned.
        vinfo.fillValue = getSDefaultFillValue(dataType, vinfo.isBE)
        // then look for this tag. Elsewhere we look for _FillValue attribute.
        val tagFV = tagidMap[tagid(dimSDD.data_nt_ref, TagEnum.FV.code)]
        if ((tagFV != null) and (tagFV is TagFV)) {
            val fillValue = (tagFV as TagFV).readFillValue(this, dataType)
            vinfo.fillValue = convertToBytes(dataType, fillValue, vinfo.isBE)
        }

        // ok to have no data
        if (data == null) {
            vinfo.hasNoData = true
        }

        // look for interesting attributes
        dataGroup.nestedTags().forEach { tag ->
            if (tag.tagEnum() == TagEnum.SDL) {
                val labels: TagTextN = tag as TagTextN
                tag.isUsed = true
                vb.addAttribute(Attribute(NUG.LONG_NAME, Datatype.STRING, labels.texts))
            }
            if (tag.tagEnum() == TagEnum.SDU) {
                val units: TagTextN = tag as TagTextN
                tag.isUsed = true
                vb.addAttribute(Attribute(NUG.UNITS, Datatype.STRING, units.texts))
            }
            if (tag.tagEnum() == TagEnum.SDF) {
                val formats: TagTextN = tag as TagTextN
                tag.isUsed = true
                vb.addAttribute(Attribute("format", Datatype.STRING, formats.texts))
            }
            if (tag.tagEnum() == TagEnum.SDM) {
                val minmax: TagSDminmax = tag as TagSDminmax
                tag.isUsed = true
                vb.addAttribute(Attribute.Builder("valid_min", dataType).setValue(minmax.getMin(dataType)).build())
                vb.addAttribute(Attribute.Builder("valid_max", dataType).setValue(minmax.getMax(dataType)).build())
            }
        }

        // look for VH style attributes
        addVariableAttributes(dataGroup, vb, vinfo)

        parent.addVariable(vb)
        return vb
    }

    private fun VStructureIterate(rootBuilder: Group.Builder) {
        if (debugConstruct) println("--VStructureIterate")
        alltags.forEachIndexed { idx, t ->
            if (t.tagEnum() == TagEnum.VH) {
                VStructureRead(t as TagVH, null, rootBuilder, false)
            }
        }
    }

    private fun VStructureRead(
        vh: TagVH,
        groupName: String?,
        parent: Group.Builder,
        addAttsToGroup: Boolean
    ): Variable.Builder<*>? {
        if (vh.isUsed) {
            return null
        }
        vh.isUsed = true

        // LOOK so we just guess that theres a VS with the same refno ? what else can we guess??
        val data: TagData? = tagidMap[tagid(vh.refno, TagEnum.VS.code)] as TagData?
        if (data == null) {
            log.error { ("Cant find tag " + vh.refno + "/" + TagEnum.VS.code) + " for TagVH=" + vh.detail() }
            return null
        }
        if (data.isUsed) {
            return null
        }
        data.isUsed = true

        // bail out if no data
        if (vh.nelems == 0) {
            return null
        }

        if (vh.className.startsWith("Attr0.")) {
            if (addAttsToGroup) {
                val attr = VStructureReadAttribute(vh)
                if (attr != null) {
                    checkEosOrPromote(attr, parent, true)
                }
            }
            return null
        }

        val vinfo = Vinfo(vh.refno)
        vinfo.tags.add(vh)
        vh.vinfo = vinfo

        vinfo.tags.add(data)
        vinfo.setData(data, vh.ivsize)
        data.vinfo = vinfo
        if (vh.nfields < 1) throw IllegalStateException()

        if (vh.className.startsWith("DimVal") || vh.className.startsWith("_HDF_CHK_TBL")) {
            return null
        }

        val vsname = if (vh.name == "Ancillary_Data") vh.className else vh.name // Lame
        val members = vh.readStructureMembers()
        val nrecords = vh.nelems

        val vb = if (members.size == 1) { // one field - dont make it into a structure
            val member = members[0]

            /* kludge
            if (member.datatype == Datatype.CHAR && member.shape.size == 1) {
                val vb0 = Variable.Builder(vsname, Datatype.STRING)
                // vinfo.elemSize = member.datatype.size // look correct the size??
                println("Change $vsname to String")
                vb0
            } else { */
                val vb1 = Variable.Builder(vsname, member.datatype)
                vinfo.elemSize = member.datatype.size // look correct the size, its not tagVH.ivsize
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
            val typedef = CompoundTypedef(vh.name, members)
            val vb2 = Variable.Builder(vsname, Datatype.COMPOUND.withTypedef(typedef))
            if (nrecords > 1) {
                vb2.setDimensionsAnonymous(intArrayOf(nrecords))
            }
            // LOOK, always put typedef into root. otherwise get duplicates
            rootBuilder.addTypedef(typedef)
            vb2
        }
        vinfo.setVariable(vb)

        if (debugConstruct) {
            println("added variable ${vb.name} from VH $vh group $groupName")
        }

        parent.addVariable(vb)
        return vb
    }

    private fun VStructureReadAttribute(vh: TagVH): Attribute<*>? {
        // LOOK assume always only one field. Maybe multiple fields are allowed to make a compound-typed attribute?
        require(vh.nfields == 1)

        var name = vh.name
        if (name.startsWith("RIATTR0")) { // VHRR
            name = vh.fld_name.first() // wtf?
        }
        val type = vh.fld_type[0].toInt()
        val datatype = H4type.getDataType(type)
        val fld_nelems = vh.fld_nelems[0]

        // LOOK the C lib probably just uses  nelems = fld_nelems
        var nelems = if (datatype == Datatype.CHAR) vh.nelems else vh.nelems * fld_nelems
        var nchars = fld_nelems

        if (vh.name.startsWith("RIATTR0")) {
            name = vh.fld_name.first()
        }

        // find the corresponding VS message
        val data: Tag = tagidMap[tagid(vh.refno, TagEnum.VS.code)] ?: throw IllegalStateException()
        val state = OpenFileState(data.offset, true)
        val att = when (type) {
            3, 4 -> {
                if (vh.name.startsWith("RIATTR0")) {
                    nelems = 1
                    nchars = vh.nelems
                }
                val vals = mutableListOf<String>()
                repeat(nelems) { vals.add(raf.readString(state, nchars, valueCharset)) }
                Attribute(name, Datatype.STRING, vals)
            }

            5 -> {
                val vals = mutableListOf<Float>()
                repeat(nelems) { vals.add(raf.readFloat(state)) }
                Attribute(name, Datatype.FLOAT, vals)
            }

            6 -> {
                val vals = mutableListOf<Double>()
                repeat(nelems) { vals.add(raf.readDouble(state)) }
                Attribute(name, Datatype.DOUBLE, vals)
            }

            20 -> {
                val vals = mutableListOf<Byte>()
                repeat(nelems) { vals.add(raf.readByte(state)) }
                Attribute(name, Datatype.BYTE, vals)
            }

            21 -> {
                val vals = mutableListOf<UByte>()
                repeat(nelems) { vals.add(raf.readByte(state).toUByte()) }
                Attribute(name, Datatype.UBYTE, vals)
            }

            22 -> {
                val vals = mutableListOf<Short>()
                repeat(nelems) { vals.add(raf.readShort(state)) }
                Attribute(name, Datatype.SHORT, vals)
            }

            23 -> {
                val vals = mutableListOf<UShort>()
                repeat(nelems) { vals.add(raf.readShort(state).toUShort()) }
                Attribute(name, Datatype.USHORT, vals)
            }

            24 -> {
                val vals = mutableListOf<Int>()
                repeat(nelems) { vals.add(raf.readInt(state)) }
                Attribute(name, Datatype.INT, vals)
            }

            25 -> {
                val vals = mutableListOf<UInt>()
                repeat(nelems) { vals.add(raf.readInt(state).toUInt()) }
                Attribute(name, Datatype.UINT, vals)
            }

            26 -> {
                val vals = mutableListOf<Long>()
                repeat(nelems) { vals.add(raf.readLong(state)) }
                Attribute(name, Datatype.LONG, vals)
            }

            27 -> {
                val vals = mutableListOf<ULong>()
                repeat(nelems) { vals.add(raf.readLong(state).toULong()) }
                Attribute(name, Datatype.ULONG, vals)
            }

            else -> null
        }
        if (debugAtt) println("      att = '${att?.name}' (class=${vh.className})")
        if (att != null) {
            vh.isUsed = true
            data.isUsed = true
        }
        return att
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////

    // look for RIG messages with an RI message not already found in a VGroup
    private fun GRiterate(rootBuilder: Group.Builder) {
        if (debugConstruct) println("--GRiterate")
        for (t: Tag in alltags) {
            if (t.tagEnum() == TagEnum.RIG) {
                GRVariableFromDataGroup(t as TagDataGroup, rootBuilder)
            }
        }
    }

    //// sometimes theres a TagDataGroup, sometimes a VGroup with the same RI in it. The VGroup takes precedence.
    //// sometimes only diff is that the VGroup has attributes

    private fun GRVariableFromDataGroup(dataGroup: TagDataGroup, group: Group.Builder): Variable.Builder<*>? {
        val name = "Raster_Image_#$imageCount"
        imageCount++
        return GRVariable(dataGroup, name, dataGroup.nestedTags(), group)
    }

    private fun GRVariableFromVGroup(vgroup: TagVGroup, group: Group.Builder): Variable.Builder<*>? {
        return GRVariable(vgroup, vgroup.name, vgroup.nestedTags(), group)
    }

    private fun GRVariable(owner: Tag, name: String, nested: List<Tag>, group: Group.Builder): Variable.Builder<*>? {
        if (owner.isUsed) {
            return null
        }
        owner.isUsed = true

        var dimTag: TagImageDim? = null
        var rasterImageTag: TagRasterImage? = null
        var lutTag: TagLookupTable? = null
        var ludTag: TagImageDim? = null
        var ip8Tag: TagIP8? = null

        val tagVHs = mutableListOf<TagVH>()
        val vinfo = Vinfo(owner.refno)

        nested.forEach { tag ->
            if (debugGR) println("   GRVariable ${owner.refno} has tag ${tag.refCode()} ")

            vinfo.tags.add(tag)
            tag.vinfo = vinfo // track which variable this tag belongs to
            val tage = tag.tagEnum()
            if (tage == TagEnum.ID) {
                dimTag = tag as TagImageDim
                // see if theres an ID8, set used
                tagidMap[tagid(tag.refno, TagEnum.ID8.code)]?.isUsed = true
            } else if (tage == TagEnum.RI) {
                rasterImageTag = tag as TagRasterImage
                if (rasterImageTag!!.isUsed) {
                    return null
                }
                // see if theres an RI8, set used
                tagidMap[tagid(tag.refno, TagEnum.RI8.code)]?.isUsed = true
            } else if (tage == TagEnum.LUT) {
                lutTag = tag as TagLookupTable
                ludTag = tagidMap[tagid(tag.refno, TagEnum.LD.code)] as TagImageDim?
                if (ip8Tag != null) ip8Tag!!.isUsed = true
                ip8Tag = tagidMap[tagid(tag.refno, TagEnum.IP8.code)] as TagIP8?
                if (ip8Tag != null) ip8Tag!!.isUsed = true
            } else if (tage == TagEnum.VH) {
                val tagVH = tag as TagVH
                tagVHs.add(tagVH)
            }
            tag.isUsed = true // mark used, to avoid redundant variables
        }

        if (dimTag == null) {
            log.warn { "Image Group ${owner.refCode()} missing TagRIDimension" }
            return null
        }
        if (rasterImageTag == null) {
            log.warn { "Image Group ${owner.refCode()} missing TagRasterImage" }
            return null
        }
        vinfo.tagDataRI = rasterImageTag!!

        // get the NT tag, referred to from the dimension tag
        val tag: Tag? = tagidMap[tagid(dimTag!!.nt_ref, TagEnum.NT.code)]
        if (tag == null) {
            log.warn { "Image Group " + owner.refCode() + " missing NT tag" }
            return null
        }
        val ntag = tag as TagNT
        ntag.isUsed = true
        ntag.usedBy = owner

        val datatype = H4type.getDataType(ntag.numberType)
        // val datatype = if (orgDataType == Datatype.CHAR) Datatype.UBYTE else orgDataType

        val vb = Variable.Builder(name, datatype)
        vinfo.start = rasterImageTag!!.offset
        vinfo.elemSize = datatype.size
        vinfo.setVariable(vb)

        // assume dimensions are not shared
        vb.dimensions.add(Dimension("", dimTag!!.ydim.toLong(), false))
        vb.dimensions.add(Dimension("", dimTag!!.xdim.toLong(), false))

        if (ip8Tag != null) {
            val lutv_name = "${name}_lookup"
            val lutvb = Variable.Builder(lutv_name, Datatype.UBYTE)
            val lutVinfo = Vinfo(owner.refno)
            lutvb.setDimensionsAnonymous(intArrayOf(256, 3))
            lutVinfo.start = ip8Tag!!.offset
            lutVinfo.elemSize = 1
            lutvb.spObject = lutVinfo
            group.addVariable(lutvb)

        } else if (lutTag != null && ludTag != null) {
            val lutv_name = "${name}_lookup"

            val lutnt = tagidMap[tagid(ludTag!!.nt_ref, TagEnum.NT.code)] as TagNT
            lutnt.isUsed = true
            lutnt.usedBy = ludTag

            val lutType = H4type.getDataType(lutnt.numberType)
            val ldatatype = if (lutType == Datatype.CHAR) Datatype.UBYTE else lutType // inconsistent C Library
            val lutvb = Variable.Builder(lutv_name, ldatatype)
            val lutVinfo = Vinfo(owner.refno)

            lutvb.setDimensionsAnonymous(intArrayOf( /* ludTag!!.ydim, */ ludTag!!.xdim, ludTag!!.nelems))
            lutVinfo.start = lutTag!!.offset
            lutVinfo.elemSize = lutvb.datatype.size
            lutvb.spObject = lutVinfo
            group.addVariable(lutvb)
        }

        tagVHs.forEach {// LOOK does this happen ??
            val att = VStructureReadAttribute(it)
            it.usedBy = owner
            if (att != null) {
                vb.addAttribute(att)
                if (att.name == NUG.FILL_VALUE) vinfo.setFillValue(att)
            }
        }

        group.addVariable(vb)
        if (debugConstruct) println("GRVariable ${vb.name} ${owner.refCode()}")

        return vb
    }

//////////////////////////////////////////////////////////////////////////////////////////////

    private fun makeVariableFromStringAttribute(group: Group.Builder, att: Attribute<*>) {
        require(att.isString)
        val svalue = att.values[0] as String
        val vb = Variable.Builder(att.name, Datatype.STRING)
        vb.spObject = Vinfo(-1).setSValue(svalue)
        group.addVariable(vb)
    }

    // look for tagVHs on the TagDataGroup (NDG); might be attributes
    private fun addVariableAttributes(ndg: TagDataGroup, vb: Variable.Builder<*>, vinfo: Vinfo) {
        ndg.nestedTags().forEach { tag ->
            tag.usedBy = ndg
            // look for attributes
            if (tag.code == 1962) { // VH
                val vh: TagVH = tag as TagVH
                if (vh.className.startsWith("Att")) {
                    val att: Attribute<*>? = VStructureReadAttribute(vh)
                    if (null != att) {
                        vb.addAttribute(att)
                        if (att.name == NUG.FILL_VALUE) vinfo.setFillValue(att)
                    }
                }
            }
        }
    }

//////////////////////////////////////////////////////////////////////

    companion object {
        val log = KotlinLogging.logger("H4builder")
        private val H4HEAD = byteArrayOf(0x0e.toByte(), 0x03.toByte(), 0x13.toByte(), 0x01.toByte())
        private val H4HEAD_STRING = H4HEAD.decodeToString(Charsets.UTF8)
        private const val maxHeaderPos: Long = 500000 // header's gotta be within this

        fun isValidFile(raf: OpenFileIF, state: OpenFileState): Boolean {
            val size: Long = raf.size()

            // search forward for the header
            var startPos = 0L
            while ((startPos < (size - H4HEAD.size)) && (startPos < maxHeaderPos)) {
                state.pos = startPos
                val magic: String = raf.readString(state, H4HEAD.size)
                if ((magic == H4HEAD_STRING)) return true
                startPos = if ((startPos == 0L)) 512 else 2 * startPos
            }
            return false
        }

        private var debugTagSummary = false // show tags after everything is done.
        private var debugTag = false // show tags when reading in first time
        private var debugTagDetail = true // when showing tags, show detail or not
        private var debugTagUsed = false // show unused tags

        private var debugVGroup = false
        private var debugVGroupDetails = false
        private var debugConstruct = false // show CDM objects as they are constructed
        private var debugAtt = false // show CDM attributes as they are constructed
        private var debugDims = false
        private var debugNG = false
        private var debugGR = false

        // this is a unique id for a message in a file
        fun tagid(refno: Int, code: Int): Int {
            val result = (code and 0x3FFF) shl 16
            val result2 = (refno and 0xffff)
            return result + result2
        }

        fun tagidName(tagid: Int): String {
            val code = (tagid shr 16) and 0x3FFF
            val refno = (tagid and 0xffff)
            return "ref=$refno code=${code}"
        }

        fun tagidNameR(tagid: Int): String {
            val refno = (tagid shr 16) and 0xFFFF
            val code = (tagid and 0x3fff)
            return "ref=$refno code=${code}"
        }
    }

    internal fun isNestedGroup(vgroup: TagVGroup): Boolean {
        if (vgroup.name.contains("RIG0")) { // ignoring to agree with the C library
            vgroup.isUsed = true
            return false
        }
        val className = vgroup.className
        if (className.startsWith("Var0") || className.startsWith("Att0") || className.startsWith("CDF0")
            || className.startsWith("RI0")
            || className.startsWith("Dim0") || className.startsWith("DimVal0") || className.startsWith("UDim0")
        ) {
            return false
        }

        var isGroup = false
        vgroup.nestedTags().forEach { tag ->
            if (tag.code == TagEnum.VG.code || tag.code == TagEnum.VH.code || tag.code == TagEnum.NDG.code) {
                isGroup = true
            }
        }
        return isGroup
    }
}

private class Group4(val vgroup: TagVGroup) {
    val subgroups = mutableListOf<Group4>()
    override fun toString(): String {
        return "${vgroup.name}/${vgroup.className}"
    }
}

fun isDimClass(className: String): Boolean {
    return (className == "Dim0.0") or (className == "UDim0.0")
}