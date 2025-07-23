@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.hdf5.H5builder.Companion.HDF5_CLASS
import com.sunya.netchdf.hdf5.H5builder.Companion.HDF5_DIMENSION_LABELS
import com.sunya.netchdf.hdf5.H5builder.Companion.HDF5_DIMENSION_LIST
import com.sunya.netchdf.hdf5.H5builder.Companion.HDF5_DIMENSION_NAME
import com.sunya.netchdf.hdf5.H5builder.Companion.HDF5_DIMENSION_SCALE
import com.sunya.netchdf.hdf5.H5builder.Companion.HDF5_REFERENCE_LIST
import com.sunya.netchdf.hdf5.H5builder.Companion.HDF5_SPECIAL_ATTS
import com.sunya.netchdf.netcdf4.Netcdf4.NETCDF4_NON_COORD
import com.sunya.netchdf.netcdf4.Netcdf4.NETCDF4_NOT_VARIABLE
import com.sunya.netchdf.netcdf4.Netcdf4.NETCDF4_SPECIAL_ATTS

const val attLengthMax = 4000

internal const val includeOriginalAttributes = false
internal const val debugDimensionScales = false

internal fun H5builder.buildCdm(h5root : H5Group) : Group.Builder {
    return buildGroup(h5root)
}

internal fun H5builder.buildGroup(group5 : H5Group) : Group.Builder {
    val groupb = Group.Builder(group5.name)

    makeDimensions(groupb, group5)

    // shared typedefs are always registered
    group5.sharedTypedefs.forEach {
        val tdef = this.buildTypedef(it, groupb)
        if (tdef != null) {
            val typeinfo = makeH5TypeInfo(it.mdt, tdef)
            typedefManager.registerTypedef(typeinfo, groupb)
        }
    }

    group5.attributes().forEach {
        val attr = buildAttribute(groupb, it)
        val promoted = !strict && attr.isString && attr.values.size == 1 && (attr.values[0] as String).length > attLengthMax
        if (promoted) { // too big for an attribute
            val vb = Variable.Builder(attr.name, Datatype.STRING)
            vb.spObject = DataContainerAttribute(it.name, makeH5TypeInfo(it.mdt), it.dataPos, it.mdt, it.mds)
            groupb.addVariable( vb)
        } else {
            groupb.addAttribute(attr)
        }
    }

    group5.variables.filter{ it.isVariable }.forEach {
        try {
            val vb = buildVariable( groupb, it )
            groupb.addVariable(vb)
            val address = it.dataObject.address
            // println("**H5builder vb.name=${vb.name} address=${it.dataObject.address}") // maybe there a byte order problem ??
            if (address > 0) datasetMap[address] = Pair(groupb, vb)
        } catch (e: RuntimeException) {
            e.printStackTrace()
            // fall through
        }
    }

    group5.nestedGroups.forEach { groupb.addGroup( buildGroup( it )) }

    val iter = groupb.attributes.iterator()
    while (iter.hasNext()) {
        val attname = iter.next().name
        if (NETCDF4_SPECIAL_ATTS.contains(attname)) {
            if (!includeOriginalAttributes) iter.remove()
            isNetcdf4 = true
        }
        if (HDF5_SPECIAL_ATTS.contains(attname)) {
            if (!includeOriginalAttributes) iter.remove()
        }
    }
    return groupb
}

internal fun H5builder.buildAttribute(gb : Group.Builder, att5 : AttributeMessage) : Attribute<*> {
    var typedef = typedefManager.findTypedef(att5.mdt.address, att5.mdt.hashCode())
    if (debugTypedefs and (typedef != null)) {
        println(" made attribute ${att5.name} from typedef ${typedef!!.name}@${att5.mdt.address}")
    }
    // private (non-shared) typedefs
    if (typedef == null && att5.mdt.type.needsTypedef()) {
        val typedef5 = H5typedef(null, att5.mdt) // name
        typedef = this.buildTypedef(typedef5, gb)
        if (typedef != null) typedefManager.registerPrivateTypedef(typedef, gb)
    }
    val h5type = makeH5TypeInfo(att5.mdt, typedef)
    val dc = DataContainerAttribute(att5.name, h5type, att5.dataPos, att5.mdt, att5.mds)
    val useType = h5type.datatype()
    val data = this.readRegularData(dc, useType, null)
    return if (useType == Datatype.CHAR) {
        val svalues = if (data is ArrayString) data else (data as ArrayUByte).makeStringsFromBytes()
        Attribute.Builder(att5.name, Datatype.STRING).setValues(svalues.toList()).build()
    } else if (data is ArrayVlen<*>) {
        // attributes want lists, not arrays
        val attValues = data.values.map { it -> it.toList() }
        Attribute.Builder(att5.name, useType).setValues(attValues).build()

    } else  {
        // TODO data.toList() ??
        Attribute.Builder(att5.name, useType).setValues(data.toList()).build()
    }
}

internal fun H5builder.buildVariable(groupb: Group.Builder, v5 : H5Variable) : Variable.Builder<*> {
    if (v5.name.startsWith(NETCDF4_NON_COORD)) {
        isNetcdf4 = true
    }

    var typedef = typedefManager.findTypedef(v5.mdt.address, v5.mdt.hashCode())
    if (debugTypedefs and (typedef != null)) {
        println(" made variable ${v5.name} from typedef ${typedef!!.name}@${v5.mdt.address}")
    }
    // private (non-shared) typedefs
    if (typedef == null && v5.mdt.type.needsTypedef()) {
        val typedef5 = H5typedef(null, v5.mdt) // name
        typedef = this.buildTypedef(typedef5, groupb)
        if (typedef != null) typedefManager.registerPrivateTypedef(typedef, groupb)
    }

    val h5type = makeH5TypeInfo(v5.mdt, typedef)
    val datatype = h5type.datatype() // typedefs added here
    val builder = Variable.Builder(v5.name.substringAfter(NETCDF4_NON_COORD), datatype)

    if (v5.dimList != null) {
        builder.dimNames = v5.dimList!!.trim().split(" ")
    } else {
        // LOOK non-shared, integrate with shared ??
        v5.mds.dims.forEach{builder.dimensions.add(Dimension(it))}
    }

    for (att5 in v5.attributes()) {
        builder.addAttribute(buildAttribute(groupb, att5))
    }
    val iter = builder.attributes.iterator()
    while (iter.hasNext()) {
        val attname = iter.next().name
        if (NETCDF4_SPECIAL_ATTS.contains(attname)) {
            if (!includeOriginalAttributes) iter.remove()
            isNetcdf4 = true
        }
        if (HDF5_SPECIAL_ATTS.contains(attname)) {
            if (!includeOriginalAttributes) iter.remove()
        }
    }

    // stuff needed to read hdf5
    require (v5.dataObject.mdl != null)
    val vdata = DataContainerVariable(builder.name, h5type, v5, this)

    if (v5.name.startsWith("StructMetadata")) {
        val data = readRegularData(vdata, Datatype.STRING, null)
        require (data is ArrayString)
        structMetadata.add(data.values[0])
    }
    builder.spObject = vdata
    return builder
}

internal interface DataContainer {
    val name: String
    val h5type: H5TypeInfo
    val dataPos: Long
    val mdt: DatatypeMessage
    val mds: DataspaceMessage
    val storageDims : LongArray
}

internal open class DataContainerAttribute(
    override val name : String,
    override val h5type: H5TypeInfo,
    override val dataPos : Long,
    override val mdt: DatatypeMessage,
    override final val mds: DataspaceMessage,
    ) : DataContainer {
        override val storageDims = mds.dims
}

val mdlClassCount = getDataLayoutCounts()

internal class DataContainerVariable(
    override val name: String,
    override val h5type: H5TypeInfo,
    v5 : H5Variable,
    h5 : H5builder,
) : DataContainer {
    override val dataPos : Long
    override val mdt: DatatypeMessage = v5.mdt
    override val mds: DataspaceMessage = v5.mds
    override val storageDims : LongArray // dimensions

    val mdl = v5.mdl
    val mfp = v5.mfp

    val elementSize : Int // total length in bytes on disk of one element
    val onlyFillValue : Boolean // no data at all
    val fillValue : ByteArray

    init {
        val mdlName = mdl::class.simpleName!!
        val mdlCount = mdlClassCount.getOrPut(mdlName) { 0 }
        mdlClassCount[mdlName] = mdlCount + 1

        // TODO if compact, do not use fileOffset
        dataPos = when (mdl) {
            is DataLayoutContiguous -> h5.getFileOffset(mdl.dataAddress)
            is DataLayoutBTreeVer1 -> mdl.btreeAddress // offset will be added in BTreeData
            is DataLayoutCompact -> -2L // data is in mdl.compactData
            is DataLayoutCompact3 -> -2L // data is in mdl.compactData
            is DataLayoutContiguous3 -> h5.getFileOffset(mdl.dataAddress)
            is DataLayoutVirtual4 -> -1  // dunno
            is DataLayoutSingleChunk4 -> h5.getFileOffset(mdl.heapAddress)
            is DataLayoutImplicit4 -> h5.getFileOffset(mdl.address)
            is DataLayoutFixedArray4 -> h5.getFileOffset(mdl.indexAddress)
            is DataLayoutExtensibleArray4 -> h5.getFileOffset(mdl.indexAddress)
            is DataLayoutBtreeVer2 -> h5.getFileOffset(mdl.heapAddress)
            else -> -1 // LOOK compact?
        }
        // deal with unallocated data
        fillValue = getFillValue(h5, v5, h5type)
        onlyFillValue = (dataPos == -1L)

        when (mdl) {
            is DataLayoutCompact -> {
                this.storageDims = mds.dims // LOOK why not mdl.dims?
                this.elementSize = mdt.elemSize
            }
            is DataLayoutContiguous -> {
                this.storageDims = mds.dims
                val nelems = this.storageDims.computeSize()
                this.elementSize = (mdt.elemSize / nelems).toInt()
            }
            is DataLayoutBTreeVer1 -> {
                // println("file ${h5.raf.location()} var=${name} btreeVer1") // TODO ??
                this.storageDims = mdl.chunkDims.toLongArray()
                this.elementSize = storageDims[storageDims.size - 1].toInt() // last number is element size
            }
            is DataLayoutCompact3 -> {
                this.storageDims = mds.dims
                this.elementSize = mdt.elemSize
            }
            is DataLayoutContiguous3 -> {
                this.storageDims = mds.dims
                val nelems = this.storageDims.computeSize()
                this.elementSize = (mdt.elemSize / nelems).toInt()
            }
            is DataLayoutVirtual4 -> {
                // TODO
                this.storageDims = longArrayOf()
                this.elementSize = 0 // storageDims[storageDims.size - 1].toInt() // last number is element size
            }
            is DataLayoutSingleChunk4 -> {
                this.storageDims = mdl.chunkDimensions.toLongArray()
                this.elementSize = storageDims[storageDims.size - 1].toInt() // last number is element size ??
            }
            is DataLayoutImplicit4 -> {
                this.storageDims = mdl.chunkDimensions.toLongArray()
                this.elementSize = storageDims[storageDims.size - 1].toInt() // last number is element size ??
            }
            is DataLayoutFixedArray4 -> {
                this.storageDims = mdl.chunkDimensions.toLongArray()
                this.elementSize = storageDims[storageDims.size - 1].toInt() // last number is element size ??
            }
            is DataLayoutExtensibleArray4 -> {
                this.storageDims = mdl.chunkDimensions.toLongArray()
                this.elementSize = storageDims[storageDims.size - 1].toInt() // last number is element size ??
            }
            is DataLayoutBtreeVer2 -> {
                this.storageDims = mdl.chunkDimensions.toLongArray()
                this.elementSize = storageDims[storageDims.size - 1].toInt() // last number is element size ??
            }

            else -> throw RuntimeException()
        }
    }
}

internal fun getFillValue(h5 : H5builder, v5 : H5Variable, h5type: H5TypeInfo): ByteArray {
    // look for fill value message
    var fillValue : ByteArray? = null
    for (mess in v5.dataObject.messages) {
        if (mess.mtype === MessageType.FillValue) {
            val fvm = mess as FillValueMessage
            if (fvm.hasFillValue) {
                fillValue = fvm.value // val value: ByteBuffer?
            }
        } else if (mess.mtype === MessageType.FillValueOld) {
            val fvm = mess as FillValueOldMessage
            if (fvm.size > 0) {
                fillValue = fvm.value // val value: ByteBuffer?
            }
        }
    }
    return fillValue ?: ByteArray(h5type.elemSize)
}


///////////////////////// Attributes

/*
   * from https://www.unidata.ucar.edu/software/netcdf/docs/netcdf.html#NetCDF_002d4-Format
   * C.3.7 Attributes
   *
   * Attributes in HDF5 and netCDF-4 correspond very closely. Each attribute in an HDF5 file is represented as an
   * attribute in the netCDF-4 file, with the exception of the attributes below, which are ignored by the netCDF-4 API.
   *
   * 1) _Netcdf4Coordinates An integer array containing the dimension IDs of a variable which is a multi-dimensional
   * coordinate variable.
   * 2) _nc3_strict When this (scalar, H5T_NATIVE_INT) attribute exists in the root group of the HDF5 file, the netCDF API
   * will enforce the netCDF classic model on the data file.
   * 3) REFERENCE_LIST This attribute is created and maintained by the HDF5 dimension scale API.
   * 4) CLASS This attribute is created and maintained by the HDF5 dimension scale API.
   * 5) DIMENSION_LIST This attribute is created and maintained by the HDF5 dimension scale API.
   * 6) NAME This attribute is created and maintained by the HDF5 dimension scale API.
 */

///////////////////////// Dimensions

/*
https://www.unidata.ucar.edu/blogs/news/entry/netcdf_shared_dimensions_vs_hdf5

A Dimension Scale is a special variable containing a set of references to dimensions in variables.
Each referenced variable has a DIMENSION_LIST attribute that contains, for each dimension, a list of references to Dimension Scales.
So we have a two-way, many-to-many linking between Dimension Scales and Dimensions.
 */

/*

   *
   * ----------
   * from dim_scales_wk9 - Nunes.ppt
   *
   * Attribute named "CLASS" with the value "DIMENSION_SCALE"
   * Optional attribute named "NAME"
   * Attribute references to any associated Dataset
   *
   * -------------
   * from https://www.unidata.ucar.edu/mailing_lists/archives/netcdfgroup/2008/msg00093.html
   *
   * Then comes the part you will have to do for your datasets. You open the data
   * dataset, get an ID, DID variable here, open the latitude dataset, get its ID,
   * DSID variable here, and "link" the 2 with this call
   *
   * if (H5DSattach_scale(did,dsid,DIM0) < 0)
   *
   * what this function does is to associated the dataset DSID (latitude) with the
   * dimension* specified by the parameter DIM0 (0, in this case, the first
   * dimension of the 2D array) of the dataset DID
   *
   * If you open HDF Explorer and expand the attributes of the "data" dataset you
   * will see an attribute called DIMENSION_LIST.
   * This is done by this function. It is an array that contains 2 HDF5 references,
   * one for the latitude dataset, other for the longitude)
   *
   * If you expand the "lat" dataset , you will see that it contains an attribute
   * called REFERENCE_LIST. It is a compound type that contains
   * 1) a reference to my "data" dataset
   * 2) the index of the data dataset this scale is to be associated with (0 for the lat, 1 for the lon)
   */

// find dimensions in h5group, add them to parentGroup
internal fun H5builder.makeDimensions(parentGroup: Group.Builder, h5group: H5Group) {

    // 1. find all objects with CLASS = "DIMENSION_SCALE", make into a dimension. use shape(0) as length.
    h5group.variables.forEach { findDimensionScales(parentGroup, h5group, it) }

    // 2. if also a variable (NAME != "This is a ...") then first dim = itself, second matches length, if multiple
    // match, use :_Netcdf4Coordinates = 0, 3 and order of dimensions.
    h5group.variables.filter { it.is2DCoordinate }.forEach { findDimensionScales2D(h5group, it) }

    // 3. use DIMENSION_LIST to assign dimensions to other variables.
    h5group.variables.forEach { findSharedDimensions(parentGroup, h5group, it) }

    for (d in h5group.dimList) {
        parentGroup.addDimensionIfNotExists(d)
    }
}

// find the Dimension Scale objects, turn them into shared dimensions
// always has attribute CLASS = "DIMENSION_SCALE"
// note that we dont bother looking at REFERENCE_LIST
internal fun H5builder.findDimensionScales(gb: Group.Builder, h5group: H5Group, h5variable: H5Variable) {

    val removeAtts = mutableListOf<AttributeMessage>()
    h5variable.attributes().filter { it.name == HDF5_CLASS }.forEach {
        val att = buildAttribute(gb, it)
        if (att.values[0] !is String) {
            buildAttribute(gb, it)
        }
        check(att.isString)
        val value: String = att.values[0] as String
        if (value == HDF5_DIMENSION_SCALE && h5variable.mds.rank() > 0) {
            // create a dimension - always use the first dataspace length
            h5variable.dimList = addSharedDimension(
                gb,
                h5group,
                h5variable.name,
                h5variable.mds.dims[0],
            )
            h5variable.hasNetcdfDimensions = true
            removeAtts.add(it)
            if (h5variable.mds.rank() > 1) {
                h5variable.is2DCoordinate = true
            }
        }
    }
    if (!includeOriginalAttributes) {
        removeAtts.forEach { h5variable.removeAtt(it)}
    }
}

// add a dimension, return its name
private fun addSharedDimension(
    parent: Group.Builder,
    h5group: H5Group,
    name: String,
    length: Long,
): String {
    val dimName = name.substringAfterLast('/')
    var d = h5group.dimMap[dimName] // first look in current group
    if (d == null) { // create if not exist
        d = Dimension(name, length, true)
        h5group.dimMap[dimName] = d
        h5group.dimList.add(d)
        parent.addDimension(d)
        if (debugDimensionScales) {
            println("addDimension name=" + name + " dim= " + d + " to group " + parent.name)
        }
    } else { // check has correct length
        check(d.length == length) { "addDimension: DimScale has different length than dimension it references dimScale=$dimName" }
    }
    return d.name
}

// look for unlimited dimensions without dimension scale - must get length from the variable
// LOOK this implies that different variables might have different dimension lengths.
//   so, underlying "h5dataset" not same as cdm variable
private fun extendDimension(parent: Group.Builder, h5group: H5Group, name: String, length: Long): String {
    val dimName = name.substringAfterLast('/')
    val d = h5group.findDimension(dimName) // first look in current group
    if (d != null) {
        // if (d.isUnlimited && length > d.length) {
        if (length > d.length) {
            parent.replaceDimension(d.copy(length = length))
        }
        // check(!(!d.isUnlimited && length != d.length)) { "extendDimension: DimScale has different length than dimension it references dimScale=$dimName" }
        return d.name
    }
    return dimName
}

internal fun findDimensionScales2D(h5group: H5Group, h5variable: H5Variable) {
    val lens: LongArray = h5variable.mds.dims
    if (debugDimensionScales and (lens.size > 2)) {
        println("DIMENSION_LIST: dimension scale > 2 = ${h5variable.name}")
        return
    }

    // first dimension is itself
    val name: String = h5variable.name
    val pos = name.lastIndexOf('/')
    val dimName = if (pos >= 0) name.substring(pos + 1) else name
    val sbuff = StringBuilder()
    sbuff.append(dimName)
    sbuff.append(" ")

    // second dimension is really an anonymous dimension, ironically now we go through amazing hoops to keep it shared
    // 1. use dimids if they exist
    // 2. if length matches and unique, use it
    // 3. if no length matches or multiple matches, then use anonymous
    val want_len = lens[1] // second dimension
    var match: Dimension? = null
    var unique = true
    for (d in h5group.dimList) {
        if (d.length == want_len) {
            if (match == null) {
                match = d
            } else {
                unique = false
            }
        }
    }
    if (match != null && unique) {
        sbuff.append(match.name) // 2. if length matches and unique, use it
    } else {
        if (match == null) { // 3. if no length matches or multiple matches, then use anonymous
            if (debugDimensionScales) println("DIMENSION_LIST: dimension scale ${h5variable.name} has second dimension $want_len but no match")
            // based on /media/twobee/netch/gilmore/data.nc, just ignore this second dimension
            // sbuff.append(want_len)
        } else {
            if (debugDimensionScales) println("DIMENSION_LIST: dimension scale ${h5variable.name} has second dimension $want_len but multiple matches")
            sbuff.append(want_len)
        }
    }
    h5variable.dimList = sbuff.toString()
}

// look for references to dimension scales, ie the variables that use them
// return true if this variable is compatible with netcdf4 data model
internal fun H5builder.findSharedDimensions(parentGroup: Group.Builder, h5group: H5Group, h5variable: H5Variable): Boolean {

    val removeAtts = mutableListOf<AttributeMessage>()
    h5variable.attributes().forEach { matt ->
        when (matt.name) {
            HDF5_DIMENSION_LIST -> {
                // LOOK theory is that a HDF5_DIMENSION_LIST that is a vlen of reference is a netcdf4 file
                if (matt.mdt.type == Datatype5.Vlen && (matt.mdt is DatatypeVlen) && (matt.mdt as DatatypeVlen).base.type == Datatype5.Reference) {
                    isNetcdf4 = true
                }

                // references : may extend the dimension rank?
                val att = buildAttribute(parentGroup, matt) // this reads in the data
                if (att.values.size != h5variable.mds.rank()) {
                    // some attempts to writing hdf5 directly fail here
                    if (debugDimensionScales) println("DIMENSION_LIST: must have same number of dimension scales as dimensions att=${att} on variable ${h5variable.name}")
                } else {
                    val sbuff = StringBuilder()
                    var i = 0
                    while (i < att.values.size) {
                        val name: String = att.values[i] as String // LOOK assumes string
                        val dimName: String = extendDimension(parentGroup, h5group, name, h5variable.mds.dims[i])
                        sbuff.append(dimName).append(" ")
                        i++
                    }
                    h5variable.dimList = sbuff.toString()
                    h5variable.hasNetcdfDimensions = true
                    if (debugDimensionScales) {
                        println("Found dimList '${h5variable.dimList}' for var '${h5variable.name}'")
                    }
                }
                removeAtts.add(matt)
            }

            HDF5_DIMENSION_NAME -> {
                val att = buildAttribute(parentGroup, matt)
                val value: String = att.values[0] as String
                if (value.startsWith(NETCDF4_NOT_VARIABLE)) {
                    h5variable.isVariable = false
                    isNetcdf4 = true
                }
                removeAtts.add(matt)
                if (debugDimensionScales) {
                    println("Found $HDF5_DIMENSION_NAME='$value'")
                }
            }

            HDF5_DIMENSION_LABELS,
            HDF5_REFERENCE_LIST -> removeAtts.add(matt)
        }
    }

    if (!includeOriginalAttributes) {
        removeAtts.forEach { h5variable.removeAtt(it) }
    }

    return h5variable.hasNetcdfDimensions || h5variable.mds.rank() == 0
}


