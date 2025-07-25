@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.netcdfClib

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.*
import com.sunya.cdm.iosp.OpenFileIF.Companion.nativeByteOrder
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.netcdfClib.ffm.nc_vlen_t
import com.sunya.netchdf.netcdfClib.ffm.netcdf_h.*
import java.io.IOException
import java.lang.foreign.MemorySegment
import java.lang.foreign.Arena
import java.lang.foreign.ValueLayout
import java.lang.foreign.ValueLayout.ADDRESS
import java.lang.foreign.ValueLayout.JAVA_BYTE

val debugUserTypes = false

//        val nvars_p = session.allocate(C_INT, 0)
//        checkErr("nc_inq_nvars", nc_inq_nvars(g4.grpid, nvars_p))
//        val nvars = nvars_p[C_INT, 0]
//
//        val varids_p = session.allocateArray(C_INT, nvars.toLong())
//        checkErr("nc_inq_varids", nc_inq_varids(g4.grpid, nvars_p, varids_p))

@Throws(IOException::class)
internal fun NCheader.readUserTypes(session: Arena, grpid: Int, gb: Group.Builder, userTypes : MutableMap<Int, UserType>) {
    val numUserTypes_p = session.allocate(C_INT, 0)
    val MAX_USER_TYPES = 1000L // bad API
    val userTypes_p = session.allocateArray(C_INT, MAX_USER_TYPES)
    //     public static int nc_inq_typeids(int ncid, MemorySegment ntypes, MemorySegment typeids) {
    checkErr("nc_inq_typeids", nc_inq_typeids(grpid, numUserTypes_p, userTypes_p))

    val numUserTypes = numUserTypes_p[C_INT, 0]
    if (numUserTypes == 0) {
        return
    }

    // for each defined "user type", get information, store in Map
    for (idx in 0 until numUserTypes) {
        val userTypeId = userTypes_p.getAtIndex(ValueLayout.JAVA_INT, idx.toLong())

        val name_p: MemorySegment = session.allocate(NC_MAX_NAME().toLong() + 1)
        val size_p = session.allocate(C_LONG, 0)
        val baseTypeId_p: MemorySegment = session.allocate(C_INT, 0)
        val nfields_p = session.allocate(C_LONG, 0)
        val typeClass_p = session.allocate(C_INT, 0)

        // grpid: the group containing the user defined type.
        // userTypeId: The typeid for this type, as returned by nc_def_compound, nc_def_opaque, nc_def_enum,
        //   nc_def_vlen, or nc_inq_var.
        // name: If non-NULL, the name of the user defined type. It will be NC_MAX_NAME bytes or less.
        // size: the (in-memory) size of the type in bytes will be copied here. VLEN type size is the size of nc_vlen_t.
        //       String size is returned as the size of a character pointer.
        //       The size may be used to malloc space for the data, no matter what the type. Ignored if NULL.
        // baseType: The base type will be copied here for enum and VLEN types. Ignored if NULL.
        // nfields: The number of fields will be copied here for enum and compound types. Ignored if NULL.
        // typeClass: the class of the user defined type, NC_VLEN, NC_OPAQUE, NC_ENUM, or NC_COMPOUND.
        checkErr("nc_inq_user_type",
            nc_inq_user_type(grpid, userTypeId, name_p, size_p, baseTypeId_p, nfields_p, typeClass_p)
        )
        val name: String = name_p.getUtf8String(0)
        val size = size_p[C_LONG, 0]
        val baseTypeId = baseTypeId_p[C_INT, 0]
        val nfields = nfields_p[C_LONG, 0]
        val typeClass = typeClass_p[C_INT, 0]

        val typedef = when (typeClass) {
            NC_ENUM() -> {
                val values: Map<Int, String> = readEnumTypedef(session, grpid, userTypeId)
                val baseType = when (baseTypeId) {
                    NC_CHAR(), NC_UBYTE(), NC_BYTE() -> Datatype.UBYTE
                    NC_BYTE() -> Datatype.BYTE
                    NC_USHORT() -> Datatype.USHORT
                    NC_SHORT() -> Datatype.SHORT
                    NC_UINT() -> Datatype.UINT
                    NC_INT() -> Datatype.INT
                    else -> throw RuntimeException("enum illegal baseType = $baseTypeId")
                }
                EnumTypedef(name, baseType, values)
            }
            NC_OPAQUE() -> OpaqueTypedef(name, size.toInt())
            NC_VLEN() -> VlenTypedef(name, convertType(baseTypeId))
            NC_COMPOUND() -> {
                val members : List<StructureMember<*>> = readCompoundFields(session, grpid, userTypeId, nfields.toInt())
                CompoundTypedef(name, members)
            }
            else -> throw RuntimeException("Unsupported type class $typeClass")
        }
        if (NCheader.debug) println(" typedef $name $size $baseTypeId $nfields ${typedef.kind} ${typedef.baseType}")

        val ut = UserType(grpid, userTypeId, name, size.toInt(), baseTypeId, typedef)
        registerTypedef(ut, gb)
    }
}

@Throws(IOException::class)
private fun NCheader.readEnumTypedef(session: Arena, grpid: Int, xtype: Int): Map<Int, String> {
    val name_p: MemorySegment = session.allocate(NC_MAX_NAME().toLong())
    val baseType_p = session.allocate(C_INT)
    val baseSize_p = session.allocate(C_LONG, 0)
    val nmembers_p = session.allocate(C_LONG, 0)
    checkErr("nc_inq_enum", nc_inq_enum(grpid, xtype, name_p, baseType_p, baseSize_p, nmembers_p))

    val baseType = baseType_p[C_INT, 0]
    val baseSize = baseSize_p[C_LONG, 0]
    val nmembers = nmembers_p[C_LONG, 0]
    val values = mutableMapOf<Int, String>()
    for (i in 0 until nmembers.toInt()) {
        val value_p = session.allocate(C_INT)
        checkErr("nc_inq_enum_member", nc_inq_enum_member(grpid, xtype, i, name_p, value_p))
        val mname = name_p.getUtf8String(0)
        val value = value_p[C_INT, 0]
        values[value] = mname
    }
    return values
}

@Throws(IOException::class)
private fun NCheader.readCompoundFields(session: Arena, grpid: Int, typeid: Int, nfields : Int, isBE : Boolean = nativeByteOrder): List<StructureMember<*>>{
    val members = mutableListOf<StructureMember<*>>()
    for (fldidx in 0 until nfields) {
        val name_p: MemorySegment = session.allocate(NC_MAX_NAME().toLong())
        val offset_p = session.allocate(C_LONG)
        val ftypeid_p = session.allocate(C_INT)
        val ndims_p = session.allocate(C_INT)
        val dims_p = session.allocateArray(C_INT, NC_MAX_DIMS())

        checkErr("nc_inq_compound_field", nc_inq_compound_field(grpid, typeid, fldidx, name_p, offset_p, ftypeid_p, ndims_p, dims_p))
        val name = name_p.getUtf8String(0)
        val offset = offset_p[C_LONG, 0]
        val ftypeid = ftypeid_p[C_INT, 0]
        val ndims = ndims_p[C_INT, 0]
        val dims = IntArray(ndims) { dims_p.getAtIndex(ValueLayout.JAVA_INT, it.toLong())}

        // class StructureMember<T>(orgName: String, val datatype : Datatype<T>, val offset: Int, val shape : IntArray, val isBE : Boolean) {
        val fld = StructureMember(name, convertType(ftypeid), offset.toInt(), dims, isBE = isBE)
        members.add(fld)
        if (debugUserTypes) println(" add StructureMember= $fld")
    }
    return members
}

internal fun NCheader.readUserAttributeValues(session: Arena, grpid: Int, varid: Int, attname: String,
                                              datatype : Datatype<*>, userType: UserType, nelems: Long
): Attribute.Builder<*> {
    return when (userType.typedef.kind) {
        TypedefKind.Compound  -> readCompoundAttValues(session, grpid, varid, attname, nelems, datatype, userType)
        TypedefKind.Enum -> readEnumAttValues(session, grpid, varid, attname, nelems, datatype, userType)
        TypedefKind.Opaque  -> readOpaqueAttValues(session, grpid, varid, attname, nelems, datatype, userType)
        TypedefKind.Vlen  -> readVlenAttValues(session, grpid, varid, attname, nelems, datatype, userType)
        else -> throw RuntimeException("Unsupported user attribute data type == ${userType.typedef.kind}")
    }
}

// Just flatten, attributes only support 1D arrays. Note need to do all array types
@Throws(IOException::class)
internal fun NCheader.readVlenAttValues(session: Arena, grpid: Int, varid: Int, attname: String, nelems: Long,
                                        datatype : Datatype<*>, userType: UserType
): Attribute.Builder<*> {
    val basetype = convertType(userType.baseTypeid)
    val attb = Attribute.Builder(attname, datatype)

    // fetch nelems of nc_vlen_t struct
    val attname_p: MemorySegment = session.allocateUtf8String(attname)
    val vlen_p = nc_vlen_t.allocateArray(nelems, session)
    checkErr("nc_get_att", nc_get_att(grpid, varid, attname_p, vlen_p))

    attb.setValues(readVlenDataList(nelems, basetype, vlen_p))
    return attb
}

// factored out to use in compound
internal fun readVlenDataList(nelems : Long, basetype : Datatype<*>, vlen_p : MemorySegment) : List<*> {
    val attValues = mutableListOf<List<*>>()
    for (elem in 0 until nelems) {
        val count = nc_vlen_t.getLength(vlen_p, elem)
        val zaddress = nc_vlen_t.getAddress(vlen_p, elem)
        // val zaddress = vlen_p.get(ADDRESS, elem)  // zero length memory segment
        // see https://stackoverflow.com/questions/77042593/indexoutofboundsexception-out-of-bound-access-on-segment-when-accessing-p
        val address = zaddress.reinterpret(Long.MAX_VALUE)

        val vlenValues = mutableListOf<Any>()
        for (idx in 0 until count) {
            val value = when (basetype) {
                Datatype.BYTE-> address.get(JAVA_BYTE, idx)
                Datatype.SHORT -> address.getAtIndex(C_SHORT, idx)
                Datatype.INT -> address.getAtIndex(C_INT, idx)
                Datatype.LONG -> address.getAtIndex(C_LONG, idx)
                Datatype.DOUBLE -> address.getAtIndex(C_DOUBLE,  idx)
                Datatype.FLOAT -> address.getAtIndex(C_FLOAT, idx)
                Datatype.STRING -> address.getUtf8String(0)
                else -> throw RuntimeException("readVlenDataList unknown type = ${basetype}")
            }
            vlenValues.add(value)
        }
        attValues.add(vlenValues)
    }
    return attValues
}

@Throws(IOException::class)
private fun readOpaqueAttValues(session: Arena, grpid: Int, varid: Int, attname: String, nelems: Long,
                                datatype : Datatype<*>, userType: UserType
): Attribute.Builder<*> {
    val attb = Attribute.Builder(attname, datatype)

    val attname_p: MemorySegment = session.allocateUtf8String(attname)
    val val_p = session.allocate(nelems * userType.size)

    // apparently have to call nc_get_att(), not readAttributeValues()
    checkErr("nc_get_att", nc_get_att(grpid, varid, attname_p, val_p))
    val raw = val_p.toArray(ValueLayout.JAVA_BYTE)!!
    // seems wrong ?? convert to Opaque types
    attb.setValue(raw)
    return attb
}

@Throws(IOException::class)
private fun <T> NCheader.readEnumAttValues(session: Arena, grpid: Int, varid: Int, attname: String, nelems: Long,
                                       datatype : Datatype<T>, userType: UserType): Attribute.Builder<T> {
    val attb = Attribute.Builder(attname, datatype)

    val attname_p: MemorySegment = session.allocateUtf8String(attname)
    val val_p = session.allocate(nelems)

    // apparently have to call nc_get_att(), not readAttributeValues()
    checkErr("nc_get_att", nc_get_att(grpid, varid, attname_p, val_p))
    val ba = val_p.toArray(ValueLayout.JAVA_BYTE)!!
    // TODO im skeptical isBE shouldnt always be nativeByteOrder
    val tba = TypedByteArray(userType.enumBasePrimitiveType, ba, 0, isBE = true)
    val result = tba.convertToArrayTyped(intArrayOf(nelems.toInt()))

    attb.setValues(result.toList())
    return attb
}

// LOOK strings vs array of strings, also duplicate readCompoundAttValues
@Throws(IOException::class)
internal fun NCheader.readCompoundAttValues(session: Arena,
                                            grpid: Int, varid: Int, attname: String, nelems: Long, datatype : Datatype<*>, userType: UserType
): Attribute.Builder<*> {
    val attb = Attribute.Builder(attname, datatype)

    val attname_p: MemorySegment = session.allocateUtf8String(attname)
    val buffSize = nelems * userType.size
    val val_p = session.allocate(buffSize)

    // apparently have to call nc_get_att(), not readAttributeValues()
    // read data into val_p
    checkErr("nc_get_att", nc_get_att(grpid, varid, attname_p, val_p))
    // convert to byte array
    val raw = val_p.toArray(ValueLayout.JAVA_BYTE)!!

    val members = (userType.typedef as CompoundTypedef).members
    // class ArrayStructureData(shape : IntArray, val ba : ByteArray, val isBE: Boolean, val recsize : Int, val members : List<StructureMember<*>>)
    val sdataArray = ArrayStructureData(intArrayOf(nelems.toInt()), raw, isBE = nativeByteOrder, userType.size, members)
    sdataArray.putVlenStringsOnHeap { member, offset ->
        // this lamda is only called when datatype.isVlenString
        val address = val_p.get(ADDRESS, offset.toLong())
        // see https://stackoverflow.com/questions/77042593/indexoutofboundsexception-out-of-bound-access-on-segment-when-accessing-p
        val cString = address.reinterpret(Long.MAX_VALUE)
        // copy the string out of user memory onto Java heap
        val s = cString.getUtf8String(0)
        if (debugUserTypes) println("OK CompoundAttribute read string offset=$offset value=$s nelems=${member.nelems}")
        // TODO what about a list of strings? do we have to look at member.nelems?
        listOf(s)
        // TODO nc_free_vlen(nc_vlen_t *vl);
        //      nc_free_string(size_t len, char **data);
        //      nc_reclaim_data()
        //   see https://docs.unidata.ucar.edu/netcdf-c/4.9.3/md__2home_2vagrant_2Desktop_2netcdf-c_2docs_2internal.html#intern_vlens
    }

    attb.setValues(sdataArray.toList())
    return attb
}

////////////////////////////////////////////////////////////////

class UserType(
        val grpid: Int,
        val typeid: Int,
        val name: String,
        val size: Int, // the size of the user defined type
        val baseTypeid: Int, // the base typeid for vlen and enum types
        val typedef: Typedef,
) {

    val enumBaseType: Datatype<*>
        get() {
            // set the enum's basetype
            if (baseTypeid in 1..NC_MAX_ATOMIC_TYPE()) {
                val cdmtype = when (baseTypeid) {
                    NC_CHAR(), NC_UBYTE(), NC_BYTE() -> Datatype.ENUM1
                    NC_USHORT(), NC_SHORT() -> Datatype.ENUM2
                    NC_UINT(), NC_INT() -> Datatype.ENUM4
                    else -> throw RuntimeException("enumBaseType illegal = $baseTypeid")
                }
                return cdmtype
            }
            throw RuntimeException("enumBaseType illegal = $baseTypeid")
        }

    val enumBasePrimitiveType: Datatype<*>
        get() {
            // set the enum's basetype
            if (baseTypeid in 1..NC_MAX_ATOMIC_TYPE()) {
                val cdmtype = when (baseTypeid) {
                    NC_CHAR(), NC_UBYTE(), NC_BYTE() -> Datatype.UBYTE
                    NC_USHORT(), NC_SHORT() -> Datatype.USHORT
                    NC_UINT(), NC_INT() -> Datatype.UINT
                    else -> throw RuntimeException("enumBaseType illegal = $baseTypeid")
                }
                return cdmtype
            }
            throw RuntimeException("enumBaseType illegal = $baseTypeid")
        }

    override fun toString(): String {
        return ("UserType" + "{grpid=" + grpid + ", typeid=" + typeid + ", name='" + name + '\'' + ", size=" + size
                + ", baseTypeid=" + baseTypeid + ", typedef=" + typedef + '}')
    }
}

