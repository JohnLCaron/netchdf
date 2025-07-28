package com.sunya.netchdf.netcdf4

// See https://docs.unidata.ucar.edu/netcdf-c/current/attribute_conventions.html
// These attributes can occur in netCDF enhanced (netcdf-4) files beginning with version 4.4.1.
// They all are associated with the root group as global attributes, although only _NCProperties is
// actually stored in the file; the others are computed. They are hidden in the sense that they have
// no attribute number, so they can only be accessed thru the netcdf-C API calls via the name.
// Additionally, these attributes will not be counted in the number of global attributes in the root group.
//
// The simplest way to view these attributes is to use the -s flag to the ncdump command.
//
// _IsNetcdf4
// This attribute is computed by using the HDF5 API to walk the file to look for attributes specific to netcdf-4.
// False negatives are possible for a small subset of netcdf-4 files, especially those not containing dimensions.
// False positives are only possible by deliberate modifications to an existing HDF5 file thru the HDF5 API.
// For files with the _NCProperties attribute, this attribute is redundant. For files created prior to the
// introduction of the _NCProperties attribute, this may be a useful indicator of the provenance of the file.

object Netcdf4 {

    // Default fill values, used when _FillValue variable attribute is not set.
    const val  NC_FILL_BYTE: Byte = -127
    const val  NC_FILL_CHAR = 0.toChar()
    const val  NC_FILL_SHORT: Short = ((-32767).toShort())
    const val  NC_FILL_INT: Int = -2147483647
    const val  NC_FILL_FLOAT: Float = 9.9692099683868690e+36f /* near 15 * 2^119 */
    const val  NC_FILL_DOUBLE: Double = 9.9692099683868690e+36

    val  NC_FILL_UBYTE: UByte = 255.toUByte()
    val  NC_FILL_USHORT: UShort = 65535.toUShort() // whu not UShort ??
    val  NC_FILL_UINT: UInt = 4294967295L.toUInt()
    const val  NC_FILL_INT64: Long = -9223372036854775806L // 0x8000000000000002. Only bits 63 and 1 set.
    val  NC_FILL_UINT64: ULong = (-0x2L).toULong()
    const val  NC_FILL_STRING: String = ""

    const val NCPROPERTIES = "_NCProperties" //  Added  at creation time and never modified.

    // only on the multi-dimensional coordinate variables of the netCDF model (2D chars).
    // appears to hold the dimension ids of the 2 dimensions.
    const val NETCDF4_COORDINATES = "_Netcdf4Coordinates"

    // on dimension scales, holds a scalar H5T_NATIVE_INT,
    // which is the (zero-based) dimension ID for this dimension. used to maintain creation order
    const val NETCDF4_DIMID = "_Netcdf4Dimid"

    // global - when using classic model
    const val NETCDF4_STRICT = "_nc3_strict"

    val NETCDF4_SPECIAL_ATTS = listOf(NCPROPERTIES, NETCDF4_COORDINATES, NETCDF4_STRICT, NETCDF4_DIMID)

    // prepended to variable when it conflicts with dimension scale
    const val NETCDF4_NON_COORD = "_nc4_non_coord_"

    const val NETCDF4_NOT_VARIABLE = "This is a netCDF dimension but not a netCDF variable"
}