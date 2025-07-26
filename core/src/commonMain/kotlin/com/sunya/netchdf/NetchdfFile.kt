@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf

import com.sunya.cdm.api.Netchdf
import com.sunya.cdm.iosp.*
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.hdf4.Hdf4File
import com.sunya.netchdf.hdf5.Hdf5File
import com.sunya.netchdf.netcdf3.Netcdf3File

// TODO whats with strict anyway ?
fun openNetchdfFile(filename : String, strict : Boolean = false) : Netchdf? {
    val useFilename = filename.trim()
    OkioFile(useFilename).use { raf ->
        val format = NetchdfFileFormat.findNetcdfFormatType(raf)
        return when (format) {
            NetchdfFileFormat.NC_FORMAT_CLASSIC,
            NetchdfFileFormat.NC_FORMAT_64BIT_OFFSET,
            NetchdfFileFormat.NC_FORMAT_64BIT_DATA -> Netcdf3File(useFilename)
            NetchdfFileFormat.NC_FORMAT_NETCDF4,
            NetchdfFileFormat.NC_FORMAT_NETCDF4_CLASSIC  -> Hdf5File(useFilename, strict)
            NetchdfFileFormat.HDF5  -> Hdf5File(useFilename, strict)
            NetchdfFileFormat.HDF4  -> Hdf4File(useFilename)
            else  -> null
        }
    }
}

fun openNetchdfFileWithFormat(filename : String, format : NetchdfFileFormat) : Netchdf? {
    val useFilename = filename.trim()
    OkioFile(useFilename).use { raf ->
        return when (format) {
            NetchdfFileFormat.NC_FORMAT_CLASSIC,
            NetchdfFileFormat.NC_FORMAT_64BIT_OFFSET,
            NetchdfFileFormat.NC_FORMAT_64BIT_DATA -> Netcdf3File(useFilename)
            NetchdfFileFormat.NC_FORMAT_NETCDF4,
            NetchdfFileFormat.NC_FORMAT_NETCDF4_CLASSIC  -> Hdf5File(useFilename, false)
            NetchdfFileFormat.HDF5  -> Hdf5File(useFilename, false)
            NetchdfFileFormat.HDF4  -> Hdf4File(useFilename)
            else  -> null
        }
    }
}
