package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.Datatype
import com.sunya.netchdf.*
import com.sunya.netchdf.netcdfClib.NClibFile
import com.sunya.netchdf.testfiles.H5Files
import com.sunya.netchdf.testfiles.N4Files
import com.sunya.netchdf.testutils.testData
import kotlin.test.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Compare header with Hdf5File and NetcdfClibFile
// some fail when they are not actually netcdf4 files
class Hdf5Compare {

    companion object {
        fun files(): Iterator<String> {
            return sequenceOf(
                N4Files.files().asSequence(),
                H5Files.files().asSequence(),
            ).flatten().iterator()
        }
    }

    @Test
    fun testNewLibrary() {
        val filename = testData + "netchdf/haberman/iso.h5"
        CompareCdmWithClib(filename, showCdl = true)
        compareDataWithClib(filename)
    }

    @Test
    fun problemChars() {
        val filename = testData + "cdmUnitTest/formats/netcdf4/files/c0_4.nc4"
        CompareCdmWithClib(filename)
        compareDataWithClib(filename, showCdl = true, varname = "c213")
    }

    @Test
    fun problemLibraryVersion() {
        val filename = testData + "devcdm/netcdf4/tst_solar_cmp.nc"
        CompareCdmWithClib(filename, showCdl = true)
        compareDataWithClib(filename)
    }

    @Test
    fun problem() {
        val filename = testData + "devcdm/netcdf4/IntTimSciSamp.nc"
        CompareCdmWithClib(filename, showCdl = true)
        compareDataWithClib(filename)
    }

    // a compound with a member thats a type thats not a seperate typedef.
    // the obvious thing to do is to be able to add a typedef when processing the member.
    // or look for it when building H5group
    @Test
    fun compoundEnumTypedef() {
        CompareCdmWithClib(testData + "devcdm/hdf5/enumcmpnd.h5")
    }

    @Test
    fun vlenData() {
        CompareCdmWithClib(testData + "devcdm/netcdf4/tst_vlen_data.nc4")
        compareDataWithClib(testData + "devcdm/netcdf4/tst_vlen_data.nc4")
    }

    @Test
    fun compoundData() {
        CompareCdmWithClib(testData + "devcdm/netcdf4/tst_compounds.nc4")
        compareDataWithClib(testData + "devcdm/netcdf4/tst_compounds.nc4")
    }

    @Test
    fun stringData() {
        CompareCdmWithClib(testData + "devcdm/netcdf4/tst_strings.nc")
        compareDataWithClib(testData + "devcdm/netcdf4/tst_strings.nc")
    }

    @Test
    fun opaqueAttribute() {
        CompareCdmWithClib(testData + "devcdm/netcdf4/tst_opaque_data.nc4")
    }

    @Test
    fun testIterateDataSumInfinite() {
        CompareCdmWithClib(testData + "cdmUnitTest/formats/hdf5/StringsWFilter.h5")
        compareDataWithClib(testData + "cdmUnitTest/formats/hdf5/StringsWFilter.h5", varname = "/observation/matrix/data")
    }

    @Test
    fun vlstra() {
        CompareCdmWithClib(testData + "devcdm/hdf5/vlstra.h5")
    }

    //// the following wont work opening as netcdf
    @Test
    fun notNetcdf() {
        CompareCdmWithClib(testData + "devcdm/hdf5/compound_complex.h5", showCdl = true)
        CompareCdmWithClib(testData + "devcdm/hdf5/bitfield.h5", showCdl = true)
        CompareCdmWithClib(testData + "devcdm/hdf5/SDS_array_type.h5", showCdl = true)
    }

    @Test
    fun privateTypedef() {
        CompareCdmWithClib(testData + "devcdm/hdf5/bitop.h5", showCdl = true)
    }

    @Test
    fun problemCompareCdl() {
        CompareCdmWithClib(testData + "devcdm/netcdf4/testNestedStructure.nc", showCdl = true)
    }


    ////////////////////////////////////////////////////////////////////

    @Test
    fun checkVersion() {
        files().forEach { filename ->
            openNetchdfFileWithFormat(filename, NetchdfFileFormat.HDF5).use { ncfile ->
                println("${ncfile!!.type()} $filename ")
                assertTrue(
                    ncfile.type().contains("hdf5") || ncfile.type().contains("hdf-eos5")
                            || (ncfile.type().contains("netcdf4"))
                )
            }
        }
    }

    @Test
    fun compareNetchdf() {
        files().forEach { filename ->
            CompareCdmWithClib(filename)
        }
    }

    @Test
    fun testCompareDataWithClib() {
        files().forEach { filename ->
            compareDataWithClib(filename)
        }
    }

    @Test
    fun readCharDataCompareNC() {
        files().forEach { filename ->
            compareSelectedDataWithClib(filename) { it.datatype == Datatype.CHAR }
        }
    }

}

fun compareCdlWithoutFileType(cdl1: String, cdl2: String) {
    val pos1 = cdl1.indexOf(' ')
    val pos2 = cdl2.indexOf(' ')
    assertEquals(cdl1.substring(pos1), cdl2.substring(pos2))
}