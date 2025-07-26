package com.sunya.netchdf

import com.sunya.netchdf.testfiles.NetchdfExtraFiles
import com.sunya.netchdf.testutils.testData
import com.sunya.netchdf.testutils.Stats
import kotlin.test.*

// Compare header using cdl(!strict) with Netchdf and NetcdfClibFile
// mostly fails in handling of types. nclib doesnt pass over all the types.
class NetchdfClibExtra {

    companion object {
        @JvmStatic
        fun files(): Iterator<String> {
            return NetchdfExtraFiles.files(false)
        }

        fun afterAll() {
            if (versions.size > 0) {
                versions.keys.forEach{ println("$it = ${versions[it]!!.size } files") }
            }
            Stats.show()
        }

        private val versions = mutableMapOf<String, MutableList<String>>()
    }

    /*
    I wonder if npp has a group cycle?
    nc_inq_natts return -107 = NetCDF: Can't open HDF5 attribute g4.grpid= 65540
    then on close:
    HDF5: infinite loop closing library
      L,D_top,G_top,T_top,F,P,P,FD,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E,E
     */
    @Test
    fun problemNPP() {
        val filename = testData + "netchdf/npp/VCBHO_npp_d20030125_t084955_e085121_b00015_c20071213022754_den_OPS_SEG.h5"
        CompareCdmWithClib(filename)
        readNetchdfData(filename, null)
        compareDataWithClib(filename)
    }

    @Test
    fun unsolved2() {
        val filename = testData + "netchdf/tomas/S3A_OL_CCDB_CHAR_AllFiles.20101019121929_1.nc4"
        // showMyHeader(filename)
        showNcHeader(filename)
        // showMyData(filename)
        CompareCdmWithClib(filename)
        //readDataCompareNC(filename)
    }

    @Test
    fun problem() {
        val filename = testData + "netchdf/martaan/RADNL_TEST_R___25PCPRR_L3__20090305T120000_20090305T120500_0001.nc"
        CompareCdmWithClib(filename)
        readNetchdfData(filename, null)
        compareDataWithClib(filename)
    }

    ///////////////////////////////////////////////////////
    @Test
    fun checkVersion() {
        files().forEach { filename ->
            openNetchdfFile(filename).use { ncfile ->
                if (ncfile == null) {
                    println("Not a netchdf file=$filename ")
                    return
                }
                println("${ncfile.type()} $filename ")
                val paths = versions.getOrPut(ncfile.type()) { mutableListOf() }
                paths.add(filename)
            }
        }
    }

    @Test
    fun compareNetchdfCdm() {
        files().forEach { filename ->
            CompareCdmWithClib(filename, showCdl = false)
        }
    }

    @Test
    fun testCompareDataWithClib() {
        files().forEach { filename ->
            compareDataWithClib(filename)
        }
    }

    // @Test
    fun testFilesAfter() {
        var skip = true
        files().forEach { filename ->
            if (filename.equals(testData + "netchdf/bird/watlev_NOAA.F.C_IKE_VIMS_3D_WITHWAVE.nc")) skip = false
            if (!skip) compareDataWithClib(filename)
        }
    }

}