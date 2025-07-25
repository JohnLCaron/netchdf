package com.sunya.netchdf.hdf4

import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.NetchdfFileFormat
import com.sunya.netchdf.openNetchdfFile
import com.sunya.netchdf.openNetchdfFileWithFormat
import com.sunya.netchdf.testfiles.H4Files
import com.sunya.netchdf.testutils.readNetchdfData
import com.sunya.netchdf.testutils.testData
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.Test
import kotlin.test.assertEquals

class H4readTest {

    companion object {
        @JvmStatic
        fun files(): Iterator<String> {
            return H4Files.files()
        }

        private val versions = mutableMapOf<String, MutableList<String>>()
    }

    @Test
    fun problem() {
        val filename = testData + "devcdm/hdfeos2/MISR_AM1_GP_GMP_P040_O003734_05.eos"
        readOneH4header(filename)
        readNetchdfData(filename, null, null, true)
    }

    @Test
    fun problem2() {
        readH4CheckUnused(testData + "hdf4/nsidc/LP_DAAC/MOD/MOD11C3.A2007032.005.2007067132438.hdf")
    }

    // * IP8/1             usedBy=false pos=18664902/32 rgb=147,0,108,144,0,111,141,0,114,138,0,117,135,0,120,132,0,123,129,0,126,126,0,129,123,0,132,120,0,135,117,0,138,114,0,141,111,0,144,108,0,147,105,0,150,102,0,153,99,0,156,96,0,159,93,0,162,90,0,165,87,0,168,84,0,171,81,0,174,78,0,177,75,0,180,72,0,183,69,0,186,66,0,189,63,0,192,60,0,195,57,0,198,54,0,201,51,0,204,48,0,207,45,0,210,42,0,213,39,0,216,36,0,219,33,0,222,30,0,225,27,0,228,24,0,231,21,0,234,18,0,237,15,0,240,12,0,243,9,0,246,6,0,249,0,0,252,0,0,255,0,5,255,0,10,255,0,16,255,0,21,255,0,26,255,0,32,255,0,37,255,0,42,255,0,48,255,0,53,255,0,58,255,0,64,255,0,69,255,0,74,255,0,80,255,0,85,255,0,90,255,0,96,255,0,101,255,0,106,255,0,112,255,0,117,255,0,122,255,0,128,255,0,133,255,0,138,255,0,144,255,0,149,255,0,154,255,0,160,255,0,165,255,0,170,255,0,176,255,0,181,255,0,186,255,0,192,255,0,197,255,0,202,255,0,208,255,0,213,255,0,218,255,0,224,255,0,229,255,0,234,255,0,240,255,0,245,255,0,250,255,0,255,255,0,255,247,0,255,239,0,255,231,0,255,223,0,255,215,0,255,207,0,255,199,0,255,191,0,255,183,0,255,175,0,255,167,0,255,159,0,255,151,0,255,143,0,255,135,0,255,127,0,255,119,0,255,111,0,255,103,0,255,95,0,255,87,0,255,79,0,255,71,0,255,63,0,255,55,0,255,47,0,255,39,0,255,31,0,255,23,0,255,15,0,255,0,8,255,0,16,255,0,24,255,0,32,255,0,40,255,0,48,255,0,56,255,0,64,255,0,72,255,0,80,255,0,88,255,0,96,255,0,104,255,0,112,255,0,120,255,0,128,255,0,136,255,0,144,255,0,152,255,0,160,255,0,168,255,0,176,255,0,184,255,0,192,255,0,200,255,0,208,255,0,216,255,0,224,255,0,232,255,0,240,255,0,248,255,0,255,255,0,255,251,0,255,247,0,255,243,0,255,239,0,255,235,0,255,231,0,255,227,0,255,223,0,255,219,0,255,215,0,255,211,0,255,207,0,255,203,0,255,199,0,255,195,0,255,191,0,255,187,0,255,183,0,255,179,0,255,175,0,255,171,0,255,167,0,255,163,0,255,159,0,255,155,0,255,151,0,255,147,0,255,143,0,255,139,0,255,135,0,255,131,0,255,127,0,255,123,0,255,119,0,255,115,0,255,111,0,255,107,0,255,103,0,255,99,0,255,95,0,255,91,0,255,87,0,255,83,0,255,79,0,255,75,0,255,71,0,255,67,0,255,63,0,255,59,0,255,55,0,255,51,0,255,47,0,255,43,0,255,39,0,255,35,0,255,31,0,255,27,0,255,23,0,255,19,0,255,15,0,255,11,0,255,7,0,255,3,0,255,0,0,250,0,0,245,0,0,240,0,0,235,0,0,230,0,0,225,0,0,220,0,0,215,0,0,210,0,0,205,0,0,200,0,0,195,0,0,190,0,0,185,0,0,180,0,0,175,0,0,170,0,0,165,0,0,160,0,0,155,0,0,150,0,0,145,0,0,140,0,0,135,0,0,130,0,0,125,0,0,120,0,0,115,0,0,110,0,0,105,0,0,0,0,0,
    // * LUT/1             usedBy=false pos=18664902/32 nelems=null
    @OptIn(InternalLibraryApi::class)
    @Test
    fun testUsedProblem() {
        val filename = testData + "hdf4/S2007329.L3m_DAY_CHLO_9"
        openNetchdfFileWithFormat(filename, NetchdfFileFormat.HDF4).use { h4file ->
            if (h4file == null) {
                println("Cant open $filename")
            } else {
                println("--- ${h4file.type()} $filename ")
                val hdf4File =  h4file as Hdf4File
                assertEquals(2, hdf4File.header.showTags(true, true, true))
            }
        }
    }

    //////////////////////////////////////////////////////////////////////
    @ParameterizedTest
    @MethodSource("files")
    fun checkVersion(filename: String) {
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

    @ParameterizedTest
    @MethodSource("files")
    fun readH4header(filename: String) {
        readOneH4header(filename)
    }

    fun readOneH4header(filename: String) {
        println("=================")
        println(filename)
        openNetchdfFileWithFormat(filename, NetchdfFileFormat.HDF4).use { h4file ->
            if (h4file == null) {
                println("Cant open $filename")
            } else {
                println(" Hdf4File = \n${h4file.cdl()}")
            }
        }
    }

    // somehow this isnt working anymore alltags not getting set ??
    //@Test
    @OptIn(InternalLibraryApi::class)
    fun readH4CheckUnused(filename: String) {
        if (!filename.endsWith("hdf4/S2007329.L3m_DAY_CHLO_9")) { // TODO remove this
            openNetchdfFileWithFormat(filename, NetchdfFileFormat.HDF4).use { h4file ->
                if (h4file == null) {
                    println("Cant open $filename")
                } else {
                    println("--- ${h4file.type()} $filename ")
                    val hdf4File =  h4file as Hdf4File
                    assertEquals(0, hdf4File.header.showTags(false, true, false))
                }
            }
        }
    }

}