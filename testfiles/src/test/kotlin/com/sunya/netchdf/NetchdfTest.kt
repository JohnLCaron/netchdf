package com.sunya.netchdf

import com.sunya.cdm.api.*
import com.sunya.netchdf.testfiles.*
import com.sunya.netchdf.testutils.Stats
import com.sunya.netchdf.testutils.readNetchdfData
import com.sunya.netchdf.testutils.showNetchdfHeader
import com.sunya.netchdf.testutils.testData
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.*

// Test files opened and read through openNetchdfFile().
class NetchdfTest {

    init {
        Stats.clear() // problem with concurrent tests
    }

    companion object {
        @JvmStatic
        fun files(): Iterator<String> {
            return sequenceOf(
                N3Files.files().asSequence(),
                N4Files.files().asSequence(),
                H5Files.files().asSequence(),
                H4Files.files().asSequence(),
                NetchdfExtraFiles.files(false).asSequence(),
            )
            .flatten()
            .iterator()
        }

        fun afterAll() {
            if (versions.size > 0) {
                val sversions = versions.entries.sortedBy { it.key }.toList()
                sversions.forEach{ (key, values) -> println("$key = ${values.size} files") }
                val total = sversions.map{ it.value.size }.sum()
                println("total # files = $total")
            }
            Stats.show()
        }

        private val versions = mutableMapOf<String, MutableList<String>>()

        var showDataRead = false
        var showData = false
        var showFailedData = false
        var showCdl = false
    }

    @Test
    fun missingChunks() {
        readNetchdfData(
            testData + "cdmUnitTest/formats/netcdf4/files/xma022032.nc",
            "/xma/dialoop_back"
        )
    }

    @Test
    fun hasMissing() {
        val filename =
            testData + "cdmUnitTest/formats/netcdf4/goes16/OR_ABI-L2-CMIPF-M6C13_G16_s20230451800207_e20230451809526_c20230451810015.nc"
        readNetchdfData(filename, "CMI", SectionPartial.fromSpec(":, :"))
        readNetchdfData(filename, "DQF", SectionPartial.fromSpec(":, :"))
    }

    // @Test
    fun testNetchIterate() {
        //  *** double UpperDeschutes_t4p10_swemelt[8395, 781, 385] skip read ArrayData too many bytes= 2524250575
        //compareNetchIterate(testData + "cdmUnitTest/formats/netcdf4/UpperDeschutes_t4p10_swemelt.nc", "UpperDeschutes_t4p10_swemelt")
    }

    @Test
    fun testFractalHeap() {
        showNetchdfHeader(testData + "cdmUnitTest/formats/hdf5/SMAP_L4_SM_aup_20140115T030000_V05007_001.h5")
    }

    @Test
    fun testSimpleXY() {
        readNetchdfData(testData + "devcdm/netcdf3/simple_xy.nc", showData = true)
    }

    // this is working
    @Test
    fun readBtreeVer1() {
        readNetchdfData(testData + "/cdmUnitTest/formats/hdf5/OMI-Aura_L2-OMTO3_2009m0829t1219-o27250_v003-2009m0829t175727-2.he5",
            "/HDFEOS/SWATHS/OMI_Column_Amount_O3/Data_Fields/fc", showCdl = false, showData = false)
    }

    // this is working
    @Test
    fun readBtreeVer1complex() {
        readNetchdfData(testData + "/cdmUnitTest/formats/hdf5/OMI-Aura_L2-OMTO3_2009m0829t1219-o27250_v003-2009m0829t175727-2.he5",
            "/HDFEOS/SWATHS/OMI_Column_Amount_O3/Data_Fields/fc", showCdl = false, showData = true)
    }

//////////////////////////////////////////////////////////////////////////////////////////////////////

    // fails on hdf5 /home/all/testdata/devcdm/netcdf4/vlenInt.nc
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
    fun testShowNetchdfHeader(filename: String) {
        showNetchdfHeader(filename)
    }

    @ParameterizedTest
    @MethodSource("files")
    fun testReadNetchdfData(filename: String) {
        readNetchdfData(filename)
    }

    /* TODO too slow
    // @Test
    fun testReadNetchIterate(filename: String) {
        compareNetchIterate(filename)
    } */
}