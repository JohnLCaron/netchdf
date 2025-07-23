@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.*
import com.sunya.netchdf.hdf4Clib.Hdf4ClibFile
import com.sunya.netchdf.testfiles.H4Files
import com.sunya.netchdf.testutils.testData
import kotlin.test.*

class H4Ccompare {

    companion object {
        fun files(): Iterator<String> {
            return H4Files.files()
        }

        private val versions = mutableMapOf<String, MutableList<String>>()
    }

    // ODL
    @Test
    fun problemWithODL() {
        val filename = testData + "hdf4/nsidc/LAADS/MOD/MODARNSS.Abracos_Hill.A2007001.1515.005.2007003050459.hdf"
        readH4header(filename)
    }
    // /home/all/testdata/hdf4/mak/MOD13Q1.2000.049.aust.005.b01.250m_ndvi.hdf
    ///home/all/testdata/hdf4/mak/MOD13Q1.2000.049.aust.005.b01.250m_ndvi.hdf
    //home/all/testdata/hdf4/mak/MOD13Q1.2008.353.aust.005.b01.250m_ndvi.hdf
    ///home/all/testdata/hdf4/nsidc/LAADS/MOD/MODCSR_8.A2007001.005.2007012175136.hdf
    ///home/all/testdata/hdf4/nsidc/LAADS/MOD/MODCSR_D.A2007001.005.2007004142531.hdf
    ///home/all/testdata/hdf4/nsidc/LAADS/MOD/MODCSR_G.A2007001.0000.005.2007003022635.hdf

    @Test
    fun odlHasZeroDimension() {
        val filename = testData + "hdf4/nsidc/GESC/AIRS/AIRS.2007.10.17.L1B.Cal_Subset.v5.0.16.0.G07292194950.hdf"
        // readH4header(filename)
        compareData(filename, "/L1B_AIRS_Cal_Subset/Data_Fields/radiances")
    }

    // https://github.com/HDFGroup/hdf4/issues/340
    @Test
    fun issue340() {
        val filename = testData + "hdf4/ssec/2006166131201_00702_CS_2B-GEOPROF_GRANULE_P_R03_E00.hdf"
        readHCheader(filename)
        // readH4header(filename)
        // compareH4header(filename)
    }

    @Test
    fun testRasterData() {
        val filename = testData + "hdf4/nsidc/GESC/AIRS/AIRS.2006.08.28.A.L1B.Browse_AMSU.v4.0.9.0.G06241184547.hdf"
        compareH4header(filename)
        // readH4CheckUnused(filename)
        compareData(filename, null)
    }

    // Using Raster Images in a VGroup. VHRR
    @Test
    fun testRasterImageGroup() {
        val filename = testData + "hdf4/eisalt/VHRR-KALPANA_20081216_070002.hdf"
        compareH4header(filename)
        readH4CheckUnusedTags(filename)
        compareData(filename, "Curves_at_2721.35_1298.84_lookup") // fails
    }

    //// * tag=DFTAG_RI8 (202) Raster-8 image refno=  2 vclass=           offset=294 length=75
    // --- hdf4      /home/all/testdata/devcdm/hdf4/TOVS_BROWSE_MONTHLY_AM_B861001.E861031_NF.HDF

    //// * tag=DFTAG_LUT (301) Image Palette  refno=  1 vclass=           offset=18664902 length=80
    // --- hdf4      /home/all/testdata/hdf4/S2007329.L3m_DAY_CHLO_9
    // --- hdf4      /home/all/testdata/hdf4/NOAA.CRW.OAPS.25km.GCR.200402.hdf

    //// * tag=DFTAG_VG (1965) Vgroup         refno=247 vclass=Ancillary  offset=124396221 length=81 name= 'Ancillary_Data' var='null' class='Ancillary' extag=0 exref=0 version=3 name='Ancillary_Data' nelems=30 elems=1962 248,1962 249,1962 250,1962 251,1962 252,1962 253,1962 254,1962 255,1962 256,1962 257,1962 258,1962 259,1962 260,1962 261,1962 262,1962 263,1962 264,1962 265,1962 266,1962 267,1962 268,1962 269,1962 270,1962 271,1962 272,1962 273,1962 274,1962 275,1962 276,1962 277,
    // --- hdf-eos2  /home/all/testdata/hdf4/AST_L1B_00307182004110047_08122004112525.hdf
    @Test
    fun testUnusedGroup2() {
        // readH4header(testData + "hdf4/AST_L1B_00307182004110047_08122004112525.hdf")
        // readHCheader(testData + "hdf4/AST_L1B_00307182004110047_08122004112525.hdf")
        compareH4header(testData + "hdf4/AST_L1B_00307182004110047_08122004112525.hdf")
        readH4CheckUnusedTags(testData + "hdf4/AST_L1B_00307182004110047_08122004112525.hdf")
    }

    //// * tag=DFTAG_VS (1963) Vdata Storage  refno=  5 vclass=           offset=3985033 length=79
    // --- hdf-eos2  /home/all/testdata/hdf4/nsidc/AMSR_E_L2A_BrightnessTemperatures_V08_200801012345_A.hdf
    @Test
    fun testUnusedVdata() {
        compareH4header(testData + "hdf4/nsidc/AMSR_E_L2A_BrightnessTemperatures_V08_200801012345_A.hdf")
        readH4CheckUnusedTags(testData + "hdf4/nsidc/AMSR_E_L2A_BrightnessTemperatures_V08_200801012345_A.hdf")
    }

    // not working
    @Test
    fun testUnusedVhrr() {
        val filename = testData + "hdf4/eisalt/VHRR-KALPANA_20081216_070002.hdf"
        // readH4header(filename)
        compareH4header(filename)
        //compareH4header(filename)
        //readH4CheckUnused(filename)
    }

    //// * tag=DFTAG_VG (1965) Vgroup         refno=  2 vclass=DATA_GRANULE offset=158466140 length=83 name= 'DATA_GRANULE' var='null' class='DATA_GRANULE' extag=0 exref=0 version=3 name='DATA_GRANULE' nelems=3 elems=1962 4,1962 5,1965 3,
    // --- hdf4      /home/all/testdata/hdf4/nsidc/GESC/Other_TRMM/1B21.071022.56609.6.HDF
    @Test
    fun testUnusedGroup() {
        compareH4header(testData + "hdf4/nsidc/GESC/Other_TRMM/1B21.071022.56609.6.HDF")
        readH4CheckUnusedTags(testData + "hdf4/nsidc/GESC/Other_TRMM/1B21.071022.56609.6.HDF")
    }

    //// * tag=DFTAG_COMPRESSED (40) Compressed special element refno=  1 vclass=           offset=2725 length=94
    @Test
    fun testUnusedCompressed() {
        readH4CheckUnusedTags(testData + "hdf4/keegstra/MODSCW_P2009173_C4_1820_1825_2000_2005_GM03_closest_chlora.hdf")
    }

    @Test
    fun problem() {
        compareH4header(testData + "hdf4/eos/modis/MOD13Q1.A2012321.h00v08.005.2012339011757.hdf")
        readH4CheckUnusedTags(testData + "hdf4/eos/modis/MOD13Q1.A2012321.h00v08.005.2012339011757.hdf")
    }

    @Test
    fun problem2() {
        compareH4header(testData + "devcdm/hdf4/TOVS_BROWSE_MONTHLY_AM_B861001.E861031_NF.HDF")
        compareData(testData + "devcdm/hdf4/TOVS_BROWSE_MONTHLY_AM_B861001.E861031_NF.HDF", "Raster_Image_#0")
    }

    //////////////////////////////////////////////////////////////////////

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
    fun readH4headerAll() {
        files().forEach { filename ->
            readH4header(filename)
        }
    }

    fun readH4header(filename: String) {
        println("=================")
        println(filename)
        openNetchdfFileWithFormat(filename, NetchdfFileFormat.HDF4).use { myfile ->
            println(" Hdf4File = \n${myfile!!.cdl()}")
        }
    }

    @Test
    fun readHCheaderAll() {
        files().forEach { filename ->
            readHCheader(filename)
        }
    }

    fun readHCheader(filename: String) {
        println("=================")
        println(filename)
        Hdf4ClibFile(filename).use { myfile ->
            println(" Hdf4ClibFile = \n${myfile.cdl()}")
        }
    }

    @Test
    fun compareH4headerAll() {
        files().forEach { filename ->
            compareH4header(filename)
        }
    }

    fun compareH4header(filename: String) {
        println("================= compareH4header $filename")
        openNetchdfFileWithFormat(filename, NetchdfFileFormat.HDF4).use { myfile ->
            println("Hdf4File = \n${myfile!!.cdl()}")
            Hdf4ClibFile(filename).use { hcfile ->
                assertEquals(hcfile.cdl(), myfile.cdl())
            }
        }
    }

    // @Test
    fun readH4CheckUnusedTagsAll() {
        files().forEach { filename ->
            readH4CheckUnusedTags(filename)
        }
    }

    fun readH4CheckUnusedTags(filename: String) {
        // unused tags 2
        //* IP8/1             usedBy=false pos=18664902/32 rgb=-109,0,108,-112,0,111,-115,0,114,-118,0,117,-121,0,120,-124,0,123,-127,0,126,126,0,-127,123,0,-124,120,0,-121,117,0,-118,114,0,-115,111,0,-112,108,0,-109,105,0,-106,102,0,-103,99,0,-100,96,0,-97,93,0,-94,90,0,-91,87,0,-88,84,0,-85,81,0,-82,78,0,-79,75,0,-76,72,0,-73,69,0,-70,66,0,-67,63,0,-64,60,0,-61,57,0,-58,54,0,-55,51,0,-52,48,0,-49,45,0,-46,42,0,-43,39,0,-40,36,0,-37,33,0,-34,30,0,-31,27,0,-28,24,0,-25,21,0,-22,18,0,-19,15,0,-16,12,0,-13,9,0,-10,6,0,-7,0,0,-4,0,0,-1,0,5,-1,0,10,-1,0,16,-1,0,21,-1,0,26,-1,0,32,-1,0,37,-1,0,42,-1,0,48,-1,0,53,-1,0,58,-1,0,64,-1,0,69,-1,0,74,-1,0,80,-1,0,85,-1,0,90,-1,0,96,-1,0,101,-1,0,106,-1,0,112,-1,0,117,-1,0,122,-1,0,-128,-1,0,-123,-1,0,-118,-1,0,-112,-1,0,-107,-1,0,-102,-1,0,-96,-1,0,-91,-1,0,-86,-1,0,-80,-1,0,-75,-1,0,-70,-1,0,-64,-1,0,-59,-1,0,-54,-1,0,-48,-1,0,-43,-1,0,-38,-1,0,-32,-1,0,-27,-1,0,-22,-1,0,-16,-1,0,-11,-1,0,-6,-1,0,-1,-1,0,-1,-9,0,-1,-17,0,-1,-25,0,-1,-33,0,-1,-41,0,-1,-49,0,-1,-57,0,-1,-65,0,-1,-73,0,-1,-81,0,-1,-89,0,-1,-97,0,-1,-105,0,-1,-113,0,-1,-121,0,-1,127,0,-1,119,0,-1,111,0,-1,103,0,-1,95,0,-1,87,0,-1,79,0,-1,71,0,-1,63,0,-1,55,0,-1,47,0,-1,39,0,-1,31,0,-1,23,0,-1,15,0,-1,0,8,-1,0,16,-1,0,24,-1,0,32,-1,0,40,-1,0,48,-1,0,56,-1,0,64,-1,0,72,-1,0,80,-1,0,88,-1,0,96,-1,0,104,-1,0,112,-1,0,120,-1,0,-128,-1,0,-120,-1,0,-112,-1,0,-104,-1,0,-96,-1,0,-88,-1,0,-80,-1,0,-72,-1,0,-64,-1,0,-56,-1,0,-48,-1,0,-40,-1,0,-32,-1,0,-24,-1,0,-16,-1,0,-8,-1,0,-1,-1,0,-1,-5,0,-1,-9,0,-1,-13,0,-1,-17,0,-1,-21,0,-1,-25,0,-1,-29,0,-1,-33,0,-1,-37,0,-1,-41,0,-1,-45,0,-1,-49,0,-1,-53,0,-1,-57,0,-1,-61,0,-1,-65,0,-1,-69,0,-1,-73,0,-1,-77,0,-1,-81,0,-1,-85,0,-1,-89,0,-1,-93,0,-1,-97,0,-1,-101,0,-1,-105,0,-1,-109,0,-1,-113,0,-1,-117,0,-1,-121,0,-1,-125,0,-1,127,0,-1,123,0,-1,119,0,-1,115,0,-1,111,0,-1,107,0,-1,103,0,-1,99,0,-1,95,0,-1,91,0,-1,87,0,-1,83,0,-1,79,0,-1,75,0,-1,71,0,-1,67,0,-1,63,0,-1,59,0,-1,55,0,-1,51,0,-1,47,0,-1,43,0,-1,39,0,-1,35,0,-1,31,0,-1,27,0,-1,23,0,-1,19,0,-1,15,0,-1,11,0,-1,7,0,-1,3,0,-1,0,0,-6,0,0,-11,0,0,-16,0,0,-21,0,0,-26,0,0,-31,0,0,-36,0,0,-41,0,0,-46,0,0,-51,0,0,-56,0,0,-61,0,0,-66,0,0,-71,0,0,-76,0,0,-81,0,0,-86,0,0,-91,0,0,-96,0,0,-101,0,0,-106,0,0,-111,0,0,-116,0,0,-121,0,0,-126,0,0,125,0,0,120,0,0,115,0,0,110,0,0,105,0,0,0,0,0
        //* LUT/1             usedBy=false pos=18664902/32 nelems=null
        if (filename.endsWith("S2007329.L3m_DAY_CHLO_9")) return

        openNetchdfFileWithFormat(filename, NetchdfFileFormat.HDF4).use { h4file ->
            println("--- ${h4file!!.type()} $filename ")
            val hdfFile = h4file as Hdf4File
            assertTrue(0 == hdfFile.header.showTags(true, true, false))
        }
    }

    @Test
    fun compareDataAll() {
        files().forEach { filename ->
            compareData(filename, null)
        }
    }

    fun compareData(filename: String, varname: String?) {
        println("=================")
        println(filename)
        openNetchdfFileWithFormat(filename, NetchdfFileFormat.HDF4).use { myfile ->
            // println("Hdf4File = \n${myfile.cdl()}")
            Hdf4ClibFile(filename).use { ncfile ->
                compareNetcdfData(myfile!!, ncfile, varname)
            }
        }
    }

    @Test
    fun readCharDataCompareAll() {
        files().forEach { filename ->
            compareSelectedDataWithClib(filename) { it.datatype == Datatype.CHAR } //  || it.datatype == Datatype.STRING }
        }
    }
}