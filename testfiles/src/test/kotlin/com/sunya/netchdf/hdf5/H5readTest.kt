package com.sunya.netchdf.hdf5

import com.sunya.netchdf.testfiles.H5Files
import com.sunya.netchdf.testutils.readNetchdfData
import com.sunya.netchdf.testutils.testData
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import kotlin.test.*

// Sanity check read Hdf5File header, for non-netcdf4 files
class H5readTest {

    companion object {
        @JvmStatic
        fun files(): Iterator<String> {
            return H5Files.files()
        }
    }

    @Test
    fun hasLinkName() {
        openH5(testData + "cdmUnitTest/formats/hdf5/aura/MLS-Aura_L2GP-BrO_v01-52-c01_2007d029.he5")
    }

    @Test
    fun opaqueAttribute() {
        openH5(testData + "devcdm/netcdf4/tst_opaque_data.nc4")
    }

    @Test
    fun groupHasCycle() {
        openH5(testData + "cdmUnitTest/formats/hdf5/groupHasCycle.h5")
    }

    @Test
    fun testEos() {
        openH5(testData + "cdmUnitTest/formats/hdf5/aura/MLS-Aura_L2GP-BrO_v01-52-c01_2007d029.he5")
    }

    @Test
    fun testNpp() {
        openH5(testData + "netchdf/npp/GATRO-SATMR_npp_d20020906_t0409572_e0410270_b19646_c20090720223122943227_devl_int.h5")
    }

    // ~/dev/github/netcdf/netchdf:$ h5dump /home/all/testdata/devcdm/netcdf4/tst_solar_cmp.nc
    //HDF5 "/home/all/testdata/devcdm/netcdf4/tst_solar_cmp.nc" {
    //GROUP "/" {
    //   ATTRIBUTE "my_favorite_wind_speeds" {
    //      DATATYPE  "/wind_vector"
    //      DATASPACE  SIMPLE { ( 3 ) / ( 3 ) }
    //      DATA {
    //      (0): {
    //            13.3,
    //            12.2
    //         },
    //      (1): {
    //            13.3,
    //            12.2
    //         },
    //      (2): {
    //            13.3,
    //            12.2
    //         }
    //      }
    //   }
    //   DATATYPE "wind_vector" H5T_COMPOUND {
    //      H5T_IEEE_F32LE "u";
    //      H5T_IEEE_F32LE "v";
    //   }
    //}
    //}
    @Test
    fun testIsNetcdf() { // why is this not isNetcdf? Because theres nothing it in to show that it is.
        val filename = testData + "devcdm/netcdf4/tst_solar_cmp.nc"
        Hdf5File(filename).use { h5file ->
            println(h5file.type())
            println(h5file.cdl())
        }
    }

    @Test
    fun testReference() {
        openH5(testData + "cdmUnitTest/formats/hdf5/msg/test.h5")
    }

    ///////////////////////////////////////////////////////////////////////////////////

    @ParameterizedTest
    @MethodSource("files")
    fun testReadH5data(filename: String) {
        openH5(filename, null)
        println("mdlClassCount")
        mdlClassCount.forEach { (key, value) -> println("  ${key} == ${value}") }
    }

    @ParameterizedTest
    @MethodSource("files")
    fun testReadNetchdfData(filename: String) {
         readNetchdfData(filename)
    }

}

/////////////////////////////////////////////////////////

fun openH5(filename: String, varname : String? = null, showCdl : Boolean = false): Boolean {
    if (showCdl) println("=================")
    println(filename)
    Hdf5File(filename).use { h5file ->
        if (showCdl) println(h5file.cdl())
        // h5file.rootGroup().allVariables().forEach { println("  ${it.fullname()}") }

        if (varname != null) {
            val h5var = h5file.rootGroup().allVariables().find { it.fullname() == varname }
                ?: throw RuntimeException("cant find $varname")
            val h5data = h5file.readArrayData(h5var)
            println(" $varname = $h5data")
        }
    }
    return true
}