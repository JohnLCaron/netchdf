package com.sunya.netchdf.hdf5

import com.sunya.netchdf.testfiles.H5Files
import com.sunya.netchdf.testutils.readNetchdfData
import kotlin.test.Test

// Sanity check read Hdf5File header, for non-netcdf4 files
class H5readTestJvm {

    companion object {
        fun files(): Iterator<String> {
            return H5Files.files()
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    // TODO this is a single test, not concurrent as it was with ParameterizedTest
    //    Anyway still dont have parallel tests

    @Test
    fun testOpenH5() {
        files().forEach { filename ->
            openH5(filename, null)
        }
    }

    @Test
    fun testReadNetchdfData() {
        files().forEach { filename ->
            readNetchdfData(filename)
        }
    }
}