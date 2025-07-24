@file:OptIn(ExperimentalAtomicApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.netchdf.testfiles.H5Files
import com.sunya.netchdf.testutils.compareChunkReading
import com.sunya.netchdf.testutils.testData
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.system.measureNanoTime

import kotlin.test.*

// Sanity check read Hdf5File header, for non-netcdf4 files
class H5readConcurrentTest {

    companion object {
        @JvmStatic
        fun files(): Iterator<String> {
            return H5Files.files()
        }
    }

    @Test
    fun sanity() {
        compareChunkReading(testData + "cdmUnitTest/formats/netcdf4/hiig_forec_20140208.nc", "salt")
    }

     // array reading is failing, btree address == -1
    @Test
    fun compareChunkReadingProblem() {
        compareChunkReading(testData + "cdmUnitTest/formats/hdf5/HIRDLS/HIRPROF-AFGL_b038_na.he5", "/HDFEOS/SWATHS/HIRDLS/Data_Fields/12.20MicronAerosolExtinction")
    }

    @Test
    fun compareChunkReadingTest() {
        files().forEach { filename ->
            compareChunkReading(filename, null)
        }
    }

    @Test
    fun compareChunkReadingTestStartFrom() {
        var skip = true
        files().forEach { filename ->
            if (filename.endsWith("HIRPROF-AFGL_b038_na.he5")) skip = false
            if (!skip) compareChunkReading(filename, null)
        }
    }

    @Test
    fun timeH5readConcurrentThreads() {
        val filename = "/home/all/testdata/cdmUnitTest/formats/netcdf4/hiig_forec_20140208.nc"
        val varname = "salt"
        Hdf5File(filename).use { myfile : Hdf5File ->
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")

            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
                ?: throw RuntimeException("cant find $varname")

            println("nthreads,   time in secs")

            for (nthreads in listOf(1, 2, 4, 8, 10, 16, 20, 24, 32, 40, 48)) {
                val time = measureNanoTime {
                    //     fun <T> readChunksConcurrent(v2: Variable<T>, lamda : (ArraySection<*>) -> Unit, done : () -> Unit,  nthreads: Int?) {
                    myfile.readChunksConcurrent(myvar, lamda = { it -> println(" section = ${it.section}") }, { }, wantSection = null, nthreads)
                }
                println("$nthreads, ${time * nano}")
            }
        }
    }
}

val nano = 1.0e-9