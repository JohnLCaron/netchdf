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
    fun timeH5compareReading() {
        // val filename = "../core/src/commonTest/data/netcdf4/tiling.nc4"
        val filename = "/home/all/testdata/cdmUnitTest/formats/netcdf4/hiig_forec_20140208.nc"
        // val varname = "Turbulence_SIGMET_AIRMET" // "salt"
        val varname = "salt"
        Hdf5File(filename).use { myfile : Hdf5File ->
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")

            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
                ?: throw RuntimeException("cant find $varname")
            println("  ${myvar.nameAndShape()} nelems = ${myvar.nelems}")

            val timing = mutableMapOf<Int, MutableMap<String, Double>>()
            println("readArrayData")
            println("nthreads,   time in secs")
            for (nthreads in listOf(1, 2, 4, 8, 10, 16, 20, 24, 32, 40, 48)) {
                myfile.useNThreads = nthreads
                val time = measureNanoTime {
                    myfile.readArrayData(myvar)
                }
                println("$nthreads, ${time * nano}")
                val map1 = timing.getOrPut(nthreads) { mutableMapOf() }
                map1["readArrayData"] = time * nano
            }

            println("\nchunkIterator")
            println("nthreads,   time in secs")
            for (nthreads in listOf(1, 2, 4, 8, 10, 16, 20, 24, 32, 40, 48)) {
                myfile.useNThreads = nthreads
                val time = measureNanoTime {
                    myfile.chunkIterator(myvar)
                }
                println("$nthreads, ${time * nano}")
                val map1 = timing.getOrPut(nthreads) { mutableMapOf() }
                map1["chunkIterator"] = time * nano
            }

            println("\nchunksConcurrent")
            println("nthreads,   time in secs")
            for (nthreads in listOf(1, 2, 4, 8, 10, 16, 20, 24, 32, 40, 48)) {
                myfile.useNThreads = nthreads
                val time = measureNanoTime {
                    //     fun <T> readChunksConcurrent(v2: Variable<T>, lamda : (ArraySection<*>) -> Unit, done : () -> Unit,  nthreads: Int?) {
                    myfile.readChunksConcurrent(myvar, lamda = { }, { }, wantSection = null, nthreads)
                }
                println("$nthreads, ${time * nano}")
                val map1 = timing.getOrPut(nthreads) { mutableMapOf() }
                map1["chunksConcurrent"] = time * nano
            }

            val table = buildString {
                    appendLine("Read chunked data")
                    appendLine("nthreads,  time in secs")
                    val categories = timing[1]!!.keys
                    append("      ,")
                    categories.forEach { append(" $it, ") }
                    appendLine()
                    timing.keys.forEach { nthread ->
                        append("    $nthread, ")
                        val catTime = timing[nthread]!!
                        catTime.forEach { cat, time ->
                            append("$time,")
                        }
                        appendLine()
                    }
            }

            println()
            println(table)
        }
    }
}

val nano = 1.0e-9