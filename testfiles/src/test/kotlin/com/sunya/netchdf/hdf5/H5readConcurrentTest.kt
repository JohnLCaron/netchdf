@file:OptIn(ExperimentalAtomicApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.Netchdf
import com.sunya.cdm.api.Variable
import com.sunya.netchdf.openNetchdfFile
import com.sunya.netchdf.testfiles.H5Files
import com.sunya.netchdf.testutils.AtomicDouble
import com.sunya.netchdf.testutils.Stats
import com.sunya.netchdf.testutils.compareChunkReading
import com.sunya.netchdf.testutils.compareChunkReadingForVar
import com.sunya.netchdf.testutils.sumValues
import com.sunya.netchdf.testutils.testData
import kotlin.collections.iterator
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.system.measureNanoTime

import kotlin.test.*
import kotlin.use

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
        readChunksConcurrent("/home/stormy/dev/github/netcdf/netchdf/core/src/commonTest/data/netcdf4/tiling.nc4", "Turbulence_SIGMET_AIRMET")
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
        val filename = "../core/src/commonTest/data/netcdf4/tiling.nc4" // /home/stormy/dev/github/netcdf/netchdf/core/src/commonTest/data/netcdf4/tiling.nc4
        //val filename = "/home/all/testdata/cdmUnitTest/formats/netcdf4/hiig_forec_20140208.nc"
        val varname = "Turbulence_SIGMET_AIRMET" // "salt"
        //val varname = "salt"
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
                    val data = myfile.readArrayData(myvar)
                    println(" sum= ${sumValues(data)}")// , null, recurse = true, countChunks = (nthreads == 1))
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
                    var sum = 0.0
                    val iter = myfile.chunkIterator(myvar)
                    iter.forEach { chunk ->
                        sum += sumValues(chunk.array)
                    }
                    println(" sum= $sum")
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
                    val suma = AtomicDouble(0.0)
                    //     fun <T> readChunksConcurrent(v2: Variable<T>, lamda : (ArraySection<*>) -> Unit, done : () -> Unit,  nthreads: Int?) {
                    myfile.readChunksConcurrent(myvar, lamda = {
                        val sum = sumValues(it.array)
                        suma.getAndAdd(sum)
                    }, { }, wantSection = null, nthreads)

                    println(" sum= ${suma.get()}")
                }
                println("$nthreads, ${time * nano}")
                val map1 = timing.getOrPut(nthreads) { mutableMapOf() }
                map1["chunksConcurrent"] = time * nano
            }

            val table = buildString {
                    appendLine("Xeon E5-2680 24 CPUs 48 threads, Samsung PM991 SSD")
                    appendLine("67M floats, 81 chunks, with deflate")
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

fun readChunksConcurrent(filename: String, varname : String) {
    openNetchdfFile(filename).use { myfile ->
        if (myfile == null) {
            println("*** not a netchdf file = $filename")
            return
        }
        println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")
        val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname } ?: throw RuntimeException("cant find $varname")
        println(myvar.nameAndShape())
        readChunksConcurrent(myfile, myvar)
    }
}

fun readChunksConcurrent(myfile: Netchdf, myvar: Variable<*>) {
    assertTrue(myfile is Hdf5File)
    val hdf5 = myfile

    val counta = AtomicInt(0)
    val suma = AtomicDouble(0.0)
    val layout = hdf5.layoutName(myvar)
    if (layout == "DataLayoutBTreeVer1") {
        val time2 = measureNanoTime {
            hdf5.readChunksConcurrent(myvar, { it ->
                val sum = sumValues(it.array)
                suma.getAndAdd(sum)
                counta.fetchAndAdd(1)
            }, done = { })
        }
        val countConcurrentRead = counta.load()
        val sumConcurrentRead = suma.get()
        println("  nelems = $countConcurrentRead sum = $sumConcurrentRead")
    }
}