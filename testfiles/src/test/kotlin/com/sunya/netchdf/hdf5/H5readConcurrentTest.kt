@file:OptIn(ExperimentalAtomicApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.util.nearlyEquals
import com.sunya.cdm.api.Variable
import com.sunya.cdm.array.ArrayTyped
import com.sunya.netchdf.testfiles.H5Files
import com.sunya.netchdf.testutils.AtomicDouble
import com.sunya.netchdf.testutils.Stats
import com.sunya.netchdf.testutils.compareChunkReading
import com.sunya.netchdf.testutils.compareChunkReadingForVar
import com.sunya.netchdf.testutils.testData
import kotlin.collections.iterator
import kotlin.concurrent.atomics.AtomicInt
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
        readH5concurrent(testData + "cdmUnitTest/formats/netcdf4/hiig_forec_20140208.nc", "salt")
    }

    @Test
    fun testReadConcurrent() {
        files().forEach { filename ->
            readH5concurrent(filename, null)
        }
    }

    @Test
    fun compareChunkReadingProblem() {
        compareChunkReading(testData + "cdmUnitTest/formats/hdf5/HIRDLS/HIRDLS2-AFGL_b027_na.he5", "/HDFEOS/SWATHS/HIRDLS/Data_Fields/Altitude")
    }

    @Test
    fun compareChunkReadingTest() {
        files().forEach { filename ->
            compareChunkReading(filename, null)
        }
    }

    @Test
    fun testH5readConcurrent() {
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
                    myfile.readChunksConcurrent(myvar, lamda = { it -> println(" section = ${it.section}") }, { }, nthreads)
                }
                println("$nthreads, ${time * nano}")
            }
        }
    }
}

val nano = 1.0e-9

fun readH5concurrent(filename: String, varname: String? = null) {
    Hdf5File(filename).use { myfile ->
        println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")
        println(myfile.cdl())
        var countChunks = 0
        if (varname != null) {
            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
                ?: throw RuntimeException("cant find $varname")
            countChunks += compareChunkReadingForVar(myfile, myvar)
        } else {
            myfile.rootGroup().allVariables().forEach { it ->
                if (it.datatype.isNumber) {
                    countChunks += compareChunkReadingForVar(myfile, it)
                }
            }
        }
        if (countChunks > 0) {
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes chunks = $countChunks")
        }
    }
}

/*
fun testOneVarConcurrent(hdf5: Hdf5File, myvar: Variable<*>): Int {
    val filename = hdf5.location().substringAfterLast('/')

    Stats.clear()

    var sumChunkIterator = 0.0
    var countChunkIterator = 0
    val time1 = measureNanoTime {
        val chunkIter = hdf5.chunkIterator(myvar)
        for (pair in chunkIter) {
            // println(" ${pair.section} = ${pair.array.shape.contentToString()}")
            sumChunkIterator += sumValues(pair.array)
            countChunkIterator++
        }
    }
    Stats.of("chunkIterator", filename, "chunk").accum(time1, countChunkIterator)

    var sumArrayRead = 0.0
    val time3 = measureNanoTime {
        val arrayData = hdf5.readArrayData(myvar, null)
        sumArrayRead += sumValues(arrayData)
    }
    Stats.of("readArrayData", filename, "chunk").accum(time3, 1)
    assertTrue(nearlyEquals(sumChunkIterator, sumArrayRead), "sumChunkIterator $sumChunkIterator != $sumArrayRead sumArrayRead")

    val counta = AtomicInt(0)
    val suma = AtomicDouble(0.0)
    val layout = hdf5.layoutName(myvar)
    if (layout == "DataLayoutBTreeVer1") {
        val time2 = measureNanoTime {
            hdf5.readChunksConcurrent(myvar, { it ->
                suma.getAndAdd(sumValues(it.array))
                counta.fetchAndAdd(1)
            }, done = { })
        }
        Stats.of("concurrentSum", filename, "chunk").accum(time2, counta.load())
        val sumConcurrentRead = suma.get()
        assertTrue(nearlyEquals(sumConcurrentRead, sumArrayRead), "sumConcurrentRead $sumConcurrentRead != $sumArrayRead sumArrayRead")
    }

    Stats.show()

    return countChunkIterator
}

fun sumValues(array: ArrayTyped<*>): Double {
    var result = 0.0
    if (!array.datatype.isNumber) return result

    for (value in array) {
        val number = (value as Number)
        val numberd: Double = number.toDouble()
        if (numberd.isFinite()) {
            result += numberd
        }
    }
    return result
}
*/