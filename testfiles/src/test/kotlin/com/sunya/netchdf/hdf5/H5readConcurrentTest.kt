package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.ArraySection
import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Netchdf
import com.sunya.cdm.api.Variable
import com.sunya.cdm.api.readChunksConcurrent
import com.sunya.cdm.array.ArrayTyped
import com.sunya.netchdf.testfiles.H5Files
import com.sunya.netchdf.testutils.AtomicDouble
import com.sunya.netchdf.testutils.Stats
import com.sunya.netchdf.testutils.compareChunkReading
import com.sunya.netchdf.testutils.testData
import kotlin.collections.iterator
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
    fun compareChunkIterateTest() {
        files().forEach { filename ->
            compareChunkReading(filename, null)
        }
    }


    @Test
    fun testH5readConcurrent() {
        val filename = "/home/all/testdata/cdmUnitTest/formats/netcdf4/hiig_forec_20140208.nc"
        val varname = "salt"
        Hdf5File(filename).use { myfile ->
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")

            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
                ?: throw RuntimeException("cant find $varname")

            println("nthreads,   time in secs")

            for (nthreads in listOf(1, 2, 4, 8, 10, 16, 20, 24, 32, 40, 48)) {
                val time = measureNanoTime {
                    val reader = H5readConcurrent(myfile, myvar)
                    reader.readChunks(nthreads) { asect: ArraySection<*> ->
                        // println(" section = ${asect.section}")
                    }
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
            countChunks += testOneVarConcurrent(myfile, myvar)
        } else {
            myfile.rootGroup().allVariables().forEach { it ->
                if (it.datatype.isNumber) {
                    countChunks += testOneVarConcurrent(myfile, it)
                }
            }
        }
        if (countChunks > 0) {
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes chunks = $countChunks")
        }
    }
}

fun testOneVarConcurrent(myFile: Netchdf, myvar: Variable<*>): Int {
    val filename = myFile.location().substringAfterLast('/')
    val sum = AtomicDouble(0.0)
    var countChunks = 0
    val time1 = measureNanoTime {
        val chunkIter = myFile.chunkIterator(myvar)
        for (pair in chunkIter) {
            // println(" ${pair.section} = ${pair.array.shape.contentToString()}")
            sumValues(pair.array)
            countChunks++
        }
    }
    val sum1 = sum.get()
    Stats.of("serialSum", filename, "chunk").accum(time1, countChunks)

    sum.set(0.0)
    val time2 = measureNanoTime {
        myFile.readChunksConcurrent(myvar, null) { sumValues(it.array) }
    }
    val sum2 = sum.get()
    Stats.of("concurrentSum", filename, "chunk").accum(time2, countChunks)

    sum.set(0.0)
    val time3 = measureNanoTime {
        val arrayData = myFile.readArrayData(myvar, null)
        sumValues(arrayData)
    }
    val sum3 = sum.get()
    Stats.of("regularSum", filename, "chunk").accum(time3, countChunks)

    println("    serialSum $time1")
    println("concurrentSum $time2")
    println("   regularSum $time3")

    Stats.show()

    /* if (sum1.isFinite() && sum2.isFinite() && sum3.isFinite()) {
    assertTrue(nearlyEquals(sum1, sum2), "$sum1 != $sum2 sum2")
    assertTrue(nearlyEquals(sum1, sum3), "$sum1 != $sum3 sum3")
}

 */
    return countChunks
}

var sum = AtomicDouble(0.0)
fun sumValues(array: ArrayTyped<*>) {
    if (!array.datatype.isNumber) return
        for (value in array) {
        val number = (value as Number)
        val numberd: Double = number.toDouble()
        if (numberd.isFinite()) {
            sum.getAndAdd(numberd)
        }
    }
}
