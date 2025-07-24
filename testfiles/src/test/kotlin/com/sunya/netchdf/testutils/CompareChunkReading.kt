@file:OptIn(ExperimentalAtomicApi::class)

package com.sunya.netchdf.testutils

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.util.nearlyEquals
import com.sunya.netchdf.hdf5.Hdf5File
import com.sunya.netchdf.openNetchdfFile
import kotlin.collections.iterator
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.system.measureNanoTime
import kotlin.test.assertTrue

//////////////////////////////////////////////////////////////////////////////////////
// compare reading data regular and through the chunkIterate API

fun compareChunkReading(filename: String, varname : String? = null) {
    openNetchdfFile(filename).use { myfile ->
        if (myfile == null) {
            println("*** not a netchdf file = $filename")
            return
        }
        println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")
        var countChunks = 0
        if (varname != null) {
            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname } ?: throw RuntimeException("cant find $varname")
            countChunks +=  compareChunkReadingForVar(myfile, myvar)
        } else {
            myfile.rootGroup().allVariables().forEach { it ->
                countChunks += compareChunkReadingForVar(myfile, it)
            }
        }
        if (countChunks > 0) {
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes chunks = $countChunks")
        }
    }
}

fun compareChunkReadingForVar(myfile: Netchdf, myvar: Variable<*>): Int {
    val filename = myfile.location().substringAfterLast('/')
    println("  ${myvar.nameAndShape()}")
    Stats.clear()

    var sumChunkIterator = 0.0
    var countChunkIterator = 0
    val time1 = measureNanoTime {
        val chunkIter = myfile.chunkIterator(myvar)
        for (pair in chunkIter) {
            // println(" ${pair.section} = ${pair.array.shape.contentToString()}")
            val sum = sumValues(pair.array)
            sumChunkIterator += sum
            countChunkIterator++
            // println("chunk ${pair.section} sum $sum")
            /* if (pair.section.toString().contains("[0:0][0:17][0:97][148:295]")) {
                println(pair)
            } */

        }
    }
    Stats.of("chunkIterator", filename, "chunk").accum(time1, countChunkIterator)

    var sumArrayRead = 0.0
    val time3 = measureNanoTime {
        val arrayData = myfile.readArrayData(myvar, null)
        sumArrayRead += sumValues(arrayData)
    }
    Stats.of("readArrayData", filename, "chunk").accum(time3, 1)
    assertTrue(nearlyEquals(sumChunkIterator, sumArrayRead), "sumChunkIterator $sumChunkIterator != $sumArrayRead sumArrayRead")

    if (myfile is Hdf5File) {
        val hdf5 = myfile as Hdf5File
        val counta = AtomicInt(0)
        val suma = AtomicDouble(0.0)
        val layout = hdf5.layoutName(myvar)
        if (layout == "DataLayoutBTreeVer1") {
            val time2 = measureNanoTime {
                hdf5.readChunksConcurrent(myvar, { it ->
                    val sum = sumValues(it.array)
                    suma.getAndAdd(sum)
                    counta.fetchAndAdd(1)
                    // println(" chunk ${it.section} sum $sum")
                    /* if (it.section.toString().contains("[0:0][0:17][0:97][148:295]")) {
                        println(it)
                    } */
                }, done = { })
            }
            val countConcurrentRead = counta.load()
            Stats.of("concurrentSum", filename, "chunk").accum(time2,countConcurrentRead )
            val sumConcurrentRead = suma.get()
            assertTrue(
                nearlyEquals(sumConcurrentRead, sumArrayRead),
                "sumConcurrentRead $sumConcurrentRead != $sumArrayRead sumArrayRead"
            )
        }
    }

    // Stats.show()

    return countChunkIterator
}

private fun sumValues(array : ArrayTyped<*>): Double {
    var result = 0.0

    if (array.datatype.isNumber) {
        for (value in array) {
            val number = (value as Number)
            val numberd: Double = number.toDouble()
            if (numberd.isFinite()) {
                result += numberd
            }
        }
    } else if (array.datatype.isIntegral) {
        for (value in array) {
            val useValue = when (value) {
                is UByte -> value.toByte()
                is UShort -> value.toShort()
                is UInt -> value.toInt()
                is ULong -> value.toLong()
                else -> value
            }
            val number = (useValue as Number)
            val numberd: Double = number.toDouble()
            if (numberd.isFinite()) {
                result += numberd
            }
        }
    }
    return result
}

//////////////////////////////////////////////////////////////////////////////////////
// compare reading data chunkIterate API with Netch and NC

fun compareIterateWithNC(myfile: Netchdf, ncfile: Netchdf, varname: String?, section: SectionPartial? = null) {
    if (varname != null) {
        val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
        if (myvar == null) {
            println(" *** cant find myvar $varname")
            return
        }
        val ncvar = ncfile.rootGroup().allVariables().find { it.fullname() == myvar.fullname() }
        if (ncvar == null) {
            throw RuntimeException(" *** cant find ncvar $varname")
        }
        compareOneVarIterate(myvar, myfile, ncvar, ncfile, section)
    } else {
        myfile.rootGroup().allVariables().forEach { myvar ->
            val ncvar = ncfile.rootGroup().allVariables().find { it.fullname() == myvar.fullname() }
            if (ncvar == null) {
                println(" *** cant find ${myvar.fullname()} in ncfile")
            } else {
                compareOneVarIterate(myvar, myfile, ncvar, ncfile, null)
            }
        }
    }
}

private fun compareOneVarIterate(myvar: Variable<*>, myfile: Netchdf, ncvar : Variable<*>, ncfile: Netchdf, section: SectionPartial?) {
    var countChunks = 0
    var sum1 = 0.0
    val time1 = measureNanoTime {
        val chunkIter = myfile.chunkIterator(myvar)
        for (pair in chunkIter) {
            // println(" ${pair.section} = ${pair.array.shape.contentToString()}")
            sum1 += sumValues(pair.array)
            countChunks++
        }
    }
    Stats.of("netchdf", myfile.location(), "chunk").accum(time1, countChunks)

    countChunks = 0
    var sum2 = 0.0
    val time2 = measureNanoTime {
        val chunkIter = ncfile.chunkIterator(ncvar)
        for (pair in chunkIter) {
            // println(" ${pair.section} = ${pair.array.shape.contentToString()}")
            sum2 += sumValues(pair.array)
            countChunks++
        }
    }
    Stats.of("nclib", ncfile.location(), "chunk").accum(time2, countChunks)

    if (sum1.isFinite() && sum2.isFinite()) {
        assertTrue(nearlyEquals(sum1, sum2), "$sum1 != $sum2 sum2")
        println("sum = $sum1")
    }
}



