package com.sunya.netchdf.testutils

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.util.nearlyEquals
import com.sunya.netchdf.openNetchdfFile
import kotlin.collections.iterator
import kotlin.test.assertTrue

//////////////////////////////////////////////////////////////////////////////////////
// compare reading data regular and through the chunkIterate API

fun compareNetchIterate(filename: String, varname : String? = null, compare : Boolean = true) {
    openNetchdfFile(filename).use { myfile ->
        if (myfile == null) {
            println("*** not a netchdf file = $filename")
            return
        }
        println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")
        var countChunks = 0
        if (varname != null) {
            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname } ?: throw RuntimeException("cant find $varname")
            countChunks +=  compareOneVarIterate(myfile, myvar, compare)
        } else {
            myfile.rootGroup().allVariables().forEach { it ->
                countChunks += compareOneVarIterate(myfile, it, compare)
            }
        }
        if (countChunks > 0) {
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes chunks = $countChunks")
        }
    }
}

// compare readArrayData with chunkIterator
private fun compareOneVarIterate(myFile: Netchdf, myvar: Variable<*>, compare : Boolean = true) : Int {
    val filename = myFile.location().substringAfterLast('/')
    val varBytes = myvar.nelems
    if (varBytes >= maxBytes) {
        println(" *** ${myvar.nameAndShape()} skip reading ArrayData too many bytes= $varBytes max = $maxBytes")
        return 0
    }

    val sum1 = AtomicDouble(0.0)
    val sumArrayData = if (compare) {
        val time3 = measureNanoTime {
            val arrayData = myFile.readArrayData(myvar, null)
            sumValues(arrayData, sum1)
        }
        Stats.of("readArrayData", filename, "chunk").accum(time3, 1)
        sum1.get()
    } else 0.0

    val sum2 = AtomicDouble(0.0)
    var countChunks = 0
    val time1 = measureNanoTime {
        val chunkIter = myFile.chunkIterator(myvar)
        for (pair in chunkIter) {
            // println(" ${pair.section} = ${pair.array.shape.contentToString()}")
            sumValues(pair.array, sum2)
            countChunks++
        }
    }
    val sumChunkIterator = sum2.get()
    if (compare) Stats.of("chunkIterator", filename, "chunk").accum(time1, countChunks)

    if (compare && sumChunkIterator.isFinite() && sumArrayData.isFinite()) {
        // println("  sumChunkIterator = $sumChunkIterator for ${myvar.nameAndShape()}")
        assertTrue(nearlyEquals(sumArrayData, sumChunkIterator), "chunkIterator $sumChunkIterator != $sumArrayData sumArrayData")
    }
    return countChunks
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
    val sum = AtomicDouble(0.0)
    var countChunks = 0
    val time1 = measureNanoTime {
        val chunkIter = myfile.chunkIterator(myvar)
        for (pair in chunkIter) {
            // println(" ${pair.section} = ${pair.array.shape.contentToString()}")
            sumValues(pair.array, sum)
            countChunks++
        }
    }
    Stats.of("netchdf", myfile.location(), "chunk").accum(time1, countChunks)
    val sum1 = sum.get()

    sum.set(0.0)
    countChunks = 0
    val time2 = measureNanoTime {
        val chunkIter = ncfile.chunkIterator(ncvar)
        for (pair in chunkIter) {
            // println(" ${pair.section} = ${pair.array.shape.contentToString()}")
            sumValues(pair.array, sum)
            countChunks++
        }
    }
    Stats.of("nclib", ncfile.location(), "chunk").accum(time2, countChunks)
    val sum2 = sum.get()

    if (sum1.isFinite() && sum2.isFinite()) {
        assertTrue(nearlyEquals(sum1, sum2), "$sum1 != $sum2 sum2")
        println("sum = $sum1")
    }
}

///////////////////////////////////////////////////////////
private fun sumValues(array : ArrayTyped<*>, sum : AtomicDouble) {
    if (array is ArraySingle || array is ArrayEmpty) {
        return // test fillValue the same ??
    }

    if (array.datatype.isNumber) {
        for (value in array) {
            val number = (value as Number)
            val numberd: Double = number.toDouble()
            if (numberd.isFinite()) {
                sum.getAndAdd(numberd)
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
                sum.getAndAdd(numberd)
            }
        }
    }
}

fun measureNanoTime (block: () -> Unit): Long {
    return 0
}


