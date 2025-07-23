package com.sunya.netchdf.testutil

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.util.nearlyEquals
import com.sunya.netchdf.openNetchdfFile
import kotlin.test.assertTrue

//////////////////////////////////////////////////////////////////////////////////////////////////////

const val nano = 1.0e-9

fun showNetchdfHeader(filename: String) {
    println(filename)
    openNetchdfFile(filename).use { myfile ->
        if (myfile == null) {
            println("*** not a netchdf file = $filename")
            return
        }
        println(myfile.cdl())
    }
}

fun readNetchdfData(filename: String, varname: String? = null, section: SectionPartial? = null, showCdl : Boolean = false, showData : Boolean = false) {
    // println("=============================================================")
    openNetchdfFile(filename).use { myfile ->
        if (myfile == null) {
            println("*** not a netchdf file = $filename")
            return
        }
        println("--- ${myfile.type()} $filename ")
        readMyData(myfile,varname, section, showCdl, showData)
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
// just read data from myfile

fun readMyData(myfile: Netchdf, varname: String? = null, section: SectionPartial? = null, showCdl : Boolean = false, showData : Boolean = false) {

    if (showCdl) {
        println(myfile.cdl())
    }
    // println(myfile.rootGroup().allVariables().map { it.fullname() })
    if (varname != null) {
        val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
        if (myvar == null) {
            println("cant find $varname")
            return
        }
        readOneVar(myvar, myfile, section, showData)
    } else {
        myfile.rootGroup().allVariables().forEach { it ->
            readOneVar(it, myfile, null, showData)
        }
    }
}

const val maxBytes = 10_000_000

fun readOneVar(myvar: Variable<*>, myfile: Netchdf, section: SectionPartial?, showData : Boolean = false) {
    println("   read ${myvar.name}")

    val sectionF = SectionPartial.fill(section, myvar.shape)
    val nbytes = sectionF.totalElements * myvar.datatype.size
    val myvarshape = myvar.shape.toIntArray()

    if (nbytes > maxBytes) {
        if (showData) println(" * ${myvar.fullname()} read too big: ${nbytes} > $maxBytes")
    } else {
        val mydata = myfile.readArrayData(myvar, section)
        if (showData) println(" ${myvar.datatype} ${myvar.fullname()}${myvar.shape.contentToString()} = " +
                "${mydata.shape.contentToString()} ${mydata.shape.computeSize()} elems" )
        if (myvar.datatype == Datatype.CHAR) {
            testCharShape(myvarshape, mydata.shape)
        } else {
            assertTrue(myvarshape.equivalent(mydata.shape), "variable ${myvar.name}")
        }
        if (showData) println(mydata)
    }

    if (myvar.nelems > 8 && myvar.datatype != Datatype.CHAR) {
        readMiddleSection(myfile, myvar, myvar.shape)
    }
}

fun testCharShape(want: IntArray, got: IntArray) {
    val org = want.equivalent(got)
    val removeLast = removeLast(want)
    val removeLastOk = removeLast.equivalent(got)
    assertTrue(org or removeLastOk)
}

fun removeLast(org: IntArray): IntArray {
    if (org.size < 1) return org
    return IntArray(org.size - 1) { org[it] }
}

fun readMiddleSection(myfile: Netchdf, myvar: Variable<*>, shape: LongArray, showData : Boolean = false) {
    val orgSection = Section(shape)
    val middleRanges = orgSection.ranges.mapIndexed { idx, range ->
        val length = orgSection.shape[idx]
        if (length < 9) range
        else LongProgression.fromClosedRange(range.first + length / 3, range.last - length / 3, range.step)
    }
    val middleSection = Section(middleRanges, myvar.shape)
    val nbytes = middleSection.totalElements * myvar.datatype.size
    if (nbytes > maxBytes) {
        if (showData) println("  * ${myvar.fullname()}[${middleSection}] read too big: ${nbytes} > $maxBytes")
        readMiddleSection(myfile, myvar, middleSection.shape)
        return
    }

    if (myvar.name == "temperature") {
        println()
    }

    val mydata = myfile.readArrayData(myvar, SectionPartial(middleSection.ranges))
    val middleShape = middleSection.shape.toIntArray()
    if (showData) println("  ${myvar.fullname()}[$middleSection] = ${mydata.shape.contentToString()} ${mydata.shape.computeSize()} elems")
    if (myvar.datatype == Datatype.CHAR) {
        testCharShape(middleShape, mydata.shape)
    } else {
        assertTrue(middleShape.equivalent(mydata.shape), "variable ${myvar.name}")
    }
    if (showData) println(mydata)
}

