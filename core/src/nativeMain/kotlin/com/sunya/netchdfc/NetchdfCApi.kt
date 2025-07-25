@file:OptIn(ExperimentalForeignApi::class)

package com.sunya.netchdfc

import com.sunya.cdm.api.*
import com.sunya.cdm.array.ArrayInt
import com.sunya.netchdf.openNetchdfFile

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.LongVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.pin

fun version() : String {
    return "netchdf version 0.4.0"
}

fun openNetchdfFile(filename : String) : Netchdf? {
    return openNetchdfFile(filename)
}

fun Netchdf.openVariable(variableFullName : String) : VariableC {
    val vc = this.findVariable(variableFullName)!!
    return VariableC(vc.name, vc.shape, vc.rank)
}

class VariableC(val varName: String, varShape: LongArray, val rank: Int) {
    val pinnedShape: CPointer<LongVar> = varShape.pin().addressOf(0)
}

fun Netchdf.readVariable(variableFullName : String) : VariableData {
    val vc = this.findVariable(variableFullName)!!
    val arrayInt = this.readArrayData(vc) as ArrayInt
    return VariableData(vc.name, vc.shape.toIntArray(), arrayInt.nelems, arrayInt.values)
}

class VariableData(val varName: String, dataShape: IntArray, val nelems: Int, data: IntArray) {
    val pinnedShape: CPointer<IntVar> = dataShape.pin().addressOf(0)
    val pinnedData: CPointer<IntVar> = data.pin().addressOf(0)
}


