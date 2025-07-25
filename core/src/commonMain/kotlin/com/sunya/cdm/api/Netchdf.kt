package com.sunya.cdm.api

import com.sunya.cdm.array.ArrayTyped
import com.sunya.cdm.util.CdmFullNames

interface Netchdf : AutoCloseable {
    fun location() : String
    fun type() : String
    val size : Long get() = 0L
    fun rootGroup() : Group
    fun cdl() : String

    fun findVariable(fullName: String): Variable<*>? {
        return CdmFullNames(rootGroup()).findVariable(fullName)
    }

    // TODO I think the output type is not always the input type
    fun <T> readArrayData(v2: Variable<T>, wantSection: SectionPartial? = null) : ArrayTyped<T>

    // iterate over all the chunks in section, order is arbitrary. TODO where is intersection with wantSection done ??
    fun <T> chunkIterator(v2: Variable<T>, wantSection: SectionPartial? = null, maxElements : Int? = null) : Iterator<ArraySection<T>>

    // iterate over all the chunks in section, order is arbitrary, callbacks are in multiple threads.
    fun <T> readChunksConcurrent(v2: Variable<T>,
                                 lamda : (ArraySection<T>) -> Unit,
                                 done : () -> Unit,
                                 wantSection: SectionPartial? = null,
                                 nthreads: Int? = null) {
        TODO()
    }
}

// the section describes the array chunk reletive to the variable's shape.
data class ArraySection<T>(val array : ArrayTyped<T>, val chunkSection : Section) {
    fun intersect(wantSection: SectionPartial) : ArrayTyped<T> {
        // TODO ??
        return array
    }
}