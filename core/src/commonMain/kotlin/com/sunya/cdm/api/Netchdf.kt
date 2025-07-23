package com.sunya.cdm.api

import com.sunya.cdm.array.ArrayTyped
import com.sunya.cdm.iosp.ReadChunkConcurrent
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
    fun <T> readArrayData(v2: Variable<T>, section: SectionPartial? = null) : ArrayTyped<T>

    // iterate over all the chunks in section, order is arbitrary.
    fun <T> chunkIterator(v2: Variable<T>, section: SectionPartial? = null, maxElements : Int? = null) : Iterator<ArraySection<T>>
}

// the section describes the array chunk reletive to the variable's shape.
data class ArraySection<T>(val array : ArrayTyped<T>, val section : Section)

// Experimental: read concurrently chunks of data, call back with lamda, order is arbitrary.
fun <T> Netchdf.readChunksConcurrent(v2: Variable<T>,
                                     section: SectionPartial? = null,
                                     maxElements : Int? = null,
                                     nthreads: Int = 20,
                                     lamda : (ArraySection<T>) -> Unit) {
    val reader = ReadChunkConcurrent()
    val chunkIter = this.chunkIterator( v2, section, maxElements)
    reader.readChunks(nthreads, chunkIter, lamda)
}