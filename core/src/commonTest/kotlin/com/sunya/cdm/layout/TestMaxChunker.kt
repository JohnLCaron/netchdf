@file:OptIn(InternalLibraryApi::class)

package com.sunya.cdm.layout

import com.sunya.cdm.api.Section
import com.sunya.cdm.api.computeSize
import com.sunya.cdm.api.toLongArray
import com.sunya.cdm.util.InternalLibraryApi
import kotlin.test.*
import kotlin.test.assertEquals

/** Test [com.sunya.cdm.layout.MaxChunker]  */
class TestMaxChunker {

    @Test
    fun testMaxChunkShape() {
        /*
        testMaxChunker(intArrayOf(20, 30, 40), 30000)
        testMaxChunker(intArrayOf(20, 30, 40), 24000)
        testMaxChunker(intArrayOf(20, 30, 40), 23999)
        testMaxChunker(intArrayOf(20, 30, 40), 20000)

         */
        testMaxChunker(intArrayOf(20, 30, 40), 10000)
        testMaxChunker(intArrayOf(20, 30, 40), 3333)
        testMaxChunker(intArrayOf(20, 30, 40), 1111)
    }

    fun testMaxChunker(shape : IntArray, max : Int) {
        var totalTransfer = 0
        val chunker = MaxChunker(max, Section(shape.toLongArray()))
        // println("shape=${shape.contentToString()} total=${shape.computeSize()} maxElems=$max ")
        for (chunk in chunker) {
            // println("  chunk=${chunk}")
            totalTransfer += chunk.totalElements.toInt()
        }
        assertEquals(shape.computeSize(), totalTransfer)
    }

}