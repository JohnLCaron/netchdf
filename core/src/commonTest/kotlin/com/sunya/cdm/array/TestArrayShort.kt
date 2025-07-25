package com.sunya.cdm.array

import com.sunya.cdm.api.*
import com.sunya.cdm.layout.IndexND
import com.sunya.cdm.layout.IndexSpace
import com.sunya.netchdf.testutil.propTestSlowConfig
import com.sunya.netchdf.testutil.runTest
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.checkAll
import kotlin.test.*
import kotlin.math.max

class TestArrayShort {

    @Test
    fun testArrayShort() {
        val shape = intArrayOf(4,5,6)
        val size = shape.computeSize()
        val bb = ShortArray(size) { it.toShort()  }

        val testArray = ArrayShort(shape, bb)
        assertEquals(Datatype.SHORT, testArray.datatype)
        assertEquals(size, testArray.nelems)

        testArray.forEachIndexed { idx, it ->
            assertEquals(idx.toShort(), it)
        }
    }

    // fuzz test that section() works
    @Test
    fun testSectionFuzz() {
        runTest {
            checkAll(
                propTestSlowConfig,
                Arb.int(min = 1, max = 4),
                Arb.int(min = 6, max = 8),
                Arb.int(min = 1, max = 4),
            ) { dim0, dim1, dim2 ->
                val shape = intArrayOf(dim0, dim1, dim2)
                val size = shape.computeSize()
                val bb = ShortArray(size) { it.toShort()  }
                val testArray = ArrayShort(shape, bb)

                val sectionStart = intArrayOf(dim0/2, dim1/3, dim2/2)
                val sectionLength = intArrayOf(max(1, dim0/2), max(1,dim1/3), max(1,dim2/2))
                val section = Section(sectionStart, sectionLength, shape.toLongArray())
                val sectionArray = testArray.section(section)
                assertEquals(sectionLength.computeSize(), sectionArray.values.size)

                assertEquals(Datatype.SHORT, sectionArray.datatype)
                assertEquals(sectionLength.computeSize(), sectionArray.nelems)

                val full = IndexND(IndexSpace(sectionStart.toLongArray(), sectionLength.toLongArray()), shape.toLongArray())
                val odo = IndexND(IndexSpace(sectionStart.toLongArray(), sectionLength.toLongArray()), shape.toLongArray())
                odo.forEachIndexed { idx, index ->
                    val have = sectionArray.values.get(idx)
                    val expect = testArray.values.get(full.element(index).toInt())
                    assertEquals(expect, have)
                }
            }
        }
    }
}