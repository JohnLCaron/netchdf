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

class TestArrayTyped {

    @Test
    fun testToString() {
        val shape = intArrayOf(1,2,3)
        val size = shape.computeSize()
        val bb = ByteArray(size) { it.toByte() }

        val testArray = ArrayByte(shape, bb)
        assertEquals("0,1,2,3,4,5", testArray.showValues())
        println(testArray)
        assertEquals("class com.sunya.cdm.array.ArrayByte shape=[1, 2, 3] data=0,1,2,3,4,5\n", testArray.toString())
    }

    @Test
    fun testEquals() {
        val shape = intArrayOf(3, 2, 1)
        val size = shape.computeSize()
        val bb = ByteArray(size) { it.toByte() }

        val testArray1 = ArrayByte(shape, bb)

        assertTrue(testArray1.equals(testArray1))
        assertEquals(testArray1.hashCode(), testArray1.hashCode())
        assertTrue(ArrayTyped.valuesEqual(testArray1, testArray1))
        assertEquals(0, ArrayTyped.countDiff(testArray1, testArray1))

        val testArraySame = ArrayByte(shape, bb) // same bb. should we copy ??

        assertTrue(testArray1.equals(testArraySame))
        assertEquals(testArray1.hashCode(), testArraySame.hashCode())
        assertTrue(ArrayTyped.valuesEqual(testArray1, testArraySame))
        assertEquals(0, ArrayTyped.countDiff(testArray1, testArraySame))

        val bb2 = ByteArray(size) { (2*it).toByte() } // diferent bb.
        val testArrayDiff = ArrayByte(intArrayOf(2,3), bb2) // different shape

        assertFalse(testArray1.equals(testArrayDiff))
        assertNotEquals(testArray1.hashCode(), testArrayDiff.hashCode())
        assertFalse(ArrayTyped.valuesEqual(testArray1, testArrayDiff))
        assertEquals(5, ArrayTyped.countDiff(testArray1, testArrayDiff))

        bb2[3] = 42.toByte() // modify bb2 outside of testArrayDiff
        assertFalse(ArrayTyped.valuesEqual(testArray1, testArrayDiff))
        assertEquals(5, ArrayTyped.countDiff(testArray1, testArrayDiff))
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
                val bb = ByteArray(size) { it.toByte() }
                val testArray = ArrayByte(shape, bb)

                val sectionStart = intArrayOf(dim0/2, dim1/3, dim2/2)
                val sectionLength = intArrayOf(max(1, dim0/2), max(1,dim1/3), max(1,dim2/2))
                val section = Section(sectionStart, sectionLength, shape.toLongArray())
                val sectionArray = testArray.section(section)

                assertEquals(Datatype.BYTE, sectionArray.datatype)
                assertEquals(sectionLength.computeSize(), sectionArray.nelems)

                val full = IndexND(IndexSpace(sectionStart.toLongArray(), sectionLength.toLongArray()), shape.toLongArray())
                val odo = IndexND(IndexSpace(sectionStart.toLongArray(), sectionLength.toLongArray()), shape.toLongArray())
                odo.forEachIndexed { idx, index ->
                    // println("$idx, ${index.contentToString()} ${full.element(index)}")
                    val have = sectionArray.values.get(idx)
                    val expect = testArray.values.get(full.element(index).toInt())
                    // println("$idx, ${expect} ${have}")
                    assertEquals(expect, have)
                }
            }
        }
    }

    @Test
    fun testSection() {
        val dim0 = 3
        val dim1 = 6
        val dim2 = 1
        val shape = intArrayOf(dim0, dim1, dim2)
        val size = shape.computeSize()
        val bb = ByteArray(size) { it.toByte() }
        val testArray = ArrayByte(shape, bb)

        val sectionStart = intArrayOf(dim0/2, dim1/3, dim2/2)
        val sectionLength = intArrayOf(max(1, dim0/2), max(1,dim1/3), max(1,dim2/2))
        val section = Section(sectionStart, sectionLength, shape.toLongArray())
        val sectionArray = testArray.section(section)
        println("section= ${section}")

        assertEquals(Datatype.BYTE, sectionArray.datatype)
        assertEquals(sectionLength.computeSize(), sectionArray.nelems)

        val full = IndexND(IndexSpace(sectionStart.toLongArray(), sectionLength.toLongArray()), shape.toLongArray())
        val odo = IndexND(IndexSpace(sectionStart.toLongArray(), sectionLength.toLongArray()), shape.toLongArray())
        odo.forEachIndexed { idx, index ->
            println("$idx, ${index.contentToString()} ${full.element(index)}")
            val have = sectionArray.values.get(idx)
            val expect = testArray.values.get(full.element(index).toInt())
            println("$idx, ${expect} ${have}")
            assertEquals(expect, have)
        }
    }

    @Test
    fun testSectionSame() {
        val shape = intArrayOf(4,5,6)
        val size = shape.computeSize()
        val bb = ByteArray(size) { it.toByte() }
        val testArray = ArrayByte(shape, bb)

        val sectionStart = intArrayOf(0, 0, 0)
        val section = Section(sectionStart, shape, shape.toLongArray())
        val sectionArray = testArray.section(section)
        assertEquals(testArray, sectionArray)
    }

    @Test
    fun testSectionBad() {
        val shape = intArrayOf(4,5,6)
        val size = shape.computeSize()
        val bb = ByteArray(size) { it.toByte() }
        val testArray = ArrayByte(shape, bb)

        val ex = assertFails {
            testArray.section(Section(longArrayOf(401)))
        }
        assertEquals("Variable does not contain requested section", ex.message)
    }
}