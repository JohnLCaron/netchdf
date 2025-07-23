package com.sunya.netchdf.netcdf4

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.array.ArrayString
import com.sunya.cdm.array.ArrayUByte
import com.sunya.cdm.array.makeStringsFromBytes
import com.sunya.netchdf.openNetchdfFile
import com.sunya.netchdf.testutils.testData
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class N4charTest {

    @Test
    fun testCharAttribute() {
        val filename = testData + "cdmUnitTest/formats/netcdf4/files/xma022032.nc5"
        openNetchdfFile(filename).use { myfile ->
            println("--- ${myfile!!.type()} $filename ")
            println(myfile.cdl())

            val gatt = myfile.rootGroup().attributes.find{ it.name == "date"}!!
            println("date = $gatt")
            assertEquals(Datatype.Companion.STRING, gatt.datatype)
            assertEquals(1, gatt.values.size)
            assertEquals("23/04/09", gatt.values[0])

            val crs = myfile.rootGroup().allVariables().find{ it.fullname() == "/xma/rtip/inp#4"}!!
            val att = crs.attributes.find{ it.name == "units"}!!
            assertEquals(Datatype.Companion.STRING, att.datatype)
            assertEquals(1, att.values.size)
            assertEquals("V", att.values[0])
        }
    }

    @Test
    fun testCharVariableIsString() {
        val filename = testData + "cdmUnitTest/formats/netcdf4/multiDimscale.nc4"
        openNetchdfFile(filename).use { myfile ->
            println("--- ${myfile!!.type()} $filename ")
            println(myfile.cdl())
            val v = myfile.rootGroup().variables.find{ it.name == "file_date"}!!
            assertEquals(Datatype.Companion.CHAR, v.datatype)
            val data = myfile.readArrayData(v)
            println("file_date data = $data")
            assertEquals(Datatype.Companion.STRING, data.datatype)
            assertIs<ArrayString>(data)

            val expectNames = listOf("2010-04-04T09:17:44.000000","2010-04-04T09:54:18.900000","2010-04-04T09:55:51.700000","2010-04-04T09:57:23.700000","2010-04-04T09:58:56.000000","2010-04-04T10:00:29.000000","2010-04-04T10:02:01.200000","2010-04-04T10:03:33.200000","2010-04-04T10:05:09.500000","2010-04-04T10:06:41.600000")
            data.forEachIndexed { idx, it ->
                assertEquals(expectNames[idx], it)
            }
        }
    }

    // havent found any char members. because why would you do that?
    // should test string members, but havent found any
    @Test
    fun testStringMember() {
        val filename = testData + "cdmUnitTest/formats/netcdf4/files/c0.nc"
        openNetchdfFile(filename).use { myfile ->
            println("--- ${myfile!!.type()} $filename ")
            println(myfile.cdl())
        }
    }

}