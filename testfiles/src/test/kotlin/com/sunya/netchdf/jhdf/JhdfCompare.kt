package com.sunya.netchdf.jhdf

import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.EnumTypedef
import com.sunya.cdm.array.*
// import com.sunya.netchdf.hdf5filters.*
import com.sunya.netchdf.openNetchdfFile
import com.sunya.netchdf.testfiles.JhdfFiles
import com.sunya.netchdf.testfiles.jhdfTestDir
import com.sunya.netchdf.testutils.readNetchdfData
import com.sunya.netchdf.testutils.testData
import io.jhdf.HdfFile
import org.junit.jupiter.api.Assertions.assertTrue
import java.nio.file.Paths
import kotlin.test.Test
import kotlin.test.assertEquals

class JhdfCompare {
    companion object {
        @JvmStatic
        fun files(): Iterator<String> {
            return JhdfFiles.Companion.files()
        }
    }

    /*
    init {
        FilterRegistrar.registerFilter(Lz4Filter())
        FilterRegistrar.registerFilter(LzfFilter())
        FilterRegistrar.registerFilter(BitShuffleFilter())
    } */

    @Test
    fun compareOneWithJhdf() {
        // this ones pretty bad
        // val filename = jhdfTestDir + "isssue-523.hdf5"
        val filename = jhdfTestDir + "test_scalar_empty_datasets_latest.hdf5"
        compareDataWithJhdf(filename, varname="empty_uint_64", showData = true, showCdl = true)
        // compareDataWithJhdf(filename, showData = false, showCdl = true)
    }

    // @Test horror show
    fun superblocks() {
        compareDataWithJhdf(testData + "netcdf-c_hdf5_superblocks/netcdf-c-test-files/v1_8/nc_test4__tst_xplatform2_3.nc", null, true, true)
    }

    /////////////////////////////////////
    @Test
    fun readAllData() {
        files().forEach { filename ->
            println(filename)
            readNetchdfData(filename, null, null, false, false)
        }
    }

    @Test
    fun compareDataWithJhdf() {
        files().forEach { filename ->
            println("=============================")
            compareDataWithJhdf(filename)
        }
    }

    fun compareDataWithJhdf(filename: String, varname: String? = null, showData: Boolean = false, showCdl: Boolean = false) {
        println(filename)
        println(varname)

        openNetchdfFile(filename).use { myfile ->
            if (myfile == null) {
                println("*** not a netchdf file = $filename")
                return
            }
            if (showCdl) println(myfile.cdl())

            val path = Paths.get(filename)
            HdfFile(path).use { hdf ->
                myfile.rootGroup().allVariables().forEach { myvar ->
                    if (varname == null || varname == myvar.fullname())
                        try {
                            val mydata = myfile.readArrayData(myvar)
                            println("  mydata = ${myvar.datatype} ${myvar.fullname()}${myvar.shape.contentToString()}")
                            if (showData) println("[${mydata.showValues()}]")

                            val jhdfDataset = hdf.getDatasetByPath(myvar.fullname())
                            val jhdfData = jhdfDataset.getDataFlat();
                            println("  yrdata = ${jhdfDataset.dataType.show()} ${myvar.fullname()}${jhdfDataset.dimensions.contentToString()}")
                            if (showData) println("[${showJhdfData(jhdfData)}]")

                            // if (jhdfData.javaClass.isArray() && )
                            compareData(jhdfData, mydata)
                            println()
                        } catch (e: Throwable) {
                            println("*** Exception ${e.message} for $myvar")
                            e.printStackTrace()
                        }
                }
            }
        }
    }

    fun compareData(datahdf: Any, mydata: ArrayTyped<*>) {
        if (mydata is ArrayStructureData)
            compareStructureData(datahdf as Map<*, *>, mydata)
        else
            compareArrayData(datahdf, mydata)
    }

    fun compareStructureData(dataMap: Map<*, *>, mydata: ArrayStructureData) {
        val members = mydata.members
        members.forEach { member ->
            val memberdata = member.values(mydata)
            val arrayjhdf = dataMap[member.name]!!
            val jhdfIter = convertToIterator(arrayjhdf)
            while (memberdata.hasNext() && jhdfIter.hasNext()) {
                val mydata = memberdata.next()
                val mydataConvert = convertToJhdfType(mydata as Any, member.datatype)
                val yrdata = jhdfIter.next()
                if (mydataConvert != yrdata)
                    print("")
                //assertTrue(mydataConvert.contentEquals(yrdata), message = "$member")
            }
        }
    }

    fun convertToJhdfType(mydata : Any, datatype: Datatype<*>) : Any {
        // UByteArray (primitive) vs Array<UByte> (Array with UByte objects) vs List<UByte>
        // Only place we use Array<UByte> is in ArrayVlen<T>(shape : IntArray, val values : List<Array<T>>, val baseType : Datatype<T>)
        // theres no superclass for the primitive arrays. can use javaClass.isArray() and javaClass.getComponentType()

        val useType = if (datatype == Datatype.VLEN)
                datatype.typedef!!.baseType
            else
                datatype

        if (mydata is Array<*>) {
            if (useType == Datatype.UBYTE) {
                val mydatau = mydata as Array<UByte>
                return IntArray(mydata.size) { mydatau[it].toInt() }
            }
        }

        return when (mydata) {
            is UByte -> mydata.toInt()
            is UByteArray -> IntArray(mydata.size) { mydata[it].toInt() }
            else -> mydata
        }
    }

    fun convertToIterator(arrayjhdf: Any) : Iterator<Any> {
        // nested arrays
        if (arrayjhdf.javaClass.isArray()) {
            if (arrayjhdf.javaClass.getComponentType().isArray) {
                println(" array ${arrayjhdf.javaClass.name} of ${arrayjhdf.javaClass.getComponentType()}")
                return NestedArrayIterator(arrayjhdf as Array<*>)
            }
        }

        val result = mutableListOf<Any>()
        when (arrayjhdf) {
            is BooleanArray -> arrayjhdf.forEach { result.add(it) }
            is ByteArray ->  arrayjhdf.forEach { result.add(it) }
            is ShortArray -> arrayjhdf.forEach { result.add(it) }
            is IntArray -> arrayjhdf.forEach { result.add(it) }
            is LongArray -> arrayjhdf.forEach { result.add(it) }
            is FloatArray -> arrayjhdf.forEach { result.add(it) }
            is DoubleArray -> arrayjhdf.forEach { result.add(it) }
            is Array<*> -> arrayjhdf.forEach { result.add(it!!) }
            else -> {} // throw RuntimeException("*** ${arrayjhdf.javaClass.simpleName} not compared")
        }
        return result.iterator()
    }

    fun compareArrayData(yrdata: Any, mydata: ArrayTyped<*>) {
        val mydataConverted = convertArrayToJhdfType(mydata, yrdata)
        when (yrdata) {
            is BooleanArray -> {
                // why does jhdf convert H5T_STD_B8LE to BooleanArray ??
                when (mydataConverted) {
                    is ArrayUByte ->  {
                        mydataConverted.forEachIndexed { index, ubyte -> assertTrue( (ubyte != 0.toUByte()) == yrdata[index]) }
                    }
                    else -> println("*** ${yrdata.javaClass.simpleName} not compared to BooleanArray")
                }
            }
            is ByteArray ->  assertTrue(yrdata.contentEquals ((mydataConverted as ArrayByte).values))
            is ShortArray -> assertTrue(yrdata.contentEquals ((mydataConverted as ArrayShort).values))
            is IntArray -> assertTrue(yrdata.contentEquals ((mydataConverted as ArrayInt).values))
            is LongArray -> assertTrue(yrdata.contentEquals ((mydataConverted as ArrayLong).values))
            is FloatArray -> assertTrue(yrdata.contentEquals ((mydataConverted as ArrayFloat).values))
            is DoubleArray -> assertTrue(yrdata.contentEquals ((mydataConverted as ArrayDouble).values))
            is Array<*> -> {
                when (mydataConverted) {
                    is ArrayString ->  {
                        mydataConverted.forEachIndexed { index, ss -> assertTrue( ss == yrdata[index]) }
                    }
                    is ArrayOpaque ->  {
                        repeat(yrdata.size) {
                            val yr : ByteArray = yrdata[it] as ByteArray
                            val mine : ByteArray = mydataConverted.values[it]
                            assertTrue(yr.contentEquals(mine))
                        }
                    }
                    else -> throw RuntimeException("*** ${yrdata.javaClass.name} not compared to ${mydata.datatype}")
                }
            }
            else -> throw RuntimeException("*** ${yrdata.javaClass.simpleName} not compared")
        }
    }

    fun convertArrayToJhdfType(mydata : ArrayTyped<*>, yrdata: Any) : ArrayTyped<*> {
        return when (yrdata) {
            is IntArray -> {
                when (mydata) {
                    is ArrayUByte -> ArrayInt(mydata.shape, IntArray(mydata.nelems) { mydata.values[it].toInt() })
                    is ArrayUShort -> ArrayInt(mydata.shape, IntArray(mydata.nelems) { mydata.values[it].toInt() })
                    is ArrayUInt -> ArrayInt(mydata.shape, IntArray(mydata.nelems) { mydata.values[it].toInt() })
                    is ArrayULong -> ArrayLong(mydata.shape, LongArray(mydata.nelems) { mydata.values[it].toLong() })
                    else -> mydata
                }
            }
            is Array<*> -> {
                when (mydata) {
                    is ArrayUByte -> {
                        val typedef = mydata.datatype.typedef as EnumTypedef
                        ArrayString(mydata.shape, mydata.values.map { typedef.convertEnum(it.toInt()) })
                    }
                    is ArrayUShort -> {
                        val typedef = mydata.datatype.typedef as EnumTypedef
                        ArrayString(mydata.shape, mydata.values.map { typedef.convertEnum(it.toInt()) })
                    }
                    is ArrayUInt -> {
                        val typedef = mydata.datatype.typedef as EnumTypedef
                        ArrayString(mydata.shape, mydata.values.map { typedef.convertEnum(it.toInt()) })
                    }
                    is ArrayULong -> {
                        val typedef = mydata.datatype.typedef as EnumTypedef
                        ArrayString(mydata.shape, mydata.values.map { typedef.convertEnum(it.toInt()) })
                    }
                    else -> mydata
                }
            }
            else -> mydata
        }
    }

    fun compareAnyData(datahdf: Any, mydata: Any) {
        when (datahdf) {
            is Boolean -> assertEquals(datahdf, (mydata != 0.toUByte()))
            is Byte -> assertEquals(datahdf, mydata as Byte)
            is Short -> assertEquals(datahdf, mydata as Short)
            is Int -> {
                when (mydata) {
                    is UByte -> assertEquals(datahdf, mydata.toInt())
                    is Byte -> assertEquals(datahdf, mydata.toInt())
                    is Int -> assertEquals(datahdf, mydata)
                    else -> throw RuntimeException("*** ${datahdf.javaClass.simpleName} not compared")
                }
            }

            is Long -> assertEquals(datahdf, mydata as Long)
            is Float -> assertEquals(datahdf, mydata as Float)
            is Double -> assertEquals(datahdf, mydata as Double)
            is String -> {
                if (mydata is String)
                    assertEquals(datahdf, mydata)
                else if (mydata is ArrayString)
                    assertEquals(datahdf, mydata.values[0])
                else
                    println("*** ${datahdf.javaClass.simpleName} not compared as String")
            }

            is Array<*> -> {
                val mydata2 = mydata as ArrayString
                datahdf.forEachIndexed { idx, datahdf2 ->
                    val mydata2v = mydata2.values.get(idx)
                    compareAnyData(datahdf2!!, mydata2v)
                }
            }

            else -> compareArrayData(datahdf, mydata as ArrayTyped<*>)
        }

    }
}

// double iterator (iterator of iterator)
class NestedArrayIterator<T>(val array: Array<*>) : AbstractIterator<T>() {
    var arrayIterator : Iterator<*>
    var nestedIterator : Iterator<T>

    init {
        arrayIterator = array.iterator()
        val currElem = arrayIterator.next()
        if (currElem is FloatArray)
            nestedIterator = currElem.iterator() as Iterator<T>
        else if (currElem is DoubleArray)
            nestedIterator = currElem.iterator() as Iterator<T>
        else if (currElem is IntArray)
            nestedIterator = currElem.iterator() as Iterator<T>
        else if (currElem is Array<*>)
            nestedIterator = currElem.iterator() as Iterator<T>
        else
            throw RuntimeException("*** ${currElem?.javaClass?.simpleName ?: "dunno"} not compared to Array<*>")
    }

    override fun computeNext() {
        if (nestedIterator.hasNext()) {
            setNext(nestedIterator.next())

        } else if (arrayIterator.hasNext()) {
            val currElem = arrayIterator.next()
            if (currElem is FloatArray)
                nestedIterator = currElem.iterator() as Iterator<T>
            else if (currElem is DoubleArray)
                nestedIterator = currElem.iterator() as Iterator<T>
            else if (currElem is IntArray)
                nestedIterator = currElem.iterator() as Iterator<T>
            else if (currElem is Array<*>)
                nestedIterator = currElem.iterator() as Iterator<T>
            else
                throw RuntimeException("*** ${currElem?.javaClass?.simpleName ?: "dunno"} not compared to Array<*>")

        } else {
            done()
        }
    }
}