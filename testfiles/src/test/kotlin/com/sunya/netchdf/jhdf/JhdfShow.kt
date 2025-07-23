package com.sunya.netchdf.jhdf


import com.sunya.cdm.util.Indent
import com.sunya.netchdf.hdf5.*
// import com.sunya.netchdf.hdf5filters.*
import com.sunya.netchdf.testfiles.JhdfFiles
import com.sunya.netchdf.testfiles.jhdfTestDir
import com.sunya.netchdf.testutils.readNetchdfData
import com.sunya.netchdf.testutils.testData
import io.jhdf.CommittedDatatype
import io.jhdf.HdfFile
import io.jhdf.api.Attribute
import io.jhdf.api.Dataset
import io.jhdf.api.Group
import io.jhdf.`object`.datatype.DataType
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.nio.file.Paths
import kotlin.test.Test
import kotlin.test.assertEquals

class JhdfShow {
    companion object {
        @JvmStatic
        fun files(): Iterator<String> {
            return JhdfFiles.Companion.files()
        }
    }

/*    init {
        FilterRegistrar.registerFilter(Lz4Filter())
        FilterRegistrar.registerFilter(LzfFilter())
        FilterRegistrar.registerFilter(BitShuffleFilter())
    } */

    @Test
    fun showJhdfData() {
        val filename = jhdfTestDir + "compound_datasets_earliest.hdf5"
        showJhdfData(filename, datasetName="chunked_compound")
    }

    @Test
    fun showJhdfCdl() {
        val filename = jhdfTestDir + "compound_datasets_earliest.hdf5"
        println(filename)
        showJhdfCdl(filename)
    }

    @Test
    fun superblocks() {
        showJhdfCdl(testData + "netcdf-c_hdf5_superblocks/netcdf-c-test-files/v1_8/nc_test4__tst_xplatform2_3.nc")
        showJhdfData(testData + "netcdf-c_hdf5_superblocks/netcdf-c-test-files/v1_8/nc_test4__tst_xplatform2_3.nc", null)
    }

    fun showJhdfCdl(filename: String) {
        val path = Paths.get(filename)
        val indent = Indent(2)
        HdfFile(path).use { hdf ->
            hdf.show(indent)
        }
    }

    fun Group.show(indent: Indent) {
        println("$indent group: ${this.name} {")

        this.children.forEach { name, child ->
            if (child is Group) {
                child.show(indent.incr())
            } else if (child is Dataset) {
                child.show(indent.incr())
            } else if (child is CommittedDatatype) {
                val dataType = child.getDataType()
                dataType.show(indent.incr(), child.name)
            }
        }

        // if (this.children.size > 0) println()
        this.attributes.forEach { (key, value) -> value.show(indent.incr())}
        println("$indent }")
    }

    fun Dataset.show(indent: Indent) {
        println("$indent variable: ${this.dataType.show()} ${this.name}${this.dimensions.contentToString()}")
        this.attributes.forEach { (key, value) -> value.show(indent.incr())}
    }

    fun Attribute.show(indent: Indent) {
        println("$indent attribute: ${this.name} = ${this.data} (${this.dataType.show()})")
     }

    fun DataType.show(indent: Indent, name: String) {
        val h5Datatype = Datatype5.of(this.dataClass)
        println("$indent type ${name} ${h5Datatype}")
    }

    /////////////////////////////////////
    @ParameterizedTest
    @MethodSource("files")
    fun testReadN3data(filename: String) {
        println(filename)
        readNetchdfData(filename, null, null, true, false)
    }
}

fun showJhdfData(filename: String, datasetName:String?) {
    println(filename)
    val path = Paths.get(filename)
    HdfFile(path).use { hdf ->
        hdf.showData("/", datasetName)
    }
}

fun Group.showData(parent : String, datasetName: String? = null) {
    println(" group: ${this.name} {")

    this.children.forEach { name, child ->
        if (child is Group) {
            child.showData(parent + "/" + name)
        } else if (child is Dataset) {
            if (datasetName == null || datasetName == child.name) {
                child.showData(parent + "/" + name)
            }
        }
    }
}

fun Dataset.showData(path: String) {
    print("   dataset: ${this.dataType.show()} ${this.name}${this.dimensions.contentToString()} Dataset.getJavaType() = ${this.getJavaType().simpleName};")

    val lookup = hdfFile.getDatasetByPath(path)
    assertEquals(this, lookup)

    // data will be a java array of the dimensions of the HDF5 dataset
    val data = this.getDataFlat();
    println(" data javaClass = ${data.javaClass.simpleName}")

    if (data is Map<*,*>)
        println(showJhdfCompoundData(data))
    else
        println("     data = ${showJhdfData(data)}")
}

fun showJhdfCompoundData(dataMap: Map<*, *>)  = buildString {
    dataMap.forEach { key, arrayjhdf ->
        append(key as String)
        append(": ")
        when (arrayjhdf) {
            is BooleanArray -> arrayjhdf.forEachIndexed { idx, datahdf -> append(" ${showJhdfData(datahdf)}") }
            is ByteArray ->  arrayjhdf.forEachIndexed { idx, datahdf -> append(" ${showJhdfData( datahdf)}")  }
            is ShortArray -> arrayjhdf.forEachIndexed { idx, datahdf -> append(" ${showJhdfData( datahdf)}")  }
            is IntArray -> arrayjhdf.forEachIndexed { idx, datahdf -> append(" ${showJhdfData( datahdf)}")  }
            is LongArray -> arrayjhdf.forEachIndexed { idx, datahdf -> append(" ${showJhdfData( datahdf)}")  }
            is FloatArray -> arrayjhdf.forEachIndexed { idx, datahdf -> append(" ${showJhdfData( datahdf)}")  }
            is DoubleArray -> arrayjhdf.forEachIndexed { idx, datahdf -> append(" ${showJhdfData( datahdf)}")  }
            is Array<*> -> arrayjhdf.forEachIndexed { idx, datahdf -> append(" ${showJhdfData( datahdf!!)}")  }
            else -> append("*** ${arrayjhdf!!.javaClass.simpleName} not compared")
        }
        appendLine()
    }
}

fun showJhdfData(data: Any): String {
    return when ( data) {
        is BooleanArray -> data.contentToString()
        is ByteArray -> data.contentToString()
        is ShortArray -> data.contentToString()
        is IntArray -> data.contentToString()
        is LongArray -> data.contentToString()
        is FloatArray -> data.contentToString()
        is DoubleArray -> data.contentToString()
        is Array<*> -> data.contentToString()
        else -> data.toString()
    }
}

fun DataType.show(): String {
    val h5Datatype = Datatype5.of(this.dataClass)
    return h5Datatype.toString()
}
