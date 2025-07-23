package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.computeSize
import com.sunya.netchdf.hdf5filters.BitShuffleFilter
import com.sunya.netchdf.hdf5filters.Lz4Filter
import com.sunya.netchdf.hdf5filters.LzfFilter
import com.sunya.netchdf.openNetchdfFile
import kotlin.test.Test

class TestFilters {
    init {
        FilterRegistrar.registerFilter(Lz4Filter())
        FilterRegistrar.registerFilter(LzfFilter())
        FilterRegistrar.registerFilter(BitShuffleFilter())
    }

    // hdf5 test_compressed_chunked_datasets_earliest.hdf5 {
    //
    //  group: float {
    //    variables:
    //      float float32(7, 5) ;
    //      float float32lzf(7, 5) ;
    //      double float64(7, 5) ;
    //      double float64lzf(7, 5) ;
    //  }
    //
    //  group: int {
    //    variables:
    //      short int16(7, 5) ;
    //      short int16lzf(7, 5) ;
    //      int int32(7, 5) ;
    //      int int32lzf(7, 5) ;
    //      byte int8(7, 5) ;
    //      byte int8lzf(7, 5) ;
    //  }
    //}
    //   read float32
    //   read float32lzf
    //   read float64
    //   read float64lzf
    //failed to read /float/float64lzf, java.lang.RuntimeException: Unimplemented filter type= lzf name = lzf
    @Test
    fun testLzfFilter() { // 37
        val filename = "/home/stormy/dev/github/netcdf/jhdf/jhdf/src/test/resources/hdf5/test_compressed_chunked_datasets_earliest.hdf5"
        println(filename)
        readNetchdfData(filename, true, true)
    }

    // HDF5 "/home/stormy/dev/github/netcdf/jhdf/jhdf/src/test/resources/hdf5/lz4_datasets.hdf5" {
    //GROUP "/" {
    //   DATASET "float32_bs0" {
    //      DATATYPE  H5T_IEEE_F32LE
    //      DATASPACE  SIMPLE { ( 20 ) / ( 20 ) }
    //   }
    // ..
    @Test
    fun testLz4Filter() { // 37
        val filename = "/home/stormy/dev/github/netcdf/jhdf/jhdf/src/test/resources/hdf5/lz4_datasets.hdf5"
        println(filename)
        readNetchdfData(filename, true, true)
    }

    @Test
    fun testBitShuffleFilter() { // 37
        val filename = "/home/stormy/dev/github/netcdf/jhdf/jhdf/src/test/resources/hdf5/bitshuffle_datasets.hdf5"
        println(filename)
        readNetchdfData(filename, true, true)
    }

    fun readNetchdfData(filename: String, showCdl : Boolean = false, showData : Boolean = false) {
        openNetchdfFile(filename).use { myfile ->
            if (myfile == null) {
                println("*** not a netchdf file = $filename")
                return
            }
            println("--- ${myfile.type()} $filename ")
            myfile.rootGroup().allVariables().forEach { myvar ->
                val mydata = myfile.readArrayData(myvar)
                if (showCdl) println(" ${myvar.datatype} ${myvar.fullname()}${myvar.shape.contentToString()} = " +
                        "${mydata.shape.contentToString()} ${mydata.shape.computeSize()} elems" )
                if (showData) println(mydata)
            }
        }
    }
}