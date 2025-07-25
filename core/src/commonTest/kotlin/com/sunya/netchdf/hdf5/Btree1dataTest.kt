@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.ArraySection
import com.sunya.cdm.api.toLongArray
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.netchdf.testutil.nano
import com.sunya.netchdf.testutil.testData
import kotlin.system.measureNanoTime
import kotlin.test.Test

class Btree1dataTest {

    // 227322 == /home/all/testdata/cdmUnitTest/formats/netcdf4/ds.mint.nc#Minimum_temperature_surface_12_Hour_Minimum#DataLayoutBTreeVer1 } files

    @Test
    fun testBTree1data() {
        val filename = testData + "netcdf4/ds.mint.nc"
        val varname = "Minimum_temperature_surface_12_Hour_Minimum"
        Hdf5File(filename).use { myfile ->
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")
            // println(myfile.cdl())

            val h5 = myfile.header

            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
                ?: throw RuntimeException("cant find $varname")
            println("  ${myvar.nameAndShape()}")

            val rafext: OpenFileExtended = h5.openFileExtended()
            val varShape = myvar.shape

            require(myvar.spObject is DataContainerVariable)
            val vinfo = myvar.spObject
            require(vinfo.mdl is DataLayoutBTreeVer1) {"must use DataLayoutBTreeVer1"}
            val mdl = vinfo.mdl
            val chunkShape = mdl.chunkDims

            // a thread-safe accessor of the btree
            val bTreeExt = BTree1data(rafext, mdl.btreeAddress, varShape, chunkShape.toLongArray())
            // val rootNode = bTreeExt.rootNode()

            bTreeExt.asSequence().forEach { (key, value) -> println("Key: ${key}, Value: ${value.show()}") }
        }
    }

    @Test
    fun testH5readConcurrent() {
        val filename = testData + "netcdf4/ds.mint.nc"
        val varname = "Minimum_temperature_surface_12_Hour_Minimum"
        Hdf5File(filename).use { myfile ->
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")

            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
                ?: throw RuntimeException("cant find $varname")

            println("nthreads,   time in secs")

            for (nthreads in listOf(1, 2, 4, 8, 10, 16, 20, 24, 32, 40, 48)) {
                val time = measureNanoTime {
                    //     fun readChunks(nthreads: Int, lamda: (ArraySection<*>) -> Unit, done: () -> Unit) {
                    val reader = H5chunkConcurrent(myfile.header, myvar, null)
                    reader.readChunks(nthreads, lamda = { asect: ArraySection<*> ->
                        // println(" section = ${asect.chunkSection}")
                    }, { }, )
                }
                println("$nthreads, ${time * nano}")
            }
        }
    }

}
