@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.computeSize
import com.sunya.cdm.api.toLongArray
import com.sunya.cdm.iosp.OkioFile
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.layout.Tiling
import com.sunya.cdm.util.InternalLibraryApi
import kotlin.test.Test

class Btree1extTest {

    @Test
    fun testBTree1ext() {
        val filename = "/home/all/testdata/cdmUnitTest/formats/netcdf4/hiig_forec_20140208.nc"
        val varname = "salt"
        Hdf5File(filename).use { myfile ->
            println("${myfile.type()} $filename ${myfile.size / 1000.0 / 1000.0} Mbytes")
            // println(myfile.cdl())

            val h5 = myfile.header

            var countChunks = 0
            val myvar = myfile.rootGroup().allVariables().find { it.fullname() == varname }
                ?: throw RuntimeException("cant find $varname")

            val raf: OpenFileIF = OkioFile(filename)
            val rafext: OpenFileExtended = OpenFileExtended(
                raf,
                h5.isLengthLong, h5.isOffsetLong, h5.superblockStart,
            )

            val varShape = myvar.shape

            require(myvar.spObject is DataContainerVariable)
            val vinfo = myvar.spObject
            require(vinfo.mdl is DataLayoutBTreeVer1)
            val mdl = vinfo.mdl
            val chunkShape = mdl.chunkDims
            val tiling = Tiling(varShape, chunkShape.toLongArray())
            val nchunks = tiling.tileShape.computeSize()

            // a thread-safe accessor of the btree
            //     val raf: OpenFileExtended,
            //    val rootNodeAddress: Long,
            //    val nodeType : Int,  // 0 = group/symbol table, 1 = raw data chunks
            //    varShape: LongArray,
            //    chunkShape: LongArray,
            val bTreeExt = BTree1ext(rafext, mdl.btreeAddress, 1, varShape, chunkShape.toLongArray())
            val rootNode = bTreeExt.rootNode()

            rootNode.asSequence().forEach { (key, value) -> println("Key: ${key}, Value: $value") }
        }
    }

}
