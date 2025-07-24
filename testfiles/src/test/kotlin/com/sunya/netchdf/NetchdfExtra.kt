package com.sunya.netchdf

import com.sunya.netchdf.testfiles.NetchdfExtraFiles
import kotlin.test.*
import com.sunya.netchdf.testutils.Stats
import com.sunya.netchdf.testutils.readNetchdfData
import com.sunya.netchdf.testutils.testData

// Compare header using cdl(!strict) with Netchdf and NetcdfClibFile
// mostly fails in handling of types. nclib doesnt pass over all the types.
class NetchdfExtra {

    fun afterAll() {
        if (versions.size > 0) {
            val sversions = versions.entries.sortedBy { it.key }.toList()
            sversions.forEach{ (key, values) -> println("$key = ${values.size} files") }
            val total = sversions.map{ it.value.size }.sum()
            println("total # files = $total")
        }
        Stats.show()
    }

    companion object {
        @JvmStatic
        fun files(): Iterator<String> {
            return NetchdfExtraFiles.files(false)
        }

        private val versions = mutableMapOf<String, MutableList<String>>()

        const val topdir = "$testData/netchdf/"
    }

    // npp filers: superblock at file offset; reference data type
    @Test
    fun h5npp() {
        readNetchdfData(topdir + "npp/VCBHO_npp_d20030125_t084955_e085121_b00015_c20071213022754_den_OPS_SEG.h5")
        readNetchdfData(topdir + "npp/GATRO-SATMR_npp_d20020906_t0409572_e0410270_b19646_c20090720223122943227_devl_int.h5")
    }

    @Test
    fun problem() {
        val filename = topdir + "new/gdas.t12z.sfcf006.nc"
        readNetchdfData(filename, null)
    }

    ///////////////////////////////////////////////////////

    @Test
    fun checkVersion() {
        files().forEach { filename ->
            openNetchdfFile(filename).use { ncfile ->
                if (ncfile == null) {
                    println("Not a netchdf file=$filename ")
                    return
                }
                println("${ncfile.type()} $filename ")
                val paths = versions.getOrPut(ncfile.type()) { mutableListOf() }
                paths.add(filename)
            }
        }
    }

    @Test
    fun readNetchdfDataAll() {
        files().forEach { filename ->
            readNetchdfData(filename, null)
        }
    }

}