package com.sunya.netchdf

import com.sunya.netchdf.hdf5.Hdf5File
import com.sunya.netchdf.testfiles.H4Files
import com.sunya.netchdf.testfiles.H5Files
import com.sunya.netchdf.testfiles.JhdfFiles
import com.sunya.netchdf.testfiles.N3Files
import com.sunya.netchdf.testfiles.N4Files
import com.sunya.netchdf.testfiles.NetchdfExtraFiles
import kotlin.test.Test

class CountVersions {

    companion object {
        fun files(): Iterator<String> {
            return sequenceOf(
                N3Files.Companion.files().asSequence(),
                N4Files.Companion.files().asSequence(),
                H5Files.Companion.files().asSequence(),
                H4Files.Companion.files().asSequence(),
                NetchdfExtraFiles.Companion.files(false).asSequence(),
                JhdfFiles.Companion.files().asSequence(),
            )
                .flatten()
                .iterator()
        }

        val versions = mutableMapOf<String, MutableList<String>>()

        val filenames = mutableMapOf<String, MutableList<String>>()
        val showAllFiles = false

        fun showVersions() {
            println("*** nfiles = ${filenames.size}")
            var dups = 0
            filenames.keys.sorted().forEach {
                val paths = filenames[it]!!
                if (paths.size > 1) {
                    println("$it")
                    paths.forEach { println("  $it") }
                }
                dups += paths.size - 1
            }
            println("*** nduplicates = ${dups}")

            if (showAllFiles) {
                println("*** nfiles = ${filenames.size}")
                filenames.keys.sorted().forEach {
                    val paths = filenames[it]!!
                    paths.forEach {path-> println("$path/$it") }
                }
            }

            val sversions = versions.toSortedMap()
            sversions.keys.forEach{ println("$it = ${sversions[it]!!.size } files") }
            val total = sversions.keys.map{ sversions[it]!!.size }.sum()
            println("total # files = $total")
        }
    }

    @Test
    fun countVersions() {
        files().forEach { filename ->
            try {
                openNetchdfFile(filename).use { ncfile ->
                    if (ncfile == null) {
                        println("Not a netchdf file=$filename ")
                    } else {
                        val paths = versions.getOrPut(ncfile.type()) { mutableListOf() }
                        paths.add(filename)
                    }
                }
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }

        showVersions()
    }

    @Test
    fun sortFileSizes() {
        val fileSizes = mutableMapOf<String, Long>()

        files().forEach { filename ->
            try {
                openNetchdfFile(filename).use { ncfile ->
                    if (ncfile == null) {
                        println("Not a netchdf file=$filename ")
                    } else {
                        fileSizes[filename ] = ncfile.size
                    }
                }
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }

        val sorted = fileSizes.toList()
            .sortedBy { (_, value) -> value }
            .toMap()

        sorted.keys.forEach{ println("${sorted[it]} == $it } files") }
    }

    @Test
    fun sortVarSizes() {
        val varSizes = mutableMapOf<String, Long>()

        files().forEach { filename ->
            try {
                openNetchdfFile(filename).use { ncfile ->
                    if (ncfile == null) {
                        println("Not a netchdf file=$filename ")
                    } else {
                        val hdf5File = if (ncfile.type() in listOf("netcdf4", "hdf")) {
                            ncfile as Hdf5File
                        } else null

                        ncfile.rootGroup().allVariables().forEach { v ->
                            val layout = hdf5File?.layoutName(v) ?: ""
                            varSizes["$filename#${v.name}#$layout"] = v.nelems
                        }
                    }
                }
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }

        val sorted = varSizes.toList()
            .sortedBy { (_, value) -> value }
            .toMap()

        sorted.keys.forEach{ println("${sorted[it]} == $it } files") }
    }
}