package com.sunya.netchdf

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

        fun afterAll() {
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

        afterAll()
    }
}