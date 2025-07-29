package com.sunya.netchdf.hdf5

import com.sunya.netchdf.testutil.readNetchdfData
import kotlin.test.Test

import com.sunya.netchdf.testutil.testData

class JhdfReadTest {
    companion object {
        // @JvmStatic
        fun files(): Iterator<String> {
            return JhdfFiles.files()
        }
    }

    /////////////////////////////////////
    @Test
    fun testReadAllJhdfFiles() {
        files().forEach { filename ->
            println(filename)
            readNetchdfData(filename, null, null, true, false)
        }
    }

}