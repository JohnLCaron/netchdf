package com.sunya.netchdf.testfiles

import com.sunya.netchdf.testutils.testData
import com.sunya.netchdf.testutils.testFilesIn

class H5Files {

    companion object {
        fun files(): Iterator<String> {
            val devcdm =
                testFilesIn(testData + "devcdm/hdf5")
                    .withRecursion()
                    .build()

            val cdmUnitTest =
                testFilesIn(testData + "cdmUnitTest/formats/hdf5")
                    .withPathFilter { p -> !p.toString().contains("exclude") && !p.toString().contains("extLink") && !p.toString().contains("problem") }
                    .addNameFilter { name -> !name.endsWith("groupHasCycle.h5") } // /home/all/testdata/cdmUnitTest/formats/hdf5/groupHasCycle.h5
                    .addNameFilter { name -> !name.endsWith(".xml") }
                    .addNameFilter { name -> !name.contains("IASI") }
                    .withRecursion()
                    .build()

            return (devcdm + cdmUnitTest).iterator()
        }
    }
}