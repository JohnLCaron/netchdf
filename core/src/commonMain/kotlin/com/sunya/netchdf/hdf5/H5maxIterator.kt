package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.ArraySection
import com.sunya.cdm.api.Section
import com.sunya.cdm.api.SectionPartial
import com.sunya.cdm.api.Variable
import com.sunya.cdm.layout.MaxChunker
import com.sunya.cdm.util.InternalLibraryApi

@OptIn(InternalLibraryApi::class)
internal class H5maxIterator<T>(val h5: Hdf5File, val v2: Variable<T>, val wantSection : Section, maxElems: Int) : AbstractIterator<ArraySection<T>>() {
    private val debugChunking = false
    private val maxIterator  = MaxChunker(maxElems,  wantSection)

    override fun computeNext() {
        if (maxIterator.hasNext()) {
            val indexSection = maxIterator.next()
            if (debugChunking) println("  chunk=${indexSection}")

            val section = indexSection.section(v2.shape)
            val array = h5.readArrayData(v2, SectionPartial( section.ranges), recurse = true)
            setNext(ArraySection(array, section))
        } else {
            done()
        }
    }
}
