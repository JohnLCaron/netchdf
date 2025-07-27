@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.ArraySection
import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Section
import com.sunya.cdm.api.SectionPartial
import com.sunya.cdm.api.Variable
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.api.toLongArray
import com.sunya.cdm.array.ArrayTyped
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.transferMissingNelems
import com.sunya.cdm.util.InternalLibraryApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield

@ExperimentalCoroutinesApi
class H5chunkConcurrent<T>(val h5: H5builder, val v2: Variable<T>, wantSection: SectionPartial?, ) {
    val rafext: OpenFileExtended = h5.makeFileExtended()

    val varShape = v2.shape
    val wantSpace: IndexSpace
    val allData : Boolean
    val chunks: DataChunkSequence

    init {
        val useSection = SectionPartial.fill(wantSection, v2.shape)
        wantSpace = IndexSpace(useSection)
        allData = (wantSection == null) || (useSection == Section(varShape))

        val vinfo = v2.spObject as DataContainerVariable
        if (vinfo.mdl is DataLayoutBTreeVer1) {
            val mdl = vinfo.mdl
            chunks = BTree1data(rafext, mdl.btreeAddress, varShape, mdl.chunkDims.toLongArray())
        } else {
            throw RuntimeException()
        }
    }

    fun readChunks(nthreads: Int, lamda: (ArraySection<T>) -> Unit, done: () -> Unit) {

        runBlocking {
            val jobs = mutableListOf<Job>()
            val workers = mutableListOf<Worker>()
            val chunkProducer = produceChunks(chunks.asSequence())
            repeat(nthreads) {
                val worker = Worker()
                jobs.add( launchJob(worker, chunkProducer, lamda))
                workers.add(worker)
            }

            // wait for all jobs to be done, then close everything
            joinAll(*jobs.toTypedArray())
            workers.forEach { it.rafext.close() }
        }
        done()
    }

    private var count = 0
    private fun CoroutineScope.produceChunks(producer: Sequence<DataChunkIF>): ReceiveChannel<DataChunkIF> =
        produce {
            for (dataChunk in producer) {
                send(dataChunk)
                yield()
                count++
            }
            channel.close()
        }

    private fun CoroutineScope.launchJob(
        worker: Worker,
        input: ReceiveChannel<DataChunkIF>,
        lamda: (ArraySection<T>) -> Unit,
    ) = launch(Dispatchers.Default) {
        for (chunk: DataChunkIF in input) {
            val arraySection = worker.work(chunk)
            if (arraySection != null) lamda(arraySection)
            yield()
        }
    }

    private inner class Worker() {
        val rafext: OpenFileExtended = h5.openNewFileExtended() // here we need a seperate raf

        val vinfo: DataContainerVariable = v2.spObject as DataContainerVariable
        val h5type: H5TypeInfo
        val elemSize: Int
        val datatype: Datatype<*>
        val filters: FilterPipeline
        val state: OpenFileState

        init {
            h5type = vinfo.h5type
            elemSize = vinfo.storageDims[vinfo.storageDims.size - 1].toInt() // last one is always the elements size
            datatype = h5type.datatype()

            filters = FilterPipeline(v2.name, vinfo.mfp, h5type.isBE)

            state = OpenFileState(0L, h5type.isBE)
        }

        fun work(dataChunk : DataChunkIF) : ArraySection<T>? {
            val dataSpace = IndexSpace(v2.rank, dataChunk.offsets(), vinfo.storageDims)
            if (!allData && !wantSpace.intersects(dataSpace)) {
                return null
            }
            val useEntireChunk = wantSpace.contains(dataSpace)
            val intersectSpace = if (useEntireChunk) dataSpace else wantSpace.intersect(dataSpace)

            val ba = if (dataChunk.isMissing()) {
                if (debugChunking) println("   missing ${dataChunk.show()}")
                val sizeBytes = intersectSpace.totalElements * elemSize
                val bbmissing = ByteArray(sizeBytes.toInt())
                transferMissingNelems(vinfo.fillValue, intersectSpace.totalElements.toInt(), bbmissing, 0)
                if (debugChunking) println("   missing transfer ${intersectSpace.totalElements} fillValue=${vinfo.fillValue}")
                bbmissing
            } else {
                if (debugChunking) println("  chunkIterator=${dataChunk.show()}")
                state.pos = dataChunk.childAddress()
                val rawdata = rafext.readByteArray(state, dataChunk.chunkSize())
                val filteredData = if (dataChunk.filterMask() == null) rawdata else filters.apply(rawdata, dataChunk.filterMask()!!)
                if (useEntireChunk) {
                    filteredData
                } else {
                    val chunker = Chunker(dataSpace, wantSpace) // each DataChunkEntry has its own Chunker iteration
                    chunker.copyOut(filteredData, 0, elemSize, intersectSpace.totalElements.toInt())
                }
            }

            val array = if (h5type.datatype5 == Datatype5.Vlen) {
                // internal fun <T> H5builder.processVlenIntoArray(h5type: H5TypeInfo, shape: IntArray, ba: ByteArray, nelems: Int, elemSize : Int): ArrayTyped<T> {
                h5.processVlenIntoArray(h5type, intersectSpace.shape.toIntArray(), ba, intersectSpace.totalElements.toInt(), elemSize)
            } else {
                h5.processDataIntoArray(ba, h5type.isBE, datatype, intersectSpace.shape.toIntArray(), h5type, elemSize)
            }

            return ArraySection(array as ArrayTyped<T>, intersectSpace.section(v2.shape))
        }
    }
    val debugChunking = false
}