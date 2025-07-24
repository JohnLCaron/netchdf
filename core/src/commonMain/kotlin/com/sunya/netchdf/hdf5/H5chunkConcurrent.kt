@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.ArraySection
import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Section
import com.sunya.cdm.api.SectionPartial
import com.sunya.cdm.api.Variable
import com.sunya.cdm.api.computeSize
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.api.toLongArray
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.layout.Chunker
import com.sunya.cdm.layout.IndexSpace
import com.sunya.cdm.layout.Tiling
import com.sunya.cdm.layout.transferMissingNelems
import com.sunya.cdm.util.InternalLibraryApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield

class H5chunkConcurrent(h5file: Hdf5File, val v2: Variable<*>, wantSection: SectionPartial?) {
    val h5 = h5file.header
    val rafext: OpenFileExtended = h5.openFileExtended()
    internal val bTree: BTree1data

    val varShape = v2.shape
    val chunkShape: IntArray
    val tiling: Tiling
    val nchunks: Long
    // internal val rootNode: BTree1data.BTreeNode
    // val rootAddress: Long
    val wantSpace: IndexSpace
    val allData : Boolean

    init {
        val useSection = SectionPartial.fill(wantSection, v2.shape)
        wantSpace = IndexSpace(useSection)
        allData = (wantSection == null) || (useSection == Section(varShape))

        require(v2.spObject is DataContainerVariable)
        val vinfo = v2.spObject
        require(vinfo.mdl is DataLayoutBTreeVer1)
        val mdl = vinfo.mdl
        chunkShape = mdl.chunkDims
        tiling = Tiling(varShape, chunkShape.toLongArray())
        nchunks = tiling.tileShape.computeSize()

        // its not obvious you actually need a seperate raf
        bTree = BTree1data(rafext, mdl.btreeAddress, varShape, chunkShape.toLongArray())
        // rootAddress = mdl.btreeAddress
    }

    fun readChunks(nthreads: Int, lamda: (ArraySection<*>) -> Unit, done: () -> Unit) {

        runBlocking {
            val jobs = mutableListOf<Job>()
            val workers = mutableListOf<Worker>()
            val chunkProducer = produceChunks(bTree.asSequence())
            repeat(nthreads) {
                val worker = Worker()
                jobs.add( launchJob(worker, chunkProducer, lamda))
                workers.add(worker)
            }

            // wait for all jobs to be done, then close everything
            joinAll(*jobs.toTypedArray())
            workers.forEach { it.rafext.close() }
        }
        rafext.close()
        done()
    }

    private var count = 0
    private fun CoroutineScope.produceChunks(producer: Sequence<Pair<Long, DataChunkIF>>): ReceiveChannel<Pair<Long, DataChunkIF>> =
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
        input: ReceiveChannel<Pair<Long, DataChunkIF>>,
        lamda: (ArraySection<*>) -> Unit,
    ) = launch(Dispatchers.Default) {
        for (pair: Pair<Long, DataChunkIF> in input) {
            val arraySection = worker.work(pair.second)
            if (arraySection != null) lamda(arraySection)
            yield()
        }
    }

    private inner class Worker() {
        val rafext: OpenFileExtended = h5.openFileExtended() // here we need a seperate raf

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

        fun work(dataChunk : DataChunkIF) : ArraySection<*>? {
            val dataSpace = IndexSpace(v2.rank, dataChunk.offsets(), vinfo.storageDims)
            if (!allData && !wantSpace.intersects(dataSpace)) {
                return null
            }
            val useEntireChunk = wantSpace.contains(dataSpace)
            val intersectSpace = if (useEntireChunk) dataSpace else wantSpace.intersect(dataSpace)

            val ba = if (dataChunk.isMissing()) {
                if (debugChunking) println("   missing ${dataChunk.show(tiling)}")
                val sizeBytes = intersectSpace.totalElements * elemSize
                val bbmissing = ByteArray(sizeBytes.toInt())
                transferMissingNelems(vinfo.fillValue, intersectSpace.totalElements.toInt(), bbmissing, 0)
                if (debugChunking) println("   missing transfer ${intersectSpace.totalElements} fillValue=${vinfo.fillValue}")
                bbmissing
            } else {
                if (debugChunking) println("  chunkIterator=${dataChunk.show(tiling)}")
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

            return ArraySection(array, intersectSpace.section(v2.shape))
        }
    }
    val debugChunking = false
}