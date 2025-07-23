@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.ArraySection
import com.sunya.cdm.api.Datatype
import com.sunya.cdm.api.Variable
import com.sunya.cdm.api.computeSize
import com.sunya.cdm.api.toIntArray
import com.sunya.cdm.api.toLongArray
import com.sunya.cdm.iosp.OkioFile
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState
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

class H5readConcurrent(val h5file: Hdf5File, val v2: Variable<*>) {
    val h5 = h5file.header
    val varShape = v2.shape
    val chunkShape: IntArray
    val tiling: Tiling
    val nchunks: Long
    internal val rootNode: BTree1ext.BTreeNode
    val rootAddress: Long

    init {
        require(v2.spObject is DataContainerVariable)
        val vinfo = v2.spObject
        require(vinfo.mdl is DataLayoutBTreeVer1)
        val mdl = vinfo.mdl
        chunkShape = mdl.chunkDims
        tiling = Tiling(varShape, chunkShape.toLongArray())
        nchunks = tiling.tileShape.computeSize()
        val h5 = h5file.header

        val rafext: OpenFileExtended = OpenFileExtended(
            h5.raf,
            h5.isLengthLong, h5.isOffsetLong, h5.superblockStart,
        )

        val bTreeExt = BTree1ext(rafext, mdl.btreeAddress, 1, varShape, chunkShape.toLongArray())
        rootNode = bTreeExt.rootNode()
        rootAddress = mdl.btreeAddress
    }

    fun readChunks(nthreads: Int, lamda: (ArraySection<*>) -> Unit) {
        runBlocking {
            val jobs = mutableListOf<Job>()

            val chunkProducer = produceChunks(rootNode.asSequence())
            repeat(nthreads) {
                val worker = Worker(h5file.filename)
                jobs.add( launchJob(worker, chunkProducer, lamda))
            }

            // wait for all jobs to be done, then close everything
            joinAll(*jobs.toTypedArray())
        }
    }

    private var count = 0
    private fun CoroutineScope.produceChunks(producer: Sequence<Pair<Long, DataChunkIF>>): ReceiveChannel<Pair<Long, DataChunkIF>> =
        produce {
            for (datatChunk in producer) {
                send(datatChunk)
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
            lamda(arraySection)
            yield()
        }
    }

    private inner class Worker(filename: String) {
        private val raf: OpenFileIF = OkioFile(filename)
        private val rafext: OpenFileExtended = OpenFileExtended(
            raf,
            h5.isLengthLong, h5.isOffsetLong, h5.superblockStart,
        )

        // a thread-safe accessor of the btree
        private val btree1 = BTree1ext(rafext, rootAddress, 1, varShape, chunkShape.toLongArray())

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

        fun work(dataChunk : DataChunkIF) : ArraySection<*> {
            val dataSpace = IndexSpace(v2.rank, dataChunk.offsets(), vinfo.storageDims)

            val ba = if (dataChunk.isMissing()) {
                if (debugChunking) println("   missing ${dataChunk.show(tiling)}")
                val sizeBytes = dataSpace.totalElements * elemSize
                val bbmissing = ByteArray(sizeBytes.toInt())
                transferMissingNelems(vinfo.fillValue, dataSpace.totalElements.toInt(), bbmissing, 0)
                if (debugChunking) println("   missing transfer ${dataSpace.totalElements} fillValue=${vinfo.fillValue}")
                bbmissing
            } else {
                if (debugChunking) println("  chunkIterator=${dataChunk.show(tiling)}")
                state.pos = dataChunk.childAddress()
                val rawdata = h5.raf.readByteArray(state, dataChunk.chunkSize())
                if (dataChunk.filterMask() == null) rawdata else filters.apply(rawdata, dataChunk.filterMask()!!)
            }

            val array = if (h5type.datatype5 == Datatype5.Vlen) {
                // internal fun <T> H5builder.processVlenIntoArray(h5type: H5TypeInfo, shape: IntArray, ba: ByteArray, nelems: Int, elemSize : Int): ArrayTyped<T> {
                h5.processVlenIntoArray(h5type, dataSpace.shape.toIntArray(), ba, dataSpace.totalElements.toInt(), elemSize)
            } else {
                h5.processDataIntoArray(ba, h5type.isBE, datatype, dataSpace.shape.toIntArray(), h5type, elemSize)
            }

            return ArraySection(array, dataSpace.section(v2.shape))
        }
    }
    val debugChunking = false
}