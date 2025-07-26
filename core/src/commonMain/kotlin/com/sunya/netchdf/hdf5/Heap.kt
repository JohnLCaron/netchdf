@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.api.*
import com.sunya.cdm.array.*
import com.sunya.cdm.iosp.*
import com.sunya.cdm.util.InternalLibraryApi

private const val debugHeap = false

internal class H5heap(val header: H5builder) {
    val raf = header.raf
    private val heapMap = mutableMapOf<Long, GlobalHeap>()

    /**
     * Fetch a Vlen data array.
     *
     * @param globalHeapIdAddress address of the heapId, used to get the data array out of the heap
     * @param datatype type of data
     * @param isBE byteOrder of the data (true = BE, false = LE)
     * @return the Array read from the heap
     */
    fun getHeapDataArray(globalHeapIdAddress: Long, datatype: Datatype<*>, isBE: Boolean): Array<*> {
        val heapId: HeapIdentifier = readHeapIdentifier(globalHeapIdAddress)
        return getHeapDataArray(heapId, datatype, isBE)
    }

    fun getHeapDataArray(heapId: HeapIdentifier, datatype: Datatype<*>, isBE: Boolean): Array<*> {
        val ho = heapId.getHeapObject() ?: return when (datatype) { // LOOK set nelems = 0 ??
            Datatype.FLOAT -> emptyArray<Float>()
            Datatype.DOUBLE -> emptyArray<Double>()
            Datatype.BYTE -> emptyArray<Byte>()
            Datatype.UBYTE, Datatype.ENUM1 -> emptyArray<UByte>()
            Datatype.SHORT -> emptyArray<Short>()
            Datatype.USHORT, Datatype.ENUM2 -> emptyArray<UShort>()
            Datatype.INT -> emptyArray<Int>()
            Datatype.UINT, Datatype.ENUM4 -> emptyArray<UInt>()
            Datatype.LONG -> emptyArray<Long>()
            Datatype.ULONG, Datatype.ENUM8 -> emptyArray<ULong>()
            Datatype.COMPOUND -> emptyArray<ArrayStructureData.StructureData>()
            else -> throw UnsupportedOperationException("getHeapDataArray heap object not found; datatype=$datatype")
        }

        val typedef = datatype.typedef
        val valueDatatype = typedef?.baseType ?: datatype

        val state = OpenFileState(ho.dataPos, isBE)
        val result = when (valueDatatype) {
            Datatype.FLOAT -> raf.readArrayOfFloat(state, heapId.nelems)
            Datatype.DOUBLE -> raf.readArrayOfDouble(state, heapId.nelems)
            Datatype.BYTE -> raf.readArrayOfByte(state, heapId.nelems)
            Datatype.UBYTE, Datatype.ENUM1 -> raf.readArrayOfUByte(state, heapId.nelems)
            Datatype.SHORT -> raf.readArrayOfShort(state, heapId.nelems)
            Datatype.USHORT, Datatype.ENUM2 -> raf.readArrayOfUShort(state, heapId.nelems)
            Datatype.INT -> raf.readArrayOfInt(state, heapId.nelems)
            Datatype.UINT, Datatype.ENUM4 -> raf.readArrayOfUInt(state, heapId.nelems)
            Datatype.LONG -> raf.readArrayOfLong(state, heapId.nelems)
            Datatype.ULONG, Datatype.ENUM8 -> raf.readArrayOfULong(state, heapId.nelems)
            Datatype.COMPOUND -> {
                val members = (datatype.typedef as CompoundTypedef).members
                val recsize = members.sumOf { it.nelems * it.datatype.size }
                val ba = raf.readByteArray(state, heapId.nelems * recsize)
                // class ArrayStructureData(shape : IntArray, val ba : ByteArray, val isBE: Boolean, val recsize : Int, val members : List<StructureMember<*>>)
                val asd = ArrayStructureData(intArrayOf(heapId.nelems), ba, isBE, recsize, members)
                // TODO not doing variable length fields processCompoundData(sdataArray, isBE) as ArrayTyped<T>
                Array(heapId.nelems) {  asd.get(it) } // convert to ArrayArrayStructureData.StructureData>
            }
            else -> throw UnsupportedOperationException("getHeapDataAsArray datatype=$datatype")
        }
        return result
    }

    /**
     * Fetch a String from the heap, when the heap identifier has already been put into a ByteBuffer at given pos
     *
     * @param bb heap id is here
     * @param pos at this position
     * @return String the String read from the heap
     */
    fun readHeapString(bb: ByteArray, pos: Int): String? {
        val heapId = HeapIdentifier(bb, pos)
        if (heapId.isEmpty()) {
            return null
        }
        val ho = heapId.getHeapObject() ?: throw IllegalStateException("Cant find Heap Object,heapId=$heapId")
        val state = OpenFileState(ho.dataPos, false) // TODO why false ??
        return raf.readString(state, ho.dataSize.toInt())
    }

    /**
     * Fetch a String from the heap.
     *
     * @param heapIdAddress address of the heapId, used to get the String out of the heap
     * @return String the String read from the heap
     */
    fun readHeapString(heapIdAddress: Long): String? {
        val heapId = this.readHeapIdentifier(heapIdAddress)
        if (heapId.isEmpty()) {
            return null // H5builder.NULL_STRING_VALUE
        }
        val ho: GlobalHeap.HeapObject = heapId.getHeapObject()
            ?: throw IllegalStateException("Cant find Heap Object,heapId=$heapId")
        if (ho.dataSize == 0L) return null
        if (ho.dataSize > 1000 * 1000) return "Bad HeapObject.dataSize=$ho"
        val state = OpenFileState(ho.dataPos, false)
        return raf.readString(state, ho.dataSize.toInt(), header.valueCharset)
    }

    // see "Global Heap Id" in http://www.hdfgroup.org/HDF5/doc/H5.format.html
    fun readHeapIdentifier(globalHeapIdAddress: Long): HeapIdentifier {
        return HeapIdentifier(globalHeapIdAddress)
    }

    // the heap id has already been read into a byte array at given pos
    fun readHeapIdentifier(bb: ByteArray, pos: Int): HeapIdentifier {
        return HeapIdentifier(bb, pos)
    }

    // see "Global Heap Id" in http://www.hdfgroup.org/HDF5/doc/H5.format.html
    internal inner class HeapIdentifier {
        val nelems: Int // "number of 'base type' elements in the sequence in the heap"
        private val heapAddress: Long
        private val index: Int

        // address must be absolute, getFileOffset already added
        constructor(address: Long) {
            if (address < 0 || address >= raf.size()) {
                throw IllegalStateException("$address out of bounds; eof=${raf.size()} ")
            }
            // header information is in le byte order
            val state = OpenFileState(address, false)
            nelems = raf.readInt(state)
            heapAddress = header.readOffset(state)
            index = raf.readInt(state)
        }

        // the heap id is in ByteArray starting at given pos
        constructor(bb: ByteArray, start: Int) {
            var pos = start
        // fun convertInt(ba: ByteArray, offset: Int, isBE: Boolean): Int {
            nelems = convertToInt(bb, pos, isBE = false)
            pos += 4
            heapAddress = if (header.isOffsetLong) convertToLong(bb, pos, isBE = false)
                          else convertToInt(bb, pos, isBE = false).toLong()
            pos += header.sizeOffsets
            index = convertToInt(bb, pos, isBE = false)
        }

        fun isEmpty(): Boolean {
            return heapAddress == 0L
        }

        override fun toString(): String {
            return " nelems=$nelems heapAddress=$heapAddress index=$index"
        }

        fun getHeapObject(): GlobalHeap.HeapObject? {
            if (isEmpty()) return null
            var gheap = heapMap[heapAddress]
            if (null == gheap) {
                gheap = GlobalHeap(header, heapAddress)
                heapMap[heapAddress] = gheap
            }
            return gheap.getHeapObject(index.toShort()) ?: throw IllegalStateException("cant find HeapObject")
        }
    } // HeapIdentifier
} // H5heap

// level 1E Global Heap
internal class GlobalHeap(h5: H5builder, address: Long) {
    private val version: Byte
    private val sizeBytes: Int
    private val hos: MutableMap<Short, HeapObject> = HashMap()

    init {
        val filePos: Long = h5.getFileOffset(address)
        if (filePos < 0 || filePos >= h5.raf.size()) {
            throw IllegalStateException("$filePos out of bounds; eof=${h5.raf.size()} ")
        }

        // header information is in le byte order
        val state = OpenFileState(filePos, false)

        // header
        val magic: String = h5.raf.readString(state, 4)
        check(magic == "GCOL") {
            "$magic should equal GCOL"
        }
        version = h5.raf.readByte(state)
        state.pos += 3
        sizeBytes = h5.raf.readInt(state)
        state.pos += 4  // pad to 8

        var count = 0
        var countBytes = 0
        while (true) {
            val o = HeapObject()
            o.id = h5.raf.readShort(state)
            if (o.id.toInt() == 0) break // ?? look
            o.refCount = h5.raf.readShort(state)
            state.pos += 4
            o.dataSize = h5.readLength(state)
            o.dataPos = state.pos
            val dsize = o.dataSize.toInt() + padding(o.dataSize.toInt(), 8)
            countBytes += dsize + 16
            if (o.dataSize < 0) break // ran off the end, must be done
            if (countBytes < 0) break // ran off the end, must be done
            if (countBytes > sizeBytes) break // ran off the end
            state.pos += dsize
            hos[o.id] = o
            count++
            if (countBytes + 16 >= sizeBytes) break // ran off the end, must be done
        }
    }

    internal fun getHeapObject(id: Short): HeapObject? {
        return hos[id]
    }

    internal inner class HeapObject {
        var id: Short = 0
        var refCount: Short = 0
        var dataSize: Long = 0
        var dataPos: Long = 0
        override fun toString(): String {
            return "id=$id, refCount=$refCount, dataSize=$dataSize, dataPos=$dataPos"
        }
    }
}

// Level 1D - Local Heaps
internal class LocalHeap(header : H5builder, address: Long) {
    val size: Int
    val freelistOffset: Long
    val dataAddress: Long
    val heap: ByteArray
    val version: Byte

    init {
        val state = OpenFileState(header.getFileOffset(address), false)
        // header
        val magic: String = header.raf.readString(state,4)
        check(magic == "HEAP") {
            "$magic should equal HEAP"
        }
        version = header.raf.readByte(state)
        state.pos += 3
        size = header.readLength(state).toInt()
        freelistOffset = header.readLength(state)
        dataAddress = header.readOffset(state)

        // data
        state.pos = header.getFileOffset(dataAddress)
        heap = header.raf.readByteArray(state, size)
        val hsize: Int = 8 + 2 * header.sizeLengths + header.sizeOffsets
        if (debugHeap) {
            println("LocalHeap hsize = $hsize")
        }
    }

    fun getStringAt(offset: Int): String {
        return makeStringZ(heap, start=offset)
    }
} // LocalHeap