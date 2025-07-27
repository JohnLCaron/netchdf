package com.sunya.netchdf.hdf5

import com.sunya.cdm.iosp.OpenFileState

import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.cdm.util.log2
import kotlin.math.ceil
import kotlin.math.pow

@OptIn(InternalLibraryApi::class)

/*  Btree version 2, for non-data. From jhdf. */
internal class BTree2(val raf: OpenFileExtended, val owner: String, address: Long) {

    val btreeType: Int
    private val nodeSize: Int // size in bytes of btree nodes
    private val recordSize: Int // size in bytes of btree records
    val treeDepth : Int
    val rootNodeAddress: Long
    val numberOfRecordsInRoot : Int
    val totalNumberOfRecordsInTree: Long

    /** The records in this b-tree */
    val records = mutableListOf<Any>()

    init {
        val state = OpenFileState(raf.getFileOffset(address), false)

        // header
        val magic = raf.readString(state, 4)
        check(magic == "BTHD") { "$magic should equal BTHD" }
        val version: Byte = raf.readByte(state)
        btreeType = raf.readByte(state).toInt()
        require(btreeType > 0 || btreeType < 10)

        nodeSize = raf.readInt(state) // This is the size in bytes of all B-tree nodes.
        recordSize = raf.readShort(state).toUShort().toInt() // This field is the size in bytes of the B-tree record.
        treeDepth = raf.readShort(state).toUShort().toInt()

        val splitPct = raf.readByte(state)
        val mergePct = raf.readByte(state)
        rootNodeAddress = raf.readOffset(state)
        numberOfRecordsInRoot = raf.readShort(state).toUShort().toInt()
        totalNumberOfRecordsInTree = raf.readLength(state) // total in entire btree
        val checksum: Int = raf.readInt(state)

        readRecords(rootNodeAddress, treeDepth, numberOfRecordsInRoot, totalNumberOfRecordsInTree)
    }

    fun readRecords(address: Long, depth: Int, numberOfRecords: Int, totalRecords: Long) {
        val state = OpenFileState(raf.getFileOffset(address), false)

        val magic = raf.readString(state, 4)
        val leafNode = if (magic == "BTIN") {
            false
        } else if (magic == "BTLF") {
            true
        } else {
            throw RuntimeException("$magic unknown tag")
        }

        val version: Byte = raf.readByte(state)
        val nodeType = raf.readByte(state).toInt() // same as the B-tree type in the header
        check(nodeType == btreeType)

        repeat(numberOfRecords) {
            records.add( readRecord(state, nodeType))
        }

        if (!leafNode) {
            repeat(numberOfRecords + 1) {
                val childAddress = raf.readOffset(state) // Child Node Pointer
                val sizeOfNumberOfRecords = getSizeOfNumberOfRecords(nodeSize, depth, totalRecords.toInt(), recordSize, raf.sizeOffsets())
                val numberOfChildRecords: Int = raf.readVariableSizeUnsigned(state, sizeOfNumberOfRecords).toInt() // readBytesAsUnsignedInt(bb, sizeOfNumberOfRecords)
                val sizeNumberOfChildRecords = getSizeOfTotalNumberOfChildRecords(nodeSize, depth, recordSize)
                val totalNumberOfChildRecords = if (depth > 1) {
                    raf.readVariableSizeUnsigned(state, sizeNumberOfChildRecords)
                } else {
                    -1
                }
                readRecords(childAddress, depth - 1, numberOfChildRecords, totalNumberOfChildRecords)
            }
        }
    }

    fun readRecord(state: OpenFileState, type: Int): Any {
        return when (type) {
            1 -> Record1(state)
            2 -> Record2(state)
            3 -> Record3(state)
            4 -> Record4(state)
            5 -> Record5(state)
            6 -> Record6(state)
            7 -> Record70(state) // TODO wrong
            8 -> Record8(state)
            9 -> Record9(state)
            else -> throw IllegalStateException()
        }
    }

    // Type 1 Record Layout - Indirectly Accessed, Non-filtered, ‘Huge’ Fractal Heap Objects
    internal inner class Record1(state: OpenFileState) {
        val hugeObjectAddress = raf.readOffset(state)
        val hugeObjectLength = raf.readLength(state)
        val hugeObjectID = raf.readLength(state)
    }

    // Type 2 Record Layout - Indirectly Accessed, Filtered, ‘Huge’ Fractal Heap Objects
    internal inner class Record2(state: OpenFileState) {
        val hugeObjectAddress = raf.readOffset(state)
        val hugeObjectLength = raf.readLength(state)
        val filterMask = raf.readInt(state)
        val hugeObjectSize = raf.readLength(state)
        val hugeObjectID = raf.readLength(state)
    }

    // Type 3 Record Layout - Directly Accessed, Non-filtered, ‘Huge’ Fractal Heap Objects
    internal inner class Record3(state: OpenFileState) {
        val hugeObjectAddress = raf.readOffset(state)
        val hugeObjectLength = raf.readLength(state)
    }

    // Type 4 Record Layout - Directly Accessed, Filtered, ‘Huge’ Fractal Heap Objects
    internal inner class Record4(state: OpenFileState) {
        val hugeObjectAddress = raf.readOffset(state)
        val hugeObjectLength = raf.readLength(state)
        val filterMask = raf.readInt(state)
        val hugeObjectSize = raf.readLength(state)
    }

    // Type 5 Record Layout - Link Name for Indexed Group
    inner class Record5(state: OpenFileState) {
        val nameHash = raf.readInt(state)
        val heapId: ByteArray = raf.readByteArray(state, 7)
    }

    // Type 6 Record Layout - Creation Order for Indexed Group
    inner class Record6(state: OpenFileState) {
        val creationOrder = raf.readLong(state)
        val heapId: ByteArray = raf.readByteArray(state, 7)
    }

    // Type 7 Record Layout - Shared Object Header Messages (Sub-type 0 - Message in Heap)
    internal inner class Record70(state: OpenFileState) {
        val location = raf.readByte(state)
        val hash = raf.readInt(state)
        val refCount = raf.readInt(state)
        val id: ByteArray = raf.readByteArray(state, 8)
    }

    // Type 7 Record Layout - Shared Object Header Messages (Sub-type 1 - Message in Object Header)
    internal inner class Record71(state: OpenFileState) {
        val location = raf.readByte(state)
        val hash = raf.readInt(state)
        val skip = raf.readByte(state)
        val messtype = raf.readByte(state)
        val index = raf.readShort(state)
        val address = raf.readOffset(state)
    }

    // Type 8 Record Layout - Attribute Name for Indexed Attributes
    inner class Record8(state: OpenFileState) {
        val heapId: ByteArray = raf.readByteArray(state, 8)
        val flags = raf.readByte(state)
        val creationOrder = raf.readInt(state)
        val nameHash = raf.readInt(state)
    }

    // Type 9 Record Layout - Creation Order for Indexed Attributes
    inner class Record9(state: OpenFileState) {
        val heapId: ByteArray = raf.readByteArray(state, 8)
        val flags = raf.readByte(state)
        val creationOrder = raf.readInt(state)
    }

    companion object {
        internal fun findRecord1byId(records: List<Any>, hugeObjectID: Int): Record1? {
            for (record in records) {
                if (record is Record1 && record.hugeObjectID == hugeObjectID.toLong()) return record
            }
            return null
        }
    }
}


// heroic jhdf
fun getSizeOfNumberOfRecords(
    nodeSize: Int,
    depth: Int,
    totalRecords: Int,
    recordSize: Int,
    sizeOfOffsets: Int
): Int {
    val NODE_OVERHEAD_BYTES = 10
    var size: Int = nodeSize - NODE_OVERHEAD_BYTES

    // If the child is not a leaf
    if (depth > 1) {
        // Need to subtract the pointers as well
        val pointerTripletBytes = bytesNeededToHoldNumber(totalRecords) * 2 + sizeOfOffsets
        size -= pointerTripletBytes

        return bytesNeededToHoldNumber(size / recordSize)
    } else {
        // Its a leaf
        return bytesNeededToHoldNumber(size / recordSize)
    }
}

// jhdf
internal fun bytesNeededToHoldNumber(number: Int): Int {
    return (Integer.numberOfTrailingZeros(Integer.highestOneBit(number)) + 8) / 8
}

/* private fun getSizeOfTotalNumberOfChildRecords(nodeSize: Int, depth: Int, recordSize: Int): Int {
    require (nodeSize % recordSize == 0)
    val recordsInLeafNode = (nodeSize / recordSize).toDouble()
    val totalRecords = recordsInLeafNode.pow(depth)
    val totalBits = log2(totalRecords)
    val totalBitsInt = totalBits.toInt()
    return (totalBitsInt + 8) / 8
} */

// no BigInteger, max depth 6
internal fun getSizeOfTotalNumberOfChildRecords(nodeSize: Int, depth: Int, recordSize: Int): Int {
    require(depth < 7 ) { "no BigInteger, max depth 6 "}
    val recordsInLeafNode = (nodeSize/ recordSize).toDouble()
    val totalRecords = recordsInLeafNode.pow(depth)
    val totalRecordsL = ceil(totalRecords).toLong()
    val alt = log2(totalRecordsL) + 1
    val alt1 =  (alt + 8) / 8
    return alt1
}

// jhdf
//private fun getSizeOfTotalNumberOfChildRecordsOrg(nodeSize: Int, depth: Int, recordSize: Int): Int {
//    val recordsInLeafNode = nodeSize / recordSize
//    return (BigInteger.valueOf(recordsInLeafNode.toLong()).pow(depth).bitLength() + 8) / 8
//}
