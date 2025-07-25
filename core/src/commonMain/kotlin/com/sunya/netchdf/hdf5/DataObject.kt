package com.sunya.netchdf.hdf5

import com.fleeksoft.charset.Charsets
import com.sunya.cdm.iosp.OpenFileState
import com.fleeksoft.charset.toByteArray
import com.sunya.cdm.util.InternalLibraryApi

// "Data Object Header" Level 2A
@OptIn(InternalLibraryApi::class)
internal fun H5builder.readDataObject(address: Long, name: String?) : DataObject? {
    if (debugFlow) println("readDataObject= $name")
    val startPos = this.getFileOffset(address)
    val state = OpenFileState( startPos, false)
    val messages = mutableListOf<MessageHeader>()

    val version = raf.readByte(state)
    if (version.toInt() == 1) { // IV.A.1.a. Version 1 Data Object Header Prefix
        state.pos += 1 // skip byte
        val nmess = raf.readShort(state).toInt()
        val objectReferenceCount: Int = raf.readInt(state)
        // This value specifies the number of bytes of header message data following this length field that contain
        // object header messages for this object header. This value does not include the size of object header
        // continuation blocks for this object elsewhere in the file.
        val objectHeaderSize: Int = raf.readInt(state)
        // Header messages are aligned on 8-byte boundaries for version 1 object headers.
        // LOOK not well documented
        state.pos += 4

        this.readMessagesVersion1(state, nmess, objectHeaderSize, messages)
        return DataObject(address, name, messages)
        
    } else { // IV.A.1.b. Version 2 Data Object Header Prefix
        // first byte was already read
        val testForMagic = raf.readByteArray(state, 3)
        if (!testForMagic.contentEquals("HDR".toByteArray(Charsets.UTF8))) {
            return null
        }
        require(raf.readByte(state).toInt() == 2) // better be 2
        val flags = raf.readByte(state).toInt()
        if (((flags shr 5) and 1) == 1) {
            val accessTime: Int = raf.readInt(state)
            val modTime: Int = raf.readInt(state)
            val changeTime: Int = raf.readInt(state)
            val birthTime: Int = raf.readInt(state)
        }
        if (((flags shr 4) and 1) == 1) {
            val maxCompactAttributes: Short = raf.readShort(state)
            val minDenseAttributes: Short = raf.readShort(state)
        }
        val sizeOfChunk: Long = this.readVariableSizeFactor(state,flags and 3)
        this.readMessagesVersion2(state, sizeOfChunk, ((flags shr 2) and 1) == 1, messages)
        return DataObject(address, name, messages)
    }
}

data class DataObject(
    val address : Long, // aka object id : obviously unique
    var name: String?, // may be null, may not be unique
    val messages : List<MessageHeader>
) {
    internal var groupMessage: SymbolTableMessage? = null
    internal var groupNewMessage: LinkInfoMessage? = null
    internal var mds: DataspaceMessage? = null
    internal var mdl: DataLayoutMessage? = null
    internal var mfp: FilterPipelineMessage? = null
    val attributes = mutableListOf<AttributeMessage>()
    val mdt: DatatypeMessage? // not present for group message

    init {
        var findMdt : DatatypeMessage? = null
        // look for group or a datatype/dataspace/layout message
        for (mess: MessageHeader in messages) {
            when (mess.mtype) {
                MessageType.SymbolTable -> groupMessage = mess as SymbolTableMessage
                MessageType.LinkInfo -> groupNewMessage = mess as LinkInfoMessage
                MessageType.Dataspace -> mds = mess as DataspaceMessage
                MessageType.Datatype -> findMdt = mess as DatatypeMessage
                MessageType.Layout -> mdl = mess as DataLayoutMessage
                MessageType.FilterPipeline -> mfp = mess as FilterPipelineMessage
                MessageType.Attribute -> attributes.add(mess as AttributeMessage)
                MessageType.AttributeInfo -> attributes.addAll((mess as AttributeInfoMessage).attributes)
                else -> { /* noop */ }
            }
        }
        this.mdt = findMdt // at least its a val not a var
    }

    fun show() = buildString {
        appendLine("DataObject(address=$address, name=$name")
        if (groupMessage != null) appendLine(" groupMessage= $groupMessage")
        if (groupNewMessage != null) appendLine(" groupNewMessage= $groupNewMessage")
        if (mds != null) appendLine(" mds= $mds")
        if (mdl != null) appendLine(" mdl= $mdl")
        if (mdt != null) appendLine(" mdt= $mdt")
        if (mfp != null) appendLine(" mfp= $mfp")
        if (attributes.isNotEmpty()) appendLine(" attributes")
        attributes.forEach {
            appendLine("   ${it.show()}")
        }
    }

}
