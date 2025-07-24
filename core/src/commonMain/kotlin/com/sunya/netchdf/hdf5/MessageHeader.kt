@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf5

import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.iosp.makeString
import com.sunya.cdm.util.InternalLibraryApi

private const val debugContinuation = false
internal const val debugMessage = false

// type safe enum
enum class MessageType(val uname: String, val num: Int) {
    NIL("NIL", 0),
    Dataspace("Dataspace", 1),
    LinkInfo("GroupNew", 2),
    Datatype("Datatype", 3),
    FillValueOld("FillValueOld", 4),
    FillValue("FillValue", 5),
    Link("Link", 6),
    ExternalDataFiles("ExternalDataFiles", 7), // not supported
    Layout("Layout", 8),
    GroupInfo("GroupInfo", 10),
    FilterPipeline("FilterPipeline", 11),
    Attribute("Attribute", 12),
    Comment("Comment", 13),
    LastModifiedOld("LastModifiedOld", 14),
    SharedObject("SharedObject", 15),
    ObjectHeaderContinuation("Continuation", 16),
    SymbolTable("OldGroup", 17),
    LastModified("LastModified", 18),
    AttributeInfo("AttributeInfo", 21),
    ObjectReferenceCount("ObjectReferenceCount", 22),
    ;

    override fun toString(): String {
        return "$uname($num)"
    }

    companion object {
        private val messbyNum = entries.associateBy { it.num }

        fun byNumber(num: Int): MessageType? {
            return messbyNum[num]
        }
    }
}

// Level 2A1
internal fun H5builder.readMessagesVersion1(
    state: OpenFileState,
    nmess: Int,
    dataSize: Int,
    messages: MutableList<MessageHeader>
): Int {
    val posLimit = state.pos + dataSize
    var count = 0
    while (count < nmess && state.pos < posLimit) {
        val mess = this.readHeaderMessage(state, 1, false)
        count++
        if (mess == null) {
            continue
        }

        // if we hit a continuation, then we go into nested reading
        if (mess.mtype == MessageType.ObjectHeaderContinuation) {
            val c: ContinueMessage = mess as ContinueMessage // LOOK does this count towards nmess?
            val continuePos = this.getFileOffset(c.offset)
            count += readMessagesVersion1(state.copy(pos = continuePos), nmess - count, c.length.toInt(), messages)

        } else {
            messages.add(mess)
        }
    }
    return count
}

internal fun H5builder.readMessagesVersion2(
    state: OpenFileState,
    dataSize: Long,
    creationOrderPresent: Boolean,
    messages: MutableList<MessageHeader>,
): Int {
    val posLimit = state.pos + dataSize - 3

    while (state.pos < posLimit) {
        // maxBytes is number of bytes of messages to be read. however, a message is at least 4 bytes long, so
        // we are done if we have read > maxBytes - 4. There appears to be an "off by one" possibility
        val mess = this.readHeaderMessage(state, 2, creationOrderPresent) ?: continue

        // if we hit a continuation, then we go into nested reading
        if (mess.mtype == MessageType.ObjectHeaderContinuation) {
            val c: ContinueMessage = mess as ContinueMessage
            val continuationBlockFilePos: Long = this.getFileOffset(c.offset)
            val cstate = state.copy(pos = continuationBlockFilePos)
            val sign = raf.readString(cstate, 4)
            // make sure were not jumping into nowhere
            if (sign != "OCHK") throw RuntimeException(" ObjectHeaderContinuation Missing signature")
            if (debugContinuation) println("***ObjectHeaderContinuation start $continuationBlockFilePos")
            readMessagesVersion2(
                cstate,
                (c.length.toInt() - 8).toLong(),
                creationOrderPresent,
                messages
            )
            if (debugContinuation) println("***ObjectHeaderContinuation end")
        } else {
            messages.add(mess)
        }
    }
    return messages.size
}

internal fun H5builder.readHeaderMessage(state: OpenFileState, version: Int, hasCreationOrder: Boolean): MessageHeader? {
    val startPos = state.pos
    val mtype: MessageType?
    val flags: Int
    val headerSize: Int
    val messageSize: Int
    if (version == 1) { //  Level 2A1 Version 1 Data Object Header Prefix - Common Header Message fields
        val rawdata1 =
            structdsl("HeaderMessage1", raf, state) {
                fld("type", 2)
                fld("size", 2)
                fld("flags", 1)
                skip(3)
            }
        if (debugMessage) rawdata1.show()

        mtype = MessageType.byNumber(rawdata1.getShort("type").toInt())
        if (mtype == null) {
            println("Unknown mtype = ${rawdata1.getShort("type")}")
            return null
        }
        flags = rawdata1.getByte("flags").toInt()
        messageSize = rawdata1.getShort("size").toUShort().toInt()
        headerSize = rawdata1.dataSize()

    } else { // Level 2A1 Version 2 Data Object Header Prefix - Common Header Message fields
        val rawdata2 =
            structdsl("HeaderMessage2", raf, state) {
                fld("type", 1)
                fld("size", 2)
                fld("flags", 1)
                if (hasCreationOrder) {
                    fld("creationOrder", 2)
                }
            }
        if (debugMessage) rawdata2.show()
        val rawtype = rawdata2.getByte("type").toInt()
        mtype = MessageType.byNumber(rawtype)
        flags = rawdata2.getByte("flags").toInt()
        messageSize = rawdata2.getShort("size").toUShort().toInt()
        headerSize = rawdata2.dataSize()
    }

    // 	If set, the message is shared and stored in another location than the object header.
    // 	The Header Message Data field contains a Shared Message (described in the Data Object Header Messages
    // 	section below) and the Size of Header Message Data field contains the size of that Shared Message.
    if (flags and 2 != 0) { // shared
        // LOOK could be other shared objects besides datatype ??
        // LOOK do we need to defer ??
        val mdt = getSharedDataObject(state, mtype!!).mdt // a shared datatype, eg enums
        if (debugFlow) {
            println(" shared Message $mtype  ${mdt?.show()}")
        }
        state.pos = startPos + messageSize + headerSize
        return mdt
    }

    val result = when (mtype) {
        MessageType.NIL -> {
            null
        }

        MessageType.Dataspace -> this.readDataspaceMessage(state) // 1
        MessageType.LinkInfo -> this.readLinkInfoMessage(state) // 2
        MessageType.Datatype -> this.readDatatypeMessage(state) // 3
        MessageType.FillValueOld -> this.readFillValueOldMessage(state) // 4
        MessageType.FillValue -> this.readFillValueMessage(state) // 5
        MessageType.Link -> this.readLinkMessage(state) // 6
        MessageType.Layout -> this.readDataLayoutMessage(state) // 8
        MessageType.GroupInfo -> this.readGroupInfoMessage(state) // 10
        MessageType.FilterPipeline -> this.readFilterPipelineMessage(state) // 11
        MessageType.Attribute -> this.readAttributeMessage(state) // 12
        MessageType.Comment -> this.readCommentMessage(state) // 13
        MessageType.LastModifiedOld -> null // 14
        MessageType.SharedObject -> this.readSharedMessage(state) // 15
        MessageType.ObjectHeaderContinuation -> this.readContinueMessage(state) // 16
        MessageType.SymbolTable -> this.readSymbolTableMessage(state) // 17
        MessageType.LastModified -> null // 18
        MessageType.AttributeInfo -> this.readAttributeInfoMessage(state) // 21
        MessageType.ObjectReferenceCount -> this.readReferenceCountMessage(state) // 22
        else -> throw RuntimeException("Unimplemented message type = $mtype")
    }
    // ignoring version 2 gap and the checksum, so we need to set position explicitly, nor rely on the raf.pos to be correct

    if (debugFlow) {
        println(" read Message $mtype  ${result?.show()}")
    }
    if (debugMessage) {
        println(" done Message $mtype pos = ${state.pos} expect ${startPos + messageSize + headerSize}")
    }
    // heres where we get the position right, no matter what
    state.pos = startPos + messageSize + headerSize
    return result
}


// Header Message: Level 2A1 and 2A2 (part of Data Object)
open class MessageHeader(val mtype: MessageType) : Comparable<MessageHeader> {
    override operator fun compareTo(other: MessageHeader): Int =
        compareValuesBy(this, other, { it.mtype.num }, { other.mtype.num })
    open fun show() : String {
        return "$mtype"
    }
}

////////////////////////////////////////// 1 IV.A.2.b. The Dataspace Message
// The dataspace message describes the number of dimensions (in other words, “rank”) and size of each dimension
// that the data object has. This message is only used for datasets which have a simple, rectilinear, array-like layout;
// datasets requiring a more complex layout are not yet supported.

internal fun H5builder.readDataspaceMessage(state: OpenFileState): DataspaceMessage {
    val version = raf.readByte(state).toInt()
    val flags = raf.readByte(state.incr(1)).toInt()
    state.incr(-3)
    val rawdata =
        structdsl("DataspaceMessage", raf, state) {
            fld("version", 1)
            fld("rank", 1)
            fld("flags", 1)
            fld("type", 1)
            if (version == 1) {
                skip(4)
            }
            array("dims", sizeLengths, "rank")
            if (flags and 1 != 0) {
                array("maxsize", sizeLengths, "rank")
            }
            if (version == 1 && (flags and 2 != 0)) {
                array("permute", sizeLengths, "rank")
            }
        }
    if (debugMessage) rawdata.show()

    val type = if (version == 1) {
        if (rawdata.getByte("rank").toInt() == 0) 0 else 1
    } else {
        rawdata.getByte("type").toInt()
    }

    val isUnlimited = if (flags and 1 != 0) {
        val maxsize = rawdata.getIntArray("maxsize")
        maxsize.isNotEmpty() && maxsize[0] < 0 // set maxsize to -1 when unlimited
    } else false

    return DataspaceMessage(
        DataspaceType.of(type),
        rawdata.getLongArray("dims"),
        isUnlimited,
    )
}

enum class DataspaceType(val num: Int) {
    Scalar(0), Simple(1), Null(2);

    companion object {
        fun of(num: Int): DataspaceType {
            return when (num) {
                0 -> Scalar
                1 -> Simple
                2 -> Null
                else -> throw RuntimeException("Unknown DataspaceType $num")
            }
        }
    }
}

data class DataspaceMessage(val type: DataspaceType, val dims: LongArray, val isUnlimited : Boolean)
    : MessageHeader(MessageType.Dataspace) {

    fun rank(): Int = dims.size

    override fun show() : String {
        return "$type dims=${dims.contentToString()} isUnlimited=$isUnlimited"
    }
}

////////////////////////////////////////// 2 IV.A.2.c. The Link Info Message
// The link info message tracks variable information about the current state of the links for a “new style” group’s behavior.
// Variable information will be stored in this message and constant information will be stored in the Group Info message.

internal fun H5builder.readLinkInfoMessage(state: OpenFileState): LinkInfoMessage {
    val flags = raf.readByte(state.copy().incr(1)).toInt()
    val rawdata =
        structdsl("LinkInfoMessage", raf, state) {
            fld("version", 1)
            fld("flags", 1)
            if ((flags and 1) != 0) {
                fld("maxCreationIndex", 8)
            }
            fld("fractalHeapAddress", sizeOffsets)
            fld("v2BtreeAddress", sizeOffsets)
            if ((flags and 2) != 0) {
                fld("v2BtreeAddressCreationOrder", 8)
            }
        }
    if (debugGroup) rawdata.show()

    return LinkInfoMessage(
        rawdata.getLong("fractalHeapAddress"),
        rawdata.getLong("v2BtreeAddress"),
        if ((flags and 2) != 0) rawdata.getLong("v2BtreeAddressCreationOrder") else null,
    )
}

internal data class LinkInfoMessage(
    val fractalHeapAddress: Long,
    val v2BtreeAddress: Long,
    val v2BtreeAddressCreationOrder: Long?
) : MessageHeader(MessageType.LinkInfo) {
    override fun show() : String {
        return "has CreationOrder ${v2BtreeAddressCreationOrder != null}"
    }
}

////////////////////////////////////////// 4 IV.A.2.e. The Data Storage - Fill Value (Old) Message
// The fill value message stores a single data value which is returned to the application when an uninitialized data
// element is read from a dataset. The fill value is interpreted with the same datatype as the dataset.
// If no fill value message is present then a fill value of all zero bytes is assumed.
//
// This fill value message is deprecated in favor of the “new” fill value message (Message Type 0x0005) and is only
// written to the file for forward compatibility with versions of the HDF5 Library before the 1.6.0 version.
// Additionally, it only appears for datasets with a user-defined fill value (as opposed to the library default fill
// value or an explicitly set “undefined” fill value).

internal fun H5builder.readFillValueOldMessage(state: OpenFileState): FillValueOldMessage {
    val rawdata =
        structdsl("FillValueOldMessage", raf, state) {
            fld("size", 4)
            array("value", 1, "size")
        }
    if (debugMessage) rawdata.show()

    return FillValueOldMessage(
        rawdata.getInt("size"),
        rawdata.getByteArray("value"),
    )
}

internal data class FillValueOldMessage(val size: Int, val value: ByteArray) : MessageHeader(MessageType.FillValueOld)

////////////////////////////////////////// 5 IV.A.2.f. The Data Storage - Fill Value Message
// The fill value message stores a single data value which is returned to the application when an uninitialized data
// element is read from a dataset. The fill value is interpreted with the same datatype as the dataset.

internal fun H5builder.readFillValueMessage(state: OpenFileState): FillValueMessage {
    val version = raf.readByte(state).toInt()
    val spaceAllocateTime: Byte
    val fillWriteTime: Byte
    val hasFillValue: Boolean
    if (version < 3) {
        spaceAllocateTime = raf.readByte(state)
        fillWriteTime = raf.readByte(state)
        hasFillValue = raf.readByte(state).toInt() != 0
    } else {
        val flags = raf.readByte(state)
        spaceAllocateTime = (flags.toInt() and 3).toByte()
        fillWriteTime = (flags.toInt() shr 2 and 3).toByte()
        hasFillValue = flags.toInt() and 32 != 0
    }

    if (hasFillValue) {
        val size = raf.readInt(state)
        if (size > 0) {
            val value = raf.readByteArray(state, size)
            return FillValueMessage(
                true,
                spaceAllocateTime,
                fillWriteTime,
                size,
                value,
            )
        }
    }

    return FillValueMessage(
        false,
        spaceAllocateTime,
        fillWriteTime,
        0,
        null,
    )
}

internal data class FillValueMessage(val hasFillValue: Boolean, val spaceAllocateTime : Byte, val fillWriteTime : Byte,
                            val size: Int, val value: ByteArray?) : MessageHeader(MessageType.FillValue) {
    override fun show() : String {
        return "has hasFillValue=${hasFillValue}"
    }
}

////////////////////////////////////////// 6 IV.A.2.g. The Link Message
// This message encodes the information for a link in a group’s object header, when the group is storing
// its links “compactly”, or in the group’s fractal heap, when the group is storing its links “densely”.
// A group is storing its links compactly when the fractal heap address in the Link Info Message is set to
// the “undefined address” value.

internal fun H5builder .readLinkMessage(state: OpenFileState): LinkMessage {
    val version = raf.readByte(state)
    val flags = raf.readByte(state).toInt()
    val vsize = variableSizeFactor(flags and 3)
    state.incr(-2)

    val rawdata =
        structdsl("LinkMessage", raf, state) {
            fld("version", 1)
            fld("flags", 1)
            if (flags and 8 != 0) {
                fld("linkType", 1)
            }
            if (flags and 4 != 0) {
                fld("creationOrder", 8)
            }
            if (flags and 0x10 != 0) {
                fld("encoding", 1)
            }
            fld("linkNameLength", vsize)
            array("linkName", 1, "linkNameLength")
        }
    if (debugMessage) rawdata.show()
    val linkName = rawdata.getString("linkName")

    // CreationOrder field - not currently used
    // This 64-bit value is an index of the link’s creation time within the group. Values start at 0 when
    // the group is created an increment by one for each link added to the group. Removing a link from a
    // group does not change existing links’ creation order field.
    // Hmm are we supposed to sort by creation order ??

    val linkType = if (flags and 8 != 0) {
        rawdata.getByte("linkType").toInt() // 0 = hard, 1 = soft, 64 = external
    } else 0
    if (debugGroup) println(" LinkSoft $linkName linkType=$linkType")

    when (linkType) {
        0 -> {
            // "A hard link (should never be stored in the file)" wtf?
            val linkAddress = this.readOffset(state)
            if (debugGroup) println(" LinkHard $linkName $linkAddress")
            return LinkHard(linkType, linkName, linkAddress)
        }

        1 -> {
            val len = raf.readShort(state)
            val linkInfo = raf.readString(state, len.toInt())
            return LinkSoft(linkType, linkName, linkInfo)
        }

        64 -> {
            val len = raf.readShort(state)
            val ba = raf.readByteArray(state, len.toInt())
            val version = ba[0]
            val fileName = makeString(ba, 1)
            val objName = makeString(ba, 2 + fileName.length)
            return LinkExternal(linkType, linkName, version, fileName, objName)
        }
        else -> {
            throw RuntimeException("Unknown link type=$linkType")
        }
    }
}

internal open class LinkMessage(val linkType: Int, val linkName: String) : MessageHeader(MessageType.Link) {
    override fun show() : String {
        val typen = when (linkType) {
            0 -> "hard"
            1 -> "soft"
            else -> linkType.toString()
        }
        return "type=$typen name=$linkName"
    }
}
internal open class LinkHard(linkType: Int, linkName: String, val linkAddress: Long) : LinkMessage(linkType, linkName)
internal open class LinkSoft(linkType: Int, linkName: String, val linkInfo: String) : LinkMessage(linkType, linkName)
internal open class LinkExternal(linkType: Int, linkName: String, val version : Byte, val fileName: String, val objName: String) : LinkMessage(linkType, linkName)

////////////////////////////////////////// 10 NOT USED

internal fun H5builder.readGroupInfoMessage(state: OpenFileState): GroupInfoMessage {
    val version = raf.readByte(state).toInt()
    val flags = raf.readByte(state).toInt()
    state.incr(-2)

    val rawdata =
        structdsl("GroupInfoMessage", raf, state) {
            fld("version", 1)
            fld("flags", 1)
            if (flags and 1 != 0) {
                fld("maxCompactValue", 2)
                fld("minDenseValue", 2)
            }
            if (flags and 2 != 0) {
                fld("estNumEntries", 2)
                fld("estLengthEntryName", 2)
            }
        }
    if (debugGroup) rawdata.show()

    return GroupInfoMessage(
        if (flags and 2 != 0) rawdata.getShort("estNumEntries") else null,
        if (flags and 2 != 0) rawdata.getShort("estLengthEntryName") else null,
    )
}

internal data class GroupInfoMessage(val estNumEntries: Short?, val estLengthEntryName: Short?) :
    MessageHeader(MessageType.GroupInfo)

////////////////////////////////////////// 11
// Message Type 11/0xB "Data Storage - Filter Pipeline" : apply a filter to the "data stream"
// This message describes the filter pipeline which should be applied to the data stream by providing filter identification numbers, flags, a name, and client data.
//
// This message may be present in the object headers of both dataset and group objects. For datasets, it specifies the
// filters to apply to raw data. For groups, it specifies the filters to apply to the group’s fractal heap. Currently,
// only datasets using chunked data storage use the filter pipeline on their raw data.

internal fun H5builder.readFilterPipelineMessage(state: OpenFileState): FilterPipelineMessage {
    val version = raf.readByte(state).toInt()
    val nfilters = raf.readByte(state).toInt()
    if (version == 1) {
        state.pos += 6
    }
    val filters = mutableListOf<FilterMessage>()
    for (i in 0 until nfilters) {
        val filterId = raf.readShort(state).toInt()
        val filterType = FilterType.fromId(filterId)
        val nameSize = if (version > 1 && filterId < 256) 0 else raf.readShort(state).toUShort().toInt()
        val flags = raf.readShort(state)
        val nValues = raf.readShort(state).toInt()
        val name = if (version == 1) {
            if (nameSize > 0) readStringZ(state, 8) else FilterType.nameFromId(filterId) // null terminated, pad to 8 bytes
        } else {
            if (nameSize > 0) raf.readString(state, nameSize) else FilterType.nameFromId(filterId) // non-null terminated
        }

        val clientValues = IntArray(nValues) { raf.readInt(state) }
        if (version == 1 && nValues and 1 != 0) { // check if odd
            state.pos += 4
        }
        val f = FilterMessage(filterId, filterType, name, clientValues)
        filters.add(f)
    }

    return FilterPipelineMessage(filters)
}

internal class FilterMessage(val filterId: Int, val filterType: FilterType, val name: String, val clientValues: IntArray)

internal data class FilterPipelineMessage(val filters: List<FilterMessage>) : MessageHeader(MessageType.FilterPipeline) {
    override fun show() : String {
        return filters.joinToString { "${it.filterType} ${it.name}, " }
    }
}

// H5Xpublic.h


///////////////////////////////////////////// 12/0xC "Attribute" : define an Attribute
// The Attribute message is used to store objects in the HDF5 file which are used as attributes, or “metadata” about
// the current object. An attribute is a small dataset; it has a name, a datatype, a dataspace, and raw data.
// Since attributes are stored in the object header, they should be relatively small (in other words, less than 64KB).
// They can be associated with any type of object which has an object header (groups, datasets, or committed (named) datatypes).
//
// In 1.8.x versions of the library, attributes can be larger than 64KB. See the “Special Issues” section of the
// Attributes chapter in the HDF5 User’s Guide for more information.
//
// Note: Attributes on an object must have unique names: the HDF5 Library currently enforces this by causing the
// creation of an attribute with a duplicate name to fail. Attributes on different objects may have the same name, however.

internal fun H5builder.readAttributeMessage(state: OpenFileState): AttributeMessage {
    val version = raf.readByte(state).toInt()
    val flags = raf.readByte(state).toInt()
    state.incr(-2)

    val rawdata =
        structdsl("AttributeMessage", raf, state) {
            fld("version", 1)
            fld("flags", 1)
            fld("nameLength", 2)
            fld("datatypeSize", 2)
            fld("dataspaceSize", 2)
            if (version == 3) {
                fld("encoding", 1)
            }
            array("name", 1, "nameLength")
        }
    if (debugMessage) rawdata.show()

    // read the attribute name
    val name = rawdata.getString("name") // this has terminating zero removed
    if (version == 1) {
        // use the full width to decide on padding
        state.pos += padding(rawdata.getShort("nameLength").toInt(), 8)
    }

    // read the datatype
    val startMdt = state.pos
    var datatypeSize = rawdata.getShort("datatypeSize").toUShort().toInt()

    //var lamda : ((Long) -> DatatypeMessage)? = null
    //var sharedMdtAddress : Long? = null
    val isShared = flags and 1 != 0

    // LOOK should we defer reading in shared objects until all objects are read ??
    val mdt = if (isShared) {
        //sharedMdtAddress = state.pos
        //lamda = { address -> this.getSharedDataObject(OpenFileState(address, false), MessageType.Datatype).mdt!! }
        this.getSharedDataObject(state.copy(), MessageType.Datatype).mdt
    } else {
        if (version == 1) {
            datatypeSize +=  padding(datatypeSize, 8)
        }
        val mdtprivate = this.readDatatypeMessage(state)
        // experimental - look for private typedef, dont have group of H5TypeInfo
        //if (!mdtprivate.isShared) {
        //    registerTypedef()
        //}
        mdtprivate
    }
    state.pos = startMdt + datatypeSize

    // read the dataspace
    val startMds = state.pos
    var dataspaceSize = rawdata.getShort("dataspaceSize").toUShort().toInt()
    val mds = this.readDataspaceMessage(state)
    if (version == 1) {
        dataspaceSize += padding(dataspaceSize, 8)
    }
    // safety check in case readDataspaceMessage is short
    state.pos = startMds + dataspaceSize

    return AttributeMessage(
        name,
        mdt!!,
        mds,
        state.pos, // where the data starts, absolute position (no offset needed)
        //sharedMdtAddress,
        //lamda,
    )
}

class AttributeMessage(val name: String, var mdt: DatatypeMessage, val mds: DataspaceMessage, val dataPos: Long) :
    MessageHeader(MessageType.Attribute) {

    override fun show() : String {
        return "name=$name mdt=(${mdt.show()}) dims=${mds.dims.contentToString()}"
    }
}

// LOOK should we defer reading in shared objects until all objects are read ??
internal class AttributeMessageDefferred(val name: String, var mdt: DatatypeMessage?, val mds: DataspaceMessage, val dataPos: Long,
                            val sharedMdt : Long?, val deferRead : ((Long) -> DatatypeMessage)?) :
    MessageHeader(MessageType.Attribute) {

    init {
        require(mdt != null || (sharedMdt != null && deferRead != null))
    }

    fun mdt() : DatatypeMessage {
        if (mdt == null) {
            mdt = deferRead!!(sharedMdt!!)
            println("defer att $name read for shared mdt@$sharedMdt")
        }
        return mdt!!
    }

    override fun show() : String {
        return "name=$name mdt=(${mdt?.show()}) dims=${mds.dims.contentToString()}"
    }
}

////////////////////////////////////////// 13
// The object comment is a short description of an object.

internal fun H5builder.readCommentMessage(state: OpenFileState): CommonMessage {
    val comment = readStringZ(state)
    return CommonMessage(comment)
}

internal data class CommonMessage(val comment: String) : MessageHeader(MessageType.Comment) {
    override fun show() : String {
        return comment
    }
}

////////////////////////////////////////// 15
// This message is used to locate the table of shared object header message (SOHM) indexes. Each index consists of
// information to find the shared messages from either the heap or object header.
// This message is only found in the superblock extension.
// LOOK not really sure what this is used for

internal fun H5builder.readSharedMessage(state: OpenFileState): SharedMessage {
    val rawdata =
        structdsl("SharedMessage", raf, state) {
            fld("version", 1)
            fld("address", sizeOffsets) //
            fld("nindices", 1)
        }
    if (debugMessage) rawdata.show()

    return SharedMessage(
        rawdata.getLong("address"),
        rawdata.getLong("nindices").toInt(),
    )
}

/**
 * @param address of the master table for shared object header message indexes.
 * @param nindices number of indices in the master table.
 */
internal data class SharedMessage(val address: Long, val nindices: Int) : MessageHeader(MessageType.SharedObject) {
    override fun show() : String {
        return "address = $address nindices = $nindices"
    }
}

// Level 2A2 - Data Object Header Messages
// “shared message” encoding
internal fun H5builder.getSharedDataObject(state : OpenFileState, mtype: MessageType): DataObject {
    val sharedVersion = raf.readByte(state).toInt()
    val sharedType = raf.readByte(state).toInt()
    if (sharedVersion == 1) {
        state.pos += 6
    }
    if (sharedVersion == 3 && sharedType == 1) {
        val heapId = raf.readLong(state)
        // LOOK Message stored in file’s shared object header message heap (a shared message).
        //   the 8-byte fractal heap ID for the message in the file’s shared object header message heap.
        //   Maybe this points into table located by a SharedMessage
        throw UnsupportedOperationException("****SHARED MESSAGE type = $mtype heapId = $heapId")
    } else {
        // The address of the object header containing the message to be shared
        val address: Long = this.readOffset(state)
        val dobj: DataObject = this.getDataObject(address, null)!! // cached here
        if (mtype === MessageType.Datatype) {
            dobj.mdt!!.isShared = true
            return dobj
        }
        throw UnsupportedOperationException("****SHARED MESSAGE type = $mtype")
    }
}

////////////////////////////////////////// 16
// The object header continuation is the location in the file of a block containing more header messages for the current
// data object. This can be used when header blocks become too large or are likely to change over time.

internal fun H5builder.readContinueMessage(state: OpenFileState): ContinueMessage {
    val rawdata =
        structdsl("ContinueMessage", raf, state) {
            fld("offset", sizeOffsets)
            fld("length", sizeLengths)
        }
    if (debugMessage) rawdata.show()

    return ContinueMessage(
        rawdata.getLong("offset"),
        rawdata.getLong("length"),
    )
}

internal data class ContinueMessage(val offset: Long, val length: Long) : MessageHeader(MessageType.ObjectHeaderContinuation) {
    override fun show() : String {
        return "offset = $offset length = $length"
    }
}

//////

////////////////////////////////////////// 17
// Each “old style” group has a v1 B-tree and a local heap for storing symbol table entries, which are located with this message.
// only one of these for the group, its where the shared messages live (I think)

internal fun H5builder.readSymbolTableMessage(state: OpenFileState): SymbolTableMessage {
    val rawdata =
        structdsl("SymbolTableMessage", raf, state) {
            fld("btreeAddress", sizeOffsets)
            fld("localHeapAddress", sizeOffsets)
        }
    if (debugMessage) rawdata.show()

    return SymbolTableMessage(
        rawdata.getLong("btreeAddress"),
        rawdata.getLong("localHeapAddress"),
    )
}

// localHeapAddress aka nameHeapAddress
internal data class SymbolTableMessage(val btreeAddress: Long, val localHeapAddress: Long) : MessageHeader(MessageType.SymbolTable) {
    override fun show() : String {
        return "btreeAddress = $btreeAddress localHeapAddress = $localHeapAddress"
    }
}

////////////////////////////////////////// 21
// This stores arbitrary more attributes in a fractal heap; we eagerly read them all into this
// message and add to the data object.

internal fun H5builder.readAttributeInfoMessage(state: OpenFileState): AttributeInfoMessage {
    val version = raf.readByte(state).toInt()
    val flags = raf.readByte(state).toInt()
    state.incr(-2)

    val rawdata =
        structdsl("AttributeInfoMessage", raf, state) {
            fld("version", 1)
            fld("flags", 1)
            if (flags and 1 != 0) {
                fld("maxCreationIndex", 2)
            }
            fld("fractalHeapAddress", sizeOffsets)
            fld("attributeNameBtreeAddress", sizeOffsets)
            if (flags and 2 != 0) {
                fld("attributeOrderBtreeAddress", sizeOffsets)
            }
        }
    if (debugMessage) rawdata.show()

    return AttributeInfoMessage(
        readAttributesFromInfoMessage2(
            rawdata.getLong("fractalHeapAddress"),
            rawdata.getLong("attributeNameBtreeAddress"),
            if (flags and 2 != 0) rawdata.getLong("attributeOrderBtreeAddress") else null,
        )
    )
}

internal data class AttributeInfoMessage(val attributes: List<AttributeMessage>) : MessageHeader(MessageType.AttributeInfo) {
    override fun show() : String {
        return "nattributes = ${attributes.size}"
    }
}

/*
private fun H5builder.readAttributesFromInfoMessage(
    fractalHeapAddress: Long,
    attributeNameBtreeAddress: Long,
    attributeOrderBtreeAddress: Long?
): List<AttributeMessage> {

    val btreeAddress: Long = attributeOrderBtreeAddress ?: attributeNameBtreeAddress
    if (btreeAddress < 0 || fractalHeapAddress < 0) return emptyList()
    val btree2 = BTree2(this, "AttributeInfoMessage", btreeAddress, 0)
    val fractalHeap = FractalHeap(this, "AttributeInfoMessage", fractalHeapAddress)

    val list = mutableListOf<AttributeMessage>()
    for (e in btree2.readEntries()) {
        val heapId: ByteArray = when (btree2.btreeType) {
            8 -> (e.record as BTree2.Record8).heapId
            9 -> (e.record as BTree2.Record9).heapId
            else -> continue
        }

        // the heapId points to an Attribute Message in the fractal Heap
        val fractalHeapId = fractalHeap.getFractalHeapId(heapId)
        val state = OpenFileState(fractalHeapId.computePosition(), false)
        if (state.pos > 0) {
            val attMessage = this.readAttributeMessage(state)
            list.add(attMessage)
            if (debugFlow) {
                println("    read attMessage ${attMessage.show()}")
            }
        }
    }
    return list
}

 */

private fun H5builder.readAttributesFromInfoMessage2(
    fractalHeapAddress: Long,
    attributeNameBtreeAddress: Long,
    attributeOrderBtreeAddress: Long?
): List<AttributeMessage> {

    val btreeAddress: Long = attributeOrderBtreeAddress ?: attributeNameBtreeAddress
    if (btreeAddress < 0 || fractalHeapAddress < 0) return emptyList()
    val btree2j = BTree2data(this, "AttributeInfoMessage", btreeAddress)
    val fractalHeapj = FractalHeap(this, "AttributeInfoMessage", fractalHeapAddress)

    val attMessages = mutableListOf<AttributeMessage>()
    for (record in btree2j.records) {
        val heapId: ByteArray = when (btree2j.btreeType) {
            8 -> (record as BTree2data.Record8).heapId
            9 -> (record as BTree2data.Record9).heapId
            else -> continue
        }

        // the heapId points to an Attribute Message in the fractal Heap
        val fractalHeapId = fractalHeapj.getFractalHeapId(heapId)
        val state = OpenFileState(fractalHeapId.computePosition(), false)
        if (state.pos > 0) {
            val attMessage = this.readAttributeMessage(state)
            attMessages.add(attMessage)
            if (debugFlow) println("    read attMessage ${attMessage.show()}") }
        }
    return attMessages
}

////////////////////////////////////////// 22
// This message stores the number of hard links (in groups or objects) pointing to an object: in other words, its reference count.

internal fun H5builder.readReferenceCountMessage(state: OpenFileState): ReferenceCountMessage {
    val rawdata =
        structdsl("ReferenceCountMessage", raf, state) {
            fld("version", 1)
            fld("referenceCount", 4)
        }
    if (debugMessage) rawdata.show()

    return ReferenceCountMessage(
        rawdata.getInt("referenceCount"),
    )
}

// localHeapAddress aka nameHeapAddress
internal data class ReferenceCountMessage(val referenceCount: Int) : MessageHeader(MessageType.ObjectReferenceCount) {
    override fun show() : String {
        return "referenceCount = $referenceCount"
    }
}




