package com.sunya.cdm.layout

/**
 * A chunk of data that is contiguous in both the source and destination.
 * Everything here is in elements, not bytes.
 * Read nelems from src at srcElem, store in destination at destElem.
 */
data class TransferChunk(
    val srcElem : Long, // start reading here in the source
    val nelems: Int,    // read these many contiguous elements
    val destElem: Long  // start transferring to here in destination
)

/**
 * Adds the srcPos to the TransferChunk.
 */
internal data class LayoutChunk(
    val srcPos: Long,   // byte position in source where to read from
    val srcElem : Long, // start reading here in the source
    val nelems: Int,    // read these many contiguous elements
    val destElem: Long  // start transferring to here in destination
) : Layout.Chunk {

    constructor(srcPos : Long, tchunk : TransferChunk) : this(srcPos, tchunk.srcElem, tchunk.nelems, tchunk.destElem)

    override fun srcPos() = srcPos
    override fun srcElem() = srcElem
    override fun nelems() = nelems
    override fun destElem() = destElem
}