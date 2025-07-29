package com.sunya.cdm.api

import com.sunya.cdm.util.makeValidCdmObjectName

/**
 * @param length: Long, not Unsigned Long. see "https://github.com/Kotlin/KEEP/blob/master/proposals/unsigned-types.md"
 */
data class Dimension(val orgName : String, val length : Long, val isShared : Boolean) {
    val name = makeValidCdmObjectName(orgName)

    constructor(name : String, len : Int) : this(name, len.toLong(), true)
    constructor(len : Int) : this("", len.toLong(), false)
    constructor(len : Long) : this("", len, false)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as Dimension

        if (length != other.length) return false
        if (isShared != other.isShared) return false
        if (name != other.name) return false

        return true
    }

    override fun hashCode(): Int {
        var result = length.hashCode()
        result = 31 * result + isShared.hashCode()
        result = 31 * result + name.hashCode()
        return result
    }

}