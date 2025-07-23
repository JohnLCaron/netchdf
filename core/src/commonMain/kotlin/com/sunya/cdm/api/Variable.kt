package com.sunya.cdm.api

import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.cdm.util.makeValidCdmObjectName

data class Variable<T>(
    val group : Group,
    val orgName: String, // artifact of being a data class
    val datatype: Datatype<T>,
    val dimensions: List<Dimension>,
    val attributes: List<Attribute<*>>,
    val spObject: Any?,
) {
    val name = makeValidCdmObjectName(orgName)
    val rank : Int = dimensions.size
    val shape : LongArray = dimensions.map { it.length }.toLongArray() // if you allow dimensions to be long, you need LongArray shape.
    val nelems : Long = this.shape.computeSize()

    fun fullname() : String {
        return if (group.fullname() == "") name else "${group.fullname()}/$name"
    }

    /** find named attribute in this Variable */
    fun findAttribute(attName: String) : Attribute<*>? {
        return attributes.find {it.name == attName}
    }

    /////////////////////////////////////////////////////////////////////////////////////////

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Variable<*>) return false

        if (name != other.name) return false
        if (datatype != other.datatype) return false
        if (dimensions != other.dimensions) return false
        if (attributes != other.attributes) return false
        return shape.contentEquals(other.shape)
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + datatype.hashCode()
        result = 31 * result + dimensions.hashCode()
        result = 31 * result + attributes.hashCode()
        result = 31 * result + shape.contentHashCode()
        return result
    }

    fun nameAndShape(): String {
        return "$datatype ${fullname()}${shape.contentToString()}"
    }

    @InternalLibraryApi
    class Builder<T>(val name : String, val datatype : Datatype<T>) {
        val dimensions = mutableListOf<Dimension>()
        val attributes = mutableListOf<Attribute<*>>()
        var spObject: Any? = null
        var dimNames: List<String>? = null

        fun addAttribute(attr : Attribute<*>) : Builder<T> {
            attributes.add(attr)
            return this
        }

        fun addDimension(dim : Dimension) : Builder<T> {
            dimensions.add(dim)
            return this
        }

        fun setDimensionsAnonymous(shape : LongArray) : Builder<T> {
            dimensions.clear()
            dimNames = null
            for (len in shape) {
                dimensions.add(Dimension("", len, false))
            }
            return this
        }

        fun setDimensionsAnonymous(shape : IntArray) : Builder<T> {
            return setDimensionsAnonymous(shape.toLongArray())
        }

        fun fullname(group : Group.Builder) : String {
            return if (group.fullname() == "") name else "${group.fullname()}/$name"
        }

        fun build(group : Group) : Variable<T> {
            var useDimensions = dimensions.toList()
            if (dimNames != null) {
                useDimensions = dimNames!!.map { getDimension(it, group) }
            }
            return Variable(group, name, datatype, useDimensions, attributes, spObject)
        }

        private fun getDimension(dimName : String, group : Group) : Dimension {
            val vname = makeValidCdmObjectName(dimName)
            var d = group.findDimension(vname)
            if (d == null) {
                d = try {
                    val length = dimName.toLong()
                    Dimension("", length, false)
                } catch(e : Exception) {
                    throw RuntimeException("unknown dimension '$dimName' in Variable '$name'")
                }
            }
            return d!!
        }

        override fun toString(): String {
            return "'$name' $datatype, dimensions=$dimensions, dimList=$dimNames"
        }
    }
}