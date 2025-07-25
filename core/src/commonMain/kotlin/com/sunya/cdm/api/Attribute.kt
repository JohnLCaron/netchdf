package com.sunya.cdm.api

import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.cdm.util.makeValidCdmObjectName

data class Attribute<T>(val orgName : String, val datatype : Datatype<T>, val values : List<T>) {
    val name = makeValidCdmObjectName(orgName)
    val isString = (datatype == Datatype.CHAR) || (datatype== Datatype.STRING)

    companion object {
        fun from(name : String, value : String) = Attribute(name, Datatype.STRING, listOf(value))
    }

    @InternalLibraryApi
    class Builder<T>(val name : String, var datatype : Datatype<T>) {
        var values : List<T> = emptyList()

        fun setValues(values : List<*>) : Builder<T> {
            this.values = values as List<T>
            return this
        }

        fun setValue(value : Any) : Builder<T> {
            this.values = listOf(value as T)
            return this
        }

        fun build() : Attribute<T> {
             return Attribute(name, datatype, values)
        }
    }
}
