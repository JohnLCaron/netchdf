@file:OptIn(InternalLibraryApi::class)

package com.sunya.cdm.api

import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.cdm.util.makeValidCdmObjectName
import io.github.oshai.kotlinlogging.KotlinLogging

class Group(orgName : String,
            val typedefs : List<Typedef>,
            val dimensions : List<Dimension>,
            val attributes : List<Attribute<*>>,
            variableBuilders : List<Variable.Builder<*>>,
            groupBuilders : List<Builder>,
            val parent: Group?
    ) {
    val name : String = makeValidCdmObjectName(orgName)
    val variables : List<Variable<*>> = variableBuilders.map { it.build(this) }
    val groups : List<Group> = groupBuilders.map { it.build(this) }

    fun fullname() : String {
        return if (parent == null) "" else "${parent.fullname()}/$name"
    }

    /** find named attribute in this group */
    fun findAttribute(attName: String) : Attribute<*>? {
        return attributes.find{it.name == attName}
    }

    /** find named dimension in this group or a parent */
    fun findDimension(dimName: String) : Dimension? {
        return dimensions.find{it.name == dimName}?: parent?.findDimension(dimName)
    }

    /** find named Typedef in this group or a parent */
    fun findTypedef(typedefName: String) : Typedef? {
        return typedefs.find{it.name == typedefName}?: parent?.findTypedef(typedefName)
    }

    /** find the first nested group that matches the short name */
    fun findNestedGroupByShortName(shortName : String) : Group? {
        var found = groups.find { it.name == shortName }
        if (found != null) return found
        groups.forEach {
            found = it.findNestedGroupByShortName(shortName)
            if (found != null) return@findNestedGroupByShortName found
        }
        return found
    }

    /** find the first nested variable with a matching string attribute */
    fun findVariableByAttribute(attName: String, attValue: String): Variable<*>? {
        for (v in variables) {
            for (att in v.attributes) {
                if (attName == att.name && att.values.isNotEmpty() && attValue == att.values[0]) {
                    return v
                }
            }
        }
        for (nested in groups) {
            val v = nested.findVariableByAttribute(attName, attValue)
            if (v != null) {
                return v
            }
        }
        return null
    }

    fun allVariables() : List<Variable<*>> {
        val allVariables = mutableListOf<Variable<*>>()
        allVariables.addAll(variables)
        groups.forEach  { allVariables.addAll(it.allVariables()) }
        return allVariables
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Group) return false

        if (name != other.name) return false
        if (typedefs != other.typedefs) return false
        if (dimensions != other.dimensions) return false
        if (attributes != other.attributes) return false
        // we cant test equality with parent, since we get a loop. Just use the name
        if (parent?.name != other.parent?.name) return false
        if (variables != other.variables) return false
        if (groups != other.groups) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + typedefs.hashCode()
        result = 31 * result + dimensions.hashCode()
        result = 31 * result + attributes.hashCode()
        result = 31 * result + (parent?.name.hashCode())
        result = 31 * result + variables.hashCode()
        result = 31 * result + groups.hashCode()
        return result
    }

    override fun toString(): String {
        return if (parent == null) "root" else name
    }

    @InternalLibraryApi
    class Builder(val name : String) {
        val dimensions = mutableListOf<Dimension>()
        val attributes = mutableListOf<Attribute<*>>()
        val typedefs = mutableListOf<Typedef>()
        val variables = mutableListOf<Variable.Builder<*>>()
        val groups = mutableListOf<Builder>()
        var parent : Builder? = null

        // add if dim name not already added, else RuntimeException.
        fun addDimension(dim: Dimension) : Builder {
            if ( dimensions.find {it.name == dim.name } == null) {
                dimensions.add(dim)
            } else {
                throw RuntimeException("tried to add duplicate dimension '${dim.name}'")
            }
            return this
        }

        // return true if did not exist and was added
        fun addDimensionIfNotExists(dim: Dimension): Boolean {
            if (dimensions.find {it.name == dim.name } != null) {
                return false
            }
            addDimension(dim)
            return true
        }

        fun replaceDimension(dim: Dimension): Boolean {
            val found = dimensions.firstOrNull{ it.name == dim.name }
            if (found != null) dimensions.remove(found)
            addDimension(dim)
            return (found != null)
        }

        fun addAttribute(att: Attribute<*>) : Builder {
            attributes.add(att)
            return this
        }

        fun addAttributeIfNotExists(att: Attribute<*>) : Boolean {
            if (attributes.find {it.name == att.name } != null) {
                return false
            }
            attributes.add(att)
            return true
        }

        // add if vb name not already added
        fun addVariable(vb: Variable.Builder<*>) : Builder {
            if (vb.datatype == Datatype.REFERENCE) {
                logger.warn{"skip REFERENCE variable $vb"}
                return this
            }
            if (variables.find {it.name == vb.name } == null) {
                variables.add(vb)
            } else {
                // throw RuntimeException("tried to add duplicate variable '${vb.name}'")
                println("tried to add duplicate variable '${vb.name}'")
            }
            return this
        }

        // add if typedef name not already added
        fun addTypedef(typedef : Typedef) : Builder {
            if (typedefs.find { it.name == typedef.name} == null)
                typedefs.add(typedef)
            return this
        }

        fun addGroup(nested: Builder) : Builder {
            this.groups.add(nested)
            nested.parent = this
            return this
        }

        fun removeGroupIfExists(groupName: String): Boolean {
            val egroup = groups.find {it.name == groupName }
            return if (egroup == null) false else groups.remove(egroup)
        }

        fun removeEmptyGroups() : Boolean {
            this.groups.removeAll { subgroup -> subgroup.removeEmptyGroups() }
            return (this.groups.isEmpty() && this.variables.isEmpty() && this.attributes.isEmpty()
                    && this.typedefs.isEmpty() && this.dimensions.isEmpty())
        }

        // find the first nested group that matches the short name
        fun findNestedGroupByShortName(shortName : String) : Builder? {
            var found : Builder? = groups.find { it.name == shortName }
            if (found != null) return found
            groups.forEach {
                found = it.findNestedGroupByShortName(shortName)
                if (found != null) return@findNestedGroupByShortName found
            }
            return found
        }

        // find the common parent
        fun commonParent(other : Builder) : Builder {
            if (this == other) return this
            if (this.isParent(other)) return this
            if (other.isParent(this)) return other
            var found : Builder? = other
            while (found != null && !found.isParent(this)) {
                found = found.parent
            }
            return found!! // single rooted tree, cant be null
        }

        // am I the parent of other?
        fun isParent(other: Builder): Boolean {
            var found: Builder = other
            while (found != this && found.parent != null) {
                found = found.parent!!
            }
            return found == this
        }

        fun fullname() : String {
            return if (parent == null) "" else "${parent!!.fullname()}/$name"
        }

        var built = false
        fun build(parent : Group?) : Group {
            check(!built) { "Group '${this.name}' was already built"}
            built = true
            val useName = makeValidCdmObjectName(name)
            return Group(useName, typedefs, dimensions, attributes, variables, groups, parent)
        }
    }

    companion object {
        val logger = KotlinLogging.logger("Group")
    }
}