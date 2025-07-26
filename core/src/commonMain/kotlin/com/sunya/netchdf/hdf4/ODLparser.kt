@file:OptIn(InternalLibraryApi::class)

package com.sunya.netchdf.hdf4

import com.sunya.cdm.api.Dimension
import com.sunya.cdm.api.Group
import com.sunya.cdm.util.Indent
import com.sunya.cdm.util.InternalLibraryApi
import com.sunya.cdm.util.makeValidCdmObjectName
import io.github.oshai.kotlinlogging.KotlinLogging

/*
 * http://newsroom.gsfc.nasa.gov/sdptoolkit/hdfeosfaq.html
 * 
 * 3.2 What types of metadata are embedded in an HDF-EOS file and what are the added storage requirements?
 * An HDF-EOS file must contain ECS "core" metadata which is essential for ECS search services. Core metadata are
 * populated using the SDP Toolkit, rather than through HDF-EOS calls. "Archive" metadata (supplementary information
 * included by the data provider) may also be present. If grid, point, or swath data types have been used,
 * there also will be structural metadata describing how these data types have been translated into standard HDF
 * data types. Metadata resides in human-readable form in the Object Descriptor Language (ODL). Structural metadata uses
 * 32K of storage, regardless of the amount actually required. The sizes of the core and archive metadata vary
 * depending on what has been entered by the user.
 * 
 * 3.3 What are the options for adding ECS metadata to standard HDF files?
 * For data products that will be accessed by ECS but which remain in native HDF, there is a choice of
 *  1) adding no ECS metadata in the HDF file,
 *  2) inserting ECS metadata into the HDF file, or
 *  3) "appending" ECS metadata to the HDF file. "Append" means updating the HDF location table so that the appended
 * metadata becomes known to the HDF libraries/tools.
 * 
 * 3.4 Some DAACs currently provide descriptor files that give background information about the data. Will this
 * information be included in an HDF-EOS file? Yes. The descriptor file will be retained. It can be viewed by
 * EOSView if it stored either as a global attribute or a file annotation.
 */
private val EOSprefix = listOf("archivemetadata",  "coremetadata", "productmetadata", "structmetadata", "oldstructmetadata",
    "gridstructure", "pointstructure", "swathstructure")

class EOS {
    companion object {
        fun isMetadata(name : String) : Boolean {
            val lowername = name.lowercase()
            return EOSprefix.map{ lowername.startsWith(it) }.reduce { a,b -> a or b}
        }
    }

}

private val renameGroups = listOf("SwathName", "GridName", "PointName")
private val wantGroup = listOf("GeoField", "DataField")
private val wantGroupNewName = listOf("Geolocation Fields", "Data Fields")

internal data class ODLobject(var name: String) {
    val attributes = mutableListOf<Pair<String, String>>()
    fun toString(indent: Indent): String {
        return buildString {
            append("${indent}var $name\n")
            val nindent = indent.incr()
            attributes.forEach { append("$nindent  '${it.component1()}'='${it.component2()}'\n") }
        }
    }
}

internal class ODLgroup(var name: String, val parent: ODLgroup?) {
    val nested = mutableListOf<ODLgroup>()
    val variables = mutableListOf<ODLobject>()
    val attributes = mutableListOf<Pair<String, String>>()

    fun isEmpty() = nested.isEmpty() and variables.isEmpty() and attributes.isEmpty()

    fun addVariable(name: String): ODLobject {
        val result = ODLobject(name)
        variables.add(result)
        return result
    }

    override fun toString(): String {
        return toString(Indent(2))
    }

    fun toString(indent: Indent): String {
        return buildString {
            append("${indent}group $name\n")
            val nindent = indent.incr()
            attributes.forEach { append("$nindent  '${it.component1()}'=${it.component2()}\n") }
            variables.forEach { append(it.toString(nindent)) }
            nested.forEach { append(it.toString(nindent)) }
        }
    }
}

internal fun ODLtransform(org: ODLgroup): ODLgroup {
    val root = ODLgroup("root", null)
    org.nested.forEach { transform(it, root) }

    // find all requested dimensions
    val dims = mutableSetOf<String>()
    findReqDimensions(root, dims)
    // println("findReqDimensions = $dims ")
    removeFoundDimensions(root, dims)
    // println("removeFoundDimensions = $dims ")
    if (dims.isNotEmpty()) {
        addMissingDimensions(root, dims)
    }
    return root
}

private fun transform(org: ODLgroup, trans: ODLgroup) {
    // println("massage(${parent.name}, ${org.name})")
    if (org.name == "Dimension" && org.variables.isNotEmpty()) {
        trans.variables.add(transformDims(org))
        return
    }

    // LOOK for groups that we want to turn into nested groups
    var wantName: String? = null
    val namePair = org.attributes.find { renameGroups.contains(it.component1()) }
    if (namePair != null) {
        wantName = namePair.component2()
        org.attributes.remove(namePair)
    } else {
        wantGroup.forEachIndexed { idx, it ->
            if (it == org.name) {
                wantName = wantGroupNewName[idx]
            }
        }
    }

    if (wantName != null) {
        val nestedTrans = ODLgroup(wantName, trans)
        trans.nested.add(nestedTrans)
        if (org.variables.isNotEmpty()) {
            nestedTrans.variables.add(transformVariables(org))
        }
        org.nested.forEach { transform(it, nestedTrans) }
        nestedTrans.attributes.addAll(org.attributes)
    } else {
        org.nested.forEach { transform(it, trans) }
        trans.attributes.addAll(org.attributes)
    }
}

private fun transformDims(dimGroup: ODLgroup): ODLobject {
    val result = ODLobject("Dimensions")
    dimGroup.variables.filter { it.name.startsWith("Dimension") }.map {
        var name = "N/A"
        var size = "N/A"
        it.attributes.forEach { att ->
            if (att.component1() == "DimensionName") {
                name = att.component2()
            }
            if (att.component1() == "Size") {
                size = att.component2()
            }
        }
        result.attributes.add(Pair(name, size))
    }
    return result
}

private fun transformVariables(fldGroup: ODLgroup): ODLobject {
    val result = ODLobject("Variables")
    fldGroup.variables.filter { it.name.contains("Field") }.map {
        var name = "N/A"
        var dims = "N/A"
        it.attributes.forEach { att ->
            if (att.component1().contains("FieldName")) {
                name = att.component2()
            }
            if (att.component1() == "DimList") {
                dims = att.component2()
            }
        }
        result.attributes.add(Pair(name, dims))
    }
    return result
}

internal fun findReqDimensions(group: ODLgroup, dims: MutableSet<String>) {
    val v = group.variables.find { it.name == "Variables" }
    v?.attributes?.forEach { att ->
        att.component2().split(",").forEach { dims.add(it) }
    }
    group.nested.forEach { findReqDimensions(it, dims) }
}

internal fun removeFoundDimensions(group: ODLgroup, dims: MutableSet<String>) {
    val v = group.variables.find { it.name == "Dimensions" }
    v?.attributes?.forEach { att -> dims.remove(att.component1()) }
    group.nested.forEach { removeFoundDimensions(it, dims) }
}

internal fun addMissingDimensions(group: ODLgroup, dims: MutableSet<String>) {
    val addDimsFromAtt = mutableListOf<Pair<String, String>>()
    group.attributes.forEach { att ->
        if (dims.contains(att.component1())) {
            addDimsFromAtt.add(att)
        }
    }
    if (addDimsFromAtt.isNotEmpty()) {
        val container = group.variables.find { it.name == "Dimensions" } ?: group.addVariable("Dimensions")
        container.attributes.addAll(addDimsFromAtt)
        addDimsFromAtt.forEach { group.attributes.remove(it) }
    }
    group.nested.forEach { addMissingDimensions(it, dims) }
}

////////////////////////////////////////////////////////////////////////////////////////////////

// parses the original ODL
internal fun ODLparseFromString(text: String): ODLgroup {
    val root = ODLgroup("root", null)
    var currentStruct = root
    var currentObject: ODLobject? = null
    val t1 = text.split("\t","\n","\r","\u000c").map { it.trim()}.filter { it.isNotEmpty() }
    /* val t2 = StringTokenizer(text, "\t\n\r\u000c").toList()
    val nt = min(t1.size, t2.size)
    repeat(nt) {
        println(" ${t1[it]}, ${t2[it]}")
        require(t1[it] == t2[it])
    } */
    //val lineFinder = StringTokenizer(text, "\t\n\r\u000c")

    t1.forEach { line ->
        if (line.startsWith("GROUP")) {
            currentStruct = startGroup(currentStruct, line)
        } else if (line.startsWith("OBJECT")) {
            currentObject = startObject(currentStruct, line)
        } else if (line.startsWith("END_OBJECT")) {
            endObject(currentObject!!, line)
            currentObject = null
        } else if (line.startsWith("END_GROUP")) {
            endGroup(currentStruct, line)
            currentStruct = currentStruct.parent!!
        } else if (line.startsWith("END")) {
            // noop
        } else if (currentObject != null) {
            addFieldToObject(currentObject, line)
        } else {
            addFieldToGroup(currentStruct, line)
        }
    }
    return root
}

private fun startGroup(parent: ODLgroup, line: String): ODLgroup {
    val stoke = line.split("=").iterator()
    val toke = stoke.next()
    require(toke == "GROUP")
    val name = stoke.next()
    val group = ODLgroup(name, parent)
    parent.nested.add(group)
    return group
}

private fun endGroup(current: ODLgroup, line: String) {
    val stoke = line.split("=").iterator()
    val toke = stoke.next()
    require(toke == "END_GROUP")
    val name = stoke.next()
    require(name == current.name)
}

private fun startObject(current: ODLgroup, line: String): ODLobject {
    val stoke = line.split("=").iterator()
    val toke = stoke.next()
    require(toke == "OBJECT")
    val name = stoke.next()
    val obj = ODLobject(name)
    current.variables.add(obj)
    return obj
}

private fun endObject(current: ODLobject, line: String) {
    val stoke = line.split("=").iterator()
    val toke = stoke.next()
    require(toke == "END_OBJECT")
    val name = stoke.next()
    require(name == current.name)
}

private fun addFieldToGroup(current: ODLgroup, line: String) {
    val stoke = line.split("=").iterator()
    val name = stoke.next()
    if (stoke.hasNext()) {
        var value = stoke.next()
        if (value.startsWith("(")) {
            current.attributes.add(Pair(name, parseValueCollection(value)))
            return
        }
        value = stripQuotes(value)
        current.attributes.add(Pair(name, value))
    }
}

private fun addFieldToObject(current: ODLobject, line: String) {
    val stoke = line.split("=").iterator()
    val name = stoke.next()
    if (stoke.hasNext()) {
        var value = stoke.next()
        if (value.startsWith("(")) {
            current.attributes.add(Pair(name, parseValueCollection(value)))
            return
        }
        value = stripQuotes(value)
        current.attributes.add(Pair(name, value))
    }
}

private fun parseValueCollection(valueInput: String): String {
    val value = stripParens(valueInput)
    val stoke = value.split(",").iterator()
    val list = mutableListOf<String>()
    while (stoke.hasNext()) {
        val t = stoke.next()
        list.add(stripQuotes(t))
    }
    return list.joinToString(",")
}

private fun stripParens(name: String): String {
    var name = name
    if (name.startsWith("(")) name = name.substring(1)
    if (name.endsWith(")")) name = name.substring(0, name.length - 1)
    return name
}

private fun stripQuotes(name: String): String {
    var name = name
    if (name.startsWith("\"")) name = name.substring(1)
    if (name.endsWith("\"")) name = name.substring(0, name.length - 1)
    return name
}

////////////////////////////////////////////////////////////////////////////////////////////////

@InternalLibraryApi
class ODLparser(val rootGroup: Group.Builder, val show : Boolean = false) {

    fun applyStructMetadata(structMetadata: String) : Boolean {
       if (showDetail) println("structMetadata = \n$structMetadata")
        val odl = ODLparseFromString((structMetadata))
        if (showDetail)  println("odl = \n$odl")
        val odlt = ODLtransform(odl)
        if (show or showDetail) println("ODL transformed = \n$odlt")

        if (!odlt.validateStructMetadata(rootGroup)) {
            if (showValidationFailures) println("***ODL did not validate")
            return false
//            throw RuntimeException("ODL did not validate")
        }
        odlt.applyStructMetadata(rootGroup)
        return true
    }

    // assumes that the existing variables are already in groups; adjust dimensions and add attributes
    private fun ODLgroup.applyStructMetadata(parent: Group.Builder) {
        this.variables.forEach { v ->
            if (v.name == "Dimensions") {
                parent.dimensions.clear() // LOOK should only delete dimensions that are replaced ??
                v.attributes.forEach { att ->
                    parent.addDimension(Dimension(att.component1(), att.component2().toInt()))
                }
            }
            if (v.name == "Variables") {
                v.attributes.forEach { att ->
                    val name = att.component1()
                    val dimList = att.component2().split(",")
                    val odlname = makeValidCdmObjectName(name)
                    val vb = parent.variables.find { it.name == name } ?: parent.variables.find { it.name == odlname }
                    if (vb == null) {
                        if (showProblems) println(" *** ODL cant find variable $name")
                    } else {
                        vb.dimNames = dimList
                        vb.dimensions.clear()
                    }
                }
            }
        }
        this.nested.forEach { odl ->
            if (!odl.isEmpty()) {
                val ngroup = parent.findNestedGroupByShortName(odl.name) ?:
                             parent.findNestedGroupByShortName(makeValidCdmObjectName(odl.name))
                if (ngroup != null) {
                    odl.applyStructMetadata(ngroup)
                }
            }
        }
    }

    // assumes that the existing variables are already in groups; adjust dimensions and add attributes
    private fun ODLgroup.validateStructMetadata(parent: Group.Builder) : Boolean {
        this.variables.forEach { v ->
            if (v.name == "Dimensions") {
                v.attributes.forEach { att ->
                    try {
                        val dimLength = att.component2().toInt()
                        if (dimLength == 0) {
                            logger.warn{" *** ODL has zero dimension length for ${att.component1()}"}
                            return false
                        }
                    } catch (ex : Exception) {
                        logger.warn{" *** ODL cant parse dimension ${att.component1()} length ${att.component2()} message=${ex.message}"}
                        return false
                    }
                }
            }
            if (v.name == "Variables") {
                v.attributes.forEach { att ->
                    val name = att.component1()
                    val odlname = makeValidCdmObjectName(name)
                    val vb = parent.variables.find { it.name == name } ?: parent.variables.find { it.name == odlname }
                    if (vb == null) {
                        if (showProblems) println(" *** ODL cant find variable $name")
                        return false
                    }
                }
            }
        }
        this.nested.forEach { odl ->
            if (!odl.isEmpty()) {
                val ngroup = parent.findNestedGroupByShortName(odl.name) ?:
                             parent.findNestedGroupByShortName(makeValidCdmObjectName(odl.name))
                if (ngroup == null) {
                    if (showProblems) println(" *** ODL cant find GROUP ${odl.name}")
                    parent.findNestedGroupByShortName(makeValidCdmObjectName(odl.name))
                    return false
                } else {
                    if (!odl.validateStructMetadata(ngroup))
                        return false
                }
            }
        }
        return true
    }

    companion object {
        private val logger = KotlinLogging.logger("ODLParser")

        private const val showDetail = false
        private const val showProblems = false
        private const val showValidationFailures = false
    }
}