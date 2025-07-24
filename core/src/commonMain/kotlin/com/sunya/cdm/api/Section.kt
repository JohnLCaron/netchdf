package com.sunya.cdm.api

/** A filled section of multidimensional array indices, plus the variable shape. */
// TODO IndexSpace mo betta?
data class Section(val ranges : List<LongProgression>, val varShape : LongArray) {
    val rank = ranges.size
    val shape : LongArray // or IntArray ??
    val totalElements : Long

    init {
        ranges.forEach { require(it.last - it.first + 1 < Int.MAX_VALUE) }
        shape = ranges.map { (it.last - it.first + 1) }.toLongArray()
        totalElements = shape.computeSize()
        require(totalElements >= 0) // make sure no overflow
    }

    constructor(varShape: LongArray) : this( varShape.map {
        LongProgression.fromClosedRange(0L, it - 1L, 1L) }, varShape)

    constructor(start : IntArray, len : IntArray, varShape : LongArray) : this(
        start.mapIndexed { idx, first -> LongProgression.fromClosedRange(first.toLong(), (first + len[idx] - 1).toLong(), 1L) },
        varShape)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as Section

        if (ranges != other.ranges) return false
        if (!varShape.contentEquals(other.varShape)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = ranges.hashCode()
        result = 31 * result + varShape.contentHashCode()
        return result
    }

    override fun toString(): String {
        return "Section(${ranges.show()}, shape=${shape.contentToString()}, totalElements=$totalElements, varShape=${varShape.contentToString()})"
    }
}

fun List<LongProgression>.show() = buildString {
    forEach { p: LongProgression ->
        append("[${p.first}:${p.last}")
        if (p.step != 1L) append(":${p.step}")
        append("]")
   }
}

/** A partially filled section of multidimensional array indices. */
data class SectionPartial(val ranges : List<LongProgression?>) {

    /**
     * Check if this Section is legal for the given shape.
     * @param shape range must fit within this shape, rank must match.
     * @return error message if illegal, null if all ok
     */
    private fun checkInRange(shape: LongArray): String? {
        if (ranges.size != shape.size) {
            return "Number of ranges in section (${ranges.size}) must be = ${shape.size}"
        }
        for (i in ranges.indices) {
            val r = ranges[i] ?: continue
            if (r.isEmpty()) {
                return if (shape[i] != 0L) "Illegal Range for dimension $i: empty range only allowed for unlimited dimension len = 0" else continue
            }
            if (r.last >= shape[i]) return "Illegal Range for dimension $i: last requested ${r.last} > max ${shape[i]-1}"
        }
        return null
    }

    companion object {
        val SCALAR = Section(longArrayOf(1))

        /**
         * Return a Section guaranteed to be non null, with no null Ranges, and within the bounds set by shape.
         * A section with no nulls is called "filled".
         *
         * @param s the original Section, may be null or not filled
         * @param varShape use this as default shape if any of the ranges are null.
         * @return a filled SectionL
         * @throws InvalidRangeException if shape and s and shape rank dont match, or if s has invalid range compared to shape
         */
        @Throws(InvalidRangeException::class)
        fun fill(s: SectionPartial?, varShape: LongArray): Section {
            // want the entire variable's data
            if (s == null) {
                return Section(varShape)
            }
            if (varShape.isEmpty() && s.ranges.isEmpty()) {
                return SCALAR
            }
            val errs = s.checkInRange(varShape)
            if (errs != null) {
                s.checkInRange(varShape)
                throw InvalidRangeException(errs)
            }
            // where the range is missing, we want the entire variable's dimension length
            val filledRanges = s.ranges.mapIndexed { idx, range ->
                range ?: LongProgression.fromClosedRange(0L, varShape[idx] - 1L, 1L)
            }
            return Section(filledRanges, varShape)
        }

        /**
         * Parse an index section String specification, return equivalent Section.
         * A null Range means "all" (i.e.":") indices in that dimension.
         *
         * The sectionSpec string uses fortran90 array section syntax, namely:
         *
         * <pre>
         * sectionSpec := dims
         * dims := dim | dim, dims
         * dim := ':' | slice | start ':' end | start ':' end ':' stride
         * slice := INTEGER
         * start := INTEGER
         * stride := INTEGER
         * end := INTEGER
         *
         * where nonterminals are in lower case, terminals are in upper case, literals are in single quotes.
         *
         * Meaning of index selector :
         * ':' = all
         * slice = hold index to that value
         * start:end = all indices from start to end inclusive
         * start:end:stride = all indices from start to end inclusive with given stride
         *
        </pre> *
         *
         * @param sectionSpec the token to parse, eg "(1:20,:,3,10:20:2)", parenthesis optional
         * @throws InvalidRangeException when the Range is illegal
         * @throws IllegalArgumentException when sectionSpec is misformed
         */
        fun fromSpec(sectionSpec: String) : SectionPartial {
            val ranges = mutableListOf<LongProgression?>()
            var range: LongProgression?
            // val stoke = StringTokenizer(sectionSpec, "(),") // TODO deal with scatterRange {1,2,3}
            val tokens = sectionSpec.split("(", ")", ",") // TODO deal with scatterRange {1,2,3}
            tokens.forEach {
                val s = it.trim { it <= ' ' }
                range = if (s == ":") {
                    null // all
                } else if (s.indexOf(':') < 0) { // just a number : slice
                    try {
                        val index = s.toLong()
                        LongProgression.fromClosedRange(index, index, 1L)
                    } catch (e: NumberFormatException) {
                        throw IllegalArgumentException(" illegal selector: $s part of <$sectionSpec>")
                    }
                } else { // gotta be "start : end" or "start : end : stride"
                    // val stoke2 = StringTokenizer(s, ":")
                    val tokens2 = s.split(":")
                    require(tokens2.size in 2..3) { " illegal selector: $s part of <$sectionSpec>" }
                    val s1 = tokens2[0]
                    val s2 = tokens2[1]
                    val s3 = if (tokens2.size > 2) tokens2[2] else null
                    try {
                        val index1 = s1.toLong()
                        val index2 = s2.toLong()
                        val stride = s3?.toLong() ?: 1L
                        LongProgression.fromClosedRange(index1, index2, stride)
                    } catch (e: NumberFormatException) {
                        throw IllegalArgumentException(" illegal selector: $s part of <$sectionSpec>")
                    }
                }
                ranges.add(range)
            }
            return SectionPartial(ranges)
        }

    }
}

fun IntArray.computeSize(): Int {
    var product = 1
    this.forEach { product *= it }
    return product
}

fun IntArray.toLongArray(): LongArray = LongArray(this.size) { this[it].toLong() }

fun IntArray.equivalent(other: IntArray): Boolean {
    return this.toLongArray().equivalent(other.toLongArray())
}

// return outerShape, innerLength
fun IntArray.breakoutInner() : Pair<IntArray, Int> {
    val rank = this.size
    val innerLength: Int = this[rank - 1]
    val outerShape = this.copyOfRange(0, rank-1)
    return Pair(outerShape, innerLength)
}

fun LongArray.computeSize(): Long {
    var product = 1L
    this.forEach { product *= it }
    return product
}

fun LongArray.toIntArray(): IntArray = IntArray(this.size) { this[it].toInt() }

fun LongArray.equivalent(other: LongArray): Boolean {
    if (this.isScalar() and other.isScalar()) {
        return true
    }
    return this.contentEquals(other)
}

fun LongArray.isScalar(): Boolean {
    return this.isEmpty() || this.size == 1 && this[0] < 2
}