package com.sunya.netchdf.netcdf3

import com.sunya.cdm.api.*
import com.sunya.cdm.iosp.OpenFileIF
import com.sunya.cdm.iosp.OpenFileState
import com.sunya.cdm.iosp.OkioFileBuffered
import com.sunya.netchdf.NetchdfFileFormat
import com.fleeksoft.charset.Charset
import com.fleeksoft.charset.Charsets
import com.sunya.cdm.iosp.OkioFile
import com.sunya.cdm.util.InternalLibraryApi
import kotlin.math.min

/**
 * @see "https://docs.unidata.ucar.edu/netcdf-c/current/file_format_specifications.html"
 * @see "http://cucis.ece.northwestern.edu/projects/PnetCDF/CDF-5.html"
 */
@InternalLibraryApi
internal class N3header(rafOrg: OpenFileIF, val root: Group.Builder) {
  private val filePos = OpenFileState(0L, true)
  private val valueCharset = Charsets.UTF8
  private val isPnetcdf : Boolean
  private val useLongOffset : Boolean
  private val isStreaming : Boolean

  val numrecs : Long // number of records written
  val recsize : Long // size of each record (padded)
  var unlimitedDimension: Dimension? = null

  // not really needed
  private val dataStart : Long // where the data starts
  private val recordDataStart : Long // where the record data starts
  private val nonRecordDataSize : Long // size of non-record variables

  fun formatType() : String {
    return if (isPnetcdf) "netcdf3.5" else
           if (useLongOffset) "netcdf3.2" else
           "netcdf3"
  }

  init {
    val actualSize: Long = rafOrg.size()

    // netcdf magic number
    val format = NetchdfFileFormat.findNetcdfFormatType(rafOrg)
    if (!format.isNetdf3format) {
      throw RuntimeException("Not a Netcdf3 File")
    }
    filePos.pos = 4

    // use buffered io for efficiency
    val rafb = OkioFileBuffered(rafOrg as OkioFile, 4)
    isPnetcdf = (format == NetchdfFileFormat.NC_FORMAT_64BIT_DATA)
    useLongOffset = (format == NetchdfFileFormat.NC_FORMAT_64BIT_OFFSET) || isPnetcdf

    // number of records
    numrecs = if (isPnetcdf) rafb.readLong(filePos) else rafb.readInt(filePos).toLong()
    isStreaming = (numrecs == -1L)

    // dimensions
    readDimensions(rafb, root)

    // global attributes
    readAttributes(rafb, root.attributes)

    // variables
    readVariables(rafb, root)

    // count stuff
    val unlimitedVariables = mutableListOf<Variable.Builder<*>>()
    var sumRecord = 0L
    var firstData = Long.MAX_VALUE
    var firstRecordData = Long.MAX_VALUE
    var nonRecordDataSum: Long = 0 // size of non-record variables
    root.variables.forEach { vb ->
      val vinfo = vb.spObject as VinfoN3
      if (vinfo.isRecordVariable) {
        sumRecord += vinfo.vsize
        firstRecordData = min(firstRecordData, vinfo.begin)
        unlimitedVariables.add(vb)
      } else {
        nonRecordDataSum = nonRecordDataSum.coerceAtLeast(vinfo.begin + vinfo.vsize)
      }
      firstData = firstData.coerceAtMost(vinfo.begin)
    }
    this.dataStart = firstData
    this.recordDataStart = firstRecordData
    this.nonRecordDataSize = nonRecordDataSum

    // Check if file affected by bug CDM-52 (netCDF-Java library used incorrect padding when
    // the file contained only one record variable and it was of type byte, char, or short).
    // Example TestDir.cdmLocalTestDataDir + "byteArrayRecordVarPaddingTest-bad.nc"
    // Example devcdm/core/src/test/data/netcdf3/WrfTimesStrUnderscore.nc
    if (unlimitedVariables.size == 1) {
      val uvar = unlimitedVariables[0]
      val dtype = uvar.datatype
      if (dtype == Datatype.CHAR || dtype == Datatype.BYTE || dtype == Datatype.SHORT) {
        var nelems = 1L
        for (dim in uvar.dimensions) {
          if (dim != unlimitedDimension) nelems *= dim.length
        }
        val vinfo = uvar.spObject as VinfoN3
        val padding = padding(nelems * dtype.size)
        val calcVsize: Long = nelems * dtype.size // no padding

        if (calcVsize != vinfo.vsize) {
          if (debugHeaderSize) {
            println(" *** Special padding (${rafOrg.location()}): vsize=  ${vinfo.vsize} != calcVsize=$calcVsize padding = $padding")
          }
          sumRecord = calcVsize
          uvar.spObject = vinfo.copy(vsize = calcVsize)
        }
      }
    }
    this.recsize = sumRecord

    if (debug && debugHeaderSize) {
      println("  filePointer = ${filePos.pos} dataStart=$dataStart")
      println("  recStart = $recordDataStart dataStart+nonRecordDataSize = ${dataStart + nonRecordDataSize}")
      println("  nonRecordDataSize size= $nonRecordDataSize")
      println("  recsize= $recsize")
      println("  numrecs= $numrecs")
      println("  actualSize= $actualSize")
    }

    rafb.close()
  }

  private fun readDimensions(raf: OpenFileIF, root: Group.Builder) {
    val magic = raf.readInt(filePos)
    val numdims = if (magic == 0) {
      if (isPnetcdf) filePos.pos += 8 else filePos.pos += 4
      0
    } else {
      if (magic != MAGIC_DIM) throw RuntimeException("Malformed netCDF file - dim magic number wrong " + raf.location())
      if (isPnetcdf) raf.readLong(filePos).toInt() else raf.readInt(filePos)
    }

    // Must keep dimensions in strict order
    repeat (numdims) {
      val name = readString(raf)!!
      val len = if (isPnetcdf) raf.readLong(filePos) else raf.readInt(filePos).toLong()

      var dim: Dimension
      if (len == 0L) {
        dim = Dimension(name, numrecs, true)
        unlimitedDimension = dim
      } else {
        dim = Dimension(name, len, true)
      }
      root.addDimension(dim)
      if (debug) println("  dim $dim pos=${filePos.pos} isRecord=${dim == unlimitedDimension}")
    }
  }

  private fun readVariables(raf: OpenFileIF, root: Group.Builder) {
    // variables
    val magic = raf.readInt(filePos)
    val nvars = if (magic == 0) {
      if (isPnetcdf) filePos.pos += 8 else filePos.pos += 4
      0
    } else {
      if (magic != MAGIC_VAR) throw RuntimeException("Malformed netCDF file - var magic number wrong ${raf.location()}")
      if (isPnetcdf) raf.readLong(filePos).toInt() else raf.readInt(filePos)
    }

    // loop over variables
    repeat (nvars) {
      val name = readString(raf)!!
      if (debug) println("  reading variable $name pos=${filePos.pos}")

      // get element count in non-record dimensions
      var nelems: Long = 1
      var isRecord = false
      val rank: Int = if (isPnetcdf) raf.readLong(filePos).toInt() else raf.readInt(filePos)
      val dims = mutableListOf<Dimension>()
      val dimIdx = mutableListOf<Int>()
      val dimLengths = mutableListOf<Long>()
      repeat (rank) {
        val dimIndex: Int = if (isPnetcdf) raf.readLong(filePos).toInt() else raf.readInt(filePos)
        val dim: Dimension = root.dimensions[dimIndex]
        if (dim == unlimitedDimension) {
          isRecord = true
        } else {
          nelems *= dim.length
        }
        dims.add(dim)
        dimIdx.add(dimIndex)
        dimLengths.add(dim.length)
      }
      if (debug) println("  reading variable $name pos=${filePos.pos} dimIdx = $dimIdx")

      // variable attributes
      val varAtts = mutableListOf<Attribute<*>>()
      readAttributes(raf, varAtts)

      // data type
      val type: Int = raf.readInt(filePos)
      val datatype = getDatatype(type)
      val ncvarb = Variable.Builder(name, datatype)
      ncvarb.dimensions.addAll(dims)
      ncvarb.attributes.addAll(varAtts)

      // Use vsize, even if it disagrees with the "calculated" value. Clib recalculates, at least sometimes.
      val vsize = if (isPnetcdf) raf.readLong(filePos) else raf.readInt(filePos).toLong()
      val begin = if (useLongOffset) raf.readLong(filePos) else raf.readInt(filePos).toLong()
      if (debugVariableSize) {
        println("  reading variable $name type=$type nelems=$nelems elemSize=${datatype.size} vsize=$vsize begin=$begin isRecord=$isRecord")
        val padding = padding(nelems * datatype.size)
        val calcVsize: Long = nelems * datatype.size + padding
        if (vsize != calcVsize) println("    *** readVsize $vsize != calcVsize $calcVsize")
      }

      ncvarb.spObject = VinfoN3(name, vsize, begin, isRecord, datatype.size)
      root.variables.add(ncvarb)
      if (debug) println("  done variable ${ncvarb.datatype} ${ncvarb.name}${dimLengths} pos=${filePos.pos}")
    }
  }

  private fun readAttributes(raf: OpenFileIF, atts: MutableList<Attribute<*>>): Int {
    val magic: Int = raf.readInt(filePos)
    val natts = if (magic == 0) {
      if (isPnetcdf) filePos.pos += 8 else filePos.pos += 4
      0
    } else {
      if (magic != MAGIC_ATT) throw RuntimeException("Malformed netCDF file  - att magic number wrong " + raf.location())
      if (isPnetcdf) raf.readLong(filePos).toInt() else raf.readInt(filePos)
    }

    repeat (natts) {
      val name = readString(raf)!!
      val type: Int = raf.readInt(filePos)
      val att = if (type == 2) { // CHAR converted to String for Attributes
        val value = readString(raf, valueCharset)
        if (value == null) Attribute(name, Datatype.STRING, emptyList()) // nelems = 0
              else Attribute.from(name, value) // may be empty string
      } else {
        val nelems: Int = if (isPnetcdf) raf.readLong(filePos).toInt() else raf.readInt(filePos)
        val dtype = getDatatype(type)
        val builder = Attribute.Builder(name, dtype)
        if (nelems > 0) {
          val nbytes = readAttributeArray(raf, dtype, nelems, builder)
          skipToBoundary(nbytes)
        }
        builder.build()
      }
      atts.add(att)
      if (debug) {
        println("    $att pos=${filePos.pos}")
        att.values.forEach { println("$it")}
      }
    }
    return natts
  }

  fun readAttributeArray(raf: OpenFileIF, type: Datatype<*>, nelems: Int, attBuilder: Attribute.Builder<*>): Int {
    return when (type) {
      Datatype.BYTE -> {
        attBuilder.setValues(raf.readArrayOfByte(filePos, nelems).toList())
        nelems
      }
      Datatype.UBYTE -> {
        attBuilder.setValues(raf.readArrayOfByte(filePos, nelems).map { it.toUByte() })
        nelems
      }
      Datatype.CHAR -> {
        // a CHAR is made into a String with UTF8 assumed. TODO make this settable ??
        attBuilder.setValue(raf.readString(filePos, nelems))
        nelems
      }
      Datatype.SHORT -> {
        attBuilder.setValues(raf.readArrayOfShort(filePos, nelems).asList())
        2 * nelems
      }
      Datatype.USHORT -> {
        attBuilder.setValues(raf.readArrayOfShort(filePos, nelems).map { it.toUShort() })
        2 * nelems
      }
      Datatype.INT -> {
        attBuilder.setValues(raf.readArrayOfInt(filePos, nelems).asList())
        4 * nelems
      }
      Datatype.UINT -> {
        attBuilder.setValues(raf.readArrayOfShort(filePos, nelems).map { it.toUInt() })
        4 * nelems
      }
      Datatype.FLOAT -> {
        attBuilder.setValues(raf.readArrayOfFloat(filePos, nelems).asList())
        4 * nelems
      }
      Datatype.DOUBLE -> {
        attBuilder.setValues(raf.readArrayOfDouble(filePos, nelems).asList())
        8 * nelems
      }
      Datatype.LONG -> {
        attBuilder.setValues(raf.readArrayOfLong(filePos, nelems).asList())
        8 * nelems
      }
      Datatype.ULONG -> {
        attBuilder.setValues(raf.readArrayOfLong(filePos, nelems).map { it.toULong() })
        8 * nelems
      }
      else -> return 0
    }
  }

  // read a string = (nelems, byte array), then skip to 4 byte boundary
  private fun readString(raf: OpenFileIF, charset: Charset = Charsets.UTF8): String? {
    val nelems: Int = if (isPnetcdf) raf.readLong(filePos).toInt() else raf.readInt(filePos)
    val s = raf.readString(filePos, nelems, charset)
    skipToBoundary(nelems) // pad to 4 byte boundary
    return if (nelems == 0) null else s // we need this to acree with C library
  }

  // skip to a 4 byte boundary in the file
  fun skipToBoundary(nbytes: Int) {
    filePos.pos += padding(nbytes)
  }

  companion object {
    const val debug = false
    const val debugVariableSize = false
    var debugHeaderSize = false

    const val MAGIC_DIM = 10
    const val MAGIC_VAR = 11
    const val MAGIC_ATT = 12

    // find number of bytes needed to pad to a 4 byte boundary
    fun padding(nbytes: Int): Int {
      var pad = nbytes % 4
      if (pad != 0) pad = 4 - pad
      return pad
    }

    // find number of bytes needed to pad to a 4 byte boundary
    fun padding(nbytes: Long): Int {
      var pad = (nbytes % 4).toInt()
      if (pad != 0) pad = 4 - pad
      return pad
    }

    fun getDatatype(type: Int): Datatype<*> {
      return when (type) {
        1 -> Datatype.BYTE
        2 -> Datatype.CHAR
        3 -> Datatype.SHORT
        4 -> Datatype.INT
        5 -> Datatype.FLOAT
        6 -> Datatype.DOUBLE
        // the rest are possible in pnetcdf
        7 -> Datatype.UBYTE
        8 -> Datatype.USHORT
        9 -> Datatype.UINT
        10 -> Datatype.LONG
        11 -> Datatype.ULONG
        else -> throw IllegalArgumentException("unknown type == $type")
      }
    }
  }
}

/**
 * @param vsize size of data in bytes; if isRecordVariable, size per record.
 * @param begin offset of start of data from start of file
 * @param isRecordVariable  is it a record variable?
 * @param elemSize  size in bytes of one element
 */
internal data class VinfoN3(
  val name: String,
  val vsize: Long, // number of bytes. if record, per record else total.
  val begin: Long,
  val isRecordVariable: Boolean,
  val elemSize: Int
)