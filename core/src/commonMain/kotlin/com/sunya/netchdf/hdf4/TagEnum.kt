package com.sunya.netchdf.hdf4

import com.sunya.cdm.util.InternalLibraryApi

/*
  hdf/src/htags.h
  java/src/hdf/hdflib/HDFConstants.java
 */
@InternalLibraryApi
enum class TagEnum(val desc: String, val code: Int) {
    NONE("", 0),
    NULL("", 1),
    RLE("Run length encoding", 11),
    IMC("IMCOMP compression alias", 12),
    IMCOMP( "IMCOMP compression", 12),
    JPEG( "JPEG compression (24-bit data)", 13),
    GREYJPEG("JPEG compression (8-bit data)", 14),
    JPEG5("JPEG compression (24-bit data)", 15),
    GREYJPEG5("JPEG compression (8-bit data)", 16),
    LINKED("Linked-block special element", 20),
    VERSION("Version", 30),
    COMPRESSED("Compressed special element", 40), // 0x28
    VLINKED("Variable-len linked-block header", 50),
    VLINKED_DATA("Variable-len linked-block data", 51),
    CHUNKED("Chunked special element header", 60),
    CHUNK("Chunk element", 61), // 0x3d
    FID("File identifier", 100),
    FD("File description", 101),
    TID("Tag identifier", 102),
    TD("Tag descriptor", 103),
    DIL("Data identifier label", 104),
    DIA("Data identifier annotation", 105),
    NT("Number type", 106),
    MT("Machine type", 107),
    FREE("Free space in the file", 108),
    ID8("8-bit Image dimension", 200), // obsolete
    IP8("8-bit Image palette", 201), // obsolete
    RI8("Raster-8 image", 202), // obsolete
    CI8("RLE compressed 8-bit image", 203), // obsolete
    II8("IMCOMP compressed 8-bit image", 204), // obsolete
    ID("Image DimRec", 300),
    LUT("Image Palette", 301),
    RI("Raster Image", 302),
    CI("Compressed Image", 303),
    NRI("New-format Raster Image", 304),
    RIG("Raster Image Group", 306),
    LD("Palette DimRec", 307),
    MD("Matte DimRec", 308),
    MA("Matte Data", 309),
    CCN("Color correction", 310),
    CFM("Color format", 311),
    AR("Cspect ratio", 312),
    DRAW("Draw these images in sequence", 400),
    RUN("Cun this as a program/script", 401),
    XYP("X-Y position", 500),
    MTO("Machine-type override", 501),
    T14("TEK 4014 data", 602),
    T105("TEK 4105 data", 603),
    SDG("SD Group", 700), // obsolete
    SDD("SD DimRec", 701),
    SD("Scientific Data", 702),
    SDS("Scales", 703),
    SDL("Labels", 704),
    SDU("Units", 705),
    SDF("Formats", 706),
    SDM("Max/Min", 707),
    SDC("Coord sys", 708),
    SDT("Transpose", 709), // obsolete
    SDLNK("Links related to the dataset", 710), // links SDG and NDG
    NDG("Numeric Group", 720),
    IGNORE("Ignore", 721),
    /* Objects of tag 721 are never actually written to the file.  The tag is needed to make things easier mixing
       DFSD and SD style objects in the same file */
    CAL("Calibration information", 731),
    FV("Fill Value information", 732),
    BREQ("Beginning of required tags", 799),
    SDRAG("List of ragged array line lengths", 781),
    EREQ("Current end of the range", 780),
    VH("Vdata Header", 1962),
    VS("Vdata Storage", 1963),
    VG("Vgroup", 1965),
    ;

    companion object {
        const val SPECIAL_LINKED = 1 /* Fixed-size Linked blocks */
        const val SPECIAL_EXT = 2 /* External */
        const val SPECIAL_COMP = 3 /* Compressed */
        const val SPECIAL_VLINKED = 4 /* Variable-length linked blocks */
        const val SPECIAL_CHUNKED = 5 /* chunked element */
        const val SPECIAL_BUFFERED = 6 /* Buffered element */
        const val SPECIAL_COMPRAS = 7 /* Compressed Raster element */

        const val COMP_CODE_NONE = 0 // don't encode at all, just store
        const val COMP_CODE_RLE = 1 // for simple RLE encoding
        const val COMP_CODE_NBIT = 2 // for N-bit encoding
        const val COMP_CODE_SKPHUFF = 3 // for Skipping huffman encoding
        const val COMP_CODE_DEFLATE = 4 // for gzip 'deflate' encoding
        const val COMP_CODE_SZIP = 5 // for szip encoding

        private var hashCodes : MutableMap<Int, TagEnum>? = null

        fun byCode(code: Int): TagEnum {
            if (hashCodes == null) { // LOOK may fail when multithreading
                val mapit = mutableMapOf<Int, TagEnum>()
                entries.forEach { mapit[it.code] = it}
                hashCodes = mapit
            }
            val te = hashCodes!![code]
            return te?: NONE
        }

        val obsolete = setOf(ID8, RI8, CI8, II8, SDG, SDLNK, IGNORE) // LOOK
    }

    override fun toString(): String {
        return "DFTAG_$name ($code) $desc"
    }

}