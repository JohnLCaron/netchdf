package com.sunya.cdm.layout

import kotlin.math.max
import kotlin.math.min

/**
 * A Tiling divides a multidimensional index space into tiles.
 * Indices are points in the original multidimensional index space.
 * Tiles are points in the "tiled space" ~ varShape / chunkShape
 * Each tile has the same index size, given by chunk.
 *
 * Allows to efficiently find the data chunks that cover an arbitrary section of the variable's index space.
 *
 * @param varShape the variable's shape
 * @param chunkShape  actual data storage has this shape. May be larger than the shape, last dim ignored if rank > varshape.
 */
class Tiling(varShape: LongArray, chunkShape: LongArray) {
    val chunk = chunkShape.copyOf()
    val rank: Int
    val tileShape : LongArray // overall shape of the dataset's tile space
    val indexShape : LongArray // overall shape of the dataset's index space - may be larger than actual variable shape
    val tileStrider : LongArray // for computing tile index

    init {
        // convenient to allow tileSize to have (an) extra dimension at the end
        // to accommodate hdf5 storage, which has the element size
        require(varShape.size <= chunk.size)
        rank = varShape.size
        this.indexShape = LongArray(rank)
        for (i in 0 until rank) {
            this.indexShape[i] = max(varShape[i], chunk[i])
        }
        this.tileShape = LongArray(rank)
        for (i in 0 until rank) {
            tileShape[i] = (this.indexShape[i] + chunk[i] - 1) / chunk[i]
        }
        tileStrider = LongArray(rank)
        var accumStride = 1L
        for (k in rank - 1 downTo 0) {
            tileStrider[k] = accumStride
            accumStride *= tileShape[k]
        }
    }

    /** Compute the tile from an index, ie which tile does this point belong to? */
    fun tile(index: LongArray): LongArray {
        val useRank = min(rank, index.size) // eg varlen (datatype 9) has mismatch
        val tile = LongArray(useRank)
        for (i in 0 until useRank) {
            // 7/30/2016 jcaron. Apparently in some cases, at the end of the array, the index can be greater than the shape.
            // eg cdmUnitTest/formats/netcdf4/UpperDeschutes_t4p10_swemelt.nc
            // Presumably to have even chunks. Could try to calculate the last even chunk.
            // For now im removing this consistency check.
            // assert shape[i] >= pt[i] : String.format("shape[%s]=(%s) should not be less than pt[%s]=(%s)", i, shape[i], i, pt[i]);
            tile[i] = index[i] / chunk[i] // LOOK seems wrong, rounding down ??
        }
        return tile
    }

    /** Compute the left upper index of a tile, inverse of tile().
     * This will match a key in the datachunk btree, up to rank dimensions */
    fun index(tile: LongArray): LongArray {
        return LongArray(rank) { idx -> tile[idx] * chunk[idx] }
    }

    /**
     * Get order based on which tile the index pt belongs to.
     * This is the linear ordering of the tile.
     *
     * @param index index point
     * @return order number based on which tile the pt belongs to
     */
    fun order(index: LongArray): Long {
        val tile = tile(index)
        var order = 0L
        val useRank = min(rank, index.size) // eg varlen (datatype 9) has mismatch
        for (i in 0 until useRank) order += tileStrider[i] * tile[i]
        return order
    }

    /** inverse of order() */
    fun orderToIndex(order: Long) : LongArray {
        // calculate tile
        val tile = LongArray(rank)
        var rem = order

        for (k in 0 until rank) {
            tile[k] = rem / tileStrider[k]
            rem = rem - (tile[k] * tileStrider[k])
        }
        print("tile $order = ${tile.contentToString()}")

        // convert to index
        return index(tile)
    }

    /**
     * Create an ordering of index points based on which tile the point is in.
     *
     * @param p1 index point 1
     * @param p2 index point 2
     * @return order(p1) - order(p2) : negative if p1 < p2, positive if p1 > p2 , 0 if equal
     */
    fun compare(p1: LongArray, p2: LongArray): Long {
        return order(p1) - order(p2)
    }

    /** create an IndexSpace in tile space from an IndexSpace in index space */
    fun section(indexSection : IndexSpace) : IndexSpace {
        require(indexSection.rank == rank)
        indexSection.last.forEachIndexed { idx, last ->
            require(last < indexShape[idx])
        }

        val start = tile(indexSection.start)
        val limit = tile(indexSection.last)
        val length = LongArray(rank) { idx -> limit[idx] - start[idx] + 1 }
        return IndexSpace(start, length)
    }

    override fun toString(): String {
        return "Tiling(chunk=${chunk.contentToString()}, tileShape=${tileShape.contentToString()}, indexShape=${indexShape.contentToString()})"
    }
}