package org.openeo.geotrellishealpix

import com.github.luben.zstd.Zstd
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import java.time.Instant

/**
 * Writer for HEALPix Pyramid Zarr stores following the spec in `agent/zarr-spec.md`.
 *
 * Produces a Zarr v3 store with the following layout:
 * {{{
 *   <store>.zarr/
 *     zarr.json                 ← root group metadata
 *     nside_<N>/                ← one group per resolution level
 *       zarr.json               ← level group metadata
 *       cell_id/                ← Zarr array, int64, shape [npix]
 *         zarr.json
 *         c/<chunk_idx>         ← binary chunks (zstd compressed, little-endian)
 *       parent_offsets/         ← Zarr array, int64, shape [n_parents + 1]
 *         zarr.json
 *         c/<chunk_idx>
 *       bands/
 *         zarr.json
 *         <band_name>/          ← Zarr array, float32, shape [npix]
 *           zarr.json
 *           c/<chunk_idx>
 * }}}
 *
 * Writing is performed on the driver (collect). For very large cubes,
 * a distributed approach that partitions by parent cell and writes
 * chunks on executors in parallel would be needed — that is left as a
 * follow-up optimisation.
 */
object HealpixZarrWriter {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Default row chunk size for cell_id and band arrays. */
  val DefaultRowChunkSize: Int = 65536

  /** Default chunk size for parent_offsets (in number of entries). */
  val DefaultOffsetsChunkSize: Int = 4096

  /** Zstd compression level. */
  val ZstdLevel: Int = 3

  // --------------------------------------------------------------------------
  // Public API
  // --------------------------------------------------------------------------

  /**
   * Write a [[HealpixDatacube]] as a Zarr v3 HEALPix Pyramid store.
   *
   * Currently writes a single resolution level (the cube's own nside).
   * Multi-level pyramid creation (downsampling to coarser nsides) can be
   * added as a follow-up.
   *
   * @param cube         the datacube to write (scalar or packed)
   * @param location     filesystem path for the store root directory
   * @param parentLevels number of hierarchy levels for the CSR parent grid.
   *                     Defaults to the packed datacube's parentLevels if packed,
   *                     or 3 for scalar cubes.
   */
  def write(cube: HealpixDatacube,
            location: String,
            parentLevels: Int = -1): Unit = {

    val root: Path = Paths.get(location)
    Files.createDirectories(root)

    val nside     = cube.nside
    val bandNames = cube.bands.map(_._1)

    val effectiveParentLevels = if (parentLevels > 0) parentLevels else {
      cube match {
        case p: PackedHealpixDatacube => p.parentLevels
        case _ => math.min(3, log2(nside)) // default: 3 levels up, cap at nside
      }
    }
    val nsideParent = math.max(1, nside / (1 << effectiveParentLevels))
    val childrenPerParent = (nside / nsideParent).toLong * (nside / nsideParent).toLong

    // --- 1. Materialise sorted data: (cell_id, band_values…) -----------------
    val scalarDf = toScalarSorted(cube)
    val rows: Array[Row] = scalarDf.collect()
    val nCells = rows.length

    logger.info(s"HealpixZarrWriter: writing $nCells cells, nside=$nside, " +
      s"nside_parent=$nsideParent, parentLevels=$effectiveParentLevels to $location")

    // --- 2. Write root zarr.json ---------------------------------------------
    writeRootMetadata(root, nside, nside, bandNames, effectiveParentLevels)

    // --- 3. Write level group ------------------------------------------------
    val levelDir = root.resolve(s"nside_$nside")
    Files.createDirectories(levelDir)

    val nParents = 12L * nsideParent.toLong * nsideParent.toLong

    writeLevelMetadata(levelDir, nside, nsideParent,
      childrenPerParent.toInt, nCells, nParents.toInt)

    // --- 4. Extract cell_ids and band value arrays ---------------------------
    val cellIdIdx        = scalarDf.schema.fieldIndex(HealpixSchema.CellId)
    val bandFieldIndices = bandNames.map(scalarDf.schema.fieldIndex)

    val cellIds = new Array[Long](nCells)
    val bandData: Array[Array[Float]] = Array.fill(bandNames.size)(new Array[Float](nCells))

    var i = 0
    while (i < nCells) {
      val row = rows(i)
      cellIds(i) = row.getLong(cellIdIdx)
      var b = 0
      while (b < bandNames.size) {
        bandData(b)(i) = if (row.isNullAt(bandFieldIndices(b))) 0.0f
                         else row.get(bandFieldIndices(b)).asInstanceOf[Number].floatValue()
        b += 1
      }
      i += 1
    }

    // --- 5. Build parent_offsets (CSR index) ----------------------------------
    val parentOffsets = buildParentOffsets(cellIds, childrenPerParent, nParents.toInt)

    // --- 6. Write arrays -----------------------------------------------------
    writeLongArray(levelDir.resolve("cell_id"), cellIds, nCells)
    writeLongArray(levelDir.resolve("parent_offsets"), parentOffsets, nParents.toInt + 1)

    val bandsDir = levelDir.resolve("bands")
    Files.createDirectories(bandsDir)
    writeGroupJson(bandsDir)

    bandNames.zipWithIndex.foreach { case (name, bi) =>
      writeFloatArray(bandsDir.resolve(name), bandData(bi), nCells)
    }

    logger.info(s"HealpixZarrWriter: done writing to $location")
  }

  // --------------------------------------------------------------------------
  // Private helpers
  // --------------------------------------------------------------------------

  private def log2(n: Int): Int = (math.log(n.toDouble) / math.log(2.0)).toInt

  /**
   * Convert any HealpixDatacube to a scalar (one row per cell) DataFrame
   * sorted by cell_id, with only cell_id + band columns.
   * Timestamps are aggregated away (uses first value per cell).
   */
  private def toScalarSorted(cube: HealpixDatacube): DataFrame = {
    val bandNames = cube.bands.map(_._1)

    val scalar: DataFrame = cube match {
      case s: ScalarHealpixDatacube =>
        s.df.groupBy(HealpixSchema.CellId)
          .agg(bandNames.map(b => first(col(b), ignoreNulls = true).alias(b)).head,
               bandNames.tail.map(b => first(col(b), ignoreNulls = true).alias(b)): _*)

      case p: PackedHealpixDatacube =>
        val startIdx = p.df.schema.fieldIndex(HealpixSchema.CellIdStart)
        val sizeIdx  = p.df.schema.fieldIndex(HealpixSchema.ChunkSize)
        val schema   = HealpixSchema.scalarSchema(p.bands)

        val exploded = p.df.rdd.flatMap { row =>
          val start = row.getLong(startIdx)
          val size  = row.getInt(sizeIdx)
          val arrs  = bandNames.map(b => row.getAs[scala.collection.Seq[Any]](b))
          (0 until size).iterator.map { i =>
            val bandVals: Seq[Any] = arrs.map { a =>
              if (a == null || i >= a.length || a(i) == null) null
              else a(i)
            }
            Row.fromSeq(Seq[Any](start + i, null) ++ bandVals)
          }
        }
        val explodedDf = p.df.sparkSession.createDataFrame(exploded, schema)
        explodedDf.groupBy(HealpixSchema.CellId)
          .agg(bandNames.map(b => first(col(b), ignoreNulls = true).alias(b)).head,
               bandNames.tail.map(b => first(col(b), ignoreNulls = true).alias(b)): _*)
    }

    scalar.orderBy(col(HealpixSchema.CellId))
  }

  /**
   * Build CSR parent_offsets from sorted cell_ids.
   * For parent cell p, its children are cell_ids in [p*cpp, (p+1)*cpp).
   */
  private def buildParentOffsets(cellIds: Array[Long],
                                 childrenPerParent: Long,
                                 nParents: Int): Array[Long] = {
    val offsets = new Array[Long](nParents + 1)
    var cellIdx = 0
    var p = 0
    while (p < nParents) {
      offsets(p) = cellIdx.toLong
      val parentEnd = (p.toLong + 1L) * childrenPerParent
      while (cellIdx < cellIds.length && cellIds(cellIdx) < parentEnd) {
        cellIdx += 1
      }
      p += 1
    }
    offsets(nParents) = cellIdx.toLong
    offsets
  }

  // --- Metadata writers ------------------------------------------------------

  private def writeRootMetadata(root: Path, baseNside: Int, minNside: Int,
                                bands: Seq[String], parentLevels: Int): Unit = {
    val bandsArr = bands.map(b => s""""$b"""").mkString(", ")
    val json =
      s"""{
         |  "zarr_format": 3,
         |  "node_type": "group",
         |  "attributes": {
         |    "base_nside": $baseNside,
         |    "min_nside": $minNside,
         |    "bands": [$bandsArr],
         |    "parent_levels": $parentLevels,
         |    "created_at": "${Instant.now()}"
         |  }
         |}
         |""".stripMargin
    Files.write(root.resolve("zarr.json"), json.getBytes("UTF-8"))
  }

  private def writeLevelMetadata(levelDir: Path, nside: Int, nsideParent: Int,
                                 childrenPerParent: Int, nCells: Int,
                                 nParents: Int): Unit = {
    val json =
      s"""{
         |  "zarr_format": 3,
         |  "node_type": "group",
         |  "attributes": {
         |    "nside": $nside,
         |    "nside_parent": $nsideParent,
         |    "children_per_parent": $childrenPerParent,
         |    "n_cells": $nCells,
         |    "n_parents": $nParents
         |  }
         |}
         |""".stripMargin
    Files.write(levelDir.resolve("zarr.json"), json.getBytes("UTF-8"))
  }

  private def writeGroupJson(dir: Path): Unit = {
    val json =
      """{
        |  "zarr_format": 3,
        |  "node_type": "group",
        |  "attributes": {}
        |}
        |""".stripMargin
    Files.write(dir.resolve("zarr.json"), json.getBytes("UTF-8"))
  }

  // --- Array writers ---------------------------------------------------------

  private def writeLongArray(arrayDir: Path, data: Array[Long], shape: Int): Unit = {
    Files.createDirectories(arrayDir)
    val chunkSize = if (shape <= DefaultOffsetsChunkSize) shape
                    else DefaultRowChunkSize
    writeArrayMetadataInt64(arrayDir, shape, chunkSize)

    val chunkDir = arrayDir.resolve("c")
    Files.createDirectories(chunkDir)

    var offset = 0
    var chunkIdx = 0
    while (offset < data.length) {
      val end = math.min(offset + chunkSize, data.length)
      val len = end - offset
      val buf = ByteBuffer.allocate(len * 8).order(ByteOrder.LITTLE_ENDIAN)
      var i = offset
      while (i < end) { buf.putLong(data(i)); i += 1 }
      val compressed = Zstd.compress(buf.array(), ZstdLevel)
      Files.write(chunkDir.resolve(chunkIdx.toString), compressed)
      offset = end
      chunkIdx += 1
    }
  }

  private def writeFloatArray(arrayDir: Path, data: Array[Float], shape: Int): Unit = {
    Files.createDirectories(arrayDir)
    val chunkSize = DefaultRowChunkSize
    writeArrayMetadataFloat32(arrayDir, shape, chunkSize)

    val chunkDir = arrayDir.resolve("c")
    Files.createDirectories(chunkDir)

    var offset = 0
    var chunkIdx = 0
    while (offset < data.length) {
      val end = math.min(offset + chunkSize, data.length)
      val len = end - offset
      val buf = ByteBuffer.allocate(len * 4).order(ByteOrder.LITTLE_ENDIAN)
      var i = offset
      while (i < end) { buf.putFloat(data(i)); i += 1 }
      val compressed = Zstd.compress(buf.array(), ZstdLevel)
      Files.write(chunkDir.resolve(chunkIdx.toString), compressed)
      offset = end
      chunkIdx += 1
    }
  }

  private def writeArrayMetadataInt64(arrayDir: Path, shape: Int, chunkSize: Int): Unit = {
    val json =
      s"""{
         |  "zarr_format": 3,
         |  "node_type": "array",
         |  "shape": [$shape],
         |  "data_type": "int64",
         |  "chunk_grid": {
         |    "name": "regular",
         |    "configuration": { "chunk_shape": [$chunkSize] }
         |  },
         |  "chunk_key_encoding": {
         |    "name": "default",
         |    "configuration": { "separator": "/" }
         |  },
         |  "fill_value": 0,
         |  "codecs": [
         |    { "name": "bytes", "configuration": { "endian": "little" } },
         |    { "name": "zstd", "configuration": { "level": $ZstdLevel } }
         |  ]
         |}
         |""".stripMargin
    Files.write(arrayDir.resolve("zarr.json"), json.getBytes("UTF-8"))
  }

  private def writeArrayMetadataFloat32(arrayDir: Path, shape: Int, chunkSize: Int): Unit = {
    val json =
      s"""{
         |  "zarr_format": 3,
         |  "node_type": "array",
         |  "shape": [$shape],
         |  "data_type": "float32",
         |  "chunk_grid": {
         |    "name": "regular",
         |    "configuration": { "chunk_shape": [$chunkSize] }
         |  },
         |  "chunk_key_encoding": {
         |    "name": "default",
         |    "configuration": { "separator": "/" }
         |  },
         |  "fill_value": 0.0,
         |  "codecs": [
         |    { "name": "bytes", "configuration": { "endian": "little" } },
         |    { "name": "zstd", "configuration": { "level": $ZstdLevel } }
         |  ]
         |}
         |""".stripMargin
    Files.write(arrayDir.resolve("zarr.json"), json.getBytes("UTF-8"))
  }
}
