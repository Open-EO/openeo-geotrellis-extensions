package org.openeo.geotrellishealpix

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.openeo.geotrellis.OpenEOProcessScriptBuilder

/**
 * Packed layout: each row holds an array of HEALPix cells that all share the
 * same parent cell in the HEALPix NESTED hierarchy.
 *
 * In HEALPix NESTED order, a parent cell at `nside_parent` contains exactly
 * `(nside / nside_parent)^2 = 4^parentLevels` children. The `childrenPerParent`
 * (= chunk size) must therefore be a '''power of 4''' so that each packed row
 * maps to exactly one parent cell.
 *
 * The parent cell index for a given chunk is simply `cell_id_start / childrenPerParent`.
 * The nside of the parent grid is `nside / sqrt(childrenPerParent)` = `nside / 2^parentLevels`.
 *
 * @param nside             HEALPix NSIDE of this datacube (power of 2)
 * @param bands             band names with their Spark SQL data types
 * @param childrenPerParent number of child cells per packed row; must be a power of 4
 *                          (= 4^parentLevels). Every packed row contains ALL children
 *                          of a single parent cell.
 * @param df                underlying Spark DataFrame
 */
final case class PackedHealpixDatacube(
  nside: Int,
  bands: Seq[(String, DataType)],
  childrenPerParent: Int,
  df: DataFrame
) extends HealpixDatacube {

  require(PackedHealpixDatacube.isPowerOf4(childrenPerParent),
    s"childrenPerParent must be a power of 4, got $childrenPerParent " +
      s"(valid values: 4, 16, 64, 256, 1024, …)")
  require(childrenPerParent <= npix,
    s"childrenPerParent ($childrenPerParent) cannot exceed npix ($npix)")

  /** Number of HEALPix hierarchy levels between this cube and its parent grid. */
  val parentLevels: Int = PackedHealpixDatacube.log4(childrenPerParent)

  /** NSIDE of the parent grid. */
  val nsideParent: Int = nside / (1 << parentLevels)

  /** Total number of parent cells. */
  val nParents: Long = 12L * nsideParent.toLong * nsideParent.toLong

  /** Alias for backward compatibility with code using `chunkSize`. */
  def chunkSize: Int = childrenPerParent

  override def applyProcess(scriptBuilder: OpenEOProcessScriptBuilder,
                            context: java.util.Map[String, Any]): HealpixDatacube = {
    val bandNames = bands.map(_._1)
    val schema    = df.schema
    val spark     = df.sparkSession

    val newRdd = df.rdd.map { row =>
      val (tile, cellIds) = HealpixTileBridge.packedRowToTile(row, bandNames)
      val processed = HealpixTileBridge.runProcess(tile, scriptBuilder, context)
      HealpixTileBridge.packedTileToRow(processed, cellIds,
        row.getAs[java.sql.Timestamp](HealpixSchema.Timestamp), bandNames)
    }

    copy(df = spark.createDataFrame(newRdd, schema))
  }
}

object PackedHealpixDatacube {

  /** Check if n is a power of 4 (i.e., 4^k for k >= 1). */
  def isPowerOf4(n: Int): Boolean = {
    n > 0 && (n & (n - 1)) == 0 && // power of 2
      (n & 0x55555555) == n        // only even bit positions set → power of 4
  }

  /** Compute k such that 4^k == n. Assumes isPowerOf4(n) is true. */
  def log4(n: Int): Int = {
    var v = n
    var k = 0
    while (v > 1) { v >>= 2; k += 1 }
    k
  }

  /**
   * Compute a valid childrenPerParent from a parentLevels value.
   * parentLevels = 1 → 4 children, parentLevels = 2 → 16, etc.
   */
  def childrenPerParentFromLevels(parentLevels: Int): Int = {
    require(parentLevels >= 1, s"parentLevels must be >= 1, got $parentLevels")
    1 << (2 * parentLevels) // 4^parentLevels
  }

  def empty(spark: SparkSession,
            nside: Int,
            bands: Seq[(String, DataType)],
            childrenPerParent: Int): PackedHealpixDatacube = {
    val schema = HealpixSchema.packedSchema(bands)
    val empty  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row]: org.apache.spark.rdd.RDD[Row], schema)
    PackedHealpixDatacube(nside, bands, childrenPerParent, empty)
  }
}
