package org.openeo.geotrellishealpix

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Aggregates raw HEALPix observations (multiple source pixels mapping to the
 * same HEALPix cell) into a single value per `(cell_id, timestamp)`.
 *
 * This is the second stage of the binning pipeline:
 *
 *  1. [[Sentinel3BinningReader.readRaw]] emits one row per valid source pixel.
 *  2. [[HealpixBinner.aggregate]] groups by `(cell_id, timestamp)` and
 *     reduces each group with the chosen aggregation strategy.
 *
 * Aggregation strategies:
 *   - '''Mean'''  – arithmetic mean of all contributing pixels (default).
 *   - '''First''' – first non-null value (useful for categorical / flag bands).
 *   - '''Count''' – number of contributing pixels (diagnostic).
 *   - '''Min / Max''' – extremes.
 */
object HealpixBinner {

  sealed trait Aggregation
  object Aggregation {
    case object Mean  extends Aggregation
    case object First extends Aggregation
    case object Count extends Aggregation
    case object Min   extends Aggregation
    case object Max   extends Aggregation
  }

  /**
   * Aggregate a raw (un-aggregated) scalar HealpixDatacube so that each
   * `(cell_id, timestamp)` pair has at most one row.
   *
   * @param raw         raw datacube produced by [[Sentinel3BinningReader.readRaw]]
   * @param aggregation aggregation strategy to use
   * @return aggregated ScalarHealpixDatacube
   */
  def aggregate(raw: ScalarHealpixDatacube,
                aggregation: Aggregation): ScalarHealpixDatacube = {

    val bandNames = raw.bands.map(_._1)
    val groupCols = Seq(HealpixSchema.CellId, HealpixSchema.Timestamp)

    val aggExprs = aggregation match {
      case Aggregation.Mean =>
        bandNames.map(b => avg(col(b)).alias(b))
      case Aggregation.First =>
        bandNames.map(b => first(col(b), ignoreNulls = true).alias(b))
      case Aggregation.Count =>
        // Count returns LongType – we keep that; caller can cast if needed.
        bandNames.map(b => count(col(b)).alias(b))
      case Aggregation.Min =>
        bandNames.map(b => min(col(b)).alias(b))
      case Aggregation.Max =>
        bandNames.map(b => max(col(b)).alias(b))
      }

    val aggregatedDf = raw.df
      .groupBy(groupCols.map(col): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)

    // If aggregation was Count, the band types change to LongType.
    // For all other strategies, stay with DoubleType.
    val newBands: Seq[(String, DataType)] = aggregation match {
      case Aggregation.Count => bandNames.map(_ -> (LongType: DataType))
      case _                 => raw.bands
    }

    ScalarHealpixDatacube(raw.nside, newBands, aggregatedDf)
  }

  /**
   * Convenience: aggregate and then repack into a [[PackedHealpixDatacube]].
   */
  def aggregatePacked(raw: ScalarHealpixDatacube,
                      childrenPerParent: Int,
                      aggregation: Aggregation = Aggregation.Mean): PackedHealpixDatacube = {
    val scalar = aggregate(raw, aggregation)
    toPacked(scalar, childrenPerParent)
  }

  /**
   * Convert a scalar datacube to a packed datacube by grouping cells that
   * share the same parent in HEALPix NESTED order.
   *
   * @param childrenPerParent must be a power of 4 (= 4^parentLevels).
   *                          Each packed row will contain all children of one
   *                          parent cell.
   */
  def toPacked(scalar: ScalarHealpixDatacube,
               childrenPerParent: Int): PackedHealpixDatacube = {
    require(PackedHealpixDatacube.isPowerOf4(childrenPerParent),
      s"childrenPerParent must be a power of 4, got $childrenPerParent")

    val spark     = scalar.df.sparkSession
    val bandNames = scalar.bands.map(_._1)
    val schema    = HealpixSchema.packedSchema(scalar.bands)

    // In NESTED order, floor(cell_id / childrenPerParent) gives the parent cell index.
    // cell_id_start = parent_index * childrenPerParent = first child of that parent.
    val withChunk = scalar.df.withColumn(
      HealpixSchema.CellIdStart,
      (col(HealpixSchema.CellId) / childrenPerParent).cast(LongType) * childrenPerParent
    )

    val aggExprs = bandNames.map { b =>
      collect_list(col(b)).alias(b)
    }

    val allAggExprs = count(lit(1)).cast(IntegerType).alias(HealpixSchema.ChunkSize) +: aggExprs
    val grouped = withChunk
      .groupBy(col(HealpixSchema.CellIdStart), col(HealpixSchema.Timestamp))
      .agg(allAggExprs.head, allAggExprs.tail: _*)
      .select(
        (Seq(col(HealpixSchema.CellIdStart), col(HealpixSchema.ChunkSize),
             col(HealpixSchema.Timestamp)) ++ bandNames.map(col)): _*
      )

    PackedHealpixDatacube(scalar.nside, scalar.bands, childrenPerParent, grouped)
  }

  /**
   * Add a "count" band to an existing raw datacube, indicating how many
   * source observations contributed to each HEALPix cell.
   * Useful for quality metrics / weighting in multi-orbit composites.
   */
  def addObservationCount(raw: ScalarHealpixDatacube,
                          countBandName: String = "obs_count"): ScalarHealpixDatacube = {
    val countDf = raw.df
      .groupBy(HealpixSchema.CellId, HealpixSchema.Timestamp)
      .agg(count(lit(1)).alias(countBandName))

    val joined = raw.df.join(
      countDf,
      Seq(HealpixSchema.CellId, HealpixSchema.Timestamp),
      "left"
    )

    val newBands = raw.bands :+ (countBandName -> (LongType: DataType))
    ScalarHealpixDatacube(raw.nside, newBands, joined)
  }
}

