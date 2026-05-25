package org.openeo.geotrellishealpix

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.Extent
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openeo.geotrellis.OpenEOProcessScriptBuilder

/**
 * An openEO datacube backed by a Spark SQL DataFrame addressed by
 * (HEALPix cell id, timestamp). A datacube corresponds to a single HEALPix
 * NSIDE / resolution level.
 *
 * Two concrete layouts are provided so they can be benchmarked:
 *   - [[ScalarHealpixDatacube]]: one cell per row.
 *   - [[PackedHealpixDatacube]]: many cells packed into an array per row.
 */
trait HealpixDatacube {
  /** HEALPix NSIDE parameter (power of two). */
  def nside: Int
  /** Band names with their Spark SQL data types. */
  def bands: Seq[(String, DataType)]
  /** Underlying Spark SQL DataFrame. */
  def df: DataFrame

  /** Total number of HEALPix cells at this resolution. */
  def npix: Long = 12L * nside.toLong * nside.toLong

  /**
   * openEO `apply` process: applies the process to every cell value of every
   * band, returning a new HealpixDatacube of the same layout.
   *
   * The implementation funnels the data through GeoTrellis `MultibandTile`s
   * via [[HealpixTileBridge]] so that the existing `OpenEOProcessScriptBuilder`
   * machinery can be reused without duplication.
   */
  def applyProcess(scriptBuilder: OpenEOProcessScriptBuilder,
                   context: java.util.Map[String, Any]): HealpixDatacube

  /**
   * Render this HEALPix datacube as a GeoTrellis `MultibandTileLayerRDD[SpaceTimeKey]`
   * at the given target CRS and layout, using nearest-neighbour resampling
   * (raster cell center -> HEALPix cell id via `ang2pix`).
   */
  def toMultibandTileLayerRDD(targetCRS: CRS,
                              layout: LayoutDefinition,
                              extent: Extent,
                              bandIndices: Seq[Int] = Seq(0)): MultibandTileLayerRDD[SpaceTimeKey] =
    HealpixToGeotrellis.render(this, targetCRS, layout, extent, bandIndices)
}

object HealpixDatacube {
  /** Storage layout selector. */
  sealed trait Layout
  object Layout {
    case object Scalar extends Layout
    /**
     * Packed layout: each row stores all children of a single parent cell.
     * `childrenPerParent` must be a power of 4 (= 4^parentLevels).
     */
    final case class Packed(childrenPerParent: Int) extends Layout
  }

  def empty(spark: SparkSession,
            nside: Int,
            bands: Seq[(String, DataType)],
            layout: Layout): HealpixDatacube = layout match {
    case Layout.Scalar       => ScalarHealpixDatacube.empty(spark, nside, bands)
    case Layout.Packed(cpp)  => PackedHealpixDatacube.empty(spark, nside, bands, cpp)
  }
}

