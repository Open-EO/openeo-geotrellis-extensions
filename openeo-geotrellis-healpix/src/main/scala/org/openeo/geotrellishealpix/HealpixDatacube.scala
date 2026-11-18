package org.openeo.geotrellishealpix

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, RasterExtent}
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.Extent
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openeo.geotrellis.OpenEOProcessScriptBuilder
import org.openeo.geotrelliscommon.OpenEOProcess

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

  protected def computeExtent(targetCRS: CRS): Extent

  /**
   * openEO `apply` process: applies the process to every cell value of every
   * band, returning a new HealpixDatacube of the same layout.
   *
   * The implementation funnels the data through GeoTrellis `MultibandTile`s
   * via [[HealpixTileBridge]] so that the existing `OpenEOProcessScriptBuilder`
   * machinery can be reused without duplication.
   */
  @OpenEOProcess(id = "apply",
    description = "Apply a process to each pixel value of each band.")
  def applyProcess(scriptBuilder: OpenEOProcessScriptBuilder,
                   context: java.util.Map[String, Any]): HealpixDatacube

  /**
   * Resample this HEALPix datacube into a GeoTrellis `MultibandTileLayerRDD[SpaceTimeKey]`
   * at the given target CRS and layout, using nearest-neighbour resampling
   * (raster cell center -> HEALPix cell id via `ang2pix`).
   */
  @OpenEOProcess(id = "resample_spatial",
    description = "Resample the HEALPix datacube to a regular grid at a chosen CRS and layout.",
    returns = "rdd")
  def resampleSpatial(targetCRSEpsg: Int,
                      targetResolution: Double): MultibandTileLayerRDD[SpaceTimeKey] = {
    import geotrellis.proj4.CRS

    val targetCRS = CRS.fromEpsgCode(targetCRSEpsg)
    val extent = computeExtent(targetCRS)
    val layout = LayoutDefinition(RasterExtent(extent,CellSize(targetResolution,targetResolution)), 256 )
    resampleSpatial(targetCRS, layout, extent)
  }

  def resampleSpatial(targetCRS: CRS,
                      layout: LayoutDefinition,
                      extent: Extent,
                      bandIndices: Seq[Int] = Seq(0)): MultibandTileLayerRDD[SpaceTimeKey] =
    HealpixToGeotrellis.render(this, targetCRS, layout, extent, bandIndices)

  /**
   * Filter the datacube by temporal extent.
   */
  @OpenEOProcess(id = "filter_temporal",
    description = "Filter the datacube to a temporal extent.")
  def filterTemporal(start: String, end: String): HealpixDatacube

  /**
   * Filter the datacube to selected bands.
   */
  @OpenEOProcess(id = "filter_bands",
    description = "Filter the datacube to a subset of bands.")
  def filterBands(bandNames: java.util.List[String]): HealpixDatacube


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

