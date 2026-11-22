package org.openeo.sar

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, RasterSource}
import geotrellis.vector.Extent
import org.openeo.sar.dem.DemItem
import org.openeo.sar.metadata.{Polarisation, S1GrdMetadata}

import java.net.URI

/** Selects the backscatter normalization convention for output sigma/gamma bands. */
sealed trait BackscatterNormalization
object BackscatterNormalization {
  /** Normalized to the reference flat-earth ground area (standard S1 calibration output). */
  case object Sigma0 extends BackscatterNormalization
  /** Normalized to the area perpendicular to the look direction, with terrain flattening
   *  applied via the local-angle approximation: sigma0 × tan(θ_local) / sin(θ_el). */
  case object Gamma0RTC extends BackscatterNormalization
}

/** Algorithm used for terrain flattening when [[BackscatterNormalization.Gamma0RTC]] is selected.
 *
 *  - [[RtcAlgorithm.Angle]]: per-pixel, angle-based correction `sigma0 × tan(θ_local) / sin(θ_el)`. Cheap and
 *    accurate on gentle slopes. Based on the projection-angle / local-incidence-angle approach of
 *    Ulander (1996) and Hoekman & Reiche (2015), as used by Vollrath, Mullissa & Reiche (2020)
 *    ("Angular-based radiometric slope correction for Sentinel-1 on Google Earth Engine",
 *    Remote Sensing 12(11):1867, doi:10.3390/rs12111867).
 *  - [[RtcAlgorithm.BilinearDistribution]]: area-based, facet-wise projection of the DEM into radar geometry,
 *    after Small (2011) ("Flattening Gamma: Radiometric Terrain Correction for SAR Imagery", IEEE TGRS
 *    49(8):3081-3093, doi:10.1109/TGRS.2011.2120616). Port of ISCE3 `computeRtcBilinearDistribution`
 *    (`cxx/isce3/geometry/RTC.cpp`, Apache-2.0, Caltech/JPL). Sums the contributions of all terrain facets that
 *    fall in one radar pixel, so remains valid on steep slopes and in layover. More expensive.
 *  - [[RtcAlgorithm.Auto]]: angle-based, except in the neighbourhood of pixels whose local incidence angle
 *    is outside [`rtcAutoMinLocalIncidenceDeg`, `rtcAutoMaxLocalIncidenceDeg`], where the area-based
 *    algorithm is used (currently [[RtcAlgorithm.BilinearDistribution]]). The area-based computation only runs
 *    for tiles that contain such pixels.
 */
sealed trait RtcAlgorithm { def name: String }
object RtcAlgorithm {
  case object Angle extends RtcAlgorithm { val name = "angle" }
  case object BilinearDistribution extends RtcAlgorithm { val name = "bilinear_distribution" }
  case object Auto extends RtcAlgorithm { val name = "auto" }

  val all: Seq[RtcAlgorithm] = Seq(Angle, BilinearDistribution, Auto)

  def parse(s: String): RtcAlgorithm = {
    val normalized = s.trim.toLowerCase.replace('-', '_')
    all.find(_.name == normalized).getOrElse(throw new IllegalArgumentException(
      s"sar_backscatter: unsupported rtc_algorithm '$s': expected one of ${all.map(_.name).mkString(", ")}"))
  }
}

/** Processing configuration shared by all tiles in a scene.
 *
 *  @param normalization          Whether to output sigma0 or gamma0_RTC (terrain-flattened).
 *                                Default is [[BackscatterNormalization.Sigma0]].
 *  @param shadowLayoverMask      When true, adds an extra output band with per-pixel
 *                                shadow/layover classification:
 *                                  0 = valid, 1 = layover (θ_local < 0), 2 = shadow (terrain
 *                                facing away from radar). Also enables masking of backscatter
 *                                on shadow/layover pixels. Default false.
 *  @param localIncidenceAngle    When true, adds an output band with the local
 *                                (terrain-relative) incidence angle in degrees. Enabling this
 *                                (or `shadowLayoverMask`, or gamma0_RTC normalization) requires
 *                                computing the terrain surface normal per pixel. Default false.
 *  @param ellipsoidIncidenceAngle When true, adds an output band with the ellipsoidal
 *                                (look vs. smooth WGS84 ellipsoid normal) incidence angle in
 *                                degrees. Default false.
 *  @param rtcAlgorithm           Terrain flattening algorithm for gamma0_RTC, see [[RtcAlgorithm]]. Default angle-based.
 *  @param rtcDemUpsampling       Area-based RTC only: integer factor by which the DEM is upsampled relative to the output
 *                                grid to form facets (ISCE3 default 2).
 *  @param rtcDemMarginPixels     Area-based RTC only: DEM margin around the tile, in output pixels, so that terrain
 *                                outside the tile that projects into the tile's radar pixels is accounted for
 *                                (ISCE3 uses 20 pixels).
 *  @param rtcMinValueDb          Area-based RTC only: radar pixels whose area normalisation factor is below this value
 *                                (dB) become NaN (ISCE3 `rtc_min_value_db`). Unset: only factors <= 0 (shadow) are NaN.
 *  @param rtcAutoMinLocalIncidenceDeg / rtcAutoMaxLocalIncidenceDeg
 *                                Auto only: local incidence angles (degrees) outside this range are "steep".
 *  @param rtcAutoDilationPixels  Auto only: steep pixels are dilated by this many output pixels before choosing the
 *                                algorithm, since area-based results depend on neighbouring facets.
 *  @param rtcAutoTileFraction  Auto only: if at least this fraction of the pixels of a tile is steep, the whole tile uses the
 *                                area-based algorithm. Below it, only steep pixels and their neighbourhood do.
 *  @param rtcMethodBand          When true, adds an output band with the algorithm used per pixel
 *                                (0 = angle-based, 1 = area-based), for validation. Default false.
 *  @param elevationModel         DEM to use: a STAC collection URL (or an alias such as `COPERNICUS_30`),
 *                                see [[org.openeo.sar.dem.ElevationModel]]. `None` is the back-end
 *                                default, `COPERNICUS_30`. The string `"null"` means no DEM: terrain height
 *                                is 0 m (orthometric), the geoid is still applied.
 *  @param demAsset               Asset key of the DEM raster in the elevation model's STAC items.
 *  @param softErrors             When true (default), an unreadable DEM asset yields a NaN DEM and is counted
 *                                as a soft error; when false it fails the read immediately.
 */
final case class SarProcessingConfig(
  normalization: BackscatterNormalization = BackscatterNormalization.Sigma0,
  shadowLayoverMask: Boolean               = false,
  localIncidenceAngle: Boolean             = false,
  ellipsoidIncidenceAngle: Boolean         = false,
  elevationModel: Option[String]           = None,
  demAsset: String                         = "data",
  softErrors: Boolean                      = true,
  rtcAlgorithm: RtcAlgorithm               = RtcAlgorithm.Angle,
  rtcDemUpsampling: Int                    = 2,
  rtcDemMarginPixels: Int                  = 20,
  rtcMinValueDb: Option[Double]            = None,
  rtcAutoMinLocalIncidenceDeg: Double      = 20.0,
  rtcAutoMaxLocalIncidenceDeg: Double      = 70.0,
  rtcAutoDilationPixels: Int               = 2,
  rtcAutoTileFraction: Double              = 0.02,
  rtcMethodBand: Boolean                   = false
) {
  require(rtcDemUpsampling >= 1, "sar_backscatter: rtc_dem_upsampling must be >= 1")
  require(rtcDemMarginPixels >= 0, "sar_backscatter: rtc_dem_margin_pixels must be >= 0")
  require(rtcAutoTileFraction >= 0.0 && rtcAutoTileFraction <= 1.0, "sar_backscatter: rtc_auto_tile_fraction must be in [0, 1]")
  require(rtcAutoDilationPixels >= 0, "sar_backscatter: rtc_auto_dilation_pixels must be >= 0")

  /** Whether area-based RTC output can be requested at all. */
  def usesAreaRtc: Boolean =
    normalization == BackscatterNormalization.Gamma0RTC && rtcAlgorithm != RtcAlgorithm.Angle

  /** Total number of output bands for `nPols` polarisations.
   *
   *  Layout:
   *    0..nPols-1                        backscatter (sigma0 or gamma0) per polarisation
   *    nPols                (optional)   ellipsoidal incidence angle (degrees)
   *    nPols+[0|1]          (optional)   local (terrain-relative) incidence angle (degrees)
   *    nPols+[0..2]                      validity mask (1=valid, 0=outside swath)
   *    nPols+[1..3]         (optional)   shadow/layover mask (0=valid, 1=layover, 2=shadow)
   *    last                 (optional)   RTC method used (0=angle, 1=area-based), config.rtcMethodBand
   */
  def bandCount(nPols: Int): Int =
    nPols + 1 +
      (if (ellipsoidIncidenceAngle) 1 else 0) +
      (if (localIncidenceAngle) 1 else 0) +
      (if (shadowLayoverMask) 1 else 0) +
      (if (rtcMethodBand) 1 else 0)
}

object SarProcessingConfig {
  val default: SarProcessingConfig = SarProcessingConfig()
}

/** Output tile description supplied by the caller. */
final case class TileRequest(
  extent: Extent,
  cellSize: CellSize,
  crs: CRS,
  polarisations: Seq[Polarisation],
  config: SarProcessingConfig = SarProcessingConfig.default,
  /** Identifies the SAR item (STAC item URL) being processed, for log and error messages. */
  sceneId: String = ""
) {
  val cols: Int = math.round(extent.width  / cellSize.width ).toInt
  val rows: Int = math.round(extent.height / cellSize.height).toInt
}

/** Lightweight, serializable description of a scene: everything needed to build a [[SceneContext]] via
 *  [[TerrainCorrectionProcessor.openScene]], without doing any I/O. Shipped to executors instead of the
 *  context itself, which is opened lazily where the data is read. */
final case class SceneSpec(
  stacItemUrl: URI,
  polarisations: Seq[Polarisation],
  config: SarProcessingConfig,
  demSearchExtentWgs84: Option[Extent],
  cellSize: CellSize,
  crs: CRS
)

/** Scene-level state that is expensive to build and shared across all tiles
 *  produced from the same SAR scene.  Constructed once by
 *  [[TerrainCorrectionProcessor.openScene]]; immutable and thread-safe.
 *
 *  - `metadata`: orbit, calibration LUTs, SRGR polynomials, timing — parsed
 *    from the SAFE annotation XMLs.
 *  - `sarSources`: one [[RasterSource]] per polarisation pointing at the full
 *    measurement GeoTIFF.  RasterSources are cheap to keep open and perform
 *    windowed reads on demand.
 *  - `demItems`: the DEM STAC items (extent + asset href) overlapping the scene; each tile opens
 *    and reads only the items that overlap its own AOI.
 *  - `geoidSourceFactory`: serializable factory (taking a WGS84 extent), invoked on the executor so the source is never shipped from the driver. */
final case class SceneContext(
  metadata: S1GrdMetadata,
  sarSources: Map[Polarisation, RasterSource],
  demItems: Seq[DemItem],
  geoidSourceFactory: Option[Extent => RasterSource],
  /** Shared CRS and cell size for all tiles produced from this scene. */
  cellSize: CellSize,
  crs: CRS,
  polarisations: Seq[Polarisation],
  config: SarProcessingConfig = SarProcessingConfig.default,
  /** Identifies the SAR item (STAC item URL) being processed, for log and error messages. */
  sceneId: String = ""
) {
  /** Build the per-tile context for a given extent without any I/O or parsing. */
  def tileContext(extent: Extent): TileComputeContext =
    TileComputeContext(
      TileRequest(extent, cellSize, crs, polarisations, config, sceneId),
      metadata, sarSources, demItems, geoidSourceFactory
    )
}

/** Fully-assembled inputs for one tile computation. The orchestrator builds
 *  this; backends consume it. Keeps backends free of I/O. */
final case class TileComputeContext(
  request: TileRequest,
  metadata: S1GrdMetadata,
  /** Per-polarisation measurement RasterSource (full scene, in SAR geometry). */
  sarSources: Map[Polarisation, RasterSource],
  /** DEM items overlapping the scene. The orchestrator selects the ones overlapping the AOI,
   *  mosaics them and reprojects to the output CRS at the target cell size. */
  demItems: Seq[DemItem],
  /** Geoid undulation RasterSource (e.g. EGM2008), or None to assume DEM is
   *  already ellipsoidal heights. */
  geoidSourceFactory: Option[Extent => RasterSource]
)
