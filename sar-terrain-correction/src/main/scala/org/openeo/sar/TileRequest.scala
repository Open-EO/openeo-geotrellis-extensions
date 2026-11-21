package org.openeo.sar

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, RasterSource}
import geotrellis.vector.Extent
import org.openeo.sar.dem.DemItem
import org.openeo.sar.metadata.{Polarisation, S1GrdMetadata}

/** Selects the backscatter normalization convention for output sigma/gamma bands. */
sealed trait BackscatterNormalization
object BackscatterNormalization {
  /** Normalized to the reference flat-earth ground area (standard S1 calibration output). */
  case object Sigma0 extends BackscatterNormalization
  /** Normalized to the area perpendicular to the look direction, with terrain flattening
   *  applied via the local-angle approximation: sigma0 × tan(θ_local) / sin(θ_el). */
  case object Gamma0RTC extends BackscatterNormalization
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
  softErrors: Boolean                      = true
) {
  /** Total number of output bands for `nPols` polarisations.
   *
   *  Layout:
   *    0..nPols-1                        backscatter (sigma0 or gamma0) per polarisation
   *    nPols                (optional)   ellipsoidal incidence angle (degrees)
   *    nPols+[0|1]          (optional)   local (terrain-relative) incidence angle (degrees)
   *    nPols+[0..2]                      validity mask (1=valid, 0=outside swath)
   *    nPols+[1..3]         (optional)   shadow/layover mask (0=valid, 1=layover, 2=shadow)
   */
  def bandCount(nPols: Int): Int =
    nPols + 1 +
      (if (ellipsoidIncidenceAngle) 1 else 0) +
      (if (localIncidenceAngle) 1 else 0) +
      (if (shadowLayoverMask) 1 else 0)
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
