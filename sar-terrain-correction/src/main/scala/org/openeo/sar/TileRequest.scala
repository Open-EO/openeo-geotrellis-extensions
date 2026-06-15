package org.openeo.sar

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, RasterSource}
import geotrellis.vector.Extent
import org.openeo.sar.metadata.{Polarisation, S1GrdMetadata}

/** Selects the backscatter normalization convention for output sigma/gamma bands. */
sealed trait BackscatterNormalization
object BackscatterNormalization {
  /** Normalized to the reference flat-earth ground area (standard S1 calibration output). */
  case object Sigma0 extends BackscatterNormalization
  /** Normalized to the area perpendicular to the look direction, with terrain flattening
   *  applied via the cheap angle-ratio method: sigma0 × sin(θ_el) / sin(θ_local). */
  case object Gamma0RTC extends BackscatterNormalization
}

/** Processing configuration shared by all tiles in a scene.
 *
 *  @param normalization     Whether to output sigma0 or gamma0_RTC (terrain-flattened).
 *                           Default is [[BackscatterNormalization.Sigma0]].
 *  @param shadowLayoverMask When true, adds an extra output band with per-pixel
 *                           shadow/layover classification:
 *                             0 = valid, 1 = layover (θ_local < 0), 2 = shadow (terrain
 *                             facing away from radar). Default false.
 */
final case class SarProcessingConfig(
  normalization: BackscatterNormalization = BackscatterNormalization.Sigma0,
  shadowLayoverMask: Boolean              = false
) {
  /** Total number of output bands for `nPols` polarisations.
   *
   *  Layout:
   *    0..nPols-1             backscatter (sigma0 or gamma0) per polarisation
   *    nPols                  ellipsoidal incidence angle (degrees)
   *    nPols+1                local (terrain-relative) incidence angle (degrees)
   *    nPols+2                validity mask (1=valid, 0=outside swath)
   *    nPols+3 (optional)     shadow/layover mask (0=valid, 1=layover, 2=shadow)
   */
  def bandCount(nPols: Int): Int = nPols + 3 + (if (shadowLayoverMask) 1 else 0)
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
  config: SarProcessingConfig = SarProcessingConfig.default
) {
  def cols: Int = math.round(extent.width  / cellSize.width ).toInt
  def rows: Int = math.round(extent.height / cellSize.height).toInt
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
 *  - `demSource` / `geoidSource`: single scene-wide raster sources; each tile
 *    will read only its own AOI window. */
final case class SceneContext(
  metadata: S1GrdMetadata,
  sarSources: Map[Polarisation, RasterSource],
  demSource: RasterSource,
  geoidSource: Option[RasterSource],
  /** Shared CRS and cell size for all tiles produced from this scene. */
  cellSize: CellSize,
  crs: CRS,
  polarisations: Seq[Polarisation],
  config: SarProcessingConfig = SarProcessingConfig.default
) {
  /** Build the per-tile context for a given extent without any I/O or parsing. */
  def tileContext(extent: Extent): TileComputeContext =
    TileComputeContext(
      TileRequest(extent, cellSize, crs, polarisations, config),
      metadata, sarSources, demSource, geoidSource
    )
}

/** Fully-assembled inputs for one tile computation. The orchestrator builds
 *  this; backends consume it. Keeps backends free of I/O. */
final case class TileComputeContext(
  request: TileRequest,
  metadata: S1GrdMetadata,
  /** Per-polarisation measurement RasterSource (full scene, in SAR geometry). */
  sarSources: Map[Polarisation, RasterSource],
  /** DEM RasterSource. Caller is responsible for picking a DEM that covers
   *  the AOI; the orchestrator will read the AOI window from it and reproject
   *  to the output CRS at the target cell size. */
  demSource: RasterSource,
  /** Geoid undulation RasterSource (e.g. EGM2008), or None to assume DEM is
   *  already ellipsoidal heights. */
  geoidSource: Option[RasterSource]
)
