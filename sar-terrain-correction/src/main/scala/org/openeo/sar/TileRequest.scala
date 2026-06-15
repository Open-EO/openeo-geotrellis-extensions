package org.openeo.sar

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, RasterSource}
import geotrellis.vector.Extent
import org.openeo.sar.metadata.{Polarisation, S1GrdMetadata}

/** Output tile description supplied by the caller. */
final case class TileRequest(
  extent: Extent,
  cellSize: CellSize,
  crs: CRS,
  polarisations: Seq[Polarisation]
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
  polarisations: Seq[Polarisation]
) {
  /** Build the per-tile context for a given extent without any I/O or parsing. */
  def tileContext(extent: Extent): TileComputeContext =
    TileComputeContext(
      TileRequest(extent, cellSize, crs, polarisations),
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
