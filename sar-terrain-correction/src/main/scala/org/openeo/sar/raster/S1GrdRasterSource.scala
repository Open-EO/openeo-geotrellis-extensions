package org.openeo.sar.raster

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleTarget, StringName, TargetCellType}
import geotrellis.vector.Extent
import org.openeo.sar.{SceneContext, TerrainCorrectionProcessor}
import org.slf4j.LoggerFactory

/** A [[RasterSource]] whose pixels are produced by SAR terrain correction
 *  (sigma0 calibration + range-Doppler orthorectification).
 *
 *  Band layout (Float32, NaN/0 outside swath):
 *    0 .. nPols-1 → sigma0 per polarisation (linear power)
 *    nPols        → local incidence angle (degrees)
 *    nPols+1      → validity mask (1.0 = valid)
 *
 *  All expensive state (orbit, LUTs, open RasterSources) lives in the
 *  [[SceneContext]] which is built once and shared across all reads.
 *  The [[TerrainCorrectionProcessor]] is injected so that the caller can
 *  choose backend (native / ONNX), DEM and geoid sources. */
final class S1GrdRasterSource(
  val sceneContext: SceneContext,
  val processor: TerrainCorrectionProcessor,
  override val gridExtent: GridExtent[Long],
  override val crs: CRS,
  override val name: StringName,
  override val targetCellType: Option[TargetCellType] = None
) extends RasterSource {

  private val logger = LoggerFactory.getLogger(getClass)

  override def metadata: RasterMetadata = this

  override def bandCount: Int = sceneContext.polarisations.size + 2

  override def cellType: CellType =
    targetCellType.map(_.cellType).getOrElse(FloatConstantNoDataCellType)

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map.empty

  override def attributesForBand(band: Int): Map[String, String] = Map.empty

  // ---- reprojection / resample -----------------------------------------------

  override protected def reprojection(targetCRS: CRS,
                                      resampleTarget: ResampleTarget,
                                      method: ResampleMethod,
                                      strategy: OverviewStrategy): RasterSource = {
    val newGridExtent = resampleTarget(gridExtent.reproject(crs, targetCRS))
    new S1GrdRasterSource(
      sceneContext.copy(crs = targetCRS, cellSize = newGridExtent.cellSize),
      processor, newGridExtent, targetCRS, name, targetCellType)
  }

  override def resample(resampleTarget: ResampleTarget,
                        method: ResampleMethod,
                        strategy: OverviewStrategy): RasterSource = {
    val newGridExtent = resampleTarget(gridExtent)
    new S1GrdRasterSource(
      sceneContext.copy(cellSize = newGridExtent.cellSize),
      processor, newGridExtent, crs, name, targetCellType)
  }

  override def convert(targetCellType: TargetCellType): RasterSource =
    new S1GrdRasterSource(sceneContext, processor, gridExtent, crs, name, Some(targetCellType))

  // ---- reads -----------------------------------------------------------------

  /** Efficient multi-extent read: shares per-pixel geometry work via the
   *  shared [[SceneContext]] (no repeated XML parsing or RasterSource opens). */
  override def readExtents(extents: Traversable[Extent],
                           bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    processor.readExtents(sceneContext, extents, bands)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    extent.intersection(gridExtent.extent) match {
      case None =>
        logger.debug(s"Requested extent $extent does not intersect scene extent ${gridExtent.extent}")
        None
      case Some(clipped) =>
        readExtents(List(clipped), bands).nextOption()
    }
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent = gridExtent.extentFor(bounds, clamp = true)
    read(extent, bands)
  }
}
