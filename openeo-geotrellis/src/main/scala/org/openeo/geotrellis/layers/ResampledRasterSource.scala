package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, OverviewStrategy}
import geotrellis.vector.Extent
import org.openeo.geotrelliscommon.ResampledTile

/** ResampledRasterSource
 * Read from a rasterSource as if it were at a different resolution.
 * Uses ResampledTiles to ensure that the original array from the raster source remains unchanged.
 * Specific use case is for layers that have bands with different resolutions, this way we don't have to
 * upsample all underlying arrays to the lowest cell size.
 */
class ResampledRasterSource(
  protected val rasterSource: RasterSource,
  sourceResolution: CellSize,
  targetResolution: CellSize,
  val targetCellType: Option[TargetCellType] = None,
) extends RasterSource {

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    if (sourceResolution == targetResolution) {
      return rasterSource.read(bounds, bands)
    }
    // Convert bounds to new resolution and read the raster.
    val newBounds = GridBounds[Long](
      colMin = Math.floor(bounds.colMin * (targetResolution.width / sourceResolution.width)).toInt,
      rowMin = Math.floor(bounds.rowMin * (targetResolution.height / sourceResolution.height)).toInt,
      colMax = Math.floor(bounds.colMax * (targetResolution.width / sourceResolution.width)).toInt,
      rowMax = Math.floor(bounds.rowMax * (targetResolution.height / sourceResolution.height)).toInt
    )
    val raster: Option[Raster[MultibandTile]] = rasterSource.read(newBounds, bands)
    // Convert tiles in raster to UpsampledTiles.
    val newRaster = raster.map(r => {
      val newTile = r.tile.mapBands((_, band) => ResampledTile(band, newBounds, bounds))
      Raster(newTile, r.extent)
    })
    newRaster
  }

  override def metadata: RasterMetadata = rasterSource.metadata

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    rasterSource.reproject(targetCRS, resampleTarget, method, strategy)
  }

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    rasterSource.resample(resampleTarget, method, strategy)
  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    rasterSource.read(extent, bands)
  }

  override def convert(targetCellType: TargetCellType): RasterSource = {
    rasterSource.convert(targetCellType)
  }

  override def name: SourceName = rasterSource.name

  override def crs: CRS = rasterSource.crs

  override def bandCount: Int = rasterSource.bandCount

  override def cellType: CellType = rasterSource.cellType

  override def gridExtent: GridExtent[Long] = {
    GridExtent(rasterSource.gridExtent.extent, targetResolution)
  }

  override def resolutions: List[CellSize] = {
    rasterSource.resolutions.map(r =>
      CellSize(
        r.width / (sourceResolution.width / targetResolution.width),
        r.height / (sourceResolution.height / targetResolution.height)
      )
    )
  }

  override def attributes: Map[String, String] = rasterSource.attributes

  override def attributesForBand(band: Int): Map[String, String] = rasterSource.attributesForBand(band)
}
