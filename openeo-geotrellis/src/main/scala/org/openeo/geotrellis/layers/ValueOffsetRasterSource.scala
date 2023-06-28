package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType, Tile}
import geotrellis.vector.Extent
import org.openeo.geotrellis.toSigned
import org.slf4j.LoggerFactory

/**
 * Same wrapping logic as in ResampledRasterSource
 * Wraps around a raster source and makes sure all pixels get offseted by a value
 * when the source is loaded.
 */
object ValueOffsetRasterSource {
  // Ignore trailing $'s in the class names for Scala objects
  private val logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  /**
   * Only wraps the rasterSources when needed
   */
  def wrapRasterSources(rasterSources: Seq[RasterSource],
                        pixelValueOffset: Double,
                        targetCellType: Option[TargetCellType] = None
                       ): Seq[RasterSource] = {
    if (pixelValueOffset == 0 && targetCellType.isEmpty) rasterSources
    else rasterSources.map(rs => new ValueOffsetRasterSource(rs, pixelValueOffset, targetCellType))
  }
}

class ValueOffsetRasterSource(protected val rasterSource: RasterSource,
                              pixelValueOffset: Double,
                              val targetCellType: Option[TargetCellType] = None, //
                             ) extends RasterSource {

  import ValueOffsetRasterSource._

  private def withOffset(bandTile: Tile): Tile = {
    if (pixelValueOffset == 0) bandTile
    else if (cellType.isFloatingPoint) bandTile.convert(cellType).mapIfSetDouble(x => x + pixelValueOffset)
    else bandTile.convert(cellType).mapIfSet(i => i + pixelValueOffset.toInt)
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val raster: Option[Raster[MultibandTile]] = rasterSource.read(bounds, bands)

    // Convert tiles in raster
    val newRaster = raster.map(r => {
      val newTile = r.tile.mapBands((_, band) => withOffset(band))
      Raster(newTile, r.extent)
    })
    newRaster
  }

  override def cellType: CellType = {
    targetCellType match {
      case Some(t) => t.cellType
      case None =>
        val originalCellType = rasterSource.cellType
        if (pixelValueOffset < 0) {
          if (toSigned(originalCellType) != originalCellType) {
            logger.warn("Offset might cause integer underflow. Best to specify targetCellType explicitly.")
          }
        }
        originalCellType
    }
  }

  override def metadata: RasterMetadata = rasterSource.metadata

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val rs = rasterSource.reproject(targetCRS, resampleTarget, method, strategy)
    new ValueOffsetRasterSource(rs, pixelValueOffset, targetCellType)
  }

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val rs = rasterSource.resample(resampleTarget, method, strategy)
    new ValueOffsetRasterSource(rs, pixelValueOffset, targetCellType)
  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  override def convert(targetCellType: TargetCellType): RasterSource = {
    val rs = rasterSource.convert(targetCellType)
    new ValueOffsetRasterSource(rs, pixelValueOffset, Some(targetCellType))
  }

  override def name: SourceName = rasterSource.name

  override def crs: CRS = rasterSource.crs

  override def bandCount: Int = rasterSource.bandCount

  override def gridExtent: GridExtent[Long] = rasterSource.gridExtent

  override def resolutions: List[CellSize] = rasterSource.resolutions

  override def attributes: Map[String, String] = rasterSource.attributes

  override def attributesForBand(band: Int): Map[String, String] = rasterSource.attributesForBand(band)
}
