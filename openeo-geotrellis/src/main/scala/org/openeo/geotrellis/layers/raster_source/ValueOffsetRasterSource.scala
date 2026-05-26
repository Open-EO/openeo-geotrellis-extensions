package org.openeo.geotrellis.layers.raster_source

import breeze.numerics.log
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, CellType, ConvertTargetCellType, DoubleConstantNoDataCellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType, Tile}
import geotrellis.vector.Extent
import org.openeo.geotrellis.GeneralUtils.toSigned
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
  def wrapRasterSource(rasterSource: RasterSource,
                       pixelValueScale: Double,
                       pixelValueOffset: Double,
                       targetCellType: Option[TargetCellType] = None
                      ): RasterSource = {
    if (pixelValueScale == 1.0 && pixelValueOffset == 0 && targetCellType.isEmpty) rasterSource
    else new ValueOffsetRasterSource(rasterSource, pixelValueScale, pixelValueOffset, targetCellType)
  }
}

class ValueOffsetRasterSource(val rasterSource: RasterSource,
                              pixelValueScale: Double,
                              pixelValueOffset: Double,
                              val targetCellType: Option[TargetCellType] = None, //
                             ) extends RasterSource {

  import ValueOffsetRasterSource._

  private def withScaleAndOffset(bandTile: Tile): Tile = {
    if (pixelValueScale == 1.0 && pixelValueOffset == 0) {
      bandTile
    } else if (cellType.isFloatingPoint) {
      bandTile.convert(cellType).mapIfSetDouble(x => pixelValueScale*x + pixelValueOffset)
    } else {
      bandTile.convert(cellType).mapIfSet(i => (i * pixelValueScale + pixelValueOffset).toInt)
    }
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val raster: Option[Raster[MultibandTile]] = rasterSource.read(bounds, bands)

    // Convert tiles in raster
    val newRaster = raster.map(r => {
      val newTile = r.tile.mapBands((_, band) => withScaleAndOffset(band))
      Raster(newTile, r.extent)
    })
    newRaster
  }

  override def cellType: CellType = {
    if (pixelValueScale != 1.0) {
      if (rasterSource.cellType.isFloatingPoint) {
        rasterSource.cellType
      } else {
        logger.warn(s"Applying a pixel value scale of $pixelValueScale to a raster with cell type ${rasterSource.cellType} forces conversion to ${DoubleConstantNoDataCellType}.")
        DoubleConstantNoDataCellType
      }
    }
    else {
      var cellType = targetCellType match {
        case Some(t) => t.cellType
        case None => rasterSource.cellType
      }
      if (!cellType.isFloatingPoint && pixelValueOffset != 0) {
        if (pixelValueOffset < 0) {
          val signedCellType = toSigned(cellType)
          if (signedCellType != cellType) {
            logger.warn(s"Applying a pixel value offset of $pixelValueOffset results in converting unsigned data to signed.")
            cellType = signedCellType
          }
        }
        if (BigDecimal(math.abs(pixelValueOffset)).toBigInt.bitLength > cellType.bits) {
          logger.warn(s"Applying a pixel value offset of $pixelValueOffset to a raster with cell type ${rasterSource.cellType} forces conversion to ${DoubleConstantNoDataCellType}.")
          cellType = DoubleConstantNoDataCellType
        }
      }
      cellType
    }
  }

  override def metadata: RasterMetadata = rasterSource.metadata

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val rs = rasterSource.reproject(targetCRS, resampleTarget, method, strategy)
    new ValueOffsetRasterSource(rs, pixelValueScale, pixelValueOffset, targetCellType)
  }

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val rs = rasterSource.resample(resampleTarget, method, strategy)
    new ValueOffsetRasterSource(rs, pixelValueScale, pixelValueOffset, targetCellType)
  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  override def convert(targetCellType: TargetCellType): RasterSource = {
    val rs = rasterSource.convert(targetCellType)
    new ValueOffsetRasterSource(rs, pixelValueScale, pixelValueOffset, Some(targetCellType))
  }

  override def name: SourceName = rasterSource.name

  override def crs: CRS = rasterSource.crs

  override def bandCount: Int = rasterSource.bandCount

  override def gridExtent: GridExtent[Long] = rasterSource.gridExtent

  override def resolutions: List[CellSize] = rasterSource.resolutions

  override def attributes: Map[String, String] = rasterSource.attributes

  override def attributesForBand(band: Int): Map[String, String] = rasterSource.attributesForBand(band)
}
