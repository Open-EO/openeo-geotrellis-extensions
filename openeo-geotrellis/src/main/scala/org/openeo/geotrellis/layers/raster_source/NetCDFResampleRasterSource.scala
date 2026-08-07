package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector.Extent

class NetCDFResampleRasterSource(
  val baseSource: NetCDFRasterSource,
  val resampleTarget: ResampleTarget,
  val method: ResampleMethod = ResampleMethod.DEFAULT,
  val strategy: OverviewStrategy = OverviewStrategy.DEFAULT,
  override val targetCellType: Option[TargetCellType] = None
) extends RasterSource {

  override def crs: CRS = baseSource.crs

  override lazy val gridExtent: GridExtent[Long] = resampleTarget(baseSource.gridExtent)

  override def resolutions: List[CellSize] = List(baseSource.gridExtent.cellSize)

  override def name: SourceName = baseSource.name

  override def metadata: RasterMetadata = this

  override def bandCount: Int = baseSource.bandCount

  override def cellType: CellType = dstCellType.getOrElse(baseSource.cellType)

  override def attributes: Map[String, String] = baseSource.attributes

  override def attributesForBand(band: Int): Map[String, String] = baseSource.attributesForBand(band)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    bounds.intersection(dimensions).flatMap { targetPixelBounds =>
      val targetExtent = gridExtent.extentFor(targetPixelBounds)
      val bufferedTargetExtent = targetExtent.buffer(cellSize.width / 2, cellSize.height / 2)
      val sourceBounds = baseSource.gridExtent.gridBoundsFor(bufferedTargetExtent)
      val targetRasterExtent = RasterExtent(targetExtent, targetPixelBounds.width.toInt, targetPixelBounds.height.toInt)

      baseSource.read(sourceBounds, bands).map { sourceRaster =>
        val sourceExtent = baseSource.gridExtent.extentFor(sourceBounds, clamp = false)
        convertRaster(Raster(sourceRaster.tile, sourceExtent).resample(targetRasterExtent, method))
      }
    }
  }

  override def convert(targetCellType: TargetCellType): RasterSource =
    new NetCDFResampleRasterSource(baseSource, resampleTarget, method, strategy, Some(targetCellType))

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NetCDFReprojectRasterSource(baseSource, targetCRS, resampleTarget, method, strategy, targetCellType = targetCellType)

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NetCDFResampleRasterSource(baseSource, resampleTarget, method, strategy, targetCellType)
}
