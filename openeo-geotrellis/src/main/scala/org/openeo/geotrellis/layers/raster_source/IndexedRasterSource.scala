package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleTarget, SourceName, TargetCellType}
import geotrellis.vector.Extent
import org.slf4j.LoggerFactory

case class IndexedRasterSource(rasterSource: RasterSource, bandIndex: Int) extends RasterSource {

  private val logger = LoggerFactory.getLogger(getClass)

  val targetCellType = None

  override def metadata: RasterMetadata = rasterSource.metadata

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = IndexedRasterSource(rasterSource.reproject(targetCRS, resampleTarget, method, strategy), bandIndex)

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = IndexedRasterSource(rasterSource.resample(resampleTarget, method, strategy), bandIndex)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    assert(bands.length == 1)
    if (bands != Seq(0)) {
      logger.warn(s"Requested bands $bands, reading data from underlying ${rasterSource} band $bandIndex.")
    }
    rasterSource.read(extent, Seq(bandIndex))
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    assert(bands.length == 1)
    if (bands != Seq(0)) {
      logger.warn(s"Requested bands $bands, reading data from underlying ${rasterSource} band $bandIndex.")
    }
    rasterSource.read(bounds, Seq(bandIndex))
  }

  override def convert(targetCellType: TargetCellType): RasterSource = IndexedRasterSource(rasterSource.convert(targetCellType), bandIndex)

  override def name: SourceName = rasterSource.name

  override def crs: CRS = rasterSource.crs

  override def bandCount: Int = rasterSource.bandCount

  override def cellType: CellType = rasterSource.cellType

  override def gridExtent: GridExtent[Long] = rasterSource.gridExtent

  override def resolutions: List[CellSize] = rasterSource.resolutions

  override def attributes: Map[String, String] = rasterSource.attributes

  override def attributesForBand(band: Int): Map[String, String] = {
    rasterSource.attributesForBand(bandIndex)
  }
}
