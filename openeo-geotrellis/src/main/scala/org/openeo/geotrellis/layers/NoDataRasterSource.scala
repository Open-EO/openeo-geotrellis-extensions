package org.openeo.geotrellis.layers

import geotrellis.layer.CRSWorldExtent
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{ByteConstantTile, CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterExtent, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType, byteNODATA}
import geotrellis.vector.Extent

object NoDataRasterSource {
  def get: NoDataRasterSource = {
    val tile = ByteConstantTile(byteNODATA, cols = 10, rows = 10)
    val crs = LatLng
    val extent = crs.worldExtent
    new NoDataRasterSource(Raster(MultibandTile(tile), extent), crs)
  }
}

class NoDataRasterSource private(raster: Raster[MultibandTile], override val crs: CRS, override val attributes: Map[String, String] = Map()) extends RasterSource {
  val targetCellType: Option[TargetCellType] = None

  override def metadata: RasterMetadata = new RasterMetadata {
    override def name: SourceName = NoDataRasterSource.this.name
    override def crs: CRS = NoDataRasterSource.this.crs
    override def bandCount: Int = NoDataRasterSource.this.bandCount
    override def cellType: CellType = NoDataRasterSource.this.cellType
    override def gridExtent: GridExtent[Long] = NoDataRasterSource.this.gridExtent
    override def resolutions: List[CellSize] = NoDataRasterSource.this.resolutions
    override def attributes: Map[String, String] = NoDataRasterSource.this.attributes
    override def attributesForBand(band: Int): Map[String, String] = NoDataRasterSource.this.attributesForBand(band)
  }

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NoDataRasterSource(raster.reproject(crs, targetCRS), targetCRS)

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val rasterExtent: RasterExtent = resampleTarget(gridExtent).toRasterExtent()
    new NoDataRasterSource(raster.resample(rasterExtent, method), crs)
  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    Some(raster.crop(extent).mapTile(_.subsetBands(bands)))

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] =
    Some(raster.crop(bounds.toGridType[Int]).mapTile(_.subsetBands(bands)))

  override def convert(targetCellType: TargetCellType): RasterSource =
    new NoDataRasterSource(raster.mapTile(_.convert(targetCellType.cellType)), crs)

  override def name: SourceName = getClass.getName

  override def bandCount: Int = raster.tile.bandCount

  override def resolutions: List[CellSize] = List(raster.cellSize)

  override def attributesForBand(band: Int): Map[String, String] = attributes

  override def cellType: CellType = raster.cellType

  override def gridExtent: GridExtent[Long] = GridExtent(raster.extent, cols = raster.cols, rows = raster.rows)
}
