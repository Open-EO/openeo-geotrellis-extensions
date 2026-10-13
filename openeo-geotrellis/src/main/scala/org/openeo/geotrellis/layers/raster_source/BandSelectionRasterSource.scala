package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleTarget, SourceName, TargetCellType}
import geotrellis.vector.Extent

/**
 * Exposes bands `bandIndices` of `rasterSource` as its bands `0 until bandIndices.size`, so that a single physical
 * multi-band dataset can serve several consecutive output bands of a
 * [[org.openeo.geotrellis.layers.BandCompositeRasterSource]] in one read.
 */
case class BandSelectionRasterSource(rasterSource: RasterSource, bandIndices: Seq[Int]) extends RasterSource {
  require(bandIndices.nonEmpty, "at least one band must be selected")

  override def targetCellType: Option[TargetCellType] = None

  override def metadata: RasterMetadata = rasterSource.metadata

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    copy(rasterSource = rasterSource.reproject(targetCRS, resampleTarget, method, strategy))

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    copy(rasterSource = rasterSource.resample(resampleTarget, method, strategy))

  override def convert(targetCellType: TargetCellType): RasterSource =
    copy(rasterSource = rasterSource.convert(targetCellType))

  // Reads every underlying band only once (GeoTiffRasterSource e.g. does not support duplicate band indices), then
  // expands the result to the requested bands.
  private def readSelected(bands: Seq[Int])(read: Seq[Int] => Iterator[Raster[MultibandTile]]): Iterator[Raster[MultibandTile]] = {
    val underlyingBands = bands.map(bandIndices)
    val distinctBands = underlyingBands.distinct
    if (distinctBands.size == underlyingBands.size) read(underlyingBands)
    else read(distinctBands).map(_.mapTile(tile => MultibandTile(underlyingBands.map(band => tile.band(distinctBands.indexOf(band))))))
  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    readSelected(bands)(rasterSource.read(extent, _).iterator).nextOption()

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] =
    readSelected(bands)(rasterSource.read(bounds, _).iterator).nextOption()

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    readSelected(bands)(rasterSource.readBounds(bounds, _))

  override def name: SourceName = rasterSource.name

  override def crs: CRS = rasterSource.crs

  override def bandCount: Int = bandIndices.size

  override def cellType: CellType = rasterSource.cellType

  override def gridExtent: GridExtent[Long] = rasterSource.gridExtent

  override def resolutions: List[CellSize] = rasterSource.resolutions

  override def attributes: Map[String, String] = rasterSource.attributes

  override def attributesForBand(band: Int): Map[String, String] = rasterSource.attributesForBand(bandIndices(band))
}
