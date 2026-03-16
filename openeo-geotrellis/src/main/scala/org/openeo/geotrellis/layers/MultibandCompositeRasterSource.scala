package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{ConstantTile, GridBounds, GridExtent, MultibandTile, Raster, RasterSource, ResampleMethod, ResampleTarget, TargetCellType, Tile}
import geotrellis.vector.Extent

// TODO: is this class necessary? Looks like a more general case of BandCompositeRasterSource so maybe the inheritance
//  relationship should be reversed; or maybe the BandCompositeRasterSource could be made more general and accept
//  multi-band RasterSources too.
class MultibandCompositeRasterSource(val sourcesListWithBandIds: NonEmptyList[(RasterSource, Seq[Int])],
                                     override val crs: CRS,
                                     override val attributes: Map[String, String] = Map.empty,
                                     val readFullTile: Boolean = false,
                                     override val predefinedExtent: Option[GridExtent[Long]] = None
                                    )
  extends BandCompositeRasterSource(sourcesListWithBandIds.map(_._1), crs, attributes, readFullTile = readFullTile) {

  override def bandCount: Int = sourcesListWithBandIds.map(_._2.size).toList.sum

  private def sourcesWithBandIds = NonEmptyList.fromListUnsafe(reprojectedSources.toList.zip(sourcesListWithBandIds.map(_._2).toList))

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val rasters = sourcesWithBandIds
      .map { s => s._1.read(extent, s._2) }
      .collect { case Some(raster) => raster }

    if (rasters.size == sources.size) Some(Raster(MultibandTile(rasters.flatMap(_.tile.bands)), rasters.head.extent))
    else None
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val rasters: Seq[Raster[MultibandTile]] = sourcesWithBandIds
      .map { s => BandCompositeRasterSource.readBounds(s._1, bounds, false, s._2) }
      .collect { case Some(raster) => raster }

    if (rasters.size == sources.size) {
      Some(Raster(MultibandTile(rasters.flatMap(_.tile.bands.map{
        case constantTile: ConstantTile => constantTile.convert(cellType)
        case tile: Tile => tile.toArrayTile().convert(cellType)}
      )), rasters.head.extent))
    }
    else None
  }

  override def resample(
                         resampleTarget: ResampleTarget,
                         method: ResampleMethod,
                         strategy: OverviewStrategy
                       ): RasterSource = new MultibandCompositeRasterSource(
    sourcesWithBandIds map { case (source, bands) => (source.resample(resampleTarget, method, strategy), bands) }, crs, readFullTile = readFullTile)

  override def convert(targetCellType: TargetCellType): RasterSource =
    new MultibandCompositeRasterSource(sourcesWithBandIds map { case (source, bands) => (source.convert(targetCellType), bands) }, crs, readFullTile = readFullTile)

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new MultibandCompositeRasterSource(
      sourcesWithBandIds map { case (source, bands) => (source.reproject(targetCRS, resampleTarget, method, strategy), bands) },
      crs, readFullTile = readFullTile
    )
}
