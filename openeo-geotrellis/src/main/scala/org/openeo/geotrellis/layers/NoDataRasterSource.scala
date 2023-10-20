package org.openeo.geotrellis.layers

import geotrellis.layer.CRSWorldExtent
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{UByteConstantNoDataCellType, UByteConstantTile, CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType, Tile, ubyteNODATA}
import geotrellis.vector.Extent

object NoDataRasterSource {
  private val supportedBandIndices: Seq[Int] = Seq(0)

  def instance(gridExtent: GridExtent[Long], crs: CRS): NoDataRasterSource =
    new NoDataRasterSource(UByteConstantNoDataCellType, gridExtent, crs)

  def instance: NoDataRasterSource =
    instance(GridExtent(LatLng.worldExtent, cols = 10, rows = 10), LatLng)
}

class NoDataRasterSource(override val cellType: CellType, override val gridExtent: GridExtent[Long], override val crs: CRS) extends RasterSource {
  import NoDataRasterSource._

  val targetCellType: Option[TargetCellType] = None

  override def metadata: RasterMetadata = this

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NoDataRasterSource(cellType, gridExtent.reproject(crs, targetCRS), targetCRS)

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NoDataRasterSource(cellType, resampleTarget(gridExtent), crs)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    require(bands == supportedBandIndices)

    extent.intersection(gridExtent.extent)
      .map { intersection =>
        val intersectionGridBounds = gridExtent.gridBoundsFor(intersection).toGridType[Int]
        val noDataTile = baseNoDataTile(intersectionGridBounds.width, intersectionGridBounds.height)
        Raster(MultibandTile(noDataTile), intersection)
      }
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    require(bands == supportedBandIndices)

    bounds.intersection(gridExtent.dimensions)
      .map { intersection =>
        val intersectionGridBounds = intersection.toGridType[Int]
        val noDataTile = baseNoDataTile(intersectionGridBounds.width, intersectionGridBounds.height)
        Raster(MultibandTile(noDataTile), gridExtent.extentFor(intersection))
      }
  }

  private def baseNoDataTile(cols: Int, rows: Int): Tile = UByteConstantTile(ubyteNODATA, cols, rows).convert(cellType)

  override def convert(targetCellType: TargetCellType): RasterSource =
    new NoDataRasterSource(targetCellType.cellType, gridExtent, crs)

  override def name: SourceName = toString

  override def bandCount: Int = supportedBandIndices.size

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map()

  override def attributesForBand(band: Int): Map[String, String] = Map()

  override def toString: String = f"${getClass.getName}($cellType, $gridExtent, $crs)"
}
