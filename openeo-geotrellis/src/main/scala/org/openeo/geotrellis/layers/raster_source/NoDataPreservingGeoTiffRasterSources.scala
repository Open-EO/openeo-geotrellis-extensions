package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.{CRS, Proj4Transform}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffReprojectRasterSource, GeoTiffResampleRasterSource}
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.reproject.{RasterRegionReproject, ReprojectRasterExtent}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellType, DefaultTarget, GridBounds, MultibandTile, NoNoData, Raster, RasterExtent, RasterSource, ResampleTarget, TargetCellType}
import org.openeo.geotrellis.GeneralUtils.{cellTypeWithNoDataPreservingRange, convertPreservingRange}

/**
 * GeoTiff raster sources for integral rasters without a NODATA value (or that are converted to such a cell type).
 *
 * GeoTrellis resamples/reprojects the raw tile and fills target cells outside the GeoTiff with NODATA; stored in a raw
 * (NoNoData) integral tile, these become 0 and are indistinguishable from valid data, so overlapping tiles cannot be
 * merged properly. These sources widen the cell type to one with a NODATA value outside the original range of values
 * *before* resampling/reprojecting, so these cells remain NODATA, and report this widened cell type.
 *
 * Sources that do not involve a raw integral cell type behave exactly like their GeoTrellis counterparts.
 */
object NoDataPreservingGeoTiff {
  def isRawIntegral(cellType: CellType): Boolean = cellType match {
    case _: NoNoData => !cellType.isFloatingPoint
    case _ => false
  }

  private[raster_source] def preservesNoData(tiffCellType: CellType, dstCellType: Option[CellType]): Boolean =
    isRawIntegral(tiffCellType) || dstCellType.exists(isRawIntegral)

  private[raster_source] def cellType(tiffCellType: CellType, dstCellType: Option[CellType]): CellType = {
    val effectiveCellType = dstCellType.getOrElse(tiffCellType)
    if (isRawIntegral(effectiveCellType)) cellTypeWithNoDataPreservingRange(effectiveCellType) else effectiveCellType
  }

  private[raster_source] def widen(tile: MultibandTile): MultibandTile =
    if (isRawIntegral(tile.cellType)) convertPreservingRange(tile) else tile

  private[raster_source] def convert(raster: Raster[MultibandTile], cellType: CellType): Raster[MultibandTile] =
    if (raster.tile.cellType == cellType) raster else raster.mapTile(_.convert(cellType))
}

class NoDataPreservingGeoTiffResampleRasterSource(
  dataPath: GeoTiffPath,
  resampleTarget: ResampleTarget,
  method: ResampleMethod = ResampleMethod.DEFAULT,
  strategy: OverviewStrategy = OverviewStrategy.DEFAULT,
  maybeTargetCellType: Option[TargetCellType] = None,
  @transient baseTiff: Option[MultibandGeoTiff] = None
) extends GeoTiffResampleRasterSource(dataPath, resampleTarget, method, strategy, maybeTargetCellType, baseTiff) {

  override def cellType: CellType = NoDataPreservingGeoTiff.cellType(tiff.cellType, dstCellType)

  @transient private lazy val closestOverview: GeoTiff[MultibandTile] =
    tiff.getClosestOverview(gridExtent.cellSize, strategy)

  override def readBounds(bounds: Iterable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    if (!NoDataPreservingGeoTiff.preservesNoData(tiff.cellType, dstCellType)) super.readBounds(bounds, bands)
    else {
      val geoTiffTile = closestOverview.tile.asInstanceOf[GeoTiffMultibandTile]

      val windows = { for {
        queryPixelBounds <- bounds
        targetPixelBounds <- queryPixelBounds.intersection(this.dimensions)
      } yield {
        val targetExtent = gridExtent.extentFor(targetPixelBounds)
        val bufferedTargetExtent = targetExtent.buffer(cellSize.width / 2, cellSize.height / 2)
        val sourcePixelBounds = closestOverview.rasterExtent.gridBoundsFor(bufferedTargetExtent)
        val targetRasterExtent = RasterExtent(targetExtent, targetPixelBounds.width.toInt, targetPixelBounds.height.toInt)
        (sourcePixelBounds, targetRasterExtent)
      }}.toMap

      geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (gb, tile) =>
        val targetRasterExtent = windows(gb)
        val sourceExtent = closestOverview.rasterExtent.extentFor(gb, clamp = false)
        val resampled = Raster(NoDataPreservingGeoTiff.widen(tile), sourceExtent).resample(targetRasterExtent, method)
        NoDataPreservingGeoTiff.convert(resampled, cellType)
      }
    }
  }

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget = DefaultTarget, method: ResampleMethod = ResampleMethod.DEFAULT, strategy: OverviewStrategy = OverviewStrategy.DEFAULT): GeoTiffReprojectRasterSource =
    new NoDataPreservingGeoTiffReprojectRasterSource(dataPath, targetCRS, resampleTarget, method, strategy, maybeTargetCellType = maybeTargetCellType, baseTiff = Some(tiff))

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NoDataPreservingGeoTiffResampleRasterSource(dataPath, resampleTarget, method, strategy, maybeTargetCellType, Some(tiff))

  override def convert(targetCellType: TargetCellType): RasterSource =
    new NoDataPreservingGeoTiffResampleRasterSource(dataPath, resampleTarget, method, strategy, Some(targetCellType), Some(tiff))

  override def toString: String = s"NoDataPreservingGeoTiffResampleRasterSource(${dataPath.value}, $resampleTarget, $method)"
}

class NoDataPreservingGeoTiffReprojectRasterSource(
  dataPath: GeoTiffPath,
  crs: CRS,
  resampleTarget: ResampleTarget = DefaultTarget,
  resampleMethod: ResampleMethod = ResampleMethod.DEFAULT,
  strategy: OverviewStrategy = OverviewStrategy.DEFAULT,
  errorThreshold: Double = 0.125,
  maybeTargetCellType: Option[TargetCellType] = None,
  @transient baseTiff: Option[MultibandGeoTiff] = None
) extends GeoTiffReprojectRasterSource(dataPath, crs, resampleTarget, resampleMethod, strategy, errorThreshold, maybeTargetCellType, baseTiff) {

  override def cellType: CellType = NoDataPreservingGeoTiff.cellType(tiff.cellType, dstCellType)

  @transient private lazy val closestOverview: GeoTiff[MultibandTile] = resampleTarget match {
    case DefaultTarget => tiff.getClosestOverview(baseGridExtent.cellSize, strategy)
    case _ =>
      val estimatedSource = ReprojectRasterExtent(gridExtent, backTransform)
      tiff.getClosestOverview(estimatedSource.cellSize, strategy)
  }

  override def readBounds(bounds: Iterable[GridBounds[Long]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    if (!NoDataPreservingGeoTiff.preservesNoData(tiff.cellType, dstCellType)) super.readBounds(bounds, bands)
    else {
      val geoTiffTile = closestOverview.tile.asInstanceOf[GeoTiffMultibandTile]

      val intersectingWindows = { for {
        queryPixelBounds <- bounds
        targetPixelBounds <- queryPixelBounds.intersection(this.dimensions)
      } yield {
        val targetExtent = gridExtent.extentFor(targetPixelBounds)
        val targetRasterExtent = RasterExtent(
          extent = targetExtent,
          cols = targetPixelBounds.width.toInt,
          rows = targetPixelBounds.height.toInt
        )

        val bufferedTargetExtent = targetExtent.buffer(cellSize.width, cellSize.height)
        // a tmp workaround for https://github.com/locationtech/proj4j/pull/29
        val sourceExtent = Proj4Transform.synchronized(bufferedTargetExtent.reprojectAsPolygon(backTransform, 0.001).getEnvelopeInternal)
        val sourcePixelBounds = closestOverview.rasterExtent.gridBoundsFor(sourceExtent)
        (sourcePixelBounds, targetRasterExtent)
      }}.toMap

      geoTiffTile.crop(intersectingWindows.keys.toSeq, bands.toArray).map { case (sourcePixelBounds, tile) =>
        val targetRasterExtent = intersectingWindows(sourcePixelBounds)
        val sourceRaster = Raster(NoDataPreservingGeoTiff.widen(tile), closestOverview.rasterExtent.extentFor(sourcePixelBounds))
        val reprojected = implicitly[RasterRegionReproject[MultibandTile]].regionReproject(
          sourceRaster,
          baseCRS,
          crs,
          targetRasterExtent,
          targetRasterExtent.extent.toPolygon(),
          resampleMethod,
          errorThreshold
        )
        NoDataPreservingGeoTiff.convert(reprojected, cellType)
      }
    }
  }

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget = DefaultTarget, method: ResampleMethod = ResampleMethod.DEFAULT, strategy: OverviewStrategy = OverviewStrategy.DEFAULT): RasterSource =
    new NoDataPreservingGeoTiffReprojectRasterSource(dataPath, targetCRS, resampleTarget, method, strategy, maybeTargetCellType = maybeTargetCellType, baseTiff = Some(tiff))

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NoDataPreservingGeoTiffReprojectRasterSource(dataPath, crs, resampleTarget, method, strategy, maybeTargetCellType = maybeTargetCellType, baseTiff = Some(tiff))

  override def convert(targetCellType: TargetCellType): RasterSource =
    new NoDataPreservingGeoTiffReprojectRasterSource(dataPath, crs, resampleTarget, resampleMethod, strategy, errorThreshold, Some(targetCellType), Some(tiff))

  override def toString: String = s"NoDataPreservingGeoTiffReprojectRasterSource(${dataPath.value}, $crs, $resampleTarget, $resampleMethod)"
}
