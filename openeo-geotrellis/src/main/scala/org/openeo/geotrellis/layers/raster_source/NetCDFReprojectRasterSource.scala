package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.{CRS, Proj4Transform, Transform}
import geotrellis.raster._
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.reproject.{Reproject, RasterRegionReproject, ReprojectRasterExtent}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector.Extent

class NetCDFReprojectRasterSource(
  val baseSource: NetCDFRasterSource,
  val crs: CRS,
  val resampleTarget: ResampleTarget = DefaultTarget,
  val resampleMethod: ResampleMethod = ResampleMethod.DEFAULT,
  val strategy: OverviewStrategy = OverviewStrategy.DEFAULT,
  val errorThreshold: Double = 0.0,
  override val targetCellType: Option[TargetCellType] = None
) extends RasterSource {

  private val baseCRS: CRS = baseSource.crs
  private val baseGridExtent: GridExtent[Long] = baseSource.gridExtent

  @transient protected lazy val transform: (Double, Double) => (Double, Double) = Transform(baseCRS, crs)
  @transient private lazy val backTransform: (Double, Double) => (Double, Double) = Transform(crs, baseCRS)

  override lazy val gridExtent: GridExtent[Long] = {
    lazy val reprojectedRasterExtent =
      ReprojectRasterExtent(
        baseGridExtent,
        transform,
        Reproject.Options.DEFAULT.copy(method = resampleMethod, errorThreshold = errorThreshold)
      )
    resampleTarget(reprojectedRasterExtent)
  }

  override def resolutions: List[CellSize] =
    List(ReprojectRasterExtent(baseGridExtent, transform, Reproject.Options.DEFAULT).cellSize)

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
      val targetRasterExtent = RasterExtent(
        extent = targetExtent,
        cols = targetPixelBounds.width.toInt,
        rows = targetPixelBounds.height.toInt
      )

      val bufferedTargetExtent = targetExtent.buffer(cellSize.width, cellSize.height)
      val sourceEnvelope = Proj4Transform.synchronized(
        bufferedTargetExtent.reprojectAsPolygon(backTransform, 0.001).getEnvelopeInternal
      )
      val sourceExtent = Extent(sourceEnvelope.getMinX, sourceEnvelope.getMinY, sourceEnvelope.getMaxX, sourceEnvelope.getMaxY)
      val sourceBounds = baseGridExtent.gridBoundsFor(sourceExtent)

      baseSource.read(sourceBounds, bands).map { sourceRaster =>
        val rr = implicitly[RasterRegionReproject[MultibandTile]]
        val reprojected = rr.regionReproject(
          sourceRaster,
          baseCRS,
          crs,
          targetRasterExtent,
          targetRasterExtent.extent.toPolygon(),
          resampleMethod,
          errorThreshold
        )
        convertRaster(reprojected)
      }
    }
  }

  override def convert(targetCellType: TargetCellType): RasterSource =
    new NetCDFReprojectRasterSource(baseSource, crs, resampleTarget, resampleMethod, strategy, errorThreshold, Some(targetCellType))

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NetCDFReprojectRasterSource(baseSource, targetCRS, resampleTarget, method, strategy, errorThreshold, targetCellType)

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new NetCDFReprojectRasterSource(baseSource, crs, resampleTarget, method, strategy, errorThreshold, targetCellType)
}
