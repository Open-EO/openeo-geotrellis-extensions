package org.openeo.geotrellis.layers.provider

import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource, GeoTiffReprojectRasterSource, GeoTiffResampleRasterSource}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, RasterExtent, RasterSource, TargetRegion}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.FileLayerProvider.vsis3ToS3
import org.openeo.geotrellis.layers.ResampledRasterSource
import org.slf4j.{Logger, LoggerFactory}

object DefaultRasterSourceProvider extends DefaultRasterSourceProvider

class DefaultRasterSourceProvider extends RasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[DefaultRasterSourceProvider])

  override def canProcess(definition: RasterSourceDefinition): Boolean = {
    true
  }

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    def alignmentFromDataPath(dataPath: String, projectedExtent: ProjectedExtent): TargetRegion = {
      // When noResampleOnRead is set, we retrieve the actual resolution from the dataPath.
      // Note: This is only supported for S2 dataPaths.
      // E.g. S2A_20190307T105021_31UFT_TOC-B05_20M_V200.tif = 20.0
      val splitPath: Array[String] = dataPath.split("_")
      val tiffResolution = splitPath(splitPath.length - 2).replace("M", "").toDouble
      val tiffCellSize = CellSize(tiffResolution, tiffResolution)
      val tiffRe = RasterExtent(expandToCellSize(projectedExtent.extent, tiffCellSize), tiffCellSize)
      TargetRegion(tiffRe)
    }

    if (definition.feature.crs.isDefined && definition.feature.crs.get != null && definition.feature.crs.get.equals(definition.targetExtent.crs)) {
      // when we don't know the feature (input) CRS, it seems that we assume it is the same as target extent???
      if (definition.experimental) {
        GDALRasterSource(definition.dataPath, options = GDALWarpOptions(alignTargetPixels = true, cellSize = Some(definition.theResolution), resampleMethod = Some(definition.resampleMethod)), targetCellType = definition.targetCellType)
      } else {
        val geotiffPath = GeoTiffPath(vsis3ToS3(definition.dataPath))
        if (definition.noResampleOnRead) {
          val tiffAlignment = alignmentFromDataPath(definition.dataPath, definition.targetExtent)
          val geotiffRasterSource = GeoTiffRasterSource(geotiffPath, definition.targetCellType)
          new ResampledRasterSource(geotiffRasterSource, tiffAlignment.region.cellSize, definition.theResolution)
        } else {
          GeoTiffResampleRasterSource(geotiffPath, definition.alignment, definition.resampleMethod, OverviewStrategy.DEFAULT, definition.targetCellType, None)
        }
      }
    } else {
      if (definition.experimental) {
        val warpOptions = GDALWarpOptions(alignTargetPixels = false, cellSize = Some(definition.theResolution), targetCRS = Some(definition.targetExtent.crs), resampleMethod = Some(definition.resampleMethod), te = Some(definition.targetExtent.extent))
        GDALRasterSource(definition.dataPath.replace("/vsis3/EODATA/", "/vsis3/eodata/").replace("https", "/vsicurl/https"), options = warpOptions, targetCellType = definition.targetCellType)
      } else {
        val geotiffPath = GeoTiffPath(vsis3ToS3(definition.dataPath))
        if (definition.noResampleOnRead) {
          val tiffAlignment = alignmentFromDataPath(definition.dataPath, definition.targetExtent)
          val geotiffRasterSource = GeoTiffReprojectRasterSource(geotiffPath, definition.targetExtent.crs, tiffAlignment, definition.resampleMethod, OverviewStrategy.DEFAULT, targetCellType = definition.targetCellType)
          new ResampledRasterSource(geotiffRasterSource, tiffAlignment.region.cellSize, definition.theResolution)
        } else {
          GeoTiffReprojectRasterSource(geotiffPath, definition.targetExtent.crs, definition.alignment, definition.resampleMethod, OverviewStrategy.DEFAULT, targetCellType = definition.targetCellType)
        }
      }
    }
  }

  private def expandToCellSize(extent: Extent, cellSize: CellSize): Extent =
    Extent(
      extent.xmin,
      extent.ymin,
      math.max(extent.xmax, extent.xmin + cellSize.width),
      math.max(extent.ymax, extent.ymin + cellSize.height),
    )

}
