package org.openeo.geotrellis.layers.provider

import geotrellis.raster.RasterSource
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import org.slf4j.{Logger, LoggerFactory}

object ZarrRasterSourceProvider extends ZarrRasterSourceProvider

class ZarrRasterSourceProvider extends RasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[ZarrRasterSourceProvider])

  override def canProcess(definition: RasterSourceDefinition): Boolean = {
    definition.dataPath.endsWith(".zarr")
  }

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    val warpOptions = GDALWarpOptions(alignTargetPixels = false, cellSize = Some(definition.theResolution), targetCRS=Some(definition.targetExtent.crs), resampleMethod = Some(definition.resampleMethod),te = Some(definition.targetExtent.extent))
    GDALRasterSource(GDALPath(definition.dataPath),options = warpOptions, targetCellType = definition.targetCellType)
  }
}
