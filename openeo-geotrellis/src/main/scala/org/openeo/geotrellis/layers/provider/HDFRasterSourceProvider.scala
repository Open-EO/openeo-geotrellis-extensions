package org.openeo.geotrellis.layers.provider

import geotrellis.raster.{CellType, ConvertTargetCellType, RasterSource}
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import org.slf4j.{Logger, LoggerFactory}

object HDFRasterSourceProvider extends HDFRasterSourceProvider

class HDFRasterSourceProvider extends RasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[HDFRasterSourceProvider])

  override def canProcess(definition: RasterSourceDefinition): Boolean = {
    definition.dataPath.contains(".hdf")
  }

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    val dataPath = definition.dataPath.replace("/vsis3/EODATA/", "/vsis3/eodata/").replace("https", "/vsicurl/https")
    logger.info(s"Creating HDFRasterSource for path: $dataPath")
    val warpOptions = GDALWarpOptions(cellSize = Some(definition.theResolution), targetCRS = Some(definition.targetExtent.crs), resampleMethod = Some(definition.resampleMethod),te = Some(definition.targetExtent.extent))
    GDALRasterSource(GDALPath(dataPath),options = warpOptions, targetCellType = definition.targetCellType)
  }
}
