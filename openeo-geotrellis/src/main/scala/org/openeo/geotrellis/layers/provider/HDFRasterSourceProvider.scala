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
    val bandName = definition.bandName
    val title = definition.link.title.getOrElse(definition.link.href.toString)
    val band = title match {
      case "MODIS Terra Snow Cover Daily Global 500m" => s":MOD_Grid_Snow_500m:$bandName"
      case _ => s":MOD_Grid_Snow_500m:$bandName"
//      case _ => throw new NotImplementedError(s"Band name $bandName currently not supported for HDF files with title $title and datapath ${definition.dataPath}")
    }

    val dataPath = s"HDF4_EOS:EOS_GRID:${definition.dataPath.replace("/vsis3/EODATA/", "/vsis3/eodata/").replace("https", "/vsicurl/https")}$band"
    logger.info(s"Creating HDFRasterSource for path: $dataPath")
    logger.info(s"Information in the definition: ${definition.link.toString()}")
    val warpOptions = GDALWarpOptions(cellSize = Some(definition.theResolution), targetCRS = Some(definition.targetExtent.crs), resampleMethod = Some(definition.resampleMethod),te = Some(definition.targetExtent.extent))
    GDALRasterSource(GDALPath(dataPath),options = warpOptions, targetCellType = definition.targetCellType)
  }
}
