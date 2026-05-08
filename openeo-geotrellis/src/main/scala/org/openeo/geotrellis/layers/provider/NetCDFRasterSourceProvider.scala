package org.openeo.geotrellis.layers.provider

import geotrellis.raster.RasterSource
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.io.geotiff.OverviewStrategy
import org.openeo.geotrellis.layers.raster_source.GDALCloudRasterSource
import org.slf4j.{Logger, LoggerFactory}

object NetCDFRasterSourceProvider extends NetCDFRasterSourceProvider

class NetCDFRasterSourceProvider extends RasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[NetCDFRasterSourceProvider])

  override def canProcess(rasterSourceDefinition: RasterSourceDefinition): Boolean = {
    val dataPath = rasterSourceDefinition.dataPath
    dataPath.contains("NETCDF:")
  }

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    val dataPath = definition.dataPath
    val warpOptionsOvr = Some(OverviewStrategy.DEFAULT)
    val alignPixels = !dataPath.contains("NETCDF:") //align target pixels does not yet work with CGLS global netcdfs
    val warpOptions = GDALWarpOptions(alignTargetPixels = alignPixels, cellSize = Some(definition.theResolution), targetCRS = Some(definition.targetExtent.crs), resampleMethod = Some(definition.resampleMethod),
      te = definition.featureExtentInLayout.map(_.extent), teCRS = Some(definition.targetExtent.crs), ovr = warpOptionsOvr
    )
    logger.debug(s"cloudpath: ${definition.cloudPath}, warp options: $warpOptions")
    if (definition.cloudPath.isDefined) {
      GDALCloudRasterSource(definition.cloudPath.get._1.replace("/vsis3", ""), vsisToHttpsCreo(definition.cloudPath.get._2), GDALPath(dataPath.replace("/vsis3", "")), options = warpOptions, targetCellType = definition.targetCellType)
    } else {
      // TODO dsamaey
      // predefinedExtent = definition.featureExtentInLayout
      GDALRasterSource(GDALPath(dataPath.replace("/vsis3/EODATA/", "/vsis3/eodata/").replace("https", "/vsicurl/https")), options = warpOptions, targetCellType = definition.targetCellType)
    }
  }

  override def usePredefinedExtent(definition: RasterSourceDefinition): Boolean = {
    definition.cloudPath.isEmpty
  }

  private def vsisToHttpsCreo(path: String): String = {
    if (path.startsWith("/vsicurl/")) path.replaceFirst("/vsicurl/", "")
    else if (path.startsWith("/vsis3/eodata/"))
      path.replaceFirst("/vsis3/eodata/", "https://finder.creodias.eu/files/")
    else if (path.startsWith("/eodata/"))
      path.replaceFirst("/eodata/", "https://zipper.creodias.eu/get-object?path=/")
    else if (path.startsWith("http")) path
    else {
      logger.warn("unexpected path: " + path)
      path
    }
  }

}
