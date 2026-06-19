package org.openeo.geotrellis.layers.provider

import geotrellis.raster.RasterSource
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
    val collectionId = definition.feature.collectionId
    val band = collectionId match {
      case "modis-terra-mod10a1" => s":MOD_Grid_Snow_500m:$bandName"
      case "modis-aqua-myd09a1" => s":MOD_Grid_500m_Surface_Reflectance:$bandName"
      case "modis-aqua-myd09q1" => s":MOD_Grid_250m_Surface_Reflectance:$bandName"
      case "modis-aqua-myd10a1" => s":MOD_Grid_Snow_500m:$bandName"
      case "modis-aqua-myd10a2" => s":MOD_Grid_Snow_500m:$bandName"
      case "modis-aqua-myd11a1" => s":MODIS_Grid_Daily_1km_LST:$bandName"
      case "modis-aqua-myd11a2" => s":MODIS_Grid_8Day_1km_LST:$bandName"
      case "modis-aqua-myd13a1" => s":MODIS_Grid_16DAY_500m_VI:$bandName"
      case "modis-aqua-myd13a2" => s":MODIS_Grid_16DAY_1km_VI:$bandName"
      case "modis-aqua-myd13q1" => s":MODIS_Grid_16DAY_250m_500m_VI:$bandName"
      case "modis-aqua-myd14a1" => s":MODIS_Grid_Daily_Fire:$bandName"
      case "modis-aqua-myd14a2" => s":MODIS_Grid_8Day_Fire:$bandName"
      case "modis-aqua-myd15a2h" => s":MOD_Grid_MOD15A2H:$bandName"
      case "modis-aqua-myd17a2h" => s":MOD_Grid_MOD17A2H:$bandName"
      case "modis-aqua-myd21a2" => s":MODIS_Grid_8Day_1km_LST21:$bandName"
      case "modis-terra-mod09a1" => s":MOD_Grid_500m_Surface_Reflectance:$bandName"
      case "modis-terra-mod09q1" => s":MOD_Grid_250m_Surface_Reflectance:$bandName"
      case "modis-terra-mod10a1" => s":MOD_Grid_Snow_500m:$bandName"
      case "modis-terra-mod10a2" => s":MOD_Grid_Snow_500m:$bandName"
      case "modis-terra-mod11a1" => s":MODIS_Grid_Daily_1km_LST:$bandName"
      case "modis-terra-mod11a2" => s":MODIS_Grid_8Day_1km_LST:$bandName"
      case "modis-terra-mod13a1" => s":MODIS_Grid_16DAY_500m_VI:$bandName"
      case "modis-terra-mod13a2" => s":MODIS_Grid_16DAY_1km_VI:$bandName"
      case "modis-terra-mod13q1" => s":MODIS_Grid_16DAY_250m_500m_VI:$bandName"
      case "modis-terra-mod14a1" => s":MODIS_Grid_Daily_Fire:$bandName"
      case "modis-terra-mod14a2" => s":MODIS_Grid_8Day_Fire:$bandName"
      case "modis-terra-mod15a2h" => s":MOD_Grid_MOD15A2H:$bandName"
      case "modis-terra-mod16a2gf" => s":MOD_Grid_MOD16A2:$bandName"
      case "modis-terra-mod17a2h" => s":MOD_Grid_MOD17A2H:$bandName"
      case "modis-terra-mod21a2" => s":MODIS_Grid_8Day_1km_LST21:$bandName"
      case "modis-terraaqua-mcd15a2h" => s":MOD_Grid_MOD15A2H:$bandName"
      case "modis-terraaqua-mcd15a3h" => s":MOD_Grid_MCD15A3H:$bandName"
      case "modis-terraaqua-mcd43a4" => s":MOD_Grid_BRDF:$bandName"
      case "modis-terraaqua-mcd64a1" => s":MOD_Grid_Monthly_500m_DB_BA:$bandName"
      case _ => throw new NotImplementedError(s"HDFRasterSource: Collection with collection id $collectionId is currently not supported for HDF files with data path ${definition.dataPath}")
    }

    val dataPath = s"HDF4_EOS:EOS_GRID:${definition.dataPath.replace("/vsis3/EODATA/", "/vsis3/eodata/").replace("https", "/vsicurl/https")}$band"
    val warpOptions = GDALWarpOptions(cellSize = Some(definition.theResolution), targetCRS = Some(definition.targetExtent.crs), resampleMethod = Some(definition.resampleMethod),te = Some(definition.targetExtent.extent))
    GDALRasterSource(GDALPath(dataPath),options = warpOptions, targetCellType = definition.targetCellType)
  }
}
