package org.openeo.geotrellis.layers

import geotrellis.raster.TargetCellType
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.vector.{Extent, Polygon}

import java.net.URL
import scala.xml.XML

class GDALCloudRasterSource(
                             val cloudDataPath: URL,
                             val metadataPath: URL,
                             override val dataPath: GDALPath,
                             override val options: GDALWarpOptions = GDALWarpOptions.EMPTY,
                             override val targetCellType: Option[TargetCellType] = None
                           ) extends GDALRasterSource(dataPath, options, targetCellType) {

  def readCloudFile(): Seq[Polygon] = {
    val xmlDoc = XML.load(cloudDataPath)
    // Cloud mask should only contain 2-dimensional points.
    val srsDimensions = (xmlDoc \\ "@srsDimension").map(_.text.toInt)
    if (srsDimensions.exists(_ != 2))
      throw new IllegalArgumentException("MSK_CLOUDS_B00 file contains points that are not 2-dimensional.")

    val posLists = xmlDoc \\ "posList"
    val pointLists = posLists.map(_.text.split(" ").map(_.toDouble).grouped(2).map(l => (l(0), l(1))).toList)
    pointLists.map(Polygon(_))
  }

  def readExtent(): Extent = {
    val xmlDoc = XML.load(metadataPath)
    val geoCoding = xmlDoc \ "Geometric_Info" \ "Tile_Geocoding"
    //val crs = CRS.fromName((geoCoding\"HORIZONTAL_CS_CODE").text)
    val position = geoCoding \ "Geoposition"  filter (va=>(va \ "@resolution" toString) == "10")
    val ulx = (position \ "ULX").text.toDouble
    val uly = (position \ "ULY").text.toDouble
    Extent(ulx,uly-(10*10980),ulx+(10*10980),uly)
  }
}

object GDALCloudRasterSource {
  def apply(cloudDataPath: URL, metadataPath: URL, dataPath: GDALPath, options: GDALWarpOptions = GDALWarpOptions.EMPTY, targetCellType: Option[TargetCellType] = None): GDALCloudRasterSource =
    new GDALCloudRasterSource(cloudDataPath, metadataPath, dataPath, options, targetCellType)
}
