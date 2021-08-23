package org.openeo.geotrellis.layers

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.{RasterRegion, RasterSource, TargetCellType}
import geotrellis.vector.{Extent, MultiPolygon, Polygon}

import java.net.URL
import java.util
import scala.xml.XML


object GDALCloudRasterSource {

  def apply(cloudDataPath: URL, metadataPath: URL, dataPath: GDALPath, options: GDALWarpOptions = GDALWarpOptions.EMPTY, targetCellType: Option[TargetCellType] = None): GDALCloudRasterSource =
    new GDALCloudRasterSource(cloudDataPath, metadataPath, dataPath, options, targetCellType)

  def getDilationDistance(maskParams: Map[String, Object]): Double = {
    maskParams.getOrElse("dilation_distance", "250").toString.toInt
  }

  def filterRasterSources(rasterSources: Seq[RasterSource],
                          maskParams: util.Map[String, Object]): Seq[RasterSource] = {
    // TODO: Find out best default dilation distance.
    val dilationDistance =  maskParams.getOrDefault("dilation_distance", "250").toString.toInt
    // Filter out the entire BandCompositeRasterSource if it is fully clouded.
    val filteredSources = rasterSources.filter(compositeSource => {
      val rasterSource = compositeSource.asInstanceOf[BandCompositeRasterSource].sources.head
      rasterSource match {
        case rs: GDALCloudRasterSource =>
          val polygons: Seq[Polygon] = rs.readCloudFile()
          // Dilate and merge polygons.
          val bufferedPolygons = polygons.map(p => MultiPolygon(p.buffer(dilationDistance).asInstanceOf[Polygon]))
          val cloudPolygon: MultiPolygon = bufferedPolygons.reduce(
            (p1,p2) => if (p1 intersects p2) {
              p1.union(p2) match {
                case x: MultiPolygon => x
                case x: Polygon => MultiPolygon(x)
              }
            } else p1)
          // Filter out rasterSources that are fully clouded.
          !cloudPolygon.covers(rs.readExtent())
        case _ => true // Keep raster sources that have no cloud data.
      }
    })
    if (filteredSources.isEmpty) throw new IllegalArgumentException("No non-clouded raster sources found")
    filteredSources
  }

  def filterRasterRegions(rasterRegions: Iterator[(SpaceTimeKey, RasterRegion)],
                          source: RasterSource,
                          layoutCrs: CRS,
                          layout: LayoutDefinition,
                          maskParams: Map[String, Object]
                         ): Iterator[(SpaceTimeKey, RasterRegion)] = {
    val dilationDistance = getDilationDistance(maskParams)
    rasterRegions.filter({ case (key, rasterRegion) =>
      source match {
        case rs: GDALCloudRasterSource =>
          val regionExtent = key.spatialKey.extent(layout).reproject(layoutCrs, rs.getCloudCrs())
          val polygons: Seq[Polygon] = rs.readCloudFile().filter(p => regionExtent.covers(p))
          if (polygons.nonEmpty) {
            // Dilate and merge polygons.
            val t = MultiPolygon(polygons.head)
            val bufferedPolygons: Seq[MultiPolygon] = polygons.map(p => MultiPolygon(p.buffer(dilationDistance).asInstanceOf[Polygon]))
            val cloudPolygon: MultiPolygon = bufferedPolygons.reduce(
              (p1,p2) => if (p1 intersects p2) {
                p1.union(p2) match {
                  case x: MultiPolygon => x
                  case x: Polygon => MultiPolygon(x)
                }
              } else p1)

            // Filter out regions that are fully clouded.
            !cloudPolygon.covers(regionExtent)
          } else true
        case _ => true // Keep regions that have no cloud data.
      }
    })
  }
}

class GDALCloudRasterSource(
                             val cloudDataPath: URL,
                             val metadataPath: URL,
                             override val dataPath: GDALPath,
                             override val options: GDALWarpOptions = GDALWarpOptions.EMPTY,
                             override val targetCellType: Option[TargetCellType] = None
                           ) extends GDALRasterSource(dataPath, options, targetCellType) {

  private var polygons: Option[Seq[Polygon]] = Option.empty
  private var cloudCrs: Option[CRS] = Option.empty

  def readCloudFile(): Seq[Polygon] = {
    if (polygons.isEmpty) {
      val xmlDoc = XML.load(cloudDataPath)
      // Cloud mask should only contain 2-dimensional points.
      val srsDimensions = (xmlDoc \\ "@srsDimension").map(_.text.toInt)
      if (srsDimensions.exists(_ != 2))
        throw new IllegalArgumentException("MSK_CLOUDS_B00 file contains points that are not 2-dimensional.")

      val posLists = xmlDoc \\ "posList"
      val pointLists = posLists.map(_.text.split(" ").map(_.toDouble).grouped(2).map(l => (l(0), l(1))).toList)
      polygons = Some(pointLists.map(Polygon(_)))
    }
    polygons.get
  }

  def readExtent(): Extent = {
    val xmlDoc = XML.load(metadataPath)
    val geoCoding = xmlDoc \ "Geometric_Info" \ "Tile_Geocoding"
    cloudCrs = Some(CRS.fromName((geoCoding\"HORIZONTAL_CS_CODE").text))
    val position = geoCoding \ "Geoposition"  filter (va=>(va \ "@resolution" toString) == "10")
    val ulx = (position \ "ULX").text.toDouble
    val uly = (position \ "ULY").text.toDouble
    Extent(ulx,uly-(10*10980),ulx+(10*10980),uly)
  }

  def getCloudCrs(): CRS = {
    if (cloudCrs.isEmpty) {
      val xmlDoc = XML.load(metadataPath)
      val geoCoding = xmlDoc \ "Geometric_Info" \ "Tile_Geocoding"
      cloudCrs = Some(CRS.fromName((geoCoding\"HORIZONTAL_CS_CODE").text))
    }
    cloudCrs.get
  }
}