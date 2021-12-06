package org.openeo.geotrellis.layers

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.{RasterRegion, RasterSource, TargetCellType}
import geotrellis.vector.{Extent, MultiPolygon, Polygon}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.xml.XML

object GDALCloudRasterSource {

  def apply(cloudDataPath: String, metadataPath: String, dataPath: GDALPath, options: GDALWarpOptions = GDALWarpOptions.EMPTY, targetCellType: Option[TargetCellType] = None): GDALCloudRasterSource =
    new GDALCloudRasterSource(cloudDataPath, metadataPath, dataPath, options, targetCellType)

  def getDilationDistance(maskParams: Map[String, Object]): Int = {
    // TODO: Find out best default dilation distance.
    maskParams.getOrElse("dilation_distance", "250").toString.toInt
  }

  def filterRasterSources(rasterSources: Seq[RasterSource],
                          maskParams: util.Map[String, Object]): Seq[RasterSource] = {
    val dilationDistance =  getDilationDistance(maskParams.asScala.toMap)
    // Filter out the entire BandCompositeRasterSource if it is fully clouded.
    val filteredSources = rasterSources.filter(compositeSource => {
      val rasterSource = compositeSource.asInstanceOf[BandCompositeRasterSource].sources.head
      rasterSource match {
        case rs: GDALCloudRasterSource =>
          // Filter out rasterSources that are fully clouded.
          !rs.getMergedPolygons(dilationDistance).exists(_.covers(rs.readExtent()))
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
    val filteredRegions = rasterRegions.filter({ case (key, rasterRegion) =>
      source match {
        case rs: GDALCloudRasterSource =>
          val regionExtent = key.spatialKey.extent(layout).reproject(layoutCrs, rs.crs)
          // Filter out regions that are fully clouded.
          !rs.getMergedPolygons(dilationDistance).exists(_.covers(regionExtent))
        case _ => true // Keep regions that have no cloud data.
      }
    })
    if (filteredRegions.isEmpty) throw new IllegalArgumentException("No non-clouded raster regions found")
    filteredRegions
  }

  def isRegionFullyClouded(rasterRegion: RasterRegion, layoutCrs: CRS, layout: LayoutDefinition, dilationDistance: Int): Boolean = {
    val source = rasterRegion.asInstanceOf[GridBoundsRasterRegion].source.asInstanceOf[BandCompositeRasterSource].sources.head.asInstanceOf[GDALCloudRasterSource]
    source match {
      case rs: GDALCloudRasterSource =>
        val regionExtent = rasterRegion.extent.reproject(layoutCrs, rs.crs)
        // Filter out regions that are fully clouded.
        rs.getMergedPolygons(dilationDistance).exists(_.covers(regionExtent))
      case _ => false // Keep regions that have no cloud data.
    }
  }
}

class GDALCloudRasterSource(
                             val cloudDataPath: String,
                             val metadataPath: String,
                             override val dataPath: GDALPath,
                             override val options: GDALWarpOptions = GDALWarpOptions.EMPTY,
                             override val targetCellType: Option[TargetCellType] = None
                           ) extends GDALRasterSource(dataPath, options, targetCellType) {

  private var cloudPolygons: Option[Seq[Polygon]] = Option.empty
  private val mergedCloudPolygons: mutable.Buffer[Polygon] = mutable.Buffer[Polygon]()

  def readCloudFile(): Seq[Polygon] = {
    if (cloudPolygons.isEmpty) {
      val xmlDoc = XML.load(cloudDataPath)
      // Cloud mask should only contain 2-dimensional points.
      val srsDimensions = (xmlDoc \\ "@srsDimension").map(_.text.toInt)
      if (srsDimensions.exists(_ != 2))
        throw new IllegalArgumentException("MSK_CLOUDS_B00 file contains points that are not 2-dimensional.")

      val posLists = xmlDoc \\ "posList"
      val pointLists = posLists.map(_.text.split(" ").map(_.toDouble).grouped(2).map(l => (l(0), l(1))).toList)
      cloudPolygons = Some(pointLists.map(Polygon(_)))
    }
    cloudPolygons.get
  }

  def getMergedPolygons(dilationDistance: Double): Seq[Polygon] = {
    if (mergedCloudPolygons.isEmpty && readCloudFile().nonEmpty) {
      // Dilate and merge polygons.
      val bufferedPolygons = readCloudFile().map(p => p.buffer(dilationDistance).asInstanceOf[Polygon]).toBuffer

      def mergeIntersectingPolygons(polygon: Polygon): Polygon = {
        val intersectingPolygons = bufferedPolygons.filter(p => p.intersects(polygon))
        if (intersectingPolygons.isEmpty) return polygon
        bufferedPolygons --= intersectingPolygons
        var mergedPolygon: Polygon = polygon
        for (iP <- intersectingPolygons) {
          mergedPolygon = mergedPolygon.union(iP) match {
            case _: MultiPolygon => throw new Exception("Intersecting polygons do not merge into single polygon.")
            case x: Polygon => x
          }
        }
        mergeIntersectingPolygons(mergedPolygon)
      }
      for (bp <- bufferedPolygons) { if (bp != null) mergedCloudPolygons += mergeIntersectingPolygons(bp)}

      // Delete polygons to save memory.
      cloudPolygons = Option.empty
    }
    mergedCloudPolygons.toVector
  }

  def readExtent(): Extent = {
    val xmlDoc = XML.load(metadataPath)
    val geoCoding = xmlDoc \ "Geometric_Info" \ "Tile_Geocoding"
    val position = geoCoding \ "Geoposition"  filter (va=>(va \ "@resolution" toString) == "10")
    val ulx = (position \ "ULX").text.toDouble
    val uly = (position \ "ULY").text.toDouble
    Extent(ulx,uly-(10*10980),ulx+(10*10980),uly)
  }
}