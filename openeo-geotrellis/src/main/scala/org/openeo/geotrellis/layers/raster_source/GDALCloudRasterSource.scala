package org.openeo.geotrellis.layers.raster_source

import geotrellis.raster.TargetCellType
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.vector.{Extent, MultiPolygon, Polygon}
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._
import scala.language.postfixOps
import scala.xml.XML

object GDALCloudRasterSource {

  def apply(cloudDataPath: String, metadataPath: String, dataPath: GDALPath, options: GDALWarpOptions = GDALWarpOptions.EMPTY, targetCellType: Option[TargetCellType] = None): GDALCloudRasterSource =
    new GDALCloudRasterSource(cloudDataPath, metadataPath, dataPath, options, targetCellType)

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
      val bufferedPolygons = readCloudFile().par.map(p => new GeometryFactory(new PrecisionModel(1e8)).createGeometry(p).buffer(dilationDistance).asInstanceOf[Polygon]).toBuffer

      @tailrec
      def mergeIntersectingPolygons(polygon: Polygon): Polygon = {
        val intersectingPolygons = bufferedPolygons.filter(p => p.intersects(polygon))
        if (intersectingPolygons.isEmpty) {
          bufferedPolygons -= polygon
          return polygon
        }
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

      while (bufferedPolygons.nonEmpty) {
        mergedCloudPolygons += mergeIntersectingPolygons(bufferedPolygons.head)
      }

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