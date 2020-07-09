package org.openeo.geotrellis

import java.net.{MalformedURLException, URL}

import _root_.io.circe.DecodingFailure
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.io.json.JsonFeatureCollection
import geotrellis.vector.{Geometry, GeometryCollection, MultiPolygon, Polygon, _}
import org.geotools.data.Query
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureIterator

import scala.collection.JavaConverters._
import scala.io.Source

case class ProjectedPolygons(polygons: Array[MultiPolygon], crs: CRS)

object ProjectedPolygons {
  private type JList[T] = java.util.List[T]

  def apply(polygons: Seq[Polygon], crs: String): ProjectedPolygons = {
    ProjectedPolygons(polygons.map(MultiPolygon(_)).toArray, CRS.fromName(crs))
  }

  def fromWkt(polygon_wkts: JList[String], polygons_srs: String): ProjectedPolygons = {
    val polygons = polygon_wkts.asScala.map(parsePolygonWkt).toArray
    val crs: CRS = CRS.fromName(polygons_srs)
    ProjectedPolygons(polygons, crs)
  }

  private def parsePolygonWkt(polygonWkt: String): MultiPolygon = {
    val geometry: Geometry = polygonWkt.parseWKT()
    geometry match {
      case multiPolygon: MultiPolygon => multiPolygon
      case _ => MultiPolygon(geometry.asInstanceOf[Polygon])
    }
  }

  def fromVectorFile(vector_file: String): ProjectedPolygons = {
    val vectorUrl = try {
      new URL(vector_file)
    } catch {
      case _: MalformedURLException => new URL(s"file://$vector_file")
    }

    val filename = vectorUrl.getPath.split("/").last

    if (filename.endsWith(".shp")) readSimpleFeatures(vectorUrl)
    else readMultiPolygonsFromGeoJson(vectorUrl)
  }

  // adapted from Geotrellis' ShapeFileReader to avoid having too much in memory
  private def readSimpleFeatures(shpUrl: URL): ProjectedPolygons = {
    val ds = new ShapefileDataStore(shpUrl)
    val featureSource = ds.getFeatureSource
    val crs = featureSource.getSchema.getCoordinateReferenceSystem
    val ftItr: SimpleFeatureIterator = featureSource.getFeatures.features

    try {
      val featureCount = ds.getCount(Query.ALL)
      require(featureCount < Int.MaxValue)

      val simpleFeatures = new Array[MultiPolygon](featureCount.toInt)

      for (i <- simpleFeatures.indices) {
        val multiPolygon = ftItr.next().getAttribute(0) match {
          case multiPolygon: MultiPolygon => multiPolygon
          case polygon: Polygon => MultiPolygon(polygon)
          case _ => MultiPolygon.EMPTY
        }

        simpleFeatures(i) = multiPolygon
      }

      val geotrellisCRS=
      if(crs == null) {
        LatLng
      }else{
        CRS.fromWKT(crs.toWKT).get
      }
      ProjectedPolygons(simpleFeatures,geotrellisCRS )
    } finally {
      ftItr.close()
      ds.dispose()
    }
  }

  private def readMultiPolygonsFromGeoJson(geoJsonUrl: URL): ProjectedPolygons = {
    // FIXME: stream it instead
    val src = Source.fromURL(geoJsonUrl)

    val multiPolygons = try {
      val geoJson = src.mkString

      def children(geometryCollection: GeometryCollection): Stream[Geometry] = {
        def from(i: Int): Stream[Geometry] =
          if (i >= geometryCollection.getNumGeometries) Stream.empty
          else geometryCollection.getGeometryN(i) #:: from(i + 1)

        from(0)
      }

      def asMultiPolygons(geometry: Geometry): Array[MultiPolygon] = geometry match {
        case polygon: Polygon => Array(MultiPolygon(polygon))
        case multiPolygon: MultiPolygon => Array(multiPolygon)
        case geometryCollection: GeometryCollection => children(geometryCollection).map {
          case polygon: Polygon => MultiPolygon(polygon)
          case multiPolygon: MultiPolygon => multiPolygon
        }.toArray
      }

      try {
        asMultiPolygons(geoJson.parseGeoJson[Geometry]())
      } catch {
        case _: DecodingFailure =>
          val featureCollection = geoJson.parseGeoJson[JsonFeatureCollection]()
          featureCollection.getAllGeometries()
            .flatMap(asMultiPolygons)
            .toArray
      }
    } finally src.close()

    ProjectedPolygons(multiPolygons, LatLng)
  }

}
