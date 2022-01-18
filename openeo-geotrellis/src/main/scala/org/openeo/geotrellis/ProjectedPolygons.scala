package org.openeo.geotrellis

import _root_.io.circe.DecodingFailure
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import org.geotools.data.Query
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureIterator

import java.net.{MalformedURLException, URL}
import scala.collection.JavaConverters._
import scala.io.Source

case class ProjectedPolygons(polygons: Array[MultiPolygon], crs: CRS) {
  def areaInSquareMeters: Double = ProjectedPolygons.areaInSquareMeters(GeometryCollection(polygons), crs)

  def extent: ProjectedExtent = ProjectedExtent(polygons.toSeq.extent,crs)
}

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

  // FIXME: make this an instance method
  def reproject(projectedPolygons: ProjectedPolygons,epsg_code:Int): ProjectedPolygons = {
    val targetCRS = CRS.fromEpsgCode(epsg_code)
    reproject(projectedPolygons, targetCRS)
  }

  def reproject(projectedPolygons: ProjectedPolygons, targetCRS: CRS): ProjectedPolygons = {
    ProjectedPolygons(projectedPolygons.polygons.map {
      _.reproject(projectedPolygons.crs, targetCRS)
    }, targetCRS)
  }

  def fromExtent(extent:Extent, crs:String): ProjectedPolygons = {
    ProjectedPolygons(Array(MultiPolygon(extent.toPolygon())),CRS.fromName(crs))
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
        val identifiers = crs.getIdentifiers.asScala
        if(identifiers.isEmpty) {
          LatLng
        } else {
          val crs = identifiers.head
          CRS.fromName(s"${crs.getCodeSpace}:${crs.getCode}")
        }
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

  /**
   * TODO: we had a very strange problem in a python unit test where this method was returning different results across multiple runs.
   * This method seems top assume that the input is always in EPSG:4326
   * @param geometry
   * @param crs
   * @return
   */
  private def areaInSquareMeters(geometry: Geometry, crs: CRS): Double = {
    val bounds = geometry.extent
    val targetCrs = CRS.fromString(s"+proj=aea +lat_0=0 +lon_0=0 +lat_1=${bounds.ymin} +lat_2=${bounds.ymax} +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs")

    val reprojectedGeometry = geometry.reproject(crs, targetCrs)
    reprojectedGeometry.getArea
  }
}
