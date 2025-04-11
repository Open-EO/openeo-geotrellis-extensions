package org.openeo.geotrellis

import _root_.io.circe.DecodingFailure
import _root_.io.circe.HCursor
import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.vector._
import geotrellis.vector.io.json.{JsonCRS, JsonFeatureCollection, NamedCRS}
import org.geotools.api.data.Query
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureIterator

import java.net.{MalformedURLException, URL}
import scala.collection.JavaConverters._
import scala.io.Source

case class ProjectedPolygons(geometries: Array[Geometry], crs: CRS) {
  import ProjectedPolygons._
  def areaInSquareMeters: Double ={
    if(polygons.nonEmpty) {
      ProjectedPolygons.areaInSquareMeters(GeometryCollection(polygons), crs)
    }else{
      0.0
    }

  }

  def polygons: Array[MultiPolygon] = geometries.filter(_.isInstanceOf[MultiPolygon]).map(_.asInstanceOf[MultiPolygon])

  def getFlatMultiPolygon: MultiPolygon = {
    MultiPolygon(geometries.flatMap {
      case multiPolygon: MultiPolygon => multiPolygon.polygons
      case polygon: Polygon => Array(polygon)
      case _ => Array.empty[Polygon] // ignore non polygon
    })
  }

  def splitPolygonsOnWrapPoint(): ProjectedPolygons = {
    // TODO: Support WebMercator, Sinusoidal and any other CRSes that go around the world
    if (this.crs != LatLng) return this.copy()
    val centerPolygon = Extent(-180, -90 * 100, 180, 90 * 100).toPolygon()


    val newGeometries = geometries.map {
      case multiPolygon: MultiPolygon =>
        MultiPolygon(multiPolygon.polygons
          .map(splitGeometry(_, centerPolygon))
          .flatMap(_.polygons)
          .map(polygon_to_min180_180_range)
        )
      case polygon: Polygon =>
        val multiPolygon = splitGeometry(polygon, centerPolygon)
        MultiPolygon(multiPolygon.polygons.map(polygon_to_min180_180_range))
      case point: Point =>
        // Here just to make case return Geometry instead of MultiPolygon
        // I did not encounter a Point here yet.
        Point(to_min180_180_range(point.x), point.y)
      case geom =>
        throw new IllegalArgumentException("Unsupported geometry type: " + geom.getClass)
    }
    this.copy(geometries = newGeometries)
  }

  def extent: ProjectedExtent = ProjectedExtent(polygons.toSeq.extent,crs)
  def reproject(crs: CRS): ProjectedPolygons = ProjectedPolygons.reproject(this, crs)
}

object ProjectedPolygons {
  private type JList[T] = java.util.List[T]

  def apply(pe: ProjectedExtent): ProjectedPolygons = {
    // Wrap in MultiPolygon for backwards compatibility
    new ProjectedPolygons(Array(MultiPolygon(pe.extent.toPolygon())), pe.crs)
  }

  def apply(geometry: Geometry, crs: CRS): ProjectedPolygons = {
    new ProjectedPolygons(Array(geometry), crs)
  }

  def apply(polygons: Array[MultiPolygon], crs: CRS): ProjectedPolygons = {
    ProjectedPolygons(polygons.toArray[Geometry], crs)
  }

  def apply(polygons: Seq[Polygon], crs: String): ProjectedPolygons = {
    ProjectedPolygons(polygons.map(MultiPolygon(_)).toArray[Geometry], CRS.fromName(crs))
  }

  def fromWkt(polygon_wkts: JList[String], polygons_srs: String): ProjectedPolygons = {
    val polygons = polygon_wkts.asScala.map(parsePolygonWkt).toArray[Geometry]
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

  def reproject(projectedPolygons: ProjectedPolygons,epsg_code:Int): ProjectedPolygons = {
    val targetCRS = CRS.fromEpsgCode(epsg_code)
    reproject(projectedPolygons, targetCRS)
  }

  def reproject(projectedPolygons: ProjectedPolygons, targetCRS: CRS): ProjectedPolygons = {
    ProjectedPolygons(projectedPolygons.geometries.map {
      _.reproject(projectedPolygons.crs, targetCRS)
    }, targetCRS)
  }

  def fromExtent(extent:Extent, crs:String): ProjectedPolygons = {
    ProjectedPolygons(Array[Geometry](MultiPolygon(extent.toPolygon())),CRS.fromName(crs))
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

      val simpleFeatures = new Array[Geometry](featureCount.toInt)

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

      def asMultiPolygons(geometry: Geometry): Array[Geometry] = geometry match {
        case point: Point => Array(point)
        case polygon: Polygon => Array(MultiPolygon(polygon))
        case multiPolygon: MultiPolygon => Array(multiPolygon)
        case geometryCollection: GeometryCollection => children(geometryCollection).map {
          case polygon: Polygon => MultiPolygon(polygon)
          case multiPolygon: MultiPolygon => multiPolygon
        }.toArray
      }

      val cursor: HCursor = geoJson.stripMargin.parseJson.hcursor
      val crs: CRS = cursor.downField("crs").as[JsonCRS].getOrElse(NamedCRS("EPSG:4326")).toCRS.getOrElse(LatLng)
      var polygons: Array[Geometry] = Array.empty
      try {
        polygons = asMultiPolygons(geoJson.parseGeoJson[Geometry]())
      } catch {
        case _: DecodingFailure =>
          val featureCollection = geoJson.parseGeoJson[JsonFeatureCollection]()
          polygons = featureCollection.getAll[Geometry]
            .flatMap(asMultiPolygons)
            .toArray
      }
      ProjectedPolygons(polygons, crs)
    } finally src.close()

    multiPolygons
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

  def polygon_to_min180_180_range(p: Polygon): Polygon = {
    // Documentation says CoordinateSequenceFilter should be used, but that has a complex interface
    val clearlyWesternHemisphere = p.getCoordinates.exists(c => (c.x < 0 && c.x > -180) || (c.x > +180 && c.x < +360))
    val clearlyEasternHemisphere = p.getCoordinates.exists(c => (c.x > 0 && c.x < +180) || (c.x < -180 && c.x > -360))
    p.getCoordinates.foreach(c => {
      var newX = to_min180_180_range(c.x)
      // Solve ambiguous coordinates when we know. Otherwise keep them as they where
      if (clearlyWesternHemisphere && newX == 180) newX = -180
      if (clearlyEasternHemisphere && newX == -180) newX = 180
      c.x = newX
    })
    p
  }

  def splitGeometry(inputPolygon: Polygon, intersector: Polygon): MultiPolygon = {
    val intersect = inputPolygon.intersection(intersector) match {
      case multiPolygon: MultiPolygon => multiPolygon.polygons.filter(!_.isEmpty)
      case polygon: Polygon => Array(polygon).filter(!_.isEmpty)
    }
    val difference = inputPolygon.difference(intersector) match {
      case multiPolygon: MultiPolygon => multiPolygon.polygons.filter(!_.isEmpty)
      case polygon: Polygon => Array(polygon).filter(!_.isEmpty)
    }
    MultiPolygon(intersect ++ difference)
  }

  /**
   * Inspired on:
   * https://github.com/pomadchin/geotrellis/blob/b071b33/vector/src/main/scala/geotrellis/vector/reproject/Reproject.scala#L94
   */
  def reprojectPolygonRefined(polygon: Polygon, transform: Transform, relError: Double): Polygon = {
    import math.{abs, pow, sqrt}

    def refine(p0: (Point, (Double, Double)), p1: (Point, (Double, Double))): List[(Point, (Double, Double))] = {
      val ((a, (x0, y0)), (b, (x1, y1))) = (p0, p1)
      val m = Point(0.5 * (a.x + b.x), 0.5 * (a.y + b.y))
      val (x2, y2) = transform(m.x, m.y)

      val deflect = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1) / sqrt(pow(y2 - y1, 2) + pow(x2 - x1, 2))
      val length = sqrt(pow(x0 - x1, 2) + pow(y0 - y1, 2))

      val p2 = m -> (x2, y2)
      if (java.lang.Double.isNaN(deflect)) {
        throw new IllegalArgumentException(s"Encountered NaN during a refinement step: ($deflect / $length). Input $polygon is likely not in source projection.")
      } else if (deflect / length < relError) {
        List(p0, p2)  // TODO: Is this correct?
      } else {
        refine(p0, p2) ++ (p2 :: refine(p2, p1))
      }
    }

    if (polygon.getNumInteriorRing > 0) {
      throw new IllegalArgumentException("Interior rings are not supported yet.")
    }
    val shell = polygon.getExteriorRing // TODO: interior rings too!
    val pts = shell.getCoordinates.map(p => Point(p.x, p.y))
      .map { p => (p, transform(p.x, p.y)) }
    val refined = pts.sliding(2).flatMap { case Array(p0, p1) => refine(p0, p1) }.toList ++ List(pts(0))
    Polygon(refined.map { case (_, (x, y)) => Point(x, y) })
  }

  def reprojectGeometryRefined(geom: Geometry, transform: Transform, relError: Double): Geometry = {
    geom match {
      case polygon: Polygon => reprojectPolygonRefined(polygon, transform, 0.001)
      case multiPolygon: MultiPolygon =>
        MultiPolygon(multiPolygon.polygons.map(reprojectPolygonRefined(_, transform, 0.001)))
      case geometry: Geometry =>
        // logger.info("Was only expecting (Multi)Polygon, but got: " + geometry)
        geometry
    }
  }
}
