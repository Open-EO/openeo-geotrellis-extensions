package org.openeo.geotrellis

import better.files.File.apply
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.openeo.geotrellis.ComputeStatsGeotrellisAdapterTest.{polygon1, polygon2}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

object ProjectedPolygonsTest {
  val outDir: java.nio.file.Path = Paths.get("tmp/ProjectedPolygonsTest/")

  @BeforeAll
  def setUpSpark_BeforeAll(): Unit = {
    Files.createDirectories(outDir)
  }
}

class ProjectedPolygonsTest {
  import ProjectedPolygonsTest._

  @Test
  def projected_polygons_from_wkt(): Unit = {
    val pp = ProjectedPolygons.fromWkt(List(polygon1.toWKT()).asJava, "EPSG:4326")
    assertEquals(1, pp.polygons.length)
    assertTrue(MultiPolygon(polygon1).equalsExact( pp.polygons(0),0.00000001))
    assertEquals(CRS.fromEpsgCode(4326), pp.crs)
  }

  @Test
  def projected_polygons_from_vector_file(): Unit = {
    val pp = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/GeometryCollection.json").getPath)
    assertEquals(2, pp.polygons.length)
    assertEquals(MultiPolygon(polygon1), pp.polygons(0))
    assertEquals(MultiPolygon(polygon2), pp.polygons(1))
    assertEquals(CRS.fromEpsgCode(4326), pp.crs)
  }

  @Test
  def projected_polygons_from_vector_file_mixed_polygons(): Unit = {
    val pp = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/test_MVP_2fields.geojson").getPath)
    assertEquals(2, pp.polygons.length)
    assertTrue(pp.polygons(0).toString.startsWith("MULTIPOLYGON (((3.82054216728"), f"Unexpected start ${pp.polygons(0).toString}")
    assertTrue(pp.polygons(1).toString.startsWith("MULTIPOLYGON (((3.64986056759"), f"Unexpected start ${pp.polygons(1).toString}")
    assertEquals(CRS.fromEpsgCode(4326), pp.crs)
  }

  @Test
  def areaInSquareMeters(): Unit = {
    val pp = ProjectedPolygons.fromExtent(Extent(xmin = 4.0, ymin = 51.0, xmax = 5.0, ymax = 52.0), "EPSG:4326")

    val expectedArea = 7725459381.443416
    val delta = expectedArea * 0.01

    assertEquals(expectedArea, pp.areaInSquareMeters, delta) // https://github.com/locationtech/geotrellis/issues/3289
  }

  @Test
  def areaInSquareMetersChangeStandardParallels(): Unit = {
    // Drastically changing the bounding box with empty polygons changes the
    // standard parallels used to calculate the area, but this has no significant influence on the calculations
    val pp = ProjectedPolygons(MultiPolygon(
      Extent(xmin = 4.0, ymin = 51.0, xmax = 5.0, ymax = 52.0).toPolygon(),
      Extent(xmin = 1.0, ymin = 70.0, xmax = 1.0, ymax = 70.0).toPolygon(),
      Extent(xmin = 170.0, ymin = -60.0, xmax = 170.0, ymax = -60.0).toPolygon(),
    ), CRS.fromEpsgCode(4326))

    // Same area as in the previous test:
    val expectedArea = 7725459381.443416
    val delta = expectedArea * 0.0001

    assertEquals(expectedArea, pp.areaInSquareMeters, delta)
  }

  @Test
  def areaInSquareMetersUnclosedExterior(): Unit = {
    // Source is in EPSG:32634. For calculating the area, it got first reprojected to another CRS using
    // the bounding box of all polygons. This bounding box needed to be reprojected to LatLng first.
    val pp = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/geojson/areaInSquareMetersUnclosedExterior.geojson").getPath)
    assertTrue(pp.getFlatMultiPolygon.isValid)

    val expectedArea = 819778.71
    val delta = expectedArea * 0.01

    assertEquals(expectedArea, pp.areaInSquareMeters, delta)
  }

  def outlineEquals(a: MultiPolygon, b: MultiPolygon, threshold: Double): Boolean = {
    if (a.getNumGeometries != b.getNumGeometries) {
      return false
    }
    for (i <- a.polygons.indices) {
      val aGeom = a.polygons(i)
      val bGeom = b.polygons(i)
      if (!outlineEqualsPolygon(aGeom, bGeom, threshold)) {
        return false
      }
    }
    true
  }

  def outlineEqualsPolygon(a: Polygon, b: Polygon, threshold: Double): Boolean = {
    def outlineEqualsToReferencePolygon(a2: Polygon, b2: Polygon): Boolean = {
      b2.getCoordinates.forall(c => {
        val point = Point(c.getX, c.getY)
        a2.distance(point) < threshold // could also calculate MSE?
      })
    }

    outlineEqualsToReferencePolygon(a, b) && outlineEqualsToReferencePolygon(b, a)
  }


  @ParameterizedTest
  @ValueSource(strings = Array(
    "/org/openeo/geotrellis/geojson/inputShapes/alaska_triangle.json",
    "/org/openeo/geotrellis/geojson/inputShapes/bering_sea_triangle.json",
    "/org/openeo/geotrellis/geojson/inputShapes/europe_triangle.json", // Should not get split
    "/org/openeo/geotrellis/geojson/inputShapes/russia_triangle.json",
    "/org/openeo/geotrellis/geojson/inputShapes/world_extent.json",
    // "/org/openeo/geotrellis/geojson/inputShapes/world_extent_0_360.json", // Not yet supported, but did not encounter anywhere too.
    "/org/openeo/geotrellis/geojson/inputShapes/world_extent_bigger.json",
    "/org/openeo/geotrellis/geojson/inputShapes/zigzag_shape.json",
  ))
  def testSplitPolygonsOnWrapPoint(path: String): Unit = {
    val pp = ProjectedPolygons.fromVectorFile(getClass.getResource(path).getPath)
    println(pp.riskOfCrossingAntimeridian)
    val ppSplit = pp.splitPolygonsOnWrapPoint()

    val filename = path.substring(path.lastIndexOf("/") + 1) + "_split.geojson"
    // Prepare to manually inspect output in QGIS:
    dumpGeoJson(toGeoJsonDebug(ppSplit), Some(outDir + "/" + filename))

    if (path != "/org/openeo/geotrellis/geojson/inputShapes/world_extent_bigger.json") {
      // world_extent_bigger goes larger than the world extent, so after cutting it will intersect itself
      // A workaround could be to make the ProjectedExtent, but it works like this
      ppSplit.geometries.foreach(g => assertTrue(g.isValid))
    }
    assertTrue(ppSplit.getFlatMultiPolygon.union().getArea > 0)

    val referencePath = getClass.getResource("/org/openeo/geotrellis/geojson/testSplitPolygonsOnWrapPointReference/" + filename)
    val reference = ProjectedPolygons.fromVectorFile(referencePath.getPath)
    assertTrue(outlineEquals(reference.getFlatMultiPolygon, ppSplit.getFlatMultiPolygon, 0.001))
  }

  @ParameterizedTest
  @ValueSource(strings = Array(
    "/org/openeo/geotrellis/geojson/inputShapes/belgium_lowres.json",
    "/org/openeo/geotrellis/geojson/inputShapes/swiss_holes.json",
    "/org/openeo/geotrellis/geojson/inputShapes/bering_sea_triangle.json",
  ))
  def testReprojectPolygonWithRefine(path: String): Unit = {
    val start = System.currentTimeMillis()
    val pp = ProjectedPolygons.fromVectorFile(getClass.getResource(path).getPath)
    val targetCrs = CRS.fromEpsgCode(32631)

    val ppReprojected = pp.safeReproject(targetCrs, refine = true)
    assertTrue(ppReprojected.getFlatMultiPolygon.union().getArea > 0)

    val filename = path.substring(path.lastIndexOf("/") + 1) + "_reprojected_" + targetCrs.toString.replace(":", "") + ".geojson"
    val referencePath = getClass.getResource("/org/openeo/geotrellis/geojson/testReprojectPolygonWithRefineReference/" + filename)
    val reference = ProjectedPolygons.fromVectorFile(referencePath.getPath)

    // Prepare to manually inspect output in QGIS:
    dumpGeoJson(toGeoJsonDebug(ppReprojected), Some(outDir / path.substring(path.lastIndexOf("/") + 1) + "_reprojected_" + targetCrs))
    assertTrue(outlineEquals(reference.getFlatMultiPolygon, ppReprojected.getFlatMultiPolygon, 0.1))

    val end = System.currentTimeMillis()
    println(s"Time elapsed: ${end - start} ms")
  }

  @Test
  def testRefineToLatLngOverAntimeridian(): Unit = {
    val pointBegin = Point(611000, 7666000)
    val pp = ProjectedPolygons(Polygon(LineString(Seq(
      pointBegin,
      Point(599000, 7801000),
      Point(728000, 7751000),
      pointBegin,
    ))), CRS.fromName("EPSG:32660"))
    dumpGeoJson(toGeoJsonDebug(pp), Some(outDir + "/testRefineToLatLngOverAntimeridian"))

    val ppReprojected = projectedPolygonWrapAntimeridian(pp.safeReproject(LatLng, refine = true))
    val pReprojected = ppReprojected.getFlatMultiPolygon
    assertTrue(pReprojected.union().getArea > 0)
    // Prepare to manually inspect output in QGIS:
    val filename = "testRefineToLatLngOverAntimeridian_reprojected.geojson"

    dumpGeoJson(toGeoJsonDebug(ppReprojected), Some(outDir + "/" + filename))
    ppReprojected.splitPolygonsOnWrapPoint()  // test if error is thrown.

    val referencePath = getClass.getResource("/org/openeo/geotrellis/geojson/testRefineToLatLngOverAntimeridianReference/" + filename)
    val reference = ProjectedPolygons.fromVectorFile(referencePath.getPath)
    assertTrue(outlineEquals(reference.getFlatMultiPolygon, ppReprojected.getFlatMultiPolygon, 0.001))
  }
}
