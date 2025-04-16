package org.openeo.geotrellis

import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.vector._
import org.junit.Assert._
import org.junit.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.openeo.geotrellis.ComputeStatsGeotrellisAdapterTest.{polygon1, polygon2}
import org.openeo.geotrellis.ProjectedPolygons.reprojectPolygonRefined

import scala.collection.JavaConverters._

class ProjectedPolygonsTest() {

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
    assertTrue(f"Unexpected start ${pp.polygons(0).toString}", pp.polygons(0).toString.startsWith("MULTIPOLYGON (((3.82054216728"))
    assertTrue(f"Unexpected start ${pp.polygons(1).toString}", pp.polygons(1).toString.startsWith("MULTIPOLYGON (((3.64986056759"))
    assertEquals(CRS.fromEpsgCode(4326), pp.crs)
  }

  @Test
  def areaInSquareMeters(): Unit = {
    val pp = ProjectedPolygons.fromExtent(Extent(xmin = 4.0, ymin = 51.0, xmax = 5.0, ymax = 52.0), "EPSG:4326")

    val expectedArea = 7725459381.443416
    val delta = expectedArea * 0.01

    assertEquals(expectedArea, pp.areaInSquareMeters, delta) // https://github.com/locationtech/geotrellis/issues/3289
  }

  @ParameterizedTest
  @ValueSource(strings = Array(
    "/org/openeo/geotrellis/geojson/alaska_triangle.json",
    "/org/openeo/geotrellis/geojson/bering_sea_triangle.json",
    "/org/openeo/geotrellis/geojson/europe_triangle.json",
    "/org/openeo/geotrellis/geojson/russia_triangle.json",
    "/org/openeo/geotrellis/geojson/world_extent.json",
    "/org/openeo/geotrellis/geojson/world_extent_bigger.json",
    "/org/openeo/geotrellis/geojson/zigzag_shape.json",
  ))
  def testSplitPolygonsOnWrapPoint(path: String): Unit = {
    val pp = ProjectedPolygons.fromVectorFile(getClass.getResource(path).getPath)
    val ppSplit = pp.splitPolygonsOnWrapPoint()

    // Prepare to manually inspect output in QGIS:
    dumpGeoJson(toGeoJsonDebug(ppSplit), Some(path.substring(path.lastIndexOf("/") + 1) + "_split"))

    // Test polygon validity:
    if (path != "/org/openeo/geotrellis/geojson/world_extent_bigger.json") {
      // world_extent_bigger goes larger than the world extent, so after cutting it will intersect itself
      // A workaround could be to make the ProjectedExtent, but it works like this
      ppSplit.geometries.foreach(g => assertTrue(g.isValid))
    }
    assertTrue(ppSplit.getFlatMultiPolygon.union().getArea > 0)
    println(pp.riskOfCrossingAntimeridian)
  }

  @ParameterizedTest
  @ValueSource(strings = Array(
    "/org/openeo/geotrellis/geojson/belgium_lowres.json",
    "/org/openeo/geotrellis/geojson/swiss_holes.json",
    "/org/openeo/geotrellis/geojson/bering_sea_triangle.json",
  ))
  def testReprojectPolygonWithTesslation(path: String): Unit = {
    val pp = ProjectedPolygons.fromVectorFile(getClass.getResource(path).getPath)
    val targetCrs = CRS.fromEpsgCode(32631)

    val ppReprojectedOld = safeReprojectPolygons(pp, targetCrs)
    val ppReprojected = pp.safeReproject(targetCrs, refine = true)
    assertTrue(ppReprojected.getFlatMultiPolygon.union().getArea > 0)

    // Prepare to manually inspect output in QGIS:
    dumpGeoJson(toGeoJsonDebug(ppReprojectedOld), Some(path.substring(path.lastIndexOf("/") + 1) + "_reprojectedOld_" + targetCrs))
    dumpGeoJson(toGeoJsonDebug(ppReprojected), Some(path.substring(path.lastIndexOf("/") + 1) + "_reprojected_" + targetCrs))
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
    dumpGeoJson(toGeoJsonDebug(pp), Some("testRefineToLatLngOverAntimeridian"))

    val ppReprojected = projectedPolygonWrapAntimeridian(pp.safeReproject(LatLng, refine = true))
    val pReprojected = ppReprojected.getFlatMultiPolygon
    assertTrue(pReprojected.union().getArea > 0)
    // Prepare to manually inspect output in QGIS:
    dumpGeoJson(toGeoJsonDebug(ppReprojected), Some("testRefineToLatLngOverAntimeridian_reprojected"))
    ppReprojected.splitPolygonsOnWrapPoint()  // test if error is thrown.
    // TODO: Compare with reference geojson?
  }
}
