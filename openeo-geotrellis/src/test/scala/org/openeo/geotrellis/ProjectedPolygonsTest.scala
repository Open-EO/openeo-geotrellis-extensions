package org.openeo.geotrellis

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.junit.Assert._
import org.junit.Test
import org.openeo.geotrellis.ComputeStatsGeotrellisAdapterTest.{polygon1, polygon2}

import scala.collection.JavaConverters._

class ProjectedPolygonsTest() {

  @Test
  def projected_polygons_from_wkt(): Unit = {
    val pp = ProjectedPolygons.fromWkt(List(polygon1.toWKT()).asJava, "EPSG:4326")
    assertEquals(1, pp.polygons.length)
    assertEquals(MultiPolygon(polygon1), pp.polygons(0))
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
    assertTrue(pp.polygons(0).toString.startsWith("MULTIPOLYGON (((3.820542167275136"))
    assertTrue(pp.polygons(1).toString.startsWith("MULTIPOLYGON (((3.649860567588494"))
    assertEquals(CRS.fromEpsgCode(4326), pp.crs)
  }

  @Test
  def areaInSquareMeters(): Unit = {
    val pp = ProjectedPolygons.fromExtent(Extent(xmin = 4.0, ymin = 51.0, xmax = 5.0, ymax = 52.0), "EPSG:4326")

    val expectedArea = 7725459381.443416
    val delta = expectedArea * 0.01

    assertEquals(expectedArea, pp.areaInSquareMeters, delta) // https://github.com/locationtech/geotrellis/issues/3289
  }
}
