package org.openeo.geotrellis.aggregate_polygon.intern

import geotrellis.vector.{MultiPolygon, multiPolygonEncoder}
import io.circe.syntax.EncoderOps
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.ProjectedPolygons

class PackageTest {

  @Test
  def testOverlap(): Unit = {
    val geometriesPath = getClass.getResource("/org/openeo/geotrellis/aggregate_polygon/intern/ehcek2.json").getPath
    val multiPolygons: Array[MultiPolygon] = ProjectedPolygons.fromVectorFile(geometriesPath).polygons
    val mapping = splitOverlappingPolygons(multiPolygons.toSeq)
    val json = mapping._1.asJson.toString()
    val referencePath = getClass.getResource("/org/openeo/geotrellis/aggregate_polygon/intern/ehcek2_reference.json").getPath
    val referenceJson = scala.io.Source.fromFile(referencePath).mkString
    assertEquals(referenceJson, json)
  }

}
