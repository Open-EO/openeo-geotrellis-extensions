package org.openeo.geotrellissentinelhub

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import geotrellis.vector.io.json.GeoJson
import org.junit.Test

class LocalGridSparseMultiPolygonSimplificationTest {
  private val flobecq = readPolygon("/org/openeo/geotrellissentinelhub/flobecq.geojson")
  private val waterloo = readPolygon("/org/openeo/geotrellissentinelhub/waterloo.geojson")

  private val multiPolygons = Array(MultiPolygon(flobecq, waterloo))

  @Test
  def latLng(): Unit = {
    val gridCrs = LatLng
    val gridTileSize = 0.1 // degrees, roughly matches SHub 10x10k grid

    val (multiPolygon, crs) = new LocalGridSparseMultiPolygonSimplification(gridCrs, gridTileSize)
      .simplify(multiPolygons, LatLng)

    println(crs)
    println(multiPolygon.reproject(crs, LatLng).toGeoJson())
  }

  @Test
  def utm(): Unit = {
    val gridCrs = CRS.fromEpsgCode(32631)
    val gridTileSize = 10000

    val (multiPolygon, crs) = new LocalGridSparseMultiPolygonSimplification(gridCrs, gridTileSize)
      .simplify(multiPolygons, LatLng)

    println(crs)
    println(multiPolygon.reproject(crs, LatLng).toGeoJson())
  }

  private def readPolygon(geoJsonClassPathResource: String): Polygon = {
    import scala.io.Source

    val in = Source.fromInputStream(getClass.getResourceAsStream(geoJsonClassPathResource))

    try GeoJson.parse[Polygon](in.mkString)
    finally in.close()
  }
}
