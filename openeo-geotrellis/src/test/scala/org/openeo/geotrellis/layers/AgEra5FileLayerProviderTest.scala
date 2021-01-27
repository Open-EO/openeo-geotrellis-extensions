package org.openeo.geotrellis.layers

import geotrellis.proj4.LatLng
import geotrellis.spark._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Test
import org.openeo.geotrellis.{LocalSparkContext, ProjectedPolygons}
import org.openeo.geotrellis.TestImplicits._

import java.time.{LocalDate, ZoneId}

object AgEra5FileLayerProviderTest extends LocalSparkContext

class AgEra5FileLayerProviderTest {
  import AgEra5FileLayerProviderTest._

  @Test
  def agEra5FileLayerProvider(): Unit = {
    val fileLayerProvider = new AgEra5FileLayerProvider(
      dewPointTemperatureGlob = "/data/worldcereal/data/AgERA5/years/2020/20200424/AgERA5_dewpoint-temperature_*.tif",
      bandFileMarkers = Seq("dewpoint-temperature", "precipitation-flux", "solar-radiation-flux"),
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})\.tif".r
    )

    val projectedExtent = ProjectedExtent(Extent(0.0, 50.0, 5.0, 55.0), LatLng)
    val projectedPolygons = ProjectedPolygons.fromExtent(projectedExtent.extent, s"EPSG:${projectedExtent.crs.epsgCode.get}")

    val from = LocalDate.of(2020, 4, 24).atStartOfDay(ZoneId.of("UTC"))
    val to = from

    val layer = fileLayerProvider.readMultibandTileLayer(from, to, projectedPolygons, fileLayerProvider.maxZoom, sc)

    val spatialLayer = layer
      .toSpatial(from)
      .cache()

    spatialLayer
      .writeGeoTiff(s"/tmp/agEra5FileLayerProvider.tif", projectedExtent)
  }
}
