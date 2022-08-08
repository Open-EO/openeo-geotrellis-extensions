package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.FloatingLayoutScheme

import java.time.{LocalDate, ZoneId}
import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.spark._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Test
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.{LocalSparkContext, ProjectedPolygons}
import org.openeo.opensearch.backends.Agera5SearchClient

import java.util
import scala.collection.JavaConverters.asScalaBuffer

object AgEra5FileLayerProviderTest extends LocalSparkContext

class AgEra5FileLayerProviderTest {
  import AgEra5FileLayerProviderTest._

  private val bands: util.List[String] = util.Arrays.asList("dewpoint-temperature", "precipitation-flux", "solar-radiation-flux")
  private def layerProvider2 = new FileLayerProvider(new Agera5SearchClient(dataGlob = "/data/MEP/ECMWF/AgERA5/2020/20200424/AgERA5_dewpoint-temperature_*.tif", bands, raw".+_(\d{4})(\d{2})(\d{2})\.tif".r),"",
    NonEmptyList.fromList(asScalaBuffer(bands).toList).get,
    rootPath = "/data/MEP/ECMWF/AgERA5",
    maxSpatialResolution = CellSize(0.099999998304108, 0.100000000000000),
    new Sentinel5PPathDateExtractor(maxDepth = 3),
    layoutScheme = FloatingLayoutScheme(256))

  @Test
  def agEra5FileLayerProvider(): Unit = {
    val fileLayerProvider = new AgEra5FileLayerProvider(
      dewPointTemperatureGlob = "/data/MEP/ECMWF/AgERA5/2020/20200424/AgERA5_dewpoint-temperature_*.tif",
      bandFileMarkers = Seq("dewpoint-temperature", "precipitation-flux", "solar-radiation-flux"),
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})\.tif".r
    )

    val projectedExtent = ProjectedExtent(Extent(0.0, 50.0, 5.0, 55.0), LatLng)
    val projectedPolygons = ProjectedPolygons.fromExtent(projectedExtent.extent, s"EPSG:${projectedExtent.crs.epsgCode.get}")

    val from = LocalDate.of(2020, 4, 24).atStartOfDay(ZoneId.of("UTC"))
    val to = from

    val layer = fileLayerProvider.readMultibandTileLayer(from, to, projectedPolygons, fileLayerProvider.maxZoom, sc,None)

    val spatialLayer = layer
      .toSpatial(from)
      .cache()

    spatialLayer
      .writeGeoTiff(s"/tmp/agEra5FileLayerProvider.tif", projectedExtent)
  }

  @Test
  def agEra5WithOpensearchClient(): Unit = {
    val projectedExtent = ProjectedExtent(Extent(0.0, 50.0, 5.0, 55.0), LatLng)
    val projectedPolygons = ProjectedPolygons.fromExtent(projectedExtent.extent, s"EPSG:${projectedExtent.crs.epsgCode.get}")

    val from = LocalDate.of(2020, 4, 24).atStartOfDay(ZoneId.of("UTC"))
    val to = from

    val layer = layerProvider2.readMultibandTileLayer(from, to, projectedExtent, projectedPolygons.polygons, projectedPolygons.crs, layerProvider2.maxZoom, sc, None)

    val spatialLayer = layer
      .toSpatial(from)
      .cache()

    spatialLayer
      .writeGeoTiff(s"/tmp/agEra5WithOpensearchClient.tif", projectedExtent)
  }

}
