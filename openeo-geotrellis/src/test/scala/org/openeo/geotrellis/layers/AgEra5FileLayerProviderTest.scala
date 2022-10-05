package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.FloatingLayoutScheme
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, FloatConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert._
import org.junit.Test
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.{LocalSparkContext, ProjectedPolygons}
import org.openeo.opensearch.backends.Agera5SearchClient

import java.time.{LocalDate, ZoneId}
import java.util
import scala.collection.JavaConverters.asScalaBuffer

object AgEra5FileLayerProviderTest extends LocalSparkContext

class AgEra5FileLayerProviderTest {
  import AgEra5FileLayerProviderTest._

  private val bands: util.List[String] = util.Arrays.asList("dewpoint-temperature", "precipitation-flux", "solar-radiation-flux")

  private def layerProvider = new FileLayerProvider(new Agera5SearchClient(dataGlob = "/data/MEP/ECMWF/AgERA5/2020/20200424/AgERA5_dewpoint-temperature_*.tif", bands, raw".+_(\d{4})(\d{2})(\d{2})\.tif".r),"",
    NonEmptyList.fromList(asScalaBuffer(bands).toList).get,
    rootPath = "/data/MEP/ECMWF/AgERA5",
    maxSpatialResolution = CellSize(0.1, 0.1),
    new Sentinel5PPathDateExtractor(maxDepth = 3),
    layoutScheme = FloatingLayoutScheme(256), experimental = false)

  private def layerProvider2 = new FileLayerProvider(new Agera5SearchClient(dataGlob = "/data/MEP/ECMWF/AgERA5/2020/20200424/AgERA5_dewpoint-temperature_*.tif", bands, raw".+_(\d{4})(\d{2})(\d{2})\.tif".r),"",
    NonEmptyList.fromList(asScalaBuffer(bands).toList).get,
    rootPath = "/data/MEP/ECMWF/AgERA5",
    maxSpatialResolution = CellSize(3000, 3000),
    new Sentinel5PPathDateExtractor(maxDepth = 3),
    layoutScheme = FloatingLayoutScheme(256), experimental = false)

  private val extent: Extent = Extent(0.0, 50.0, 5.0, 55.0)


  @Test
  def agEra5FileLayerProvider(): Unit = {

    val projectedExtent = ProjectedExtent(extent, LatLng)
    val projectedPolygons = ProjectedPolygons.fromExtent(projectedExtent.extent, s"EPSG:${projectedExtent.crs.epsgCode.get}")

    val from = LocalDate.of(2020, 4, 24).atStartOfDay(ZoneId.of("UTC"))
    val to = from

    val layer = layerProvider.readMultibandTileLayer(from, to, projectedExtent, projectedPolygons.polygons, projectedPolygons.crs, layerProvider.maxZoom, sc, None)

    val spatialLayer = layer
      .toSpatial(from)
      .cache()

    val histogram = spatialLayer.histogram()

    assertEquals(27779.0,histogram(0).mean().get,1.0)
    assertEquals(21.69,histogram(1).mean().get,0.01)
    assertEquals(16672090,histogram(2).mean().get,1.0)
    assertEquals(48336,histogram(0).totalCount())
    assertEquals(45601,histogram(1).totalCount())
    assertEquals(65536,histogram(2).totalCount())

    assertEquals(FloatConstantNoDataCellType, spatialLayer.metadata.cellType)
    assertEquals(LatLng, spatialLayer.metadata.crs)
    assertEquals(extent, spatialLayer.metadata.extent)
    spatialLayer
      .writeGeoTiff(s"/tmp/agEra5FileLayerProvider.tif", projectedExtent)
  }

  @Test
  def agEra5WithOpensearchClient(): Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val projectedExtent = ProjectedExtent(ProjectedExtent(extent, LatLng).reproject(utm31),utm31)
    val projectedPolygons = ProjectedPolygons.fromExtent(projectedExtent.extent, s"EPSG:${projectedExtent.crs.epsgCode.get}")

    val from = LocalDate.of(2020, 4, 24).atStartOfDay(ZoneId.of("UTC"))
    val to = from

    val layer = layerProvider2.readMultibandTileLayer(from, to, projectedExtent, projectedPolygons.polygons, projectedPolygons.crs, layerProvider2.maxZoom, sc, None)

    val spatialLayer = layer
      .toSpatial(from)
      .cache()

    val histogram = spatialLayer.histogram()

    assertEquals(27899.0,histogram(0).mean().get,1.0)
    assertEquals(0.24366,histogram(1).mean().get,0.00001)
    assertEquals(11426619,histogram(2).mean().get,1.0)
    assertEquals(10087,histogram(0).totalCount())
    assertEquals(11278,histogram(1).totalCount())
    assertEquals(21913,histogram(2).totalCount())

    assertEquals(FloatConstantNoDataCellType, spatialLayer.metadata.cellType)
    assertEquals(utm31, spatialLayer.metadata.crs)

    spatialLayer.writeGeoTiff(s"/tmp/agEra5WithOpensearchClient2.tif", projectedExtent)
    assertEquals(projectedExtent.extent, spatialLayer.metadata.extent)
  }

}
