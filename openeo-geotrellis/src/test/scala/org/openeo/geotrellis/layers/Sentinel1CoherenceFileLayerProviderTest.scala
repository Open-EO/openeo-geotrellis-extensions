package org.openeo.geotrellis.layers

import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}

import cats.data.NonEmptyList
import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.PolygonalSummaryResult
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.summary.types.MeanValue
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

object Sentinel1CoherenceFileLayerProviderTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[2]",
    appName = Sentinel1CoherenceFileLayerProviderTest.getClass.getName)

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class Sentinel1CoherenceFileLayerProviderTest {
  import Sentinel1CoherenceFileLayerProviderTest._

  @Test
  def polygonalMean(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygon = Polygon((5.333628277543832, 51.125675727017786), (5.275056319942021, 51.120766442610417), (5.271964011621427, 51.148274537190268), (5.329808367265453, 51.150784833330405), (5.333628277543832, 51.125675727017786))
    val bbox = ProjectedExtent(polygon.extent, LatLng)

    val layer = coherenceLayerProvider().readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    val reprojected = polygon.reproject(LatLng, spatialLayer.metadata.crs)

    val summary: PolygonalSummaryResult[Array[MeanValue]] = spatialLayer.polygonalSummaryValue(reprojected, MeanVisitor)

    val qgisZonalStatisticsPluginResult = Array(149.25818472185253, 100.41442640404578)

    assertTrue(summary.toOption.isDefined)
    val meanList = summary.toOption.get
    assertEquals(2, meanList.length)
    assertArrayEquals(qgisZonalStatisticsPluginResult, meanList.map(_.mean), 0.2)
  }

  @Test
  def filterByAttributeValue(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygon = Polygon((5.333628277543832, 51.125675727017786), (5.275056319942021, 51.120766442610417), (5.271964011621427, 51.148274537190268), (5.329808367265453, 51.150784833330405), (5.333628277543832, 51.125675727017786))
    val bbox = ProjectedExtent(polygon.extent, LatLng)

    val layer = coherenceLayerProvider(Map("relativeOrbitNumber" -> 161)).readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)

    assertFalse(spatialLayer.isEmpty())
  }

  @Test
  def emptyFilterByAttributeValue(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygon = Polygon((5.333628277543832, 51.125675727017786), (5.275056319942021, 51.120766442610417), (5.271964011621427, 51.148274537190268), (5.329808367265453, 51.150784833330405), (5.333628277543832, 51.125675727017786))
    val bbox = ProjectedExtent(polygon.extent, LatLng)

    try{

      val layer = coherenceLayerProvider(Map("relativeOrbitNumber" -> 99)).readMultibandTileLayer(from = date, to = date, bbox, sc = sc)
      val spatialLayer = layer
        .toSpatial(date)
      fail()
    } catch{
      case e: IllegalArgumentException => return
      case t: Exception => throw t
    }


  }

  private def coherenceLayerProvider(attributeValues: Map[String, Any] = Map()) =
    new FileLayerProvider(
      openSearchEndpoint = new URL("http://oscars-01.vgt.vito.be:8080"),
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1",
      openSearchLinkTitles = NonEmptyList.of("VH", "VV"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/",
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor = SplitYearMonthDayPathDateExtractor,
      attributeValues
    )
}
