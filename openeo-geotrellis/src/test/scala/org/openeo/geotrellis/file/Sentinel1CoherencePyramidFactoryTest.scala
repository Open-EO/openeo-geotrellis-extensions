package org.openeo.geotrellis.file

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}
import java.util.Arrays.asList

import geotrellis.proj4.LatLng
import geotrellis.raster.summary.polygonal.PolygonalSummaryResult
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.summary.types.MeanValue
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.{AfterClass, BeforeClass, Test}

object Sentinel1CoherencePyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    sc = SparkUtils.createLocalSparkContext("local[*]", classOf[Sentinel2PyramidFactoryTest].getName, sparkConf)
  }

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class Sentinel1CoherencePyramidFactoryTest {

  @Test
  def polygonalMean(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygon = Polygon((5.333628277543832, 51.125675727017786), (5.275056319942021, 51.120766442610417), (5.271964011621427, 51.148274537190268), (5.329808367265453, 51.150784833330405), (5.333628277543832, 51.125675727017786))
    val bbox = ProjectedExtent(polygon.extent, LatLng)

    val bbox_srs = s"EPSG:${bbox.crs.epsgCode.get}"
    val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format date
    val to_date = from_date

    val (_, baseLayer) = sentinel1CoherencePyramidFactory.pyramid_seq(bbox.extent, bbox_srs, from_date, to_date, correlationId = "")
      .maxBy { case (zoom, _) => zoom }

    val spatialLayer = baseLayer
      .toSpatial(date)
      .cache()

    val reprojected = polygon.reproject(LatLng, spatialLayer.metadata.crs)

    val summary: PolygonalSummaryResult[Array[MeanValue]] = spatialLayer.polygonalSummaryValue(reprojected, MeanVisitor)

    val qgisZonalStatisticsPluginResult = Array(149.25818472185253, 100.41442640404578)

    assertTrue(summary.toOption.isDefined)
    val meanList = summary.toOption.get
    assertEquals(2, meanList.length)
    assertArrayEquals(qgisZonalStatisticsPluginResult, meanList.map(_.mean), 0.1)
  }

  private def sentinel1CoherencePyramidFactory = new Sentinel2PyramidFactory(
    oscarsCollectionId = "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1",
    oscarsLinkTitles = asList("VH", "VV"),
    rootPath = "/data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE"
  )
}
