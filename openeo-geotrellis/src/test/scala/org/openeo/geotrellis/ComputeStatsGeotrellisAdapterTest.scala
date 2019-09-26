package org.openeo.geotrellis

import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteCells, ByteConstantTile}
import geotrellis.spark.ContextRDD
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Polygon
import geotrellis.vector.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

import scala.collection.JavaConverters._

object ComputeStatsGeotrellisAdapterTest {
  private var sc: SparkContext = _

  @BeforeClass
  def assertKerberosAuthentication(): Unit = {
    assertNotNull(getClass.getResourceAsStream("/core-site.xml"))
  }

  @BeforeClass
  def setUpSpark(): Unit = {
    sc = {
      val conf = new SparkConf().set("spark.driver.bindAddress", "127.0.0.1")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, conf)
    }
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    sc.stop()
  }

  private val polygon1 =
    """
      |{
      |  "type": "Polygon",
      |  "coordinates": [
      |    [
      |      [
      |        4.6770629938691854,
      |        50.82172692290532
      |      ],
      |      [
      |        4.6550903376191854,
      |        50.80697613242405
      |      ],
      |      [
      |        4.6866760309785604,
      |        50.797429020705295
      |      ],
      |      [
      |        4.7196350153535604,
      |        50.795692972629176
      |      ],
      |      [
      |        4.7402343805879354,
      |        50.81738893871384
      |      ],
      |      [
      |        4.6770629938691854,
      |        50.82172692290532
      |      ]
      |    ]
      |  ]
      |}
      """.stripMargin.parseGeoJson[Polygon]()

  private val polygon2 =
    """
      |{
      |  "type": "Polygon",
      |  "coordinates": [
      |    [
      |      [
      |        3.950237888725339,
      |        51.01001898590911
      |      ],
      |      [
      |        3.950237888725339,
      |        51.03442207171108
      |      ],
      |      [
      |        4.032635349662839,
      |        51.03442207171108
      |      ],
      |      [
      |        4.032635349662839,
      |        51.01001898590911
      |      ],
      |      [
      |        3.950237888725339,
      |        51.01001898590911
      |      ]
      |    ]
      |  ]
      |}
      """.stripMargin.parseGeoJson[Polygon]()
}

class ComputeStatsGeotrellisAdapterTest {
  import ComputeStatsGeotrellisAdapterTest._

  private val computeStatsGeotrellisAdapter = new ComputeStatsGeotrellisAdapter

  @Test
  def compute_average_timeseries(): Unit = {
    val from = ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusWeeks 1

    val polygons = Seq(polygon1.toWKT(), polygon2.toWKT())

    val stats = computeStatsGeotrellisAdapter.compute_average_timeseries(
      "S1_GRD_SIGMA0_ASCENDING",
      polygons.asJava,
      polygons_srs = "EPSG:4326",
      from_date = ISO_OFFSET_DATE_TIME format from,
      to_date = ISO_OFFSET_DATE_TIME format to,
      zoom = 14,
      band_index = 2
    ).asScala

    for ((date, means) <- stats) {
      println(s"$date: $means")
    }

    assertFalse(stats.isEmpty)

    val means = stats
      .flatMap { case (_, dailyMeans) => dailyMeans.asScala }

    assertTrue(means.exists(mean => !mean.isNaN))
  }

  @Test
  def compute_average_timeseries_on_datacube(): Unit = {

    val minDate = ZonedDateTime.parse("2017-01-01T00:00:00Z")
    val maxDate = ZonedDateTime.parse("2017-03-10T00:00:00Z")

    val polygons = Seq(polygon1.toWKT(), polygon2.toWKT())

    val tile10 = new ByteConstantTile(10.toByte, 256, 256, ByteCells.withNoData(Some(255.byteValue())))
    val tile5 = new ByteConstantTile(5.toByte, 256, 256, ByteCells.withNoData(Some(255.byteValue())))
    //val datacube = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, new ArrayMultibandTile(Array[Tile](tile10, tile5)), new TileLayout(1, 1, 256, 256))

    val datacube = TestOpenEOProcesses.tileToSpaceTimeDataCube(tile10)
    val polygonExtent = polygon1.envelope.combine(polygon2.envelope)
    val updatedMetadata = datacube.metadata.copy(extent = polygonExtent,crs = LatLng,layout=LayoutDefinition(polygonExtent,datacube.metadata.tileLayout))

    val stats = computeStatsGeotrellisAdapter.compute_average_timeseries_from_datacube(
      ContextRDD(datacube,updatedMetadata),
      polygons.asJava,
      polygons_srs = "EPSG:4326",
      from_date = ISO_OFFSET_DATE_TIME format minDate,
      to_date = ISO_OFFSET_DATE_TIME format maxDate,
      band_index = 0
    ).asScala

    for ((date, means) <- stats) {
      println(s"$date: $means")
    }

    assertFalse(stats.isEmpty)

    val means = stats
      .flatMap { case (_, dailyMeans) => dailyMeans.asScala }.filter(!_.isEmpty)

    assertTrue(means.exists(mean => !mean.get(0).isNaN))
  }

  @Test
  def compute_histogram_timeseries_on_datacube(): Unit = {

    val minDate = ZonedDateTime.parse("2017-01-01T00:00:00Z")
    val maxDate = ZonedDateTime.parse("2017-03-10T00:00:00Z")

    val polygons = Seq(polygon1.toWKT(), polygon2.toWKT())

    val tile10 = new ByteConstantTile(10.toByte, 256, 256, ByteCells.withNoData(Some(255.byteValue())))
    val tile5 = new ByteConstantTile(5.toByte, 256, 256, ByteCells.withNoData(Some(255.byteValue())))
    //val datacube = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, new ArrayMultibandTile(Array[Tile](tile10, tile5)), new TileLayout(1, 1, 256, 256))

    val datacube = TestOpenEOProcesses.tileToSpaceTimeDataCube(tile10)
    val polygonExtent = polygon1.envelope.combine(polygon2.envelope)
    val updatedMetadata = datacube.metadata.copy(extent = polygonExtent,crs = LatLng,layout=LayoutDefinition(polygonExtent,datacube.metadata.tileLayout))

    val stats = computeStatsGeotrellisAdapter.compute_histograms_time_series_from_datacube(
      ContextRDD(datacube,updatedMetadata),
      polygons.asJava,
      polygons_srs = "EPSG:4326",
      from_date = ISO_OFFSET_DATE_TIME format minDate,
      to_date = ISO_OFFSET_DATE_TIME format maxDate,
      band_index = 0
    ).asScala

    for ((date, means) <- stats) {
      println(s"$date: $means")
    }

    assertFalse(stats.isEmpty)

    val histogramlist = stats.get("2017-01-01T00:00:00Z")
    val histogramPoly1 = histogramlist.get.get(0)
    assertEquals(507,histogramPoly1.get(0).get(10.0),0.01)

    //assertTrue(means.exists(mean => !mean.get(0).isNaN))
  }

  @Test
  def compute_histograms_time_series(): Unit = {
    val from = ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusWeeks 1

    val polygons = Seq(polygon1.toWKT(), polygon2.toWKT())

    val histograms = computeStatsGeotrellisAdapter.compute_histograms_time_series(
      "S1_GRD_SIGMA0_ASCENDING",
      polygons.asJava,
      polygons_srs = "EPSG:4326",
      from_date = ISO_OFFSET_DATE_TIME format from,
      to_date = ISO_OFFSET_DATE_TIME format to,
      zoom = 14,
      band_index = 2
    ).asScala

    for ((date, polygonalHistograms) <- histograms) {
      println(s"$date: $polygonalHistograms")
    }

    assertFalse(histograms.isEmpty)

    val buckets = for {
      (_, polygonalHistograms) <- histograms.toSeq
      histogram <- polygonalHistograms.asScala
      bucket <- histogram.asScala
    } yield bucket

    assertFalse(buckets.isEmpty)
  }

  @Test
  def compute_histogram_time_series(): Unit = {
    val from = ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusWeeks 1

    val polygon = polygon1.toWKT()

    val histograms = computeStatsGeotrellisAdapter.compute_histogram_time_series(
      "S1_GRD_SIGMA0_ASCENDING",
      polygon,
      polygon_srs = "EPSG:4326",
      from_date = ISO_OFFSET_DATE_TIME format from,
      to_date = ISO_OFFSET_DATE_TIME format to,
      zoom = 14,
      band_index = 2
    ).asScala

    for ((date, polygonalHistogram) <- histograms) {
      println(s"$date: $polygonalHistogram")
    }

    assertFalse(histograms.isEmpty)

    val buckets = for {
      (_, polygonalHistogram) <- histograms
      bucket <- polygonalHistogram.asScala
    } yield bucket

    assertFalse(buckets.isEmpty)
  }
}
