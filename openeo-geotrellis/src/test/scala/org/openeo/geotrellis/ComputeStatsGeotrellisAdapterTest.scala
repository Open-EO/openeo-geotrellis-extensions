package org.openeo.geotrellis

import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.{ByteCells, ByteConstantTile, MultibandTile}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.util.SparkUtils
import geotrellis.spark.{ContextRDD, Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.vector.io._
import geotrellis.vector.{Extent, Polygon}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.{AfterClass, Before, BeforeClass, Test}
import org.openeo.geotrellisaccumulo.PyramidFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object ComputeStatsGeotrellisAdapterTest {
  private var sc: SparkContext = _

  @Parameters(name = "PixelThreshold: {0}") def data: java.lang.Iterable[Array[Integer]] = {
    val list = new util.ArrayList[Array[Integer]]()
    list.add(Array[Integer](0))
    list.add(Array[Integer](500000000))
    list
  }

  @BeforeClass
  def assertKerberosAuthentication(): Unit = {
    //assertNotNull(getClass.getResourceAsStream("/core-site.xml"))
  }

  @BeforeClass
  def setUpSpark(): Unit = {
    sc = {
      val config = new HdfsConfiguration
      config.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(config)

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

  //this polygon is also used in python tests with S2 data
  private val polygon3 =
    """
      |{
      |  "type": "Polygon",
      |  "coordinates": [
      |    [
      |            [7.022705078125007, 51.75432477678571],
      |            [7.659912109375007, 51.74333844866071],
      |            [7.659912109375007, 51.29289899553571],
      |            [7.044677734375007, 51.31487165178571],
      |            [7.022705078125007, 51.75432477678571]
      |    ]
      |  ]
      |}
      """.stripMargin.parseGeoJson[Polygon]()
}

@RunWith(classOf[Parameterized])
class ComputeStatsGeotrellisAdapterTest(threshold:Int) {
  import ComputeStatsGeotrellisAdapterTest._



  @Before
  def setup():Unit = {
    System.setProperty("pixels.treshold","" + threshold)

  }


  private val computeStatsGeotrellisAdapter = new ComputeStatsGeotrellisAdapter(
    zookeepers = "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181",
    accumuloInstanceName = "hdp-accumulo-instance"
  )

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


  private lazy val accumuloPyramidFactory = {new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")}


  /**
    * This test is similar in setup to the openeo integrationtest:
    * test_validate_timeseries.Test#test_zonal_statistics
    *
    * But it is easier to debug.
    *
    * Testing against accumulo is necessary, because we have a custom RangePartitioner for performance, that affects the result if not implemented correctly
    */
  @Test
  def compute_median_timeseries_on_accumulo_datacube(): Unit = {

    val minDateString = "2017-11-01T00:00:00Z"
    val maxDateString = "2017-11-16T02:00:00Z"
    val minDate = ZonedDateTime.parse(minDateString)
    val maxDate = ZonedDateTime.parse(maxDateString)

    val polygons = Seq(polygon3)

    //bbox = new Extent(10.5, 46.5, 11.4, 46.9);
    //srs = "EPSG:4326";


    val datacube = accumuloDataCube("S2_FAPAR_PYRAMID_20190708", minDateString, maxDateString,  polygons.envelope,  "EPSG:4326")

    assertMedianComputedCorrectly(datacube, minDateString, minDate, maxDate, polygons)
  }

  private def accumuloDataCube(layer: String, minDateString: String, maxDateString: String, bbox: Extent, srs: String) = {
    val pyramid: Seq[(Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]])] = accumuloPyramidFactory.pyramid_seq(layer, bbox, srs, minDateString, maxDateString)
    System.out.println("pyramid = " + pyramid)

    val pyramidAsMap = pyramid.toMap
    val maxZoom = pyramidAsMap.keys.max
    val datacube = pyramidAsMap.get(maxZoom).get
    datacube
  }

  @Test
  def compute_median_ndvi_timeseries_on_accumulo_datacube(): Unit = {

    val minDateString = "2017-11-01T00:00:00Z"
    val maxDateString = "2017-11-16T02:00:00Z"
    val minDate = ZonedDateTime.parse(minDateString)

    val maxDate = ZonedDateTime.parse(maxDateString)

    val polygons = Seq(polygon3)
    val datacube= accumuloDataCube("CGS_SENTINEL2_RADIOMETRY_V102_EARLY", minDateString, maxDateString, polygons.envelope, "EPSG:4326")

    val selectedBands = datacube.withContext(_.mapValues(_.subsetBands(1,3)))
    val ndviProcess = TestOpenEOProcessScriptBuilder.createNormalizedDifferenceProcess
    val ndviDataCube = new OpenEOProcesses().mapBandsGeneric(selectedBands, ndviProcess)

    assertMedianComputedCorrectly(ndviDataCube, minDateString, minDate, maxDate, polygons)
  }

  private def assertMedianComputedCorrectly(ndviDataCube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], minDateString: String, minDate: ZonedDateTime, maxDate: ZonedDateTime, polygons: Seq[Polygon]) = {
    //alternative way to compute a histogram that works for this simple case
    val histogram: Histogram[Double] = ndviDataCube.toSpatial(minDate).mask(polygon3.reproject(LatLng, WebMercator)).histogram(255)(0)
    print("MEDIAN: ")
    val expectedMedian = histogram.median()
    print(expectedMedian)

    val stats = computeStatsGeotrellisAdapter.compute_median_time_series_from_datacube(
      ndviDataCube,
      polygons.map(_.toWKT()).asJava,
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
    assertEquals(stats(minDateString).get(0).get(0), expectedMedian.get, 0.1)
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
  def compute_median_timeseries_on_datacube(): Unit = {

    val minDate = ZonedDateTime.parse("2017-01-01T00:00:00Z")
    val maxDate = ZonedDateTime.parse("2017-03-10T00:00:00Z")

    val polygons = Seq(polygon1.toWKT(), polygon2.toWKT())

    val tile10 = new ByteConstantTile(10.toByte, 256, 256, ByteCells.withNoData(Some(255.byteValue())))
    val tile5 = new ByteConstantTile(5.toByte, 256, 256, ByteCells.withNoData(Some(255.byteValue())))
    //val datacube = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, new ArrayMultibandTile(Array[Tile](tile10, tile5)), new TileLayout(1, 1, 256, 256))

    val datacube = TestOpenEOProcesses.tileToSpaceTimeDataCube(tile10)
    val polygonExtent = polygon1.envelope.combine(polygon2.envelope)
    val updatedMetadata = datacube.metadata.copy(extent = polygonExtent,crs = LatLng,layout=LayoutDefinition(polygonExtent,datacube.metadata.tileLayout))

    val stats = computeStatsGeotrellisAdapter.compute_median_time_series_from_datacube(
      ContextRDD(datacube,updatedMetadata),
      polygons.asJava,
      polygons_srs = "EPSG:4326",
      from_date = ISO_OFFSET_DATE_TIME format minDate,
      to_date = ISO_OFFSET_DATE_TIME format maxDate,
      band_index = 0
    ).asScala

    for ((date, medians) <- stats) {
      println(s"$date: $medians")
    }

    assertFalse(stats.isEmpty)

    val histogramlist = stats.get("2017-01-01T00:00:00Z")
    val histogramPoly1 = histogramlist.get.get(0)
    assertEquals(10.0,histogramPoly1.get(0),0.01)

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
