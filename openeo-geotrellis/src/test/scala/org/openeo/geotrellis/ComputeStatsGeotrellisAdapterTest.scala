package org.openeo.geotrellis

import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.{ByteCells, ByteConstantTile, FloatConstantNoDataCellType, MultibandTile, UByteUserDefinedNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Polygon, _}
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._
import org.junit.runners.Parameterized.Parameters
import org.openeo.geotrellis.AggregateSpatialTest.parseCSV
import org.openeo.geotrellis.LayerFixtures._
import org.openeo.geotrellis.TimeSeriesServiceResponses._

import java.nio.file.Files
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.immutable.Seq

object ComputeStatsGeotrellisAdapterTest {
  type JMap[K, V] = java.util.Map[K, V]
  type JList[T] = java.util.List[T]

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
      //val config = new HdfsConfiguration
      //config.set("hadoop.security.authentication", "kerberos")
      //UserGroupInformation.setConfiguration(config)

      val conf = new SparkConf().set("spark.driver.bindAddress", "127.0.0.1")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, conf)
    }
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    sc.stop()
  }

  //11km²
  val polygon1: Polygon =
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

  //16km²
  val polygon2: Polygon =
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
  //polygon roughly 2147km², so 21470000 10m pixels
  val polygon3: Polygon =
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

  //14164m² so 141 10m pixels: small field
  val polygon4: Polygon =
    """
      |{
      |  "type": "Polygon",
      |  "coordinates": [[[5.608215013735893, 51.032165152086264], [5.608271547800293, 51.03221788952301], [5.608276702058292, 51.03222293623101], [5.60849596808989, 51.032448502828444], [5.608716500868006, 51.032663757587436], [5.608825832560376, 51.032770514885485], [5.608945000622374, 51.032880067275485], [5.609034654329857, 51.032962466956384], [5.6108271092295565, 51.03200900180982], [5.6108243802523665, 51.032005046553195], [5.610765550277478, 51.031906904685506], [5.610730775512867, 51.03185731710986], [5.610728864439129, 51.03185450621938], [5.61068097777416, 51.031781806466725], [5.610648884176528, 51.0317343801373], [5.610647867072976, 51.03173285113396], [5.61056103219013, 51.031600028841], [5.610464592173136, 51.031620528338046], [5.610108166306372, 51.03169612090491], [5.610099694700648, 51.03169779497791], [5.6098587750999505, 51.031741960985435], [5.609583519929178, 51.03179242194306], [5.6095812925908355, 51.0317928220292], [5.609462795599489, 51.031813670192285], [5.609462557298256, 51.03181371202481], [5.6094265107381105, 51.0318200256364], [5.609420285041724, 51.03182105270213], [5.609335088450095, 51.03183424727698], [5.609264660589062, 51.03184521822665], [5.609258526965428, 51.03184611349288], [5.609158373400448, 51.03185975557609], [5.6091581558557895, 51.03185978513339], [5.609101330081307, 51.0318674864013], [5.6090929559728915, 51.031868511547074], [5.608913831137317, 51.031888110200775], [5.608906493782254, 51.031888830479645], [5.608726551315478, 51.03190448174909], [5.60871912778759, 51.031905044225965], [5.608609468745772, 51.0319121289672], [5.608538666998535, 51.031916736443236], [5.60851084566201, 51.03191739795041], [5.608487750205862, 51.0319169979628], [5.608215013735893, 51.032165152086264]]]
      |}
      """.stripMargin.parseGeoJson[Polygon]()

  def assertEqualTimeseriesStats(expected: Seq[Seq[Double]], actual: JList[JList[Double]]): Unit = {
    assertEquals("should have same polygon count", expected.length, actual.size())
    expected.indices.foreach { i =>
      assertArrayEquals("should have same band stats", expected(i).toArray, actual.get(i).asScala.toArray, 1e-6)
    }
  }

  def assertEqualTimeseriesStats(expected: Seq[Seq[Double]], actual: scala.Seq[scala.Seq[Double]]): Unit = {
    assertEquals("should have same polygon count", expected.length, actual.size)
    expected.indices.foreach { i =>
      assertArrayEquals("should have same band stats", expected(i).toArray, actual(i).toArray, 1e-6)
    }
  }
}


class ComputeStatsGeotrellisAdapterTest() {
  import ComputeStatsGeotrellisAdapterTest._



  @Before
  def setup():Unit = {
    //System.setProperty("pixels.treshold","" + threshold)

  }


  val computeStatsGeotrellisAdapter = new ComputeStatsGeotrellisAdapter()



  private def buildCubeRdd(from: ZonedDateTime, to: ZonedDateTime): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val tile10 = new ByteConstantTile(10.toByte, 256, 256, ByteCells.withNoData(Some(255.byteValue())))
    val datacube = TestOpenEOProcesses.tileToSpaceTimeDataCube(tile10).withContext(_.filter { case (key, _) => !(key.time isBefore from) && !(key.time isAfter to) })
    val polygonExtent = polygon1.extent.combine(polygon2.extent)
    val updatedMetadata = datacube.metadata.copy(
      extent = polygonExtent,
      crs = LatLng,
      layout = LayoutDefinition(polygonExtent, datacube.metadata.tileLayout)
    )

    ContextRDD(datacube, updatedMetadata)
  }

  @Test
  def compute_average_timeseries_on_datacube_from_polygons(): Unit = {
    val from_date = "2017-01-01T00:00:00Z"
    val to_date = "2017-03-10T00:00:00Z"

    val datacube = buildCubeRdd(ZonedDateTime.parse(from_date), ZonedDateTime.parse(to_date)).withContext{
      _.mapValues(t => MultibandTile(Array(t.bands(0),t.bands(1),t.bands(0),t.bands(0),t.bands(0))))
    }

    val outDir = "/tmp/csvoutput"
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube("mean", datacube, ProjectedPolygons(Seq(polygon1, polygon2),  "EPSG:4326"), outDir)

    val groupedStats: Map[String, scala.Seq[scala.Seq[Double]]] = parseCSV(outDir).toSeq.sortBy(_._1).map(t=>(t._1.substring(0,10),t._2)).toMap
    groupedStats.foreach(println)

    val keys = Seq("2017-01-01", "2017-01-15", "2017-02-01")
    keys.foreach(k => assertEqualTimeseriesStats(
      Seq(Seq(10.0, Double.NaN,10.0,10.0,10.0), Seq(10.0, Double.NaN,10.0,10.0,10.0)),
      groupedStats.get(k).get
    ))
  }

  @Test
  def compute_average_timeseries_on_datacube_from_GeoJson_file(): Unit = {
    val from_date = "2017-01-01T00:00:00Z"
    val to_date = "2017-03-10T00:00:00Z"


    val outDir = "/tmp/csvoutput"
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube("mean", buildCubeRdd(ZonedDateTime.parse(from_date), ZonedDateTime.parse(to_date)),ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/GeometryCollection.json").getPath), outDir)

    val groupedStats: Map[String, scala.Seq[scala.Seq[Double]]] = parseCSV(outDir).toSeq.sortBy(_._1).map(t => (t._1.substring(0, 10), t._2)).toMap

    groupedStats.foreach(println)

    val keys = Seq("2017-01-01", "2017-01-15", "2017-02-01")
    keys.foreach(k => assertEqualTimeseriesStats(
      Seq(Seq(10.0, Double.NaN), Seq(10.0, Double.NaN)),
      groupedStats.get(k).get
    ))
  }


  @Test
  def compute_median_ndvi_timeseries_on_accumulo_datacube(): Unit = {

    val minDateString = "2017-11-01T00:00:00Z"
    val maxDateString = "2017-11-16T02:00:00Z"
    val minDate = ZonedDateTime.parse(minDateString)

    val maxDate = ZonedDateTime.parse(maxDateString)

    val polygons = Seq(polygon1.reproject(LatLng,CRS.fromEpsgCode(32631)))

    val datacube= s2_ndvi_bands( minDateString, maxDateString, polygons,"EPSG:32631")

    //val selectedBands = datacube.withContext(_.mapValues(_.subsetBands(0,1)))
    val ndviProcess = TestOpenEOProcessScriptBuilder.createNormalizedDifferenceProcess10AddXY
    assertEquals(FloatConstantNoDataCellType, ndviProcess.getOutputCellType())
    val ndviDataCube = new OpenEOProcesses().mapBandsGeneric(datacube, ndviProcess, new util.HashMap[String, Any]).convert(ndviProcess.getOutputCellType())
    assertEquals(FloatConstantNoDataCellType,ndviDataCube.metadata.cellType)

    assertMedianComputedCorrectly(ndviDataCube, minDateString, minDate, maxDate, Seq(polygon1))
  }

  @Test
  def validateAccumuloDataCubeAgainstTimeSeriesServiceMeans(): Unit = {
    val minDateString = "2017-11-01T00:00:00Z"
    val maxDateString = "2017-11-16T00:00:00Z"

    val polygons = Seq(polygon1, polygon2, polygon4).map(_.reproject(LatLng,CRS.fromEpsgCode(32631)))
    val srs = "EPSG:4326"
    val band = 0


    val datacube = LayerFixtures.s2_fapar(minDateString, maxDateString,polygons,"EPSG:32631")

    val outDir = "/tmp/csvoutput_validateFAPARAgainstTSService"
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube("mean", datacube, ProjectedPolygons(polygons, "EPSG:32631"), outDir)

    val groupedStats: Map[String, scala.Seq[scala.Seq[Double]]] = parseCSV(outDir).toSeq.sortBy(_._1).toMap
    groupedStats.foreach(println)


    val scaleFactor = 0.005
    val offset = 0.0

    val actualAverages = (for {
      (date, polygonalMeans) <- groupedStats
      physicalMeans = polygonalMeans.map(_.applyOrElse(0, default = (_: Int) => Double.NaN) * scaleFactor + offset)
    } yield (ZonedDateTime.parse(date).toLocalDate.atStartOfDay(ZoneOffset.UTC), physicalMeans)).toMap

    // run scripts/tsservice_means to regenerate these reference values
    val referenceJson = IOUtils.toString(this.getClass.getResource("/org/openeo/geotrellis/TimeSeriesServiceFaparGeometriesMeans.json"))

    val referenceAverages = GeometriesMeans.parse(referenceJson)
      .results
      .map { case (date, results) => (date.atStartOfDay(ZoneOffset.UTC), results.map(_.average)) }
      .filter(_._2.exists(!_.isNaN))


    for {
      (date, expectedAverages) <- referenceAverages
      (expectedAverage, actualAverage) <- expectedAverages zip actualAverages(date)
    } assertEquals(expectedAverage, actualAverage, 0.08)//large delta because we changed to computing in UTM

    assertArrayEquals(referenceAverages.keys.map((_.toEpochSecond)).toArray.sorted,actualAverages.filter(_._2.exists(!_.isNaN)).keys.map((_.toEpochSecond)).toArray.sorted)
  }


  @Test
  def compute_median_masked_ndvi_timeseries_on_accumulo_datacube(): Unit = {

    val minDateString = "2022-04-24T00:00:00Z"
    val maxDateString = "2022-04-30T02:00:00Z"
    val minDate = ZonedDateTime.parse(minDateString)

    val maxDate = ZonedDateTime.parse(maxDateString)

    val polygons = Seq(polygon4.reproject(LatLng, CRS.fromEpsgCode(32631)))

    val datacube = s2_ndvi_bands(minDateString, maxDateString, polygons, "EPSG:32631")

    val selectedBands = datacube.convert(FloatConstantNoDataCellType)
    val ndviProcess = TestOpenEOProcessScriptBuilder.createNormalizedDifferenceProcess10AddXY
    val processes = new OpenEOProcesses()
    val ndviDataCube = processes.mapBandsGeneric(selectedBands, ndviProcess, new util.HashMap[String, Any])//.withContext(_.mapValues(_.mapDouble(0)(pixel => 0.1)))

    val mask = s2_scl(minDateString, maxDateString, polygons, "EPSG:32631")
    val binaryMask = mask.convert(UByteUserDefinedNoDataCellType(255.byteValue())).withContext(_.mapValues( _.map(0)(pixel => if ( pixel < 5) 1 else 0)))

    print(binaryMask.partitioner)

    val maskedCube = processes.rasterMask(ndviDataCube,binaryMask,Double.NaN)
    //NetCDFRDDWriter.writeRasters(maskedCube,"/tmp/out_NDVI.nc", options)
    assertMedianComputedCorrectly(maskedCube, minDateString, minDate, maxDate, Seq(polygon4))
  }

  private def assertMedianComputedCorrectly(ndviDataCube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], minDateString: String, minDate: ZonedDateTime, maxDate: ZonedDateTime, polygons: Seq[Polygon]) = {
    //alternative way to compute a histogram that works for this simple case

    val stats: _root_.scala.collection.immutable.Map[_root_.java.lang.String, scala.Seq[scala.Seq[Double]]] = computeAggregateSpatial("median",ndviDataCube, polygons)

    assertFalse(stats.isEmpty)

    val dates: immutable.Iterable[String] = stats.map(_._1)
    val theDate = dates.head
    val histogram: Histogram[Double] = ndviDataCube.toSpatial(LocalDate.parse(theDate).atStartOfDay(ZoneOffset.UTC)).mask(polygons.map(_.reproject(LatLng, ndviDataCube.metadata.crs))).histogram(1000)(0)
    print("MEDIAN: ")
    val expectedMedian = histogram.median()
    print(expectedMedian)


    val means = stats
      .flatMap { case (_, dailyMeans) => dailyMeans }.filter(!_.isEmpty)

    assertTrue(means.exists(mean => !mean(0).isNaN))
    assertEquals( expectedMedian.get,stats(theDate)(0)(0), 0.001)
  }


  private def computeAggregateSpatial(reducer:String,ndviDataCube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], polygons: Seq[Polygon]): Map[String, scala.Seq[scala.Seq[Double]]] = {
    computeAggregateSpatial(reducer,ndviDataCube,ProjectedPolygons(polygons, "EPSG:4326"))
  }

  private def computeAggregateSpatial(reducer: String, ndviDataCube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], polygons: ProjectedPolygons): Map[String, scala.Seq[scala.Seq[Double]]] = {
    val outDir = Files.createTempDirectory("aggregateSpatial").toString
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube(reducer, ndviDataCube, polygons, outDir)

    val stats: Map[String, scala.Seq[scala.Seq[Double]]] = parseCSV(outDir).toSeq.sortBy(_._1).map(t => (t._1.substring(0, 10), t._2)).toMap
    stats.foreach(println)
    stats
  }

  @Test
  def compute_median_timeseries_on_datacube_from_polygons(): Unit = {
    val from_date = "2017-01-01T00:00:00Z"
    val to_date = "2017-03-10T00:00:00Z"

    val stats = computeAggregateSpatial(
      "median",
      buildCubeRdd(ZonedDateTime.parse(from_date), ZonedDateTime.parse(to_date)), Seq(polygon1, polygon2))

    val keys = Seq("2017-01-01", "2017-01-15", "2017-02-01")
    keys.foreach(k => assertEqualTimeseriesStats(
      Seq(Seq(10.0, Double.NaN), Seq(10.0, Double.NaN)),
      stats.get(k).get
    ))
  }


  @Test
  def compute_median_timeseries_on_datacube_from_GeoJson_file(): Unit = {
    val from_date = "2017-01-01T00:00:00Z"
    val to_date = "2017-03-10T00:00:00Z"

    val polygons = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/GeometryCollection.json").getPath)
    val stats = computeAggregateSpatial(
      "median",
      buildCubeRdd(ZonedDateTime.parse(from_date), ZonedDateTime.parse(to_date)), polygons)


    val keys = Seq("2017-01-01", "2017-01-15", "2017-02-01")
    keys.foreach(k => assertEqualTimeseriesStats(
      Seq(Seq(10.0, Double.NaN), Seq(10.0, Double.NaN)),
      stats.get(k).get
    ))
  }


  @Test
  def compute_sd_timeseries_on_datacube_from_polygons(): Unit = {
    val from_date = "2017-01-01T00:00:00Z"
    val to_date = "2017-03-10T00:00:00Z"

    val stats = computeAggregateSpatial(
      "sd",
      buildCubeRdd(ZonedDateTime.parse(from_date), ZonedDateTime.parse(to_date)), Seq(polygon1, polygon2))

    val keys = Seq("2017-01-01", "2017-01-15", "2017-02-01")
    keys.foreach(k => assertEqualTimeseriesStats(
      Seq(Seq(0.0, Double.NaN), Seq(0.0, Double.NaN)),
      stats.get(k).get
    ))
  }

  @Test
  def compute_sd_timeseries_on_datacube_from_GeoJson_file(): Unit = {
    val from_date = "2017-01-01T00:00:00Z"
    val to_date = "2017-03-10T00:00:00Z"

    val polygons = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/GeometryCollection.json").getPath)

    val stats = computeAggregateSpatial(
      "sd",
      buildCubeRdd(ZonedDateTime.parse(from_date), ZonedDateTime.parse(to_date)), polygons)

    val keys = Seq("2017-01-01", "2017-01-15", "2017-02-01")
    keys.foreach(k => assertEqualTimeseriesStats(
      Seq(Seq(0.0, Double.NaN), Seq(0.0, Double.NaN)),
      stats.get(k).get
    ))
  }


  @Test
  def testHandlingOfEmptyGeomtries(): Unit = {
    val from_date = "2017-01-01T00:00:00Z"
    val to_date = "2017-03-10T00:00:00Z"

    val empty = MultiPolygon(polygon1).buffer(-10)

    val myEmptyPolygons = ProjectedPolygons(Array(empty),LatLng)
    val stats = computeAggregateSpatial(
      "mean",
      buildCubeRdd(ZonedDateTime.parse(from_date), ZonedDateTime.parse(to_date)), myEmptyPolygons)

    assertTrue(stats.isEmpty)
  }


}
