package org.openeo.geotrellis.integrationtests

import com.azavea.gdal.GDALWarp
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, ShortConstantNoDataCellType}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.spark.util.SparkUtils
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.geotrellis.file.{ProbaVPyramidFactoryTest, PyramidFactory}
import org.openeo.geotrellis.netcdf.{NetCDFOptions, NetCDFRDDWriter}
import org.openeo.geotrellis.{AggregateSpatialTest, ComputeStatsGeotrellisAdapter, LayerFixtures, MergeCubesSpec, OpenEOProcesses, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient
import ucar.nc2.dataset.NetcdfDataset

import java.net.URL
import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime
import java.util
import scala.collection.JavaConverters._

object CollectionTests {
  // Methods with attributes get called in a non-intuitive order:
  // - BeforeAll
  // - ParameterizedTest
  // - AfterAll
  // - BeforeClass
  // - AfterClass
  //
  // This order feels arbitrary, so I made the code robust against order changes.

  private var _sc: Option[SparkContext] = None

  private implicit def sc: SparkContext = {
    if (_sc.isEmpty) {

      val conf = new SparkConf()
        //        .setMaster("local[*]")
        //.set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.kryoserializer.buffer.max", "512m")
        .set("spark.rdd.compress", "true")
      //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      _sc = Some(SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, conf))
    }
    _sc.get
  }


  @BeforeAll
  def setUpSpark_BeforeAll(): Unit = sc

  var gotAfterAll = false

  @AfterAll
  def tearDownSpark_AfterAll(): Unit = {
    GDALWarp.init(0)
    gotAfterAll = true
    maybeStopSpark()
  }

  var gotAfterClass = false

  def maybeStopSpark(): Unit = {
    if (gotAfterAll && gotAfterClass) {
      if (_sc.isDefined) {
        _sc.get.stop()
        _sc = None
      }
    }
  }


  def jsonEquals(json1: JsonNode, json2: JsonNode, tolerance: Double = 0.0001): Boolean = {
    if (!json1.isContainerNode && !json2.isContainerNode) {
      if (json1.isNumber && json2.isNumber) {
        val diff = Math.abs(json1.doubleValue - json2.doubleValue)
        diff <= tolerance
      } else {
        json1.equals(json2)
      }
    } else if (json1.isArray && json2.isArray) {
      json1.size == json2.size &&
        json1.elements.asScala.zip(json2.elements.asScala).forall(pair => jsonEquals(pair._1, pair._2, tolerance))
    } else {
      if (json1.size != json2.size) {
        println("Size diff: " + json1 + json2)
        false
      } else {
        val tups = json1
          .fields()
          .asScala
          .map(pair => {
            val other = json2.get(pair.getKey)
            (pair.getValue, other)
          })
          .toList
        tups.forall(pair => jsonEquals(pair._1, pair._2, tolerance))
      }
    }
  }

  def fileToString(path: String): String = {
    val source = scala.io.Source.fromFile(path)
    try source.mkString finally source.close()
  }

  def fileToJSON(path: String): JsonNode = {
    val s = fileToString(path)
    val mapper = new ObjectMapper()
    mapper.readTree(s)
  }

  def testLayerParams: java.util.stream.Stream[Arguments] = java.util.Arrays.stream(Array(
    //    arguments("TERRASCOPE_S1_SLC_COHERENCE_V1"),
    //    arguments("TERRASCOPE_S1_GAMMA0_V1"),
    arguments("TERRASCOPE_S2_FAPAR_V2"), // Has layer
//    arguments("TERRASCOPE_S2_NDVI_V2"), // Has layer
    //    arguments("TERRASCOPE_S2_LAI_V2"),
    //    arguments("TERRASCOPE_S2_FCOVER_V2"),
    //    arguments("TERRASCOPE_S2_TOC_V2"), // Has layer. Took 55min in CI
    //    arguments("S1_GRD_SIGMA0_ASCENDING"),
    //    arguments("S1_GRD_SIGMA0_DESCENDING"),
//    arguments("PROBAV_L3_S5_TOC_100M"), // Has layer
//    arguments("PROBAV_L3_S10_TOC_333M"), // Has layer
    //    arguments("COPERNICUS_30"),
    //    arguments("COPERNICUS_90"),
    //    arguments("CGLS_FAPAR_V2_GLOBAL"),
    //    arguments("CGLS_LAI_V2_GLOBAL"),
    //    arguments("CGLS_LAI300_V1_GLOBAL"),
//    arguments("CGLS_DMP300_V1_GLOBAL"),
  ))
}

class CollectionTests {

  import CollectionTests._

  val opensearchEndpoint = "https://services.terrascope.be/catalogue"

  def faparPyramidFactory: PyramidFactory = {
    val openSearchClient = OpenSearchClient(new URL(opensearchEndpoint), isUTM = true)
    val p = new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      openSearchLinkTitles = util.Collections.singletonList("FAPAR_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
      maxSpatialResolution = CellSize(10, 10)
    )
    p.crs = CRS.fromEpsgCode(32631)
    p
  }

  def cglsPyramidFactory(collection:String, bands: util.List[String]): PyramidFactory = {
    val openSearchClient = OpenSearchClient("https://globalland.vito.be/catalogue", isUTM = false, dateRegex = "",bands= null, clientType = "oscars")
    val p = new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = collection,
      openSearchLinkTitles = bands,
      rootPath = "",
      maxSpatialResolution = CellSize(0.002976190476190, 0.002976190476190)
    )
    p.crs = LatLng
    p
  }

  @Test
  def jsonEqualsTest(): Unit = {
    val mapper = new ObjectMapper()
    val json1 = mapper.readTree("""{"a": 1, "b": {"c": 2.0}}""")
    val json2 = mapper.readTree("""{"a": 1.0000001, "b": {"c": 2.0000002}}""")
    val isEqual = jsonEquals(json1, json2)
    assertTrue(isEqual)
  }

  @Test
  def jsonEqualsMixedTest(): Unit = {
    val mapper = new ObjectMapper()
    val json1 = mapper.readTree("""{"a": 1, "b": {"c": 2.0}}""")
    val json2 = mapper.readTree("""{"b": {"c": 2.0000002}, "a": 1.0000001}""")
    val isEqual = jsonEquals(json1, json2)
    assertTrue(isEqual)
  }

  @Test
  def jsonEqualsListTest(): Unit = {
    val mapper = new ObjectMapper()
    val json1 = mapper.readTree("""[1, 3.1415]""")
    val json2 = mapper.readTree("""[1, 3.1415926]""")
    val isEqual = jsonEquals(json1, json2)
    assertTrue(isEqual)
  }

  @Test
  def jsonDifferentListTest(): Unit = {
    val mapper = new ObjectMapper()
    val json1 = mapper.readTree("""[1, 3.1415]""")
    val json2 = mapper.readTree("""[5, 3.1415]""")
    val isEqual = jsonEquals(json1, json2)
    assertFalse(isEqual)
  }

  @Test
  def jsonDifferentLengthListTest(): Unit = {
    val mapper = new ObjectMapper()
    val json1 = mapper.readTree("""[1, 3.1415]""")
    val json2 = mapper.readTree("""[1, 3.1415, 1]""")
    val isEqual = jsonEquals(json1, json2)
    assertFalse(isEqual)
  }

  @Test
  def jsonNotEqualsTest(): Unit = {
    val mapper = new ObjectMapper()
    val json1 = mapper.readTree("""{"a": 1, "b": {"c": 2.0}}""")
    val json2 = mapper.readTree("""{"a": null, "b": {"c": 2.0000002}}""")
    val isEqual = jsonEquals(json1, json2)
    println(isEqual)
    assertFalse(isEqual)
  }

  @ParameterizedTest
  @MethodSource(Array("testLayerParams"))
  def testLayers(layerStr: String): Unit = {
    testLayerImpl(layerStr)
  }

  /**
   * This function shares the same structure as the one of
   * https://git.vito.be/projects/BIGGEO/repos/openeo-collection-tests/browse
   * But for the moment it has different expected results.
   */
  private def testLayerImpl(layerStr: String,
                            from_date: String = "2020-03-01T00:00:00Z",
                            to_date: String = "2020-04-01T00:00:00Z",
                           ): Unit = {
    val output_dir = new java.io.File("./tmp_collectiontests/").getCanonicalPath
    Files.createDirectories(Paths.get(output_dir))
    val expected_dir = getClass.getResource("/org/openeo/geotrellis/integrationtests/collectiontests/expected/").getPath

    val vector_file = getClass.getResource("/org/openeo/geotrellis/integrationtests/collectiontests/"
      + (if (layerStr.contains("CGLS")) "cgls_test.json" else "50testfields.json")).getFile

    val targetCRS = if (layerStr.contains("CGLS")) LatLng else CRS.fromEpsgCode(32631)
    var polygons = ProjectedPolygons.fromVectorFile(vector_file)
    polygons = ProjectedPolygons.reproject(polygons, targetCRS)
    val from_date_parsed = ZonedDateTime.parse(from_date)
    val to_date_parsed = ZonedDateTime.parse(to_date)

    val datacubeParams = new DataCubeParameters()
    datacubeParams.layoutScheme = "FloatingLayoutScheme"
    datacubeParams.globalExtent = Some(polygons.extent)

    val layer: MultibandTileLayerRDD[SpaceTimeKey] = layerStr match {
      case "TERRASCOPE_S2_FAPAR_V2" =>
        val seqThing = faparPyramidFactory.datacube_seq(
          polygons,
          from_date,
          to_date,
          util.Collections.emptyMap[String, Any](),
          "correlationid",
          datacubeParams
        )
        val Seq((_, layer)) = seqThing
        layer
      case "TERRASCOPE_S2_NDVI_V2" =>
        val seqThing = LayerFixtures.ClearNDVIPyramid().datacube_seq(
          polygons,
          from_date,
          to_date,
          util.Collections.emptyMap[String, Any](),
          "correlationid",
          datacubeParams
        )
        val Seq((_, layer)) = seqThing
        layer
      case "TERRASCOPE_S2_TOC_V2" => LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(
        from_date_parsed,
        to_date_parsed,
        polygons.extent,
        1,
        sc
      )
      case "PROBAV_L3_S5_TOC_100M" =>
        val seqThing = new ProbaVPyramidFactoryTest().pyramidFactoryS5().datacube_seq(
          polygons,
          from_date,
          to_date,
          util.Collections.emptyMap[String, Any](),
          "correlationid",
          datacubeParams
        )
        val Seq((_, layer)) = seqThing
        layer
      case "PROBAV_L3_S10_TOC_333M" =>
        val seqThing = new ProbaVPyramidFactoryTest().pyramidFactoryS10.datacube_seq(
          polygons,
          from_date,
          to_date,
          util.Collections.emptyMap[String, Any](),
          "correlationid",
          datacubeParams
        )
        val Seq((_, layer)) = seqThing
        layer
      case "CGLS_DMP300_V1_GLOBAL" =>
        val bandNames = util.Arrays.asList("GDMP")
        val seqThing = cglsPyramidFactory("clms_global_gdmp_300m_v1_10daily_netcdf",bandNames).datacube_seq(
          polygons,
          from_date,
          to_date,
          util.Collections.emptyMap[String, Any](),
          "correlationid",
          datacubeParams
        )
        val Seq((_, layer)) = seqThing
        val wrapped = new OpenEOProcesses().wrapCube(layer)
        wrapped.openEOMetadata.setBandNames(bandNames)
        wrapped

      case _ => throw new IllegalStateException(s"Layer $layerStr not supported")
    }

    val file_name = s"${layerStr}_${from_date_parsed.toLocalDate}_${to_date_parsed.minusDays(1).toLocalDate}"
    new ComputeStatsGeotrellisAdapter().compute_generic_timeseries_from_datacube(
      "mean",
      layer,
      polygons,
      Paths.get(output_dir, file_name).toString,
    )

    val newPath = Paths.get(output_dir, file_name).toString + "/openEo.nc"
    val options = new NetCDFOptions
    options.setBandNames(new util.ArrayList(util.Arrays.asList("band")))
    NetCDFRDDWriter.writeRasters(layer, newPath, options)

    println("Outputted path: " + Paths.get(output_dir, file_name).toString)

    if (Files.exists(Paths.get(expected_dir, file_name))) {
      val groupedStats1 = AggregateSpatialTest.parseCSV(Paths.get(output_dir, file_name).toString).toList.sortBy(_._1)
      val groupedStats2 = AggregateSpatialTest.parseCSV(Paths.get(expected_dir, file_name).toString).toList.sortBy(_._1)
      groupedStats1.zip(groupedStats2).foreach(pair => {
        assertEquals(ZonedDateTime.parse(pair._1._1).toInstant, ZonedDateTime.parse(pair._2._1).toInstant)
        AggregateSpatialTest.assertEqualTimeseriesStats(pair._1._2, pair._2._2, 0.028)
      })
      println(groupedStats1.mkString("\n"))
    } else {
      println("Please copy output to expected path location. Example command:")
      println("cp " + Paths.get(output_dir, file_name).toString + " " + Paths.get(expected_dir, file_name).toString)
      assertTrue(false)
    }
  }
}
