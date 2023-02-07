package org.openeo.geotrellis.collectiontests

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.WebMercator
import geotrellis.raster.CellSize
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.spark.util.SparkUtils
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.file.PyramidFactory
import org.openeo.geotrellis.{ComputeStatsGeotrellisAdapterTest, LayerFixtures, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient

import java.net.URL
import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime
import java.util
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.junit.Assert.{assertFalse, assertTrue}

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
      val config = new HdfsConfiguration
      //config.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(config)

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

  @BeforeClass
  def setUpSpark_BeforeClass(): Unit = sc

  @BeforeAll
  def setUpSpark_BeforeAll(): Unit = sc

  var gotAfterAll = false

  @AfterAll
  def tearDownSpark_AfterAll(): Unit = {
    gotAfterAll = true
    maybeStopSpark()
  }

  var gotAfterClass = false

  @AfterClass
  def tearDownSpark_AfterClass(): Unit = {
    gotAfterClass = true
    maybeStopSpark()
  }

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
}

class CollectionTests {

  import CollectionTests._

  private def get_layers() =
    List(
      //      "TERRASCOPE_S1_SLC_COHERENCE_V1",
      //      "TERRASCOPE_S1_GAMMA0_V1",
      "TERRASCOPE_S2_FAPAR_V2", // OK
      //      "TERRASCOPE_S2_NDVI_V2", // OK
      //      "TERRASCOPE_S2_LAI_V2",
      //      "TERRASCOPE_S2_FCOVER_V2",
      //      "TERRASCOPE_S2_TOC_V2", // OK
      //      "S1_GRD_SIGMA0_ASCENDING",
      //      "S1_GRD_SIGMA0_DESCENDING",
      //      "PROBAV_L3_S5_TOC_100M",
      //      "PROBAV_L3_S10_TOC_333M",
      //      "COPERNICUS_30",
      //      "COPERNICUS_90",
      //      "CGLS_FAPAR_V2_GLOBAL",
      //      "CGLS_LAI_V2_GLOBAL",
      //      "CGLS_LAI300_V1_GLOBAL"
    )


  val opensearchEndpoint = "https://services.terrascope.be/catalogue"

  def faparPyramidFactory: PyramidFactory = {
    val openSearchClient = OpenSearchClient(new URL(opensearchEndpoint), isUTM = true)
    new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      openSearchLinkTitles = util.Collections.singletonList("FAPAR_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
      maxSpatialResolution = CellSize(10, 10)
    )
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

  @Test
  def tester(): Unit = {
    test_layer()
  }

  private def test_layer(input_file: Option[String] = None,
                         from_date: String = "2020-03-01T00:00:00Z",
                         to_date: String = "2020-03-31T00:00:00Z",
                         output_dir: Option[String] = None,
                         expected_dir: Option[String] = None,
                        ) = {


    val output_dir_get = output_dir.getOrElse("tmp_collectiontests/")
    Files.createDirectories(Paths.get(output_dir_get))

    val expected_dir_get = expected_dir.getOrElse(input_file.getOrElse(getClass.getResource("/org/openeo/geotrellis/collectiontests/expected/").getPath))


    get_layers().map(layerStr => {
      val vector_file = input_file.getOrElse(getClass.getResource("/org/openeo/geotrellis/collectiontests/"
        + (if (layerStr.contains("CGLS")) "cgls_test.json" else "50testfields.json")).getFile)
      var polygons = ProjectedPolygons.fromVectorFile(vector_file)
      polygons = ProjectedPolygons.reproject(polygons, WebMercator)
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
        case _ => throw new IllegalStateException(s"Layer $layerStr not supported")
      }
      val file_name = s"expected_${layerStr}_${from_date_parsed.toLocalDate}_${to_date_parsed.toLocalDate}.json"
      new ComputeStatsGeotrellisAdapterTest(1).computeStatsGeotrellisAdapter.compute_average_timeseries_from_datacube(
        layer,
        polygons,
        from_date,
        to_date,
        band_index = 0,
        output_file = output_dir_get + file_name
      )

      println("Outputted JSON: " + output_dir_get + file_name)
      val json1 = fileToJSON(output_dir_get + file_name)
      val json2 = fileToJSON(expected_dir_get + file_name)

      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      // Only keeps 1 band values in JSON
      def pruneCopyJsonObject(json: JsonNode, depth: Int = 0): JsonNode = {
        if (!json.isContainerNode) {
          json
        } else {
          if (json.isObject) {
            val map = json.fields.asScala.flatMap { entry =>
              if (depth >= 1)
                List()
              else
                List(entry.getKey -> pruneCopyJsonObject(entry.getValue, depth + 1))
            }.toMap
            mapper.valueToTree(map)
          } else {
            val map = json.elements().asScala.zipWithIndex.flatMap { entry =>
              if ((depth >= 2 && entry._2 >= 1) || entry._1.isNull)
                List()
              else
                List(pruneCopyJsonObject(entry._1, depth + 1))
            }.toList
            mapper.valueToTree(map)
          }
        }
      }

      val prunedJson1 = pruneCopyJsonObject(json1)
      val prunedJson2 = pruneCopyJsonObject(json2)
      println("pruneCopyJsonObject: " + mapper.writeValueAsString(json1))
      println("pruneCopyJsonObject: " + mapper.writeValueAsString(json2))
      println("pruneCopyJsonObject: " + mapper.writeValueAsString(prunedJson1))
      println("pruneCopyJsonObject: " + mapper.writeValueAsString(prunedJson2))


      val isEqual = jsonEquals(prunedJson1, prunedJson2, 3)
      assertTrue(isEqual)
    })
  }
}
