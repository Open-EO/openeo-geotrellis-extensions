package org.openeo.geotrellis

import geotrellis.layer.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.MultibandTile
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Polygon, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test}
import org.openeo.geotrellis.LayerFixtures._
import org.openeo.geotrellis.udf.Udf

import scala.reflect.io.{File, Path}

object AggregateSpatialWindowTest {
  type JMap[K, V] = java.util.Map[K, V]
  type JList[T] = java.util.List[T]

  private var sc: SparkContext = _

  def pythonEnvDefined(): Boolean = {
    sys.env.contains("LD_LIBRARY_PATH") && File(Path(sys.env.getOrElse("LD_LIBRARY_PATH", ""))).exists
  }

  @BeforeAll
  def setUpSpark(): Unit = {
    sc = {
      val conf = new SparkConf().set("spark.driver.bindAddress", "127.0.0.1")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, conf)
    }
  }

  @AfterAll
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
}

@EnabledIf("pythonEnvDefined")
class AggregateSpatialWindowTest() {

  import AggregateSpatialWindowTest._


  @BeforeEach
  def setup(): Unit = {
    //System.setProperty("pixels.treshold","" + threshold)

  }

  @Test
  def max_16x16_1_band(): Unit = {
    val minDateString = "2022-04-24T00:00:00Z"
    val maxDateString = "2022-04-30T02:00:00Z"

    val polygons = Seq(polygon2.reproject(LatLng, CRS.fromEpsgCode(32631)))
    val inputCube = s2_scl(minDateString, maxDateString, polygons, "EPSG:32631")

    val code =
      """
        |udf_reduce=np.max
        |""".stripMargin
    val outputCube = Udf.runUserCodeSpatialWindowReduce(dataCube = inputCube, window = (16, 16), code)

    val results = outputCube.collect()
    assertEquals("Array(" +
      "(SpaceTimeKey(0,0,1650844800000),ArrayMultibandTile(16,16,1,float32raw)), " +
      "(SpaceTimeKey(0,1,1650844800000),ArrayMultibandTile(16,16,1,float32raw)), " +
      "(SpaceTimeKey(1,1,1650844800000),ArrayMultibandTile(16,16,1,float32raw)), " +
      "(SpaceTimeKey(2,1,1650844800000),ArrayMultibandTile(16,16,1,float32raw)), " +
      "(SpaceTimeKey(1,0,1650844800000),ArrayMultibandTile(16,16,1,float32raw)), " +
      "(SpaceTimeKey(2,0,1650844800000),ArrayMultibandTile(16,16,1,float32raw)))", results.mkString("Array(", ", ", ")"))
  }

  @Test
  def min_32x32_1_band(): Unit = {
    val minDateString = "2022-04-24T00:00:00Z"
    val maxDateString = "2022-04-30T02:00:00Z"

    val polygons = Seq(polygon2.reproject(LatLng, CRS.fromEpsgCode(32631)))
    val inputCube = s2_scl(minDateString, maxDateString, polygons, "EPSG:32631")

    val code =
      """
        |udf_reduce=np.min
        |""".stripMargin
    val outputCube = Udf.runUserCodeSpatialWindowReduce(dataCube = inputCube, window = (32, 32), code)

    val results = outputCube.collect()
    assertEquals("Array(" +
      "(SpaceTimeKey(0,0,1650844800000),ArrayMultibandTile(8,8,1,float32raw)), " +
      "(SpaceTimeKey(0,1,1650844800000),ArrayMultibandTile(8,8,1,float32raw)), " +
      "(SpaceTimeKey(1,1,1650844800000),ArrayMultibandTile(8,8,1,float32raw)), " +
      "(SpaceTimeKey(2,1,1650844800000),ArrayMultibandTile(8,8,1,float32raw)), " +
      "(SpaceTimeKey(1,0,1650844800000),ArrayMultibandTile(8,8,1,float32raw)), " +
      "(SpaceTimeKey(2,0,1650844800000),ArrayMultibandTile(8,8,1,float32raw)))", results.mkString("Array(", ", ", ")"))
  }

  @Test
  def custom_32x32_2_bands(): Unit = {
    val polygons = Seq(polygon2.reproject(LatLng, CRS.fromEpsgCode(32631)))
    val inputCube = s2_ndvi_bands("2022-04-24T00:00:00Z", "2022-04-30T02:00:00Z", polygons, "EPSG:32631")

    val code =
      """
        |def custom(array_like, axis=None):
        |  return np.min(array_like, axis)
        |udf_reduce=custom
        |""".stripMargin
    val outputCube = Udf.runUserCodeSpatialWindowReduce(dataCube = inputCube, window = (32, 32), code)

    val results = outputCube.collect()
    assertEquals("Array(" +
      "(SpaceTimeKey(0,0,1650844800000),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpaceTimeKey(0,1,1650844800000),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpaceTimeKey(1,1,1650844800000),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpaceTimeKey(2,1,1650844800000),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpaceTimeKey(1,0,1650844800000),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpaceTimeKey(2,0,1650844800000),ArrayMultibandTile(8,8,2,float32raw)))", results.mkString("Array(", ", ", ")"))
  }

  @Test
  def custom_32x32_2_bands_spatial_only(): Unit = {
    val polygons = Seq(polygon2.reproject(LatLng, CRS.fromEpsgCode(32631)))
    val inputCube = s2_ndvi_bands("2022-04-24T00:00:00Z", "2022-04-30T02:00:00Z", polygons, "EPSG:32631")

    def pickFirst(first: MultibandTile, second: MultibandTile): MultibandTile = {
      first
    }

    val spatialCube: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = inputCube.toSpatialReduce(pickFirst)

    val code =
      """
        |def custom(array_like, axis=None):
        |  return np.min(array_like, axis)
        |udf_reduce=custom
        |""".stripMargin
    val outputCube = Udf.runUserCodeSpatialWindowReduce(dataCube = spatialCube, window = (32, 32), code)

    val results = outputCube.collect()
    assertEquals("Array(" +
      "(SpatialKey(0,1),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpatialKey(0,0),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpatialKey(2,1),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpatialKey(1,0),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpatialKey(2,0),ArrayMultibandTile(8,8,2,float32raw)), " +
      "(SpatialKey(1,1),ArrayMultibandTile(8,8,2,float32raw)))", results.mkString("Array(", ", ", ")"))
  }
}