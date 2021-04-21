import geotrellis.spark.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Ignore, Test}
import org.openeo.geotrellisseeder.{Band, TileSeeder}


object WorldCoverLayerTest {
  private implicit var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m")

    sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, sparkConf)
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    sc.stop()
  }
}

@Ignore
class WorldCoverLayerTest {

  import WorldCoverLayerTest._
  @Test
  def testWorldCover(): Unit = {

    new TileSeeder(5, false, Option.empty)
      .renderPng("/tmp/tiles/WorldCover", "", "1970-01-01", Some("worldcover.txt"), Option.empty,
        Some("/data/worldcover/runs/tenpercent/10percent_expert/v3/latlon/mexico/ESA_WorldCover_10m_2019_v100_N21W091_Map.tif"),
        datePattern = Some("yyyy/yyyyMMdd"))
  }
}
