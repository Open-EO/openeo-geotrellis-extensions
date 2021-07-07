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
      .renderPng(
        path = "/tmp/tiles/WorldCover",
        productType = "Map",
        dateStr = "",
        colorMap = Some("worldcover.txt"),
        oscarsEndpoint = Some("https://oscars-dev.vgt.vito.be"),
        oscarsCollection = Some("urn:eop:VITO:ESA_WorldCover_10m_2020_V1")
      )
  }
}
