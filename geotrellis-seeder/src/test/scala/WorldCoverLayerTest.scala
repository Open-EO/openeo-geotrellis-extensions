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
  def testWorldCoverMap(): Unit = {
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

  @Test
  def testWorldCoverRaster(): Unit = {
    val bands = Array(
      Band("S2RGBNIR", 0, 0, 10000),
      Band("S2RGBNIR", 1, 0, 10000),
      Band("S2RGBNIR", 2, 0, 10000)
    )

    new TileSeeder(14, false, Option.empty)
      .renderPng(
        path = "/tmp/tiles/WorldCover",
        productType = "S2RGBNIR",
        dateStr = "",
        bands = Some(bands),
        productGlob = Some("/data/MTDA/WORLDCOVER/ESA_WORLDCOVER_10M_2020_V100/S2RGBNIR/ESA_WorldCover_10m_2020_v100_S79W178_S2RGBNIR.tif")
      )
  }
}
