import geotrellis.spark.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Ignore, Test}
import org.openeo.geotrellisseeder.{Band, TileSeeder}

object ProbaVLayerTest {
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
class ProbaVLayerTest {

  import ProbaVLayerTest._

  @Test
  def testProbaVRadiometry(): Unit = {

    val bands = Array(
      Band("RADIOMETRY", 0, 0, 1200),
      Band("RADIOMETRY", 1, 150, 1400),
      Band("RADIOMETRY", 2, 150, 1400)
    )

    new TileSeeder(7, false, Option.empty)
      .renderPng("/tmp/tiles/PROBAV/RADIOMETRY", "", "2019-08-01", Option.empty, Some(bands),
        Some("/data/MTDA/TIFFDERIVED/PROBAV_L3_S5_TOC_100M/#DATE#/PROBAV_S5_TOC_*_100M_V101/PROBAV_S5_TOC_*_100M_V101_#BAND#.tif"),
        datePattern = Some("yyyy/yyyyMMdd"))
  }

  @Test
  def testProbaVNDVI(): Unit = {

    new TileSeeder(7, false, Option.empty)
      .renderPng("/tmp/tiles/PROBAV/NDVI", "", "2019-08-01", Some("ColorTable_NDVI_PROBAV.sld"), Option.empty,
        Some("/data/MTDA/TIFFDERIVED/PROBAV_L3_S5_TOC_100M/#DATE#/PROBAV_S5_TOC_*_100M_V101/PROBAV_S5_TOC_*_100M_V101_NDVI.tif"),
        datePattern = Some("yyyy/yyyyMMdd"))
  }
}
