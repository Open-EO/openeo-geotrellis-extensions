package geopyspark.geotrellis

import geotrellis.layer.{KeyBounds, SpaceTimeKey, TemporalKey}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.openeo.geotrellis.LayerFixtures
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZonedDateTime

object TestTiledRasterLayer{
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[TestTiledRasterLayer])

  var sc: SparkContext = _

  @BeforeClass
  def setupSpark() = {
    sc = {
      val conf = new SparkConf().setMaster("local[8]").setAppName(getClass.getSimpleName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName)
        .set("spark.ui.enabled", "true")
      SparkContext.getOrCreate(conf)
    }
    if (sc.uiWebUrl.isDefined) logger.info("Spark uiWebUrl: " + sc.uiWebUrl.get)
  }

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}


class TestTiledRasterLayer {

  @Test
  def testAddTemporal():Unit= {
    val (rdd,_) = LayerFixtures.aSpacetimeTileLayerRdd(10,10,5)
    val bounds = rdd.metadata.bounds.get
    val tempKey = TemporalKey(ZonedDateTime.parse("2017-01-01T00:00:00Z"))
    val temporalLayer = TemporalTiledRasterLayer(1,rdd)
    val spatialLayer = temporalLayer.toSpatialLayer()
    val spatialWithTemp = spatialLayer.addTemporal(tempKey)
    spatialWithTemp.rdd.map(p => {
      assert(p._1.isInstanceOf[SpaceTimeKey])
      assertEquals(tempKey,p._1.temporalKey)
    }).collect
    val temporalWithTemp = temporalLayer.addTemporal(tempKey)
    temporalWithTemp.rdd.map(p => {
      assert(p._1.isInstanceOf[SpaceTimeKey])
      assertEquals(tempKey,p._1.temporalKey)
    }).collect
    val boundsWithTemp = KeyBounds[SpaceTimeKey](SpaceTimeKey(bounds.minKey.spatialKey,tempKey),SpaceTimeKey(bounds.maxKey.spatialKey,tempKey))
    val metadataWithTemp = rdd.metadata.copy(bounds = boundsWithTemp)
    assertEquals(metadataWithTemp,spatialWithTemp.rdd.metadata)
    assertEquals(metadataWithTemp,temporalWithTemp.rdd.metadata)

  }
}
