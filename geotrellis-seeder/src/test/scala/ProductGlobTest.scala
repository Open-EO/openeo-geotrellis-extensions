import geotrellis.spark.SpatialKey
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Ignore, Test}
import org.openeo.geotrellisseeder.{Band, TileSeeder}

class ProductGlobTest {
  
  @Test
  @Ignore
  def testProductGlob(): Unit = {
    implicit val sc = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("ProductGlobTest")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "1024m"))
    
    val rootPath = "/home/niels/mtdadev/tiles_key"
    val productType = "CGS_MTDA_DEV"
    val date = "2019-09-07"
    val bands = Some(Array(Band("B04", 200, 1600), Band("B03", 200, 1600), Band("B02", 200, 1600)))

    val productGlob = Some("/home/niels/mtdadev/#DATE#/*/S2*_TOC-#BAND#_10M_V110.tif")
//    val productGlob = Some("/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/#DATE#/*/*/S2*_TOC-#BAND#_10M_V102.tif") 
    val maskValues = Some(Array(21000))
    val key = Some(SpatialKey(1071, 656))
    
    new TileSeeder(11, 50, false)
      .renderPng(rootPath, productType, date, None, bands, productGlob, maskValues, key)
  }

}
