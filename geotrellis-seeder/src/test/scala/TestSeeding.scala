import geotrellis.layer.SpatialKey
import geotrellis.raster.resample.Mode
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import org.openeo.geotrellisseeder.TileSeeder

import java.lang.String.join

class TestSeeding {

  @Test
  def testSeeding(): Unit = {
    implicit val sc = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("local[2]")
        .setAppName(join(":", "GeotrellisSeeder", "MAXD", "2020-01-01"))
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "1024m"))

//    for {
//      x <- 33 to 36
//      y <- 20 to 22
//    } {
//      new TileSeeder(6, false, None, true).renderPng("/tmp/seeding/MAXD-S2", "MAXD", "2020-01-01", Some("hrvpp_dates_2020.txt"), None, spatialKey = Some(SpatialKey(x, y)),
//        oscarsEndpoint = Some("http://oscars-1.hrvpp.vgt.vito.be:8080"), oscarsCollection = Some("copernicus_r_utm-wgs84_10_m_hrvpp-vpp_p_2017-ongoing_v01_r01"), oscarsSearchFilters = Some(Map("productGroupId" -> "s2")))
//    }

    new TileSeeder(6, false, None).renderPng("/tmp/seeding/QFLAG2", "QFLAG2", "2019-02-15", Some("hrvpp_qflag2.txt"), None, spatialKey = Some(SpatialKey(31, 19)),
      oscarsEndpoint = Some("http://oscars-1.hrvpp.vgt.vito.be:8080"), oscarsCollection = Some("copernicus_r_utm-wgs84_10_m_hrvpp-vi_p_2017-ongoing_v01_r01"))
  }

}
