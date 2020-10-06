package org.openeo.geotrellissentinelhub

import java.util

import geotrellis.vector.Extent
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Ignore, Test}

class TestSameStartEndDate {

  @Test
  @Ignore
  def testSameStartEndDate(): Unit = {
    val extent = Extent(-55.8071, -6.7014, -55.7933, -6.6703)
    
    val bbox_srs = "EPSG:4326"
    
    val from = "2019-07-26T00:00:00Z"
    
    val to = "2019-07-26T00:00:00Z"
    
    val bands = util.Arrays.asList(0, 1, 2, 3)
    
    implicit val sc = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("local[1]")
        .setAppName("TestSentinelHub")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "1024m"))

    val pyramid = new S1PyramidFactory().pyramid_seq(extent, bbox_srs, from, to, bands)

    val topLevelRdd = pyramid.filter(r => r._1 == 14).head._2
    
    val results = topLevelRdd.collect()

    for {
      r <- results
      b <- r._2.bands
    } assert(b.isNoDataTile)
  }
}
