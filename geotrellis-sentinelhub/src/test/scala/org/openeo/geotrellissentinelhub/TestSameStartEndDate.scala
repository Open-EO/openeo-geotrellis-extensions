package org.openeo.geotrellissentinelhub

import geotrellis.proj4.CRS
import geotrellis.raster.CellSize
import geotrellis.vector.{Extent, MultiPolygon}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, DataCubeParameters}

import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.Wrappers.SeqWrapper

class TestSameStartEndDate {

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret

  @Test
  def testSameStartEndDate(): Unit = {
    val extent = Extent(-55.8071, -6.7014, -55.7933, -6.6703)

    val bbox_srs = "EPSG:4326"

    val from = "2019-06-01T00:00:00Z"

    val to = "2019-06-01T00:00:00Z"

    val bandNames = Seq("VV", "VH", "HV", "HH").asJava

    implicit val sc = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("local[1]")
        .setAppName("TestSentinelHub")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "1024m"))

    val endpoint = "https://services.sentinel-hub.com"
    val pyramid = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint),
      new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(clientId, clientSecret),
      rateLimitingGuard = NoRateLimitingGuard)
      .pyramid_seq(extent, bbox_srs, from, to, bandNames, metadata_properties = util.Collections.emptyMap[String, util.Map[String, Any]])

    val (_, topLevelRdd) = pyramid.filter { case (zoom, _) => zoom == 14 }.head

    val results = topLevelRdd.collect()

    for {
      (_, multibandTile) <- results
      tile <- multibandTile.bands
    } assert(tile.isNoDataTile)
  }

  @Test
  def testMetadata(): Unit = {
    val extent = Extent(-55.8071, -6.7014, -55.7933, -6.6703)
    BatchJobMetadataTracker.setGlobalTracking(true)
    val from = "2019-06-01T00:00:00Z"
    val to = "2019-06-11T00:00:00Z"
    val bandNames = Seq("VV", "VH", "HV", "HH").asJava

    implicit val sc: SparkContext = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("local[1]")
        .setAppName("TestSentinelHub")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "1024m"))
    try {
      val endpoint = "https://services.sentinel-hub.com"
      val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint),
        new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(clientId, clientSecret),
        rateLimitingGuard = NoRateLimitingGuard)

      val multiPolygons = Array(MultiPolygon(extent.toPolygon()))
      val pyramid = pyramidFactory.datacube_seq(
        multiPolygons,
        CRS.fromEpsgCode(4326),
        from,
        to,
        bandNames,
        metadata_properties = util.Collections.emptyMap[String, util.Map[String, Any]],
        new DataCubeParameters,
      )
      println(pyramid.length)

      val inputs = BatchJobMetadataTracker.tracker("").asDict()
      val links = inputs.get("links")
      links match {
        case hm: util.HashMap[String, SeqWrapper[String]] =>
          val seq = hm.get("sentinel-1-grd")
          print(seq)
          seq.forEach(s => assert(s.startsWith("http")))
        case x =>
          throw new RuntimeException("Problem: " + x)
      }

    } finally sc.stop()
  }
}
