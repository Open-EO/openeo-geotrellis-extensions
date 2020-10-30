package org.openeo.geotrellissentinelhub

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import geotrellis.proj4.util.UTM
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkConf
import org.junit.Test

import scala.collection.JavaConverters._

class PyramidFactoryTest {

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret

  @Test
  def testGamma0(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2019, 10, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new PyramidFactory("S1GRD", clientId, clientSecret), "gamma0", date, Seq("VV", "VH"))
  }

  @Test
  def testSentinel2L1C(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new PyramidFactory("S2L1C", clientId, clientSecret), "sentinel2-L1C", date, Seq("B04", "B03", "B02"))
  }

  @Test
  def testSentinel2L2A(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new PyramidFactory("S2L2A", clientId, clientSecret), "sentinel2-L2A", date, Seq("B08", "B04", "B03"))
  }

  @Test
  def testLandsat8(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 22), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new PyramidFactory("L8L1C", clientId, clientSecret), "landsat8", date, Seq("B10", "B11"))
  }

  @Test
  def testDigitalNumbersOutput(): Unit = { // TODO: check output values programmatically
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new PyramidFactory("S2L2A", clientId, clientSecret), "sentinel2-L2A_mix", date, Seq("B04", "sunAzimuthAngles", "SCL"))
  }
  
  private def testLayer(pyramidFactory: PyramidFactory, layer: String, date: ZonedDateTime, bandNames: Seq[String]): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

      val isoDate = ISO_OFFSET_DATE_TIME format date
      val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs, isoDate, isoDate, bandNames.asJava)

      val (zoom, baseLayer) = pyramid
        .maxBy { case (zoom, _) => zoom }

      baseLayer.cache()

      println(s"got ${baseLayer.count()} tiles")

      val Raster(multibandTile, extent) = baseLayer
        .toSpatial()
        .crop(boundingBox.reproject(baseLayer.metadata.crs).extent)
        .stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs)
      tif.write(s"/tmp/${layer}_${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format date}.tif")
    } finally {
      sc.stop()
    }
  }

  @Test
  def testUtm(): Unit = {
    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
      val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

      val utmBoundingBox = {
        val center = boundingBox.extent.center
        val utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)
        ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)
      }

      val pyramidFactory = new PyramidFactory("S2L2A", clientId, clientSecret)

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmBoundingBox.crs,
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B08", "B04", "B03").asJava
      )

      val spatialLayer = layer
        .toSpatial()
        .crop(utmBoundingBox.extent)
        .cache()

      val Raster(multibandTile, extent) = spatialLayer.stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs)
      tif.write(s"/tmp/utm.tif")
    } finally sc.stop()
  }
}
