package org.openeo.geotrellissentinelhub

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import geotrellis.proj4.LatLng
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkConf
import org.junit.{Ignore, Test}
import org.openeo.geotrellissentinelhub.bands.{Gamma0Bands, Sentinel2Bands}
import org.openeo.geotrellissentinelhub.bands.Gamma0Bands.{IW_VH, IW_VV}
import org.openeo.geotrellissentinelhub.bands.Sentinel2Bands.{B02, B03, B04, B08}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class PyramidFactoryTest {

  @Ignore
  @Test
  def testGamma0(): Unit = {
    val pyramidFactory = new Sentinel1Gamma0PyramidFactory(System.getProperty("uuid-gamma0"))
    
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 10, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
      
      val bandIndices = ArrayBuffer(IW_VH, IW_VV).map(Gamma0Bands.allBands.indexOf(_)).asJava
      
      val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
        ISO_OFFSET_DATE_TIME format from, ISO_OFFSET_DATE_TIME format to, bandIndices)
      
      val zoom = 14

      val baseLayer = pyramid
        .find { case (index, _) => index == zoom }
        .map { case (_, layer) => layer }
        .get.cache()

      println(s"got ${baseLayer.count()} tiles")

      val timestamps = baseLayer.keys
        .map(_.time)
        .distinct()
        .collect()
        .sortWith(_ isBefore _)

      for (timestamp <- timestamps) {
        val Raster(multibandTile, extent) = baseLayer
          .toSpatial(timestamp)
          .stitch()

        val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs)
        tif.write(s"/home/niels/pyramidFactory/gamma0/${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
      }
    } finally {
      sc.stop()
    }
  }

  @Ignore
  @Test
  def testSentinel2L1C(): Unit = {
    val pyramidFactory = new Sentinel2PyramidFactory(System.getProperty("uuid-sentinel2-L1C"))

    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

      val bandIndices = ArrayBuffer(B04, B03, B02).map(Sentinel2Bands.allBands.indexOf(_)).asJava

      val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
        ISO_OFFSET_DATE_TIME format from, ISO_OFFSET_DATE_TIME format to, bandIndices)

      val zoom = 14

      val baseLayer = pyramid
        .find { case (index, _) => index == zoom }
        .map { case (_, layer) => layer }
        .get.cache()

      println(s"got ${baseLayer.count()} tiles")

      val timestamps = baseLayer.keys
        .map(_.time)
        .distinct()
        .collect()
        .sortWith(_ isBefore _)

      for (timestamp <- timestamps) {
        val Raster(multibandTile, extent) = baseLayer
          .toSpatial(timestamp)
          .stitch()

        val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs)
        tif.write(s"/home/niels/pyramidFactory/sentinel2-L1C/${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
      }
    } finally {
      sc.stop()
    }
  }

  @Ignore
  @Test
  def testSentinel2L2A(): Unit = {
    val pyramidFactory = new Sentinel2PyramidFactory(System.getProperty("uuid-sentinel2-L2A"))

    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

      val bandIndices = ArrayBuffer(B08, B04, B03).map(Sentinel2Bands.allBands.indexOf(_)).asJava

      val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
        ISO_OFFSET_DATE_TIME format from, ISO_OFFSET_DATE_TIME format to, bandIndices)

      val zoom = 14

      val baseLayer = pyramid
        .find { case (index, _) => index == zoom }
        .map { case (_, layer) => layer }
        .get.cache()

      println(s"got ${baseLayer.count()} tiles")

      val timestamps = baseLayer.keys
        .map(_.time)
        .distinct()
        .collect()
        .sortWith(_ isBefore _)

      for (timestamp <- timestamps) {
        val Raster(multibandTile, extent) = baseLayer
          .toSpatial(timestamp)
          .stitch()

        val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs)
        tif.write(s"/home/niels/pyramidFactory/sentinel2-L2A/${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
      }
    } finally {
      sc.stop()
    }
  }
}
