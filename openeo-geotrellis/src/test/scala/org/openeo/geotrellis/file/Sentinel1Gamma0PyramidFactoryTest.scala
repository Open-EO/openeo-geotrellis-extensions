package org.openeo.geotrellis.file

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
import org.openeo.geotrellissentinelhub.Gamma0Bands.{IW_VH, IW_VV, gamma0Bands}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class Sentinel1Gamma0PyramidFactoryTest {

  private val pyramidFactory = new Sentinel1Gamma0PyramidFactory

  @Ignore
  @Test
  def writeGeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 3, 25), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
      val bandIndices = ArrayBuffer(IW_VH, IW_VV).map(gamma0Bands.indexOf(_)).asJava

      val uuid = System.getProperty("uuid")
      
      val pyramid = pyramidFactory.pyramid_seq(uuid, 
        boundingBox.extent, srs,
        ISO_OFFSET_DATE_TIME format from, ISO_OFFSET_DATE_TIME format to,
        bandIndices)
      
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

        MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs)
          .write(s"/home/niels/gamma0/${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
      }
    } finally {
      sc.stop()
    }
  }
}
