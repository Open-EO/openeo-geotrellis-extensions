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
import org.openeo.geotrellissentinelhub.bands.Band
import org.openeo.geotrellissentinelhub.bands.Sentinel1Bands.{IW_VH, IW_VV}
import org.openeo.geotrellissentinelhub.bands.Sentinel2Bands.{B02, B03, B04, B08}

import scala.collection.JavaConverters._

class PyramidFactoryTest {

  @Ignore
  @Test
  def testGamma0(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2019, 10, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new S1PyramidFactory(System.getProperty("uuid-gamma0")), "gamma0", date, Seq(IW_VV, IW_VH))
  }

  @Ignore
  @Test
  def testSentinel2L1C(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new S2PyramidFactory(System.getProperty("uuid-sentinel2-L1C")), "sentinel2-L1C", date, Seq(B04, B03, B02))
  }

  @Ignore
  @Test
  def testSentinel2L2A(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new S2PyramidFactory(System.getProperty("uuid-sentinel2-L2A")), "sentinel2-L2A", date, Seq(B08, B04, B03))
  }
  
  def testLayer[P <: Band](pyramidFactory: PyramidFactory[P], layer: String, date: ZonedDateTime, bands: Seq[P]): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

      val bandIndices = bands.map(pyramidFactory.allBands.indexOf(_)).asJava

      val isoDate = ISO_OFFSET_DATE_TIME format date
      val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs, isoDate, isoDate, bandIndices)

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
        tif.write(s"/home/niels/pyramidFactory/$layer/${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
      }
    } finally {
      sc.stop()
    }
  }
}
