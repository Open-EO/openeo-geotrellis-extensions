package org.openeo.geotrellis.file

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import geotrellis.proj4.LatLng
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkConf
import org.junit.{Assert, Ignore, Test}
import Sentinel2RadiometryPyramidFactory.Band._
import geotrellis.spark.SpaceTimeKey
import geotrellis.spark.partition.SpacePartitioner

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class Sentinel2RadiometryPyramidFactoryTest {

  private val pyramidFactory = new Sentinel2RadiometryPyramidFactory

  @Ignore
  @Test
  def writeGeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 3, 25), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusDays  2

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
      val bandIndices = ArrayBuffer(B02, B8A, B01).map(_.id).asJava

      val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
        DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to,
        bandIndices)

      val baseLayer = pyramid
        .find { case (index, _) => index == 14 }
        .map { case (_, layer) => layer }
        .get.cache()

      Assert.assertTrue(baseLayer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
      println(s"got ${baseLayer.count()} tiles")

      val timestamps = baseLayer.keys
        .map(_.time)
        .distinct()
        .collect()
        .sortWith(_ isBefore _)

      for (timestamp <- timestamps) {
        val Raster(multibandTile, extent) = baseLayer
          .toSpatial(timestamp)
          .crop(boundingBox.reproject(baseLayer.metadata.crs))
          .stitch()

        MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs)
          .write(s"/tmp/stitched_${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
      }
    } finally {
      sc.stop()
    }
  }
}
