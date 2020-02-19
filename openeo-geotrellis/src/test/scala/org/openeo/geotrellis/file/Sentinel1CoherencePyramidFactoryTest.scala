package org.openeo.geotrellis.file

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkConf
import org.junit.{Assert, Ignore, Test}
import org.openeo.geotrellis.file.Sentinel1CoherencePyramidFactory.Sentinel1Bands._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class Sentinel1CoherencePyramidFactoryTest {

  private val pyramidFactory = new Sentinel1CoherencePyramidFactory

  @Ignore
  @Test
  def writeGeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 5, ymin = 50, xmax = 5.2, ymax = 50.2), LatLng)
    val date = ZonedDateTime.of(LocalDate.of(2018, 8, 18), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
      val bandIndices = ArrayBuffer(VV, VH).map(allBands.indexOf).asJava

      val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
        DateTimeFormatter.ISO_OFFSET_DATE_TIME format date, DateTimeFormatter.ISO_OFFSET_DATE_TIME format date,
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
