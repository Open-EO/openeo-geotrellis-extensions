package org.openeo.geotrellis.file

import geotrellis.layer._
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.{GeoTiffReader, MultibandGeoTiff}
import geotrellis.raster.{CellSize, Raster}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkConf
import org.junit.{Assert, Ignore, Test}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util

class ProbaVPyramidFactoryTest {
  val openSearchEndpoint = "https://services.terrascope.be/catalogue"
  val bands: util.List[String] = util.Arrays.asList("NDVI", "RED", "NIR", "BLUE", "SWIR", "SZA", "SAA", "SWIRVAA", "SWIRVZA", "VNIRVAA", "VNIRVZA", "SM")
  val pyramidFactoryS5 = new ProbaVPyramidFactory(openSearchEndpoint, "urn:eop:VITO:PROBAV_S5_TOC_100M_COG_V2", bands,  "/data/MTDA/PROBAV_C2/COG/PROBAV_L3_S5_TOC_100M", CellSize(0.000992063492063, 0.000992063492063))
  val pyramidFactoryS10 =  new ProbaVPyramidFactory(openSearchEndpoint, "urn:eop:VITO:PROBAV_S10_TOC_333M_COG_V2", bands, "/data/MTDA/PROBAV_C2/COG/PROBAV_L3_S10_TOC_333M", CellSize(0.000992063492063, 0.000992063492063))
  val pyramidFactoryS10NDVI = new ProbaVPyramidFactory(openSearchEndpoint, "urn:eop:VITO:PROBAV_S10_TOC_NDVI_333M_COG_V2", util.Arrays.asList("NDVI"), "/data/MTDA/PROBAV_C2/COG/PROBAV_L3_S10_TOC_NDVI_333M", CellSize(0.000992063492063, 0.000992063492063))

  @Test
  def writeS5GeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.591, ymax = 51.080), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusDays 2

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

      val pyramid = pyramidFactoryS5.pyramid_seq(boundingBox.extent, srs,
        DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to)

      val baseLayer = pyramid
        .find { case (index, _) => index == 11 }
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
          .write(s"/tmp/stitched_S5_${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
      }
    } finally {
      sc.stop()
    }
  }

  @Test
  def writeS10GeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.5, ymin = 49.5, xmax = 2.55, ymax = 49.55), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 8, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusDays 2

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

      val pyramid = pyramidFactoryS10.pyramid_seq(boundingBox.extent, srs,
        DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to)

      val baseLayer = pyramid
        .find { case (index, _) => index == 9 }
        .map { case (_, layer) => layer }
        .get.cache()

      Assert.assertTrue(baseLayer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
      println(s"got ${baseLayer.count()} tiles")
      val cropBounds = boundingBox.reproject(baseLayer.metadata.crs)
      val timestampedFiles = org.openeo.geotrellis.geotiff.saveRDDTemporal(baseLayer,"./",cropBounds = Some(cropBounds))
      Assert.assertEquals(1, timestampedFiles.size())
      val (fileName, timestamp, bbox) = timestampedFiles.get(0)
      Assert.assertEquals("2019-08-01T00:00:00Z", timestamp)
      Assert.assertTrue(s"actual $bbox does not equal expected $cropBounds", bbox.equalsExact(cropBounds, 0.1))
      val tiff = GeoTiffReader.readMultiband(fileName)
      Assert.assertEquals(LatLng, tiff.crs)
      Assert.assertEquals(bands.size(), tiff.bandCount)
    } finally {
      sc.stop()
    }
  }

  @Test
  def writeS10NDVIGeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.5, ymin = 49.5, xmax = 2.55, ymax = 49.55), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 8, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusDays 2

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

      val pyramid = pyramidFactoryS10NDVI.pyramid_seq(boundingBox.extent, srs,
        DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to)

      val baseLayer = pyramid
        .find { case (index, _) => index == 9 }
        .map { case (_, layer) => layer }
        .get.cache()

      Assert.assertTrue(baseLayer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
      println(s"got ${baseLayer.count()} tiles")
      val cropBounds = boundingBox.reproject(baseLayer.metadata.crs)
      val timestampedFiles = org.openeo.geotrellis.geotiff.saveRDDTemporal(baseLayer,"./",cropBounds = Some(cropBounds))
      Assert.assertEquals(1, timestampedFiles.size())
      val (fileName, timestamp, bbox) = timestampedFiles.get(0)
      Assert.assertEquals("2019-08-01T00:00:00Z", timestamp)
      Assert.assertTrue(s"actual $bbox does not equal expected $cropBounds", bbox.equalsExact(cropBounds, 0.1))
      val tiff = GeoTiffReader.readMultiband(fileName)
      Assert.assertEquals(LatLng, tiff.crs)
      Assert.assertEquals(1, tiff.bandCount)
    } finally {
      sc.stop()
    }
  }

}
