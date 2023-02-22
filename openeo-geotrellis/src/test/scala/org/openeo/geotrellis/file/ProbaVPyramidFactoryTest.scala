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
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.file.ProbaVPyramidFactory.Band._
import org.openeo.geotrelliscommon.DataCubeParameters

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class ProbaVPyramidFactoryTest {

  val openSearchEndpoint = "https://services.terrascope.be/catalogue"
  val pyramidFactoryS5 = new ProbaVPyramidFactory(openSearchEndpoint, "urn:ogc:def:EOP:VITO:PROBAV_S5-TOC_100M_V001", "/data/MTDA/TIFFDERIVED/PROBAV_L3_S5_TOC_100M",CellSize(0.000992063492063, 0.000992063492063))
  val pyramidFactoryS10 =  new ProbaVPyramidFactory(openSearchEndpoint, "urn:ogc:def:EOP:VITO:PROBAV_S10-TOC_333M_V001", "/data/MTDA/TIFFDERIVED/PROBAV_L3_S10_TOC_333M",CellSize(0.000992063492063, 0.000992063492063))

  @Test
  def writeReadMultibandTileLayer(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 20.8949, ymax = 51.2206), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val date = from

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
      val bandIndices = ArrayBuffer(RED, NIR, BLUE).map(_.id).asJava

      val parameters = new DataCubeParameters
      parameters.setLayoutScheme("FloatingLayoutScheme")
      val pyramid = pyramidFactoryS5.datacube_seq(
        ProjectedPolygons(Seq(boundingBox.extent.toPolygon(), boundingBox.extent.toPolygon(), boundingBox.extent.toPolygon()), srs),
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        metadata_properties = Collections.emptyMap[String, Any],
        correlationId = "correlationId",
        dataCubeParameters = parameters,
        band_indices = bandIndices,
      )
      println(pyramid.size)
    } finally {
      Thread.sleep(999999999)
      sc.stop()
    }
  }

  @Ignore
  @Test
  def writeS5GeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusDays 2

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
      val bandIndices = ArrayBuffer(RED, NIR, BLUE).map(_.id).asJava

      val pyramid = pyramidFactoryS5.pyramid_seq(boundingBox.extent, srs,
        DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to,
        bandIndices)

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

        Assert.assertTrue(multibandTile.bandCount == bandIndices.size())

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
      val bandIndices = ArrayBuffer(NDVI, SWIR, NIR, SAA, SZA, SM).map(_.id).asJava

      val pyramid = pyramidFactoryS10.pyramid_seq(boundingBox.extent, srs,
        DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to,
        bandIndices)

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
      Assert.assertEquals(LatLng,tiff.crs)
      Assert.assertEquals(6,tiff.bandCount)
    } finally {
      sc.stop()
    }
  }

}
