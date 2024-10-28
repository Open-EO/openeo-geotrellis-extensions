package org.openeo.geotrellis.file

import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.{GeoTiffReader, MultibandGeoTiff}
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{CellSize, MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util
import scala.reflect.io.Directory

object ProbaVPyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)
  }

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class ProbaVPyramidFactoryTest extends RasterMatchers {
  private val openSearchEndpoint = "https://services.terrascope.be/catalogue"
  private val allTocBands: util.List[String] = util.Arrays.asList("NDVI", "RED", "NIR", "BLUE", "SWIR", "SZA", "SAA", "SWIRVAA", "SWIRVZA", "VNIRVAA", "VNIRVZA", "SM")

  def pyramidFactoryS5(bands: util.List[String] = allTocBands): ProbaVPyramidFactory = new ProbaVPyramidFactory(openSearchEndpoint, "urn:eop:VITO:PROBAV_S5_TOC_100M_COG_V2", bands, "/data/MTDA/PROBAV_C2/COG/PROBAV_L3_S5_TOC_100M", CellSize(0.000992063492063, 0.000992063492063))
  val pyramidFactoryS10 =  new ProbaVPyramidFactory(openSearchEndpoint, "urn:eop:VITO:PROBAV_S10_TOC_333M_COG_V2", allTocBands, "/data/MTDA/PROBAV_C2/COG/PROBAV_L3_S10_TOC_333M", CellSize(0.000992063492063, 0.000992063492063))
  private val pyramidFactoryS10NDVI = new ProbaVPyramidFactory(openSearchEndpoint, "urn:eop:VITO:PROBAV_S10_TOC_NDVI_333M_COG_V2", util.Arrays.asList("NDVI"), "/data/MTDA/PROBAV_C2/COG/PROBAV_L3_S10_TOC_NDVI_333M", CellSize(0.000992063492063, 0.000992063492063))

  @Test
  def writeS5GeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.591, ymax = 51.080), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusDays 2

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

    val pyramid = pyramidFactoryS5().pyramid_seq(boundingBox.extent, srs,
      DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to)

    val baseLayer = pyramid
      .find { case (zoom, _) => zoom == 11 }
      .map { case (_, layer) => layer }
      .get.cache()

    assertTrue(baseLayer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
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
  }

  @Test
  def writeS10GeoTiffs(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.5, ymin = 49.5, xmax = 2.55, ymax = 49.55), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 8, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusDays 2

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

    val pyramid = pyramidFactoryS10.pyramid_seq(boundingBox.extent, srs,
      DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to)

    val baseLayer = pyramid
      .find { case (zoom, _) => zoom == 9 }
      .map { case (_, layer) => layer }
      .get.cache()

    assertTrue(baseLayer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
    println(s"got ${baseLayer.count()} tiles")
    val cropBounds = boundingBox.reproject(baseLayer.metadata.crs)
    val timestampedFiles = org.openeo.geotrellis.geotiff.saveRDDTemporal(baseLayer,"./",cropBounds = Some(cropBounds))
    assertEquals(1, timestampedFiles.size())
    val (fileName, timestamp, bbox) = timestampedFiles.get(0)
    assertEquals("2019-08-01T00:00:00Z", timestamp)
    assertTrue(bbox.equalsExact(cropBounds, 0.1), s"actual $bbox does not equal expected $cropBounds")
    val tiff = GeoTiffReader.readMultiband(fileName)
    assertEquals(LatLng, tiff.crs)
    assertEquals(allTocBands.size(), tiff.bandCount)
  }

  @Test
  def writeS10NDVIGeoTiffs(): Unit = {
    val outDir = Paths.get("tmp/writeS10NDVIGeoTiffs/")
    new Directory(outDir.toFile).deepFiles.foreach(_.delete())
    Files.createDirectories(outDir)

    val boundingBox = ProjectedExtent(Extent(xmin = 2.5, ymin = 49.5, xmax = 2.55, ymax = 49.55), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 8, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusDays 2

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

    val pyramid = pyramidFactoryS10NDVI.pyramid_seq(boundingBox.extent, srs,
      DateTimeFormatter.ISO_OFFSET_DATE_TIME format from, DateTimeFormatter.ISO_OFFSET_DATE_TIME format to)

    val baseLayer = pyramid
      .find { case (zoom, _) => zoom == 9 }
      .map { case (_, layer) => layer }
      .get.cache()

    assertTrue(baseLayer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
    println(s"got ${baseLayer.count()} tiles")
    val cropBounds = boundingBox.reproject(baseLayer.metadata.crs)
    val timestampedFiles = org.openeo.geotrellis.geotiff.saveRDDTemporal(baseLayer,outDir.toString,cropBounds = Some(cropBounds))
    assertEquals(1, timestampedFiles.size())
    val (fileName, timestamp, bbox) = timestampedFiles.get(0)
    assertEquals("2019-08-01T00:00:00Z", timestamp)
    assertTrue(bbox.equalsExact(cropBounds, 0.1), s"actual $bbox does not equal expected $cropBounds")
    val tiff = GeoTiffReader.readMultiband(fileName)
    assertEquals(LatLng, tiff.crs)
    assertEquals(1, tiff.bandCount)
  }

  @Test
  def testResultReflectsBandsOrder(): Unit = {
    val (raster1, _) = s5Raster(bands = util.Arrays.asList("SWIRVAA", "NDVI", "SWIRVZA"))
    val (raster2, _) = s5Raster(bands = util.Arrays.asList("SWIRVAA", "SWIRVZA", "NDVI"))

    assertEquals(raster1.tile.band(0), raster2.tile.band(0))
    assertEquals(raster1.tile.band(1), raster2.tile.band(2))
    assertEquals(raster1.tile.band(2), raster2.tile.band(1))
  }

  @Test
  def testRequestDuplicateBand(): Unit = {
    val (raster, _) = s5Raster(bands = util.Arrays.asList("NDVI", "NDVI"))

    assertTrue(raster.tile.bandCount > 0)

    // not sure what the right behavior should be but one band with data and the other one without must be wrong
    for (bandIndex <- 0 until raster.tile.bandCount) {
      assertFalse(raster.tile.band(bandIndex).isNoDataTile)
    }
  }

  @Test
  def compareS5ReferenceImage(): Unit = {
    val (referenceRaster, referenceCrs) = this.referenceRaster("PROBAV_S5_20200101.tif")

    val bandMix = util.Arrays.asList(
      "SM", "NDVI", "VNIRVZA", "RED", "VNIRVAA", "NIR", "SWIRVZA", "BLUE", "SWIRVAA", "SWIR", "SAA", "SZA")
    val (actualRaster, actualCrs) = s5Raster(bandMix)

    assertEqual(referenceRaster, actualRaster)
    assertEquals(referenceCrs, actualCrs)
  }

  private def referenceRaster(name: String): (Raster[MultibandTile], CRS) = {
    // TODO: get it from Artifactory instead?
    val referenceGeoTiff = MultibandGeoTiff(s"/data/projects/OpenEO/automated_test_files/$name")
    (referenceGeoTiff.raster, referenceGeoTiff.crs)
  }

  private def s5Raster(bands: util.List[String]): (Raster[MultibandTile], CRS) = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.56003, ymin = 51.039, xmax = 2.632, ymax = 51.110), LatLng)
    val date = ZonedDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

    val pyramid = pyramidFactoryS5(bands).pyramid_seq(boundingBox.extent, srs,
      DateTimeFormatter.ISO_OFFSET_DATE_TIME format date, DateTimeFormatter.ISO_OFFSET_DATE_TIME format date)

    val Some((_, baseLayer)) = pyramid
      .find { case (zoom, _) => zoom == 11 }

    val crs = baseLayer.metadata.crs

    val raster = baseLayer
      .toSpatial()
      .stitch()
      .crop(boundingBox.reproject(crs))

    (raster, crs)
  }
}
