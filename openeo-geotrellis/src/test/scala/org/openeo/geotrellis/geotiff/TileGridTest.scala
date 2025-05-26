package org.openeo.geotrellis.geotiff

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.{GeoTiff, Tiled}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel.DISK_ONLY
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeAll, Test}
import org.junit.{AfterClass, Assert}
import org.openeo.geotrellis.LayerFixtures.rgbLayerProvider
import org.openeo.geotrellis.png.PngTest
import org.openeo.geotrellis.tile_grid.TileGrid
import org.openeo.geotrellis.{LayerFixtures, geotiff}

import java.nio.file.Path
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME
import java.time.{LocalDate, ZonedDateTime}
import scala.collection.JavaConverters._

object TileGridTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = {
    // originally geotrellis.spark.util.SparkUtils.createLocalSparkContext
    val conf = SparkUtils.createSparkConf
      .setMaster("local[*]")
      .setAppName(PngTest.getClass.getName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
      // .set("spark.kryo.registrationRequired", "true") // this requires e.g. RasterSource to be registered too
      .set("spark.kryo.registrator", Seq(
        classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName,
        classOf[org.openeo.geotrellis.png.KryoRegistrator].getName) mkString ","
      )

    sc = new SparkContext(conf)
  }

  @AfterClass
  def tearDownSpark(): Unit =
    sc.stop()
}

class TileGridTest {
  import TileGridTest._

  @Test
  def testSaveStitchWithTileGrids(@TempDir outDir: Path): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng)

    val layer = rgbLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial()
      .persist(DISK_ONLY)

    val tiles = geotiff.saveStitchedTileGrid(spatialLayer, outDir + "/testSaveStitched.tiff", "10km", DeflateCompression(6))
    val expectedPaths = Set(
      outDir + "/testSaveStitched-31UDS_3_4.tiff",
      outDir + "/testSaveStitched-31UDS_2_4.tiff",
      outDir + "/testSaveStitched-31UDS_3_5.tiff",
      outDir + "/testSaveStitched-31UDS_2_5.tiff",
    )

    // TODO: check if extents (in the layer CRS) are 10000m wide/high (in UTM)
    Assert.assertEquals(expectedPaths, tiles.asScala.map { case (path, _) => path }.toSet)

    val extent = bbox.reproject(spatialLayer.metadata.crs)
    val cropBounds = mapAsJavaMap(Map("xmin" -> extent.xmin, "xmax" -> extent.xmax, "ymin" -> extent.ymin, "ymax" -> extent.ymax))

    val croppedTiles = geotiff.saveStitchedTileGrid(spatialLayer, outDir + "/testSaveStitched_cropped.tiff", "10km", cropBounds, DeflateCompression(6))
    val expectedCroppedPaths = Set(
      outDir + "/testSaveStitched_cropped-31UDS_3_4.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_2_4.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_3_5.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_2_5.tiff",
    )

    // TODO: also check extents
    Assert.assertEquals(expectedCroppedPaths, croppedTiles.asScala.map { case (path, _) => path }.toSet)
  }

  @Test
  def testSaveStitchWithTileGridsWithOptions(@TempDir outDir: Path): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng)
    val layer = rgbLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial()
      .persist(DISK_ONLY)

    val gtiffOptions = new GTiffOptions
    gtiffOptions.setTileSize(128)

    val tiles = geotiff.saveStitchedTileGrid(spatialLayer, outDir + "/testSaveStitched.tiff", "10km", DeflateCompression(6),gtiffOptions)
    val expectedPaths = Set(
      outDir + "/testSaveStitched-31UDS_3_4.tiff",
      outDir + "/testSaveStitched-31UDS_2_4.tiff",
      outDir + "/testSaveStitched-31UDS_3_5.tiff",
      outDir + "/testSaveStitched-31UDS_2_5.tiff",
    )
    // TODO: check if extents (in the layer CRS) are 10000m wide/high (in UTM)
    Assert.assertEquals(expectedPaths, tiles.asScala.map { case (path, _) => path }.toSet)

    for (path <- expectedPaths){
      val tile = GeoTiff.readMultiband(path)
      Assert.assertEquals(3,tile.overviews.size)
      Assert.assertEquals(Tiled(128,128),tile.overviews.head.options.storageMethod)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      Assert.assertEquals(math.ceil(colSize.toDouble/4).toInt,tile.overviews(0).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/4).toInt,tile.overviews(0).tile.rows)
      Assert.assertEquals(math.ceil(colSize.toDouble/8).toInt,tile.overviews(1).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/8).toInt,tile.overviews(1).tile.rows)
      Assert.assertEquals(math.ceil(colSize.toDouble/16).toInt,tile.overviews(2).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/16).toInt,tile.overviews(2).tile.rows)
    }

    val extent = bbox.reproject(spatialLayer.metadata.crs)
    val cropBounds = mapAsJavaMap(Map("xmin" -> extent.xmin, "xmax" -> extent.xmax, "ymin" -> extent.ymin, "ymax" -> extent.ymax))

    val croppedTiles = geotiff.saveStitchedTileGrid(spatialLayer, outDir + "/testSaveStitched_cropped.tiff", "10km", cropBounds, DeflateCompression(6),gtiffOptions)
    val expectedCroppedPaths = Set(
      outDir + "/testSaveStitched_cropped-31UDS_3_4.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_2_4.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_3_5.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_2_5.tiff",
    )

    Assert.assertEquals(expectedCroppedPaths, croppedTiles.asScala.map { case (path, _) => path }.toSet)

    for (path <- expectedCroppedPaths){
      val tile = GeoTiff.readMultiband(path)
      Assert.assertEquals(3,tile.overviews.size)
      Assert.assertEquals(Tiled(128,128),tile.overviews.head.options.storageMethod)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      Assert.assertEquals(math.ceil(colSize.toDouble/4).toInt,tile.overviews(0).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/4).toInt,tile.overviews(0).tile.rows)
      Assert.assertEquals(math.ceil(colSize.toDouble/8).toInt,tile.overviews(1).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/8).toInt,tile.overviews(1).tile.rows)
      Assert.assertEquals(math.ceil(colSize.toDouble/16).toInt,tile.overviews(2).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/16).toInt,tile.overviews(2).tile.rows)
    }
  }


  @Test
  def testGetFeatures():Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31),utm31)
    val features = TileGrid.computeFeaturesForTileGrid("20km", bbox)
    Assert.assertEquals(1, features.size)
    Assert.assertEquals("31UDS_1_2", features.head._1)
    val extent = features.head._2

    Assert.assertEquals(extent.xmin,420000.0,0.01)
    Assert.assertEquals(extent.ymin,5640000.0,0.01)
    Assert.assertEquals(extent.xmax,440000.0,0.01)
    Assert.assertEquals(extent.ymax,5660000.0,0.01)

  }

  @Test
  def testGetFeatures10km():Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31),utm31)
    val features = TileGrid.computeFeaturesForTileGrid("10km", bbox)
    Assert.assertEquals(4,features.size)
    val f = features.find(_._1 == "31UDS_2_5").get

    var extent = f._2

    Assert.assertEquals(420000.0,extent.xmin,0.01)
    Assert.assertEquals(5640000.0,extent.ymin,0.01)
    Assert.assertEquals(430000.0,extent.xmax,0.01)
    Assert.assertEquals(5650000.0,extent.ymax,0.01)

    val f2 = features.find(_._1 == "31UDS_2_4").get

    extent = f2._2

    Assert.assertEquals(420000.0,extent.xmin,0.01)
    Assert.assertEquals(5650000.0,extent.ymin,0.01)
    Assert.assertEquals(430000.0,extent.xmax,0.01)
    Assert.assertEquals(5660000.0,extent.ymax,0.01)

  }

  @Test
  def testSaveStitchWithTileGridsTemporal(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val isoFormattedDate = date format ISO_ZONED_DATE_TIME
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31), utm31)

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val tiles = geotiff.saveStitchedTileGridTemporal(layer, "/tmp/", "10km", DeflateCompression(6))
    val expectedTiles = Set(
      ("/tmp/openEO_2020-04-05Z_31UDS_3_4.tif", isoFormattedDate),
      ("/tmp/openEO_2020-04-05Z_31UDS_2_4.tif", isoFormattedDate),
      ("/tmp/openEO_2020-04-05Z_31UDS_3_5.tif", isoFormattedDate),
      ("/tmp/openEO_2020-04-05Z_31UDS_2_5.tif", isoFormattedDate)
    )

    Assert.assertEquals(expectedTiles, tiles.asScala.map { case (path, timestamp, _) => (path, timestamp) }.toSet)
  }

  @Test
  def testSaveStitchWithTileGridsTemporalWithOptions(@TempDir outDir: Path): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val isoFormattedDate = date format ISO_ZONED_DATE_TIME
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31), utm31)

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)
    val gtiffOptions = new GTiffOptions
    gtiffOptions.setOverview("ALL")
    gtiffOptions.setTileSize(128)

    val tiles = geotiff.saveStitchedTileGridTemporal(layer, outDir + "/", "10km", DeflateCompression(6),gtiffOptions)
    val expectedTiles = Set(
      (outDir + "/openEO_2020-04-05Z_31UDS_3_4.tif", isoFormattedDate),
      (outDir + "/openEO_2020-04-05Z_31UDS_2_4.tif", isoFormattedDate),
      (outDir + "/openEO_2020-04-05Z_31UDS_3_5.tif", isoFormattedDate),
      (outDir + "/openEO_2020-04-05Z_31UDS_2_5.tif", isoFormattedDate)
    )

    Assert.assertEquals(expectedTiles, tiles.asScala.map { case (path, timestamp, _) => (path, timestamp) }.toSet)


    for (path <- expectedTiles){
      val tile = GeoTiff.readMultiband(path._1)
      Assert.assertEquals(3,tile.overviews.size)
      Assert.assertEquals(Tiled(128,128),tile.overviews.head.options.storageMethod)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      Assert.assertEquals(math.ceil(colSize.toDouble/4).toInt,tile.overviews(0).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/4).toInt,tile.overviews(0).tile.rows)
      Assert.assertEquals(math.ceil(colSize.toDouble/8).toInt,tile.overviews(1).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/8).toInt,tile.overviews(1).tile.rows)
      Assert.assertEquals(math.ceil(colSize.toDouble/16).toInt,tile.overviews(2).tile.cols)
      Assert.assertEquals(math.ceil(rowSize.toDouble/16).toInt,tile.overviews(2).tile.rows)
    }
  }

  @Test
  def testSaveStitchWithTileGridsTemporalPrefix(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val isoFormattedDate = date format ISO_ZONED_DATE_TIME
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31), utm31)

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val tiles = geotiff.saveStitchedTileGridTemporal(layer, "/tmp/", "10km", DeflateCompression(6), filenamePrefix = Some("testPrefix"))
    val expectedTiles = Set(
      ("/tmp/testPrefix_2020-04-05Z_31UDS_3_4.tif", isoFormattedDate),
      ("/tmp/testPrefix_2020-04-05Z_31UDS_2_4.tif", isoFormattedDate),
      ("/tmp/testPrefix_2020-04-05Z_31UDS_3_5.tif", isoFormattedDate),
      ("/tmp/testPrefix_2020-04-05Z_31UDS_2_5.tif", isoFormattedDate)
    )

    Assert.assertEquals(expectedTiles, tiles.asScala.map { case (path, timestamp, _) => (path, timestamp) }.toSet)
  }

  @Test
  def testWriteRDDTileGrid(): Unit ={
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng)

    val layer = LayerFixtures.rgbLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial()
      .persist(DISK_ONLY)

    val paths = saveRDDTileGrid(spatialLayer, 3, "/tmp/testSaveRdd.tiff", "10km")
    val expectedPaths = List("/tmp/testSaveRdd-31UDS_3_4.tiff", "/tmp/testSaveRdd-31UDS_2_4.tiff", "/tmp/testSaveRdd-31UDS_3_5.tiff", "/tmp/testSaveRdd-31UDS_2_5.tiff")

    Assert.assertEquals(paths.groupBy(identity), expectedPaths.groupBy(identity))
  }

}
