package org.openeo.geotrellis.geotiff

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffOptions, Tiled}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel.DISK_ONLY
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, Assertions, BeforeAll, Test}
import org.openeo.geotrellis.LayerFixtures.rgbLayerProvider
import org.openeo.geotrellis.png.PngTest
import org.openeo.geotrellis.tile_grid.TileGrid
import org.openeo.geotrellis.{LayerFixtures, geotiff}

import java.nio.file.Path
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME
import java.time.{LocalDate, ZonedDateTime}
import scala.jdk.CollectionConverters._

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

  @AfterAll
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
    val actualPaths = for {
      item <- tiles.asScala
      asset <- item.assets.values().asScala
    } yield asset.path

    Assertions.assertEquals(expectedPaths, actualPaths.toSet)

    val extent = bbox.reproject(spatialLayer.metadata.crs)
    val cropBounds = Map("xmin" -> extent.xmin, "xmax" -> extent.xmax, "ymin" -> extent.ymin, "ymax" -> extent.ymax).asJava

    val croppedTiles = geotiff.saveStitchedTileGrid(spatialLayer, outDir + "/testSaveStitched_cropped.tiff", "10km", cropBounds, DeflateCompression(6))
    val expectedCroppedPaths = Set(
      outDir + "/testSaveStitched_cropped-31UDS_3_4.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_2_4.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_3_5.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_2_5.tiff",
    )

    // TODO: also check extents
    val actualCroppedPaths = for {
      item <- croppedTiles.asScala
      asset <- item.assets.values().asScala
    } yield asset.path

    Assertions.assertEquals(expectedCroppedPaths, actualCroppedPaths.toSet)
  }

  @Test
  def testSaveStitchWithTileGridsWithOptions(@TempDir outDir: Path): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.90, 50.95, 2.10, 51.05), LatLng)
    val layer = rgbLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial()
      .persist(DISK_ONLY)

    val gtiffOptions = new GTiffOptions
    gtiffOptions.setTileSize(128)
    gtiffOptions.setOverview("ALL")

    val tiles = geotiff.saveStitchedTileGrid(spatialLayer, outDir + "/testSaveStitched.tiff", "10km", DeflateCompression(6), gtiffOptions)
    val expectedPaths = Set(
      outDir + "/testSaveStitched-31UDS_3_4.tiff",
      outDir + "/testSaveStitched-31UDS_2_4.tiff",
      outDir + "/testSaveStitched-31UDS_3_5.tiff",
      outDir + "/testSaveStitched-31UDS_2_5.tiff",
    )
    // TODO: check if extents (in the layer CRS) are 10000m wide/high (in UTM)
    Assertions.assertEquals(expectedPaths, tiles.asScala.map { case item => item.assets.values().iterator().next().path }.toSet)

    for (path <- expectedPaths) {
      val tile = GeoTiff.readMultiband(path)
      Assertions.assertEquals(1, tile.overviews.size)
      Assertions.assertEquals(Tiled(128, 128), tile.overviews.head.options.storageMethod)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      Assertions.assertEquals(math.ceil(colSize.toDouble / 2).toInt, tile.overviews(0).tile.cols)
      Assertions.assertEquals(math.ceil(rowSize.toDouble / 2).toInt, tile.overviews(0).tile.rows)
    }

    val extent = bbox.reproject(spatialLayer.metadata.crs)
    val cropBounds = Map("xmin" -> extent.xmin, "xmax" -> extent.xmax, "ymin" -> extent.ymin, "ymax" -> extent.ymax).asJava

    val croppedTiles = geotiff.saveStitchedTileGrid(spatialLayer, outDir + "/testSaveStitched_cropped.tiff", "10km", cropBounds, DeflateCompression(6), gtiffOptions)
    val expectedCroppedPaths = Set(
      outDir + "/testSaveStitched_cropped-31UDS_3_4.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_2_4.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_3_5.tiff",
      outDir + "/testSaveStitched_cropped-31UDS_2_5.tiff",
    )

    Assertions.assertEquals(expectedCroppedPaths, croppedTiles.asScala.map { case item => item.assets.values().iterator().next().path }.toSet)

    for (path <- expectedCroppedPaths) {
      val tile = GeoTiff.readMultiband(path)
      Assertions.assertEquals(1, tile.overviews.size)
      Assertions.assertEquals(Tiled(128, 128), tile.overviews.head.options.storageMethod)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      Assertions.assertEquals(math.ceil(colSize.toDouble / 4).toInt, tile.overviews(0).tile.cols)
      Assertions.assertEquals(math.ceil(rowSize.toDouble / 4).toInt, tile.overviews(0).tile.rows)
    }
  }


  @Test
  def testGetFeatures(): Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31), utm31)
    val features = TileGrid.computeFeaturesForTileGrid("20km", bbox)
    Assertions.assertEquals(1, features.size)
    Assertions.assertEquals("31UDS_1_2", features.head._1)
    val extent = features.head._2

    Assertions.assertEquals(extent.xmin, 420000.0, 0.01)
    Assertions.assertEquals(extent.ymin, 5640000.0, 0.01)
    Assertions.assertEquals(extent.xmax, 440000.0, 0.01)
    Assertions.assertEquals(extent.ymax, 5660000.0, 0.01)

  }

  @Test
  def testGetFeatures10km(): Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31), utm31)
    val features = TileGrid.computeFeaturesForTileGrid("10km", bbox)
    Assertions.assertEquals(4, features.size)
    val f = features.find(_._1 == "31UDS_2_5").get

    var extent = f._2

    Assertions.assertEquals(420000.0, extent.xmin, 0.01)
    Assertions.assertEquals(5640000.0, extent.ymin, 0.01)
    Assertions.assertEquals(430000.0, extent.xmax, 0.01)
    Assertions.assertEquals(5650000.0, extent.ymax, 0.01)

    val f2 = features.find(_._1 == "31UDS_2_4").get

    extent = f2._2

    Assertions.assertEquals(420000.0, extent.xmin, 0.01)
    Assertions.assertEquals(5650000.0, extent.ymin, 0.01)
    Assertions.assertEquals(430000.0, extent.xmax, 0.01)
    Assertions.assertEquals(5660000.0, extent.ymax, 0.01)

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

    val actualTiles = for {
      item <- tiles.asScala
      asset <- item.assets.values().asScala
    } yield (asset.path, item.datetime)

    Assertions.assertEquals(expectedTiles, actualTiles.toSet)
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

    val tiles = geotiff.saveStitchedTileGridTemporal(layer, outDir + "/", "10km", DeflateCompression(6), gtiffOptions)
    val expectedTiles = Set(
      (outDir + "/openEO_2020-04-05Z_31UDS_3_4.tif", isoFormattedDate),
      (outDir + "/openEO_2020-04-05Z_31UDS_2_4.tif", isoFormattedDate),
      (outDir + "/openEO_2020-04-05Z_31UDS_3_5.tif", isoFormattedDate),
      (outDir + "/openEO_2020-04-05Z_31UDS_2_5.tif", isoFormattedDate)
    )

    Assertions.assertEquals(expectedTiles, tiles.asScala.map { case item => (item.assets.values().iterator().next().path, item.datetime) }.toSet.asInstanceOf[Set[(String, String)]])

    for (path <- expectedTiles) {
      val tile = GeoTiff.readMultiband(path._1)
      Assertions.assertEquals(0, tile.overviews.size) // gtiff is only 1000x1000 so no overviews
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

    val actualTiles = for {
      item <- tiles.asScala
      asset <- item.assets.values().asScala
    } yield (asset.path, item.datetime)

    Assertions.assertEquals(expectedTiles, actualTiles.toSet)
  }

  @Test
  def testWriteRDDTileGrid(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng)

    val layer = LayerFixtures.rgbLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial()
      .persist(DISK_ONLY)

    val paths = saveRDDTileGrid(spatialLayer, 3, "/tmp/testSaveRdd.tiff", "10km")
    val expectedPaths = List("/tmp/testSaveRdd-31UDS_3_4.tiff", "/tmp/testSaveRdd-31UDS_2_4.tiff", "/tmp/testSaveRdd-31UDS_3_5.tiff", "/tmp/testSaveRdd-31UDS_2_5.tiff")

    Assertions.assertEquals(paths.groupBy(identity), expectedPaths.groupBy(identity))
  }

}
