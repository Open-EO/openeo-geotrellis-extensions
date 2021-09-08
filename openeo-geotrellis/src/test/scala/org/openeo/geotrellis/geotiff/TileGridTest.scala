package org.openeo.geotrellis.geotiff

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel.DISK_ONLY
import org.junit._
import org.openeo.geotrellis.LayerFixtures.rgbLayerProvider
import org.openeo.geotrellis.png.PngTest
import org.openeo.geotrellis.tile_grid.TileGrid
import org.openeo.geotrellis.{LayerFixtures, geotiff}

import scala.collection.JavaConversions._

object TileGridTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    // originally geotrellis.spark.util.SparkUtils.createLocalSparkContext
    val conf = SparkUtils.createSparkConf
      .setMaster("local[*]")
      .setAppName(PngTest.getClass.getName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
  def testSaveStitchWithTileGrids(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng)

    val layer = rgbLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial()
      .persist(DISK_ONLY)

    val paths = geotiff.saveStitchedTileGrid(spatialLayer, "/tmp/testSaveStitched.tiff", "10km", DeflateCompression(6))
    val expectedPaths = List("/tmp/testSaveStitched-31UDS_3_4.tiff", "/tmp/testSaveStitched-31UDS_2_4.tiff", "/tmp/testSaveStitched-31UDS_3_5.tiff", "/tmp/testSaveStitched-31UDS_2_5.tiff")

    Assert.assertEquals(paths.groupBy(identity), expectedPaths.groupBy(identity))

    val extent = bbox.reproject(spatialLayer.metadata.crs)
    val cropBounds = mapAsJavaMap(Map("xmin" -> extent.xmin, "xmax" -> extent.xmax, "ymin" -> extent.ymin, "ymax" -> extent.ymax))

    val croppedPaths = geotiff.saveStitchedTileGrid(spatialLayer, "/tmp/testSaveStitched_cropped.tiff", "10km", cropBounds, DeflateCompression(6))
    val expectedCroppedPaths = List("/tmp/testSaveStitched_cropped-31UDS_3_4.tiff", "/tmp/testSaveStitched_cropped-31UDS_2_4.tiff", "/tmp/testSaveStitched_cropped-31UDS_3_5.tiff", "/tmp/testSaveStitched_cropped-31UDS_2_5.tiff")

    Assert.assertEquals(croppedPaths.groupBy(identity), expectedCroppedPaths.groupBy(identity))
  }


  @Test
  def testGetFeatures():Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31),utm31)
    val features = TileGrid.computeFeaturesForTileGrid("20km", bbox)
    Assert.assertEquals(1,features.size)
    Assert.assertEquals(features.get(0)._1,"31UDS_1_2")
    val extent = features.get(0)._2

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
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31),utm31)


    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val paths = geotiff.saveStitchedTileGridTemporal(layer, "/tmp/", "10km", DeflateCompression(6))
    val expectedPaths = List("/tmp/openEO_2020-04-05Z_31UDS_3_4.tif", "/tmp/openEO_2020-04-05Z_31UDS_2_4.tif", "/tmp/openEO_2020-04-05Z_31UDS_3_5.tif", "/tmp/openEO_2020-04-05Z_31UDS_2_5.tif")

    Assert.assertEquals(paths.groupBy(identity), expectedPaths.groupBy(identity))

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
