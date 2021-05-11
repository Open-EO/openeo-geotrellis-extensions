package org.openeo.geotrellis.geotiff

import cats.data.NonEmptyList
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ByteArrayTile, CellSize, MultibandTile, TileLayout}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel.DISK_ONLY
import org.junit.Assert.assertArrayEquals
import org.junit.{AfterClass, Assert, BeforeClass, Test}
import org.openeo.geotrellis.geotiff
import org.openeo.geotrellis.layers.{FileLayerProvider, OpenSearch, SplitYearMonthDayPathDateExtractor}
import org.openeo.geotrellis.png.PngTest

import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
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
  def testWriteRDDTileGrid(): Unit ={
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng)

    val layer = rgbLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial()
      .persist(DISK_ONLY)

    val paths = saveRDDTileGrid(spatialLayer, 3, "/tmp/testSaveRdd.tiff", "10km")
    val expectedPaths = List("/tmp/testSaveRdd-31UDS_3_4.tiff", "/tmp/testSaveRdd-31UDS_2_4.tiff", "/tmp/testSaveRdd-31UDS_3_5.tiff", "/tmp/testSaveRdd-31UDS_2_5.tiff")

    Assert.assertEquals(paths.groupBy(identity), expectedPaths.groupBy(identity))
  }

  private def rgbLayerProvider =
    new FileLayerProvider(
      openSearch = OpenSearch(new URL("https://services.terrascope.be/catalogue")),
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor = SplitYearMonthDayPathDateExtractor
    )
}
