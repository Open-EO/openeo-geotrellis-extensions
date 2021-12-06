package org.openeo.geotrellis.png

import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchClient
import cats.data.NonEmptyList
import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel.DISK_ONLY
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.layers.{FileLayerProvider, SplitYearMonthDayPathDateExtractor}

import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}

object PngTest {
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

class PngTest {
  import PngTest._

  @Test
  def testSaveStitched(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)

    val layer = rgbLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial()
      .persist(DISK_ONLY)

    saveStitched(spatialLayer, "/tmp/testSaveStitched.png")
    saveStitched(spatialLayer, "/tmp/testSaveStitched_cropped.png", bbox.reproject(spatialLayer.metadata.crs))
  }

  private def rgbLayerProvider =
    new FileLayerProvider(
      openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue")),
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor = SplitYearMonthDayPathDateExtractor
    )
}
