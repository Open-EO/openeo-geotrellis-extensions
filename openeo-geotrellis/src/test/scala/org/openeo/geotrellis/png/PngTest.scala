package org.openeo.geotrellis.png

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}

import cats.data.NonEmptyList
import geotrellis.proj4.LatLng
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.layers.Sentinel2FileLayerProvider

object PngTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", PngTest.getClass.getName)

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
      .cache()

    saveStitched(spatialLayer, "/tmp/testSaveStitched.png")
    saveStitched(spatialLayer, "/tmp/testSaveStitched_cropped.png", bbox.reproject(spatialLayer.metadata.crs))
  }

  private def rgbLayerProvider =
    new Sentinel2FileLayerProvider(
      oscarsCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      oscarsLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2"
    )
}
