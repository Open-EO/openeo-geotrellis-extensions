package org.openeo.geotrellis.layers

import java.net.URL
import java.time.{LocalDate, ZoneId}

import cats.data.NonEmptyList
import geotrellis.layer.ZoomedLayoutScheme
import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.junit.Assert.{assertEquals, assertNotSame, assertSame, assertTrue}
import org.junit.{AfterClass, BeforeClass, Test}

object FileLayerProviderTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[FileLayerProviderTest].getName)

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class FileLayerProviderTest {
  import FileLayerProviderTest._

  private def sentinel5PFileLayerProvider = new FileLayerProvider(
    openSearch = OpenSearch(new URL("https://services.terrascope.be/catalogue")),
    openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1",
    NonEmptyList.one("NO2"),
    rootPath = "/data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1",
    maxSpatialResolution = CellSize(0.05, 0.05),
    new Sentinel5PPathDateExtractor(maxDepth = 3),
    layoutScheme = ZoomedLayoutScheme(LatLng)
  )

  @Test
  def cache(): Unit = {
    // important: multiple instances like in openeo-geopyspark-driver
    val layerProvider1 = sentinel5PFileLayerProvider
    val layerProvider2 = sentinel5PFileLayerProvider

    assertNotSame(layerProvider1, layerProvider2)

    val metadataCall1 = layerProvider1.loadMetadata(sc = null)
    val metadataCall2 = layerProvider2.loadMetadata(sc = null)
    assertSame(metadataCall1, metadataCall2)
  }

  @Test
  def smallBoundingBox(): Unit = {
    val smallBbox = ProjectedExtent(Point(x = 4.9754, y = 50.3244).buffer(0.001).extent, LatLng)

    assertTrue(s"${smallBbox.extent.width}", smallBbox.extent.width < 0.05)
    assertTrue(s"${smallBbox.extent.height}", smallBbox.extent.height < 0.05)

    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))

    val baseLayer = sentinel5PFileLayerProvider.readTileLayer(from = date, to = date, smallBbox, sc = sc)

    val Summary(singleBandMean) = baseLayer
      .toSpatial(date)
      .polygonalSummaryValue(smallBbox.extent.toPolygon(), MeanVisitor)

    val physicalMean = (singleBandMean.mean * 5).toInt

    assertEquals(25, physicalMean)
  }
}
