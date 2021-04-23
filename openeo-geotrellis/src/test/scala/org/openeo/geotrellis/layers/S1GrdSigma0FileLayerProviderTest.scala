package org.openeo.geotrellis.layers

// import org.openeo.geotrellis.TestImplicits._
import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}

import cats.data.NonEmptyList
import geotrellis.layer.SpatialKey
import geotrellis.proj4.CRS
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.Assert.assertEquals
import org.junit.{AfterClass, BeforeClass, Test}

object S1GrdSigma0FileLayerProviderTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]",
    appName = S1GrdSigma0FileLayerProviderTest.getClass.getName)

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class S1GrdSigma0FileLayerProviderTest {
  import S1GrdSigma0FileLayerProviderTest._

  @Test
  def polygonalMeanMultiband(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(682463.2469290665, 5706687.916789337, 685321.364715595, 5708951.4296454685), CRS.fromEpsgCode(32631))

    val layer = sigma0LayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)
    val spatialLayer: MultibandTileLayerRDD[SpatialKey] = layer.toSpatial(date).cache()

    // spatialLayer.writeGeoTiff(bbox, "/tmp/sigma0__.tif")

    val polygon = bbox.reprojectAsPolygon(spatialLayer.metadata.crs)

    val Summary(Array(vhMean, vvMean, angleMean)) = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor)

    // all derived with the QGIS Zonal Statistics plugin
    assertEquals(0.025856676236086, vhMean.mean, 0.001)
    assertEquals(0.0895277254625895, vvMean.mean, 0.001)
    assertEquals(33460.7361532273, angleMean.mean, 1)
  }

  private def sigma0LayerProvider = new FileLayerProvider(
    openSearch = OpenSearch(new URL("https://services.terrascope.be/catalogue")),
    openSearchCollectionId = "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
    openSearchLinkTitles = NonEmptyList.of("VH", "VV", "angle"),
    rootPath = "/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1",
    maxSpatialResolution = CellSize(10, 10),
    pathDateExtractor = SplitYearMonthDayPathDateExtractor
  )
}
