package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.FloatingLayoutScheme
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.CellSize
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.geotrellis.layers.provider.{ConstantMultibandRasterSource, TestMultibandRasterSourceProvider}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.time.ZonedDateTime
import java.util.UUID

/**
 * Exercises `FileLayerProvider`'s "whole-feature" multiband raster source strategy (see
 * `RasterSourceProvider.multibandRasterSource`) using [[TestMultibandRasterSourceProvider]] - a
 * synthetic, non-Sentinel1 provider that opens one physical 3-band [[org.openeo.geotrellis.layers.provider.ConstantMultibandRasterSource]]
 * per scene and serves several output bands from it in one go.
 *
 * Verifies:
 *  - output bands come back in the *requested* order, which is deliberately different from the
 *    physical band order of the underlying source, and
 *  - (via [[ConstantMultibandRasterSource.readCalls]]) how many times, and with which band indices,
 *    the physical source is actually read - for both `loadPerProduct` settings - so that future
 *    changes to the read/consolidation machinery that regress read counts are caught.
 *
 * Note on mocking: a mocking library (mockito-scala, already a test dependency of this module) is the
 * natural first choice for call counting, and was tried here (`Mockito.spy`/`mock(..., withSettings()
 * .serializable())`). It does not work in this pipeline: raster sources end up embedded inside Spark
 * task closures (via `CompositeRasterSource`/`Feature`) that get Kryo-serialized even in local mode, and
 * Mockito's dynamic proxies carry non-serializable invocation-history state that breaks Kryo regardless
 * of the `serializable()` mock setting. [[ConstantMultibandRasterSource]] therefore records its own
 * `read(...)` calls into a plain, Kryo-friendly, JVM-static registry instead - which works for local
 * Spark tests because the driver and "executors" share one JVM.
 */
object MultibandRasterSourceProviderTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setUpSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[2]", appName = classOf[MultibandRasterSourceProviderTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class MultibandRasterSourceProviderTest {
  import MultibandRasterSourceProviderTest._

  private val outputCrs      = CRS.fromEpsgCode(32631)
  private val outputCellSize = CellSize(1.0, 1.0)
  // 32x32 output, well inside sceneBboxWgs84 below.
  private val outputExtent   = Extent(500000.0, 5650000.0, 500032.0, 5650032.0)
  private val sceneBboxWgs84 = Extent(-0.450784, 50.25919, 3.680459, 52.16246)
  private val acquisitionTime = ZonedDateTime.parse("2021-06-01T10:00:00Z")

  // Physical scene band order (see TestMultibandRasterSourceProvider.PhysicalBandValues):
  // physical band 0 -> "A" (10.0), physical band 1 -> "B" (20.0), physical band 2 -> "C" (30.0).
  // Requested in a different order to verify correct re-ordering of output bands.
  private val requestedBandNames = NonEmptyList.of("C", "A", "B")
  private val expectedBandValues = Seq(30.0, 10.0, 20.0) // physical bands C, A, B respectively

  private def buildFeature(sceneId: String): Feature = {
    val links = Array(
      Link(href = TestMultibandRasterSourceProvider.href(sceneId, 0), title = Some("A"), bandNames = Some(Seq("A"))),
      Link(href = TestMultibandRasterSourceProvider.href(sceneId, 1), title = Some("B"), bandNames = Some(Seq("B"))),
      Link(href = TestMultibandRasterSourceProvider.href(sceneId, 2), title = Some("C"), bandNames = Some(Seq("C")))
    )

    Feature(
      id           = sceneId,
      bbox         = sceneBboxWgs84,
      nominalDate  = acquisitionTime,
      links        = links,
      resolution   = Some(outputCellSize.width),
      crs          = Some(LatLng),
      rasterExtent = Some(sceneBboxWgs84),
      collectionId = "test-multiband-collection"
    )
  }

  private def runAndCollect(sceneId: String, loadPerProduct: Boolean) = {
    val feature = buildFeature(sceneId)

    val openSearchClient = new FixedFeaturesOpenSearchClient
    openSearchClient.addFeature(feature)

    val provider = FileLayerProvider(
      openSearch             = openSearchClient,
      openSearchCollectionId = "test-multiband-collection",
      openSearchLinkTitles   = requestedBandNames,
      rootPath               = null,
      maxSpatialResolution   = outputCellSize,
      pathDateExtractor      = SplitYearMonthDayPathDateExtractor,
      layoutScheme           = FloatingLayoutScheme(8),
    )

    val bbox           = ProjectedExtent(outputExtent, outputCrs)
    val datacubeParams  = new DataCubeParameters
    datacubeParams.layoutScheme = "FloatingLayoutScheme"
    datacubeParams.globalExtent = Some(bbox)
    datacubeParams.loadPerProduct = loadPerProduct

    val cube = provider.readMultibandTileLayer(
      from           = acquisitionTime,
      to             = acquisitionTime,
      boundingBox    = bbox,
      polygons       = Array(MultiPolygon(outputExtent.toPolygon())),
      polygons_crs   = outputCrs,
      zoom           = 0,
      sc             = sc,
      datacubeParams = Some(datacubeParams)
    )

    cube.values.collect()
  }

  private def check(loadPerProduct: Boolean): Unit = {
    val sceneId = s"test-scene-${UUID.randomUUID()}"
    TestMultibandRasterSourceProvider.reset()

    val tiles = runAndCollect(sceneId, loadPerProduct)

    assertTrue(tiles.nonEmpty, "must have at least one output tile")
    tiles.foreach { tile =>
      assertEquals(3, tile.bandCount, "each tile must carry 3 bands")
      for (b <- expectedBandValues.indices) {
        assertEquals(expectedBandValues(b), tile.band(b).getDouble(0, 0), 1e-9,
          s"band $b (loadPerProduct=$loadPerProduct) should carry the value of the requested band")
      }
    }

    // The physical source must be opened exactly once for the whole feature, regardless of how many
    // output bands / spatial keys are ultimately read from it.
    assertEquals(1, TestMultibandRasterSourceProvider.openCount(sceneId),
      s"physical source must be opened exactly once (loadPerProduct=$loadPerProduct)")

    val calls = ConstantMultibandRasterSource.readCalls(sceneId)
    assertTrue(calls.nonEmpty, "physical source must have been read at least once")
    // Every physical read call must request a single band (current architecture: even a shared
    // physical source is still read one output band at a time - see IndexedRasterSource).
    assertTrue(calls.forall(_.size == 1), s"expected single-band read calls, got: $calls")

    if (loadPerProduct) {
      // "load per product" consolidates reads spatially: one physical read call per band, regardless
      // of how many output tiles/keys are produced.
      assertEquals(3, calls.size,
        s"loadPerProduct=true should read each of the 3 bands exactly once regardless of tile count, got: $calls")
    } else {
      // Without consolidation, the physical source is read once per band *per output tile*.
      assertEquals(3 * tiles.length, calls.size,
        s"loadPerProduct=false should read each band once per output tile, got: $calls")
    }

    // Regardless of consolidation strategy, exactly the 3 physical band indices {0,1,2} are read
    // (possibly duplicated across tiles), never any extra/foreign index.
    assertEquals(Set(0, 1, 2), calls.flatten.toSet)
  }

  @Test
  def bandsAreReturnedInRequestedOrder_loadPerProductTrue(): Unit = check(loadPerProduct = true)

  @Test
  def bandsAreReturnedInRequestedOrder_loadPerProductFalse(): Unit = check(loadPerProduct = false)
}
