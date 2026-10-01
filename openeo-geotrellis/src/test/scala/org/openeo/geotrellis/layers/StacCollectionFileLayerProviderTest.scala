package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.FloatingLayoutScheme
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiffReader
import geotrellis.raster.{CellSize, isNoData}
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.geotrellis.geotiff.saveRDDTemporal
import org.openeo.geotrellis.testutil.stac.{StacTestGenerator, StandardStacTestCollections}
import org.openeo.geotrelliscommon.DataCubeParameters

import java.nio.file.Path
import java.time.ZonedDateTime

object StacCollectionFileLayerProviderTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setUpSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[2]", appName = classOf[StacCollectionFileLayerProviderTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

/**
 * Integration test for [[FileLayerProvider.readMultibandTileLayer]] against the
 * generated STAC test collection.
 *
 * Verifies that:
 *  - Bands from different asset types (single-band GeoTIFF, multi-band GeoTIFF, NetCDF)
 *    can be loaded together in an arbitrary order.
 *  - The output resolution corresponds to the highest-resolution asset (10 m UTM).
 *  - The materialized pixel values match the fill patterns used when generating the assets.
 */
@EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasGdalInstalled")
class StacCollectionFileLayerProviderTest {

  import StacCollectionFileLayerProviderTest._

  // -------------------------------------------------------------------------
  // Shared spatial parameters (must match StandardStacTestCollections)
  // -------------------------------------------------------------------------

  private val utmCrs: CRS    = CRS.fromEpsgCode(32631)
  private val utmExtent: Extent = Extent(600000, 5650000, 602560, 5652560)
  private val bbox: ProjectedExtent = ProjectedExtent(utmExtent, utmCrs)

  // -------------------------------------------------------------------------
  // Test
  // -------------------------------------------------------------------------

  /**
   * Loads three bands from two different GeoTIFF assets and a NetCDF asset in
   * an order that differs from their native order in the STAC items:
   *
   *   Requested order: ["B11", "temperature", "B04"]
   *   Native order per asset:
   *     multiband_200m.tif → B08 (index 0), B11 (index 1)
   *     meteo_5km.nc       → temperature (index 0), humidity (index 1)
   *     B04_10m.tif        → B04 (index 0)
   *
   * Expected pixel value ranges (based on XGradient fill + band-index offsets):
   *   band 0 – B11:         [10 000, 20 000]  (XGradient + 10 000 offset for band index 1)
   *   band 1 – temperature: [0, 10 000]        (XGradient, variable index 0)
   *   band 2 – B04:         [0, 10 000]        (XGradient, band index 0)
   *
   * B04 and B11 ranges are non-overlapping, making band-order assertions unambiguous.
   * B04 is at the native 10 m resolution, so we can verify the XGradient pattern precisely.
   * B11 is at 200 m (resampled to 10 m), verifying multi-resolution loading.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array( true))
  def readMultibandTileLayerFromMultipleAssets(loadPerProduct: Boolean, @TempDir outDir: Path): Unit = {

    // 1. Ensure test data is present on disk.
    val collection = StacTestGenerator.ensureGenerated(
      StandardStacTestCollections.fileLayerProviderCollection)

    // 2. Register all four items with a FixedFeaturesOpenSearchClient.
    val osClient = new FixedFeaturesOpenSearchClient
    collection.toOpenSearchFeatures.foreach(osClient.addFeature)

    // 3. Request bands in a non-native order that spans three different asset files.
    val requestedBands = NonEmptyList.of("B11", "temperature", "B04")

    val datacubeParams = new DataCubeParameters
    datacubeParams.layoutScheme = "FloatingLayoutScheme"
    datacubeParams.globalExtent = Some(bbox)
    datacubeParams.loadPerProduct = loadPerProduct

    val provider = FileLayerProvider(
      openSearch               = osClient,
      openSearchCollectionId   = StandardStacTestCollections.fileLayerProviderCollection.id,
      openSearchLinkTitles     = requestedBands,
      rootPath                 = collection.outputDir.toString,
      maxSpatialResolution     = CellSize(10.0, 10.0),
      pathDateExtractor        = SplitYearMonthDayPathDateExtractor,
      layoutScheme             = FloatingLayoutScheme(256),
    )

    val date = ZonedDateTime.parse("2021-01-01T00:00:00Z")

    // 4. Load – no resampling parameters, original resolution and alignment.
    val cube = provider.readMultibandTileLayer(
      from          = date,
      to            = date,
      boundingBox   = bbox,
      polygons      = Array(MultiPolygon(utmExtent.toPolygon())),
      polygons_crs  = utmCrs,
      zoom          = 0,
      sc            = sc,
      datacubeParams = Some(datacubeParams),
    )

    assertEquals(utmCrs, cube.metadata.crs,
      "Output CRS must match the highest-resolution (UTM) asset")

    // 5. Materialise to GeoTIFF.
    val savedItems = saveRDDTemporal(
      cube,
      outDir.toString + "/",
      cropBounds = Some(utmExtent),
    )
    assertFalse(savedItems.isEmpty, "saveRDDTemporal must produce at least one file")
    val tiffPath = savedItems.get(0)._1

    // 6. Load the materialised GeoTIFF and assert on pixel values.
    val tiff = GeoTiffReader.readMultiband(tiffPath)
    val tile = tiff.tile.toArrayTile()

    assertEquals(3, tile.bandCount, "Output GeoTIFF must contain exactly the three requested bands")

    // --- Resolution ---
    // 2 560 m × 2 560 m at 10 m → 256 × 256 pixels (allow ±1 pixel for boundary rounding)
    assertTrue(tile.cols >= 255 && tile.cols <= 257,
      s"Expected ~256 columns (10 m resolution), got ${tile.cols}")
    assertTrue(tile.rows >= 255 && tile.rows <= 257,
      s"Expected ~256 rows (10 m resolution), got ${tile.rows}")

    val b11  = tile.band(0)   // B11:         XGradient + band-index offset → [10 000, 20 000]
    val temp = tile.band(1)   // temperature: XGradient, var index 0          → [0, 10 000]
    val b04  = tile.band(2)   // B04:         XGradient, band index 0         → [0, 10 000]

    // --- Collect valid (non-NoData) values for each band ---
    def validValues(band: geotrellis.raster.Tile): IndexedSeq[Double] =
      for {
        row <- 0 until band.rows
        col <- 0 until band.cols
        v    = band.getDouble(col, row)
        if !isNoData(v)
      } yield v

    val b11Values   = validValues(b11)
    val tempValues  = validValues(temp)
    val b04Values   = validValues(b04)

    assertFalse(b11Values.isEmpty,  "B11 band must contain valid pixels")
    assertFalse(tempValues.isEmpty, "temperature band must contain valid pixels")
    assertFalse(b04Values.isEmpty,  "B04 band must contain valid pixels")

    // --- Band-0 (B11): range [10 000, 20 000] ---
    // Non-overlapping with B04/temperature → proves correct band ordering.
    assertTrue(b11Values.min >= 10000.0 - 1.0,
      s"B11 min=${b11Values.min}: expected >= 10 000 (band-index offset)")
    assertTrue(b11Values.max <= 20000.0 + 1.0,
      s"B11 max=${b11Values.max}: expected <= 20 000")

    // B11 is XGradient resampled from 200 m → values increase left-to-right.
    val b11Left  = (0 until b11.rows).map(r => b11.getDouble(0, r)).filterNot(v => isNoData(v)).sum
    val b11Right = (0 until b11.rows).map(r => b11.getDouble(b11.cols - 1, r)).filterNot(v => isNoData(v)).sum
    //this assert was failing because the predefined extent on MultibandCompositeRasterSource did not match the TargetExtent on Geotiff rastersource, when a pixel buffer is in play
    //It is also triggered by the fact that different raster source providers are used in the same item
    assertTrue(b11Left < b11Right,
      s"B11 must be larger on the right (XGradient): left sum=$b11Left, right sum=$b11Right")

    // --- Band-1 (temperature): range [0, 10 000] ---
    // Resampled from 5 km NetCDF in EPSG:4326 to 10 m UTM.
    assertTrue(tempValues.min >= -1.0,
      s"temperature min=${tempValues.min}: expected >= 0")
    assertTrue(tempValues.max <= 10001.0,
      s"temperature max=${tempValues.max}: expected <= 10 000")

    // --- Band-2 (B04): range [0, 10 000], fine XGradient at native 10 m ---
    assertTrue(b04Values.min >= -1.0,
      s"B04 min=${b04Values.min}: expected >= 0")
    assertTrue(b04Values.max <= 10001.0,
      s"B04 max=${b04Values.max}: expected <= 10 000")

    // B04 is XGradient at native 10 m: leftmost column ≈ 0, rightmost ≈ 10 000.
    def columnMean(band: geotrellis.raster.Tile, col: Int): Double = {
      val vs = (0 until band.rows).map(band.getDouble(col, _)).filterNot(v => isNoData(v))
      if (vs.isEmpty) Double.NaN else vs.sum / vs.size
    }

    val b04Left  = columnMean(b04, 0)
    val b04Right = columnMean(b04, b04.cols - 1)

    assertTrue(b04Left < 200.0,
      s"B04 leftmost column mean=$b04Left: expected near 0 (XGradient)")
    assertTrue(b04Right > 9800.0,
      s"B04 rightmost column mean=$b04Right: expected near 10 000 (XGradient)")
    assertTrue(b04Left < b04Right,
      "B04 must be a left-to-right gradient (XGradient)")

    // B11 leftmost 200-m block should be ~10 000, rightmost ~20 000.
    val b11LeftMean  = columnMean(b11, 0)
    val b11RightMean = columnMean(b11, b11.cols - 1)
    assertTrue(b11LeftMean >= 10000.0 - 1.0,
      s"B11 leftmost mean=$b11LeftMean: expected ~10 000")
    assertTrue(b11RightMean >= 19000.0,
      s"B11 rightmost mean=$b11RightMean: expected ~20 000")
  }
}
