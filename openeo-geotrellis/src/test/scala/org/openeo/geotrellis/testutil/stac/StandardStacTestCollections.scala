package org.openeo.geotrellis.testutil.stac

import geotrellis.proj4.CRS
import geotrellis.raster.{FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType}
import geotrellis.vector.Extent

/**
 * Pre-built [[TestCollectionSpec]] instances ready for use in
 * [[org.openeo.geotrellis.layers.FileLayerProvider]] tests.
 *
 * Usage example:
 * {{{
 * @BeforeAll
 * def generateTestData(): Unit = {
 *   collection = StandardStacTestCollections.fileLayerProviderCollection.ensureGenerated()
 * }
 *
 * @Test
 * def myTest(): Unit = {
 *   val osClient = new FixedFeaturesOpenSearchClient
 *   collection.toOpenSearchFeatures.foreach(osClient.addFeature)
 *   // … use osClient with FileLayerProvider …
 * }
 * }}}
 */
object StandardStacTestCollections {

  // -------------------------------------------------------------------------
  // Spatial parameters shared across items
  // -------------------------------------------------------------------------

  // Use an explicit numeric fill value instead of NaN so that GDAL can reliably read
  // the NoData metadata from the written GeoTIFFs.
  private val geoTiffNoData: FloatUserDefinedNoDataCellType = FloatUserDefinedNoDataCellType(-9999f)
  private val utmCrs: CRS    = CRS.fromEpsgCode(32631)
  private val utmExtent: Extent = Extent(600000, 5650000, 602560, 5652560)

  /**
   * Global LatLng extent spanning UTM zones 24 W to 36 E.
   * At 0.05° resolution → 600 × 400 pixels (~28 MB for 2 float32 variables × 4 items).
   * Approximates "5 km" at mid-latitudes.
   */
  private val latLngExtent: Extent = Extent(-20.0, 40.0, 10.0, 60.0)
  private val ncResolution: Double = 0.05   // degrees ≈ 5 km

  // -------------------------------------------------------------------------
  // Asset specs – same set used for every item (different dates)
  // -------------------------------------------------------------------------

  /**
   * Single-band GeoTIFF at 10 m resolution in UTM.
   * XGradient pattern: pixel value = (column / (cols-1)) × 10 000.
   * Easy to verify that X-axis alignment is preserved after loading.
   */
  def singleBandTiff10m(assetKey: String = "B04"): GeoTiffAssetSpec =
    GeoTiffAssetSpec(
      assetKey   = assetKey,
      fileName   = s"${assetKey}_10m.tif",
      bandNames  = Seq(assetKey),
      resolution = 10.0,
      crs        = utmCrs,
      extent     = utmExtent,
      pattern    = XGradient,
      cellType   = geoTiffNoData,
    )

  /**
   * Multi-band GeoTIFF at 200 m resolution in UTM with two bands.
   *  - Band 0 (B08): XGradient  – values 0 – 10 000
   *  - Band 1 (B11): XGradient + 10 000 offset – values 10 000 – 20 000
   * The per-band offset makes band identity unambiguous in assertions.
   */
  def multiBandTiff200m(assetKey: String = "multiband_200m"): GeoTiffAssetSpec =
    GeoTiffAssetSpec(
      assetKey   = assetKey,
      fileName   = s"${assetKey}.tif",
      bandNames  = Seq("B08", "B11"),
      resolution = 200.0,
      crs        = utmCrs,
      extent     = utmExtent,
      pattern    = XGradient,
      cellType   = geoTiffNoData,
    )

  /**
   * Two-variable NetCDF at ~5 km resolution in EPSG:4326 spanning multiple UTM zones.
   *  - Variable 0 (temperature): XGradient  - longitude proxy
   *  - Variable 1 (humidity):    YGradient + 10 000 offset - latitude proxy
   *
   * The two different patterns make it easy to distinguish variables after loading.
   */
  def globalNetCDF(assetKey: String = "meteo"): NetCDFAssetSpec =
    NetCDFAssetSpec(
      assetKey   = assetKey,
      fileName   = s"${assetKey}_5km.nc",
      variables  = Seq("temperature", "humidity"),
      resolution = ncResolution,
      crs        = CRS.fromEpsgCode(4326),
      extent     = latLngExtent,
      pattern    = XGradient,
      cellType   = FloatConstantNoDataCellType,
    )

  // -------------------------------------------------------------------------
  // Default 4-item collection
  // -------------------------------------------------------------------------

  /**
   * A STAC test collection with 4 items at quarterly dates.
   * Each item carries:
   *  - 1 single-band GeoTIFF (10 m, UTM-31N)
   *  - 1 multi-band GeoTIFF (200 m, UTM-31N, 2 bands)
   *  - 1 NetCDF (0.05°, EPSG:4326, 2 variables spanning multiple UTM zones)
   */
  val fileLayerProviderCollection: TestCollectionSpec = TestCollectionSpec(
    id = "file-layer-provider-test",
    items = Seq("2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01").zipWithIndex.map {
      case (date, idx) =>
        TestItemSpec(
          id       = f"item-$idx%02d",
          datetime = s"${date}T00:00:00Z",
          assets   = Seq(
            singleBandTiff10m(),
            multiBandTiff200m(),
            globalNetCDF(),
          ),
        )
    },
  )
}
