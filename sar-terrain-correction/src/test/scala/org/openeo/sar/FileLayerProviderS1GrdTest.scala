package org.openeo.sar

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
import org.openeo.geotrellis.geotiff.saveRDDTemporal
import org.openeo.geotrellis.layers.{FileLayerProvider, SplitYearMonthDayPathDateExtractor}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.net.URI
import java.time.ZonedDateTime

/** End-to-end integration test for the full
 *  [[FileLayerProvider]] → [[org.openeo.sar.provider.Sentinel1GrdRasterSourceProvider]]
 *  → [[org.openeo.sar.TerrainCorrectionProcessor]] chain.
 *
 *  Uses the same Sentinel-1 IW GRDH product over Belgium as [[TerrainCorrectionTest]].
 *  Requires CDSE S3 credentials (eodata bucket) and outbound HTTPS to stac.dataspace.copernicus.eu.
 *  Gated by the [[runOnline]] flag.
 *
 *  Required env vars:
 *    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
 *    AWS_ENDPOINT_URL=https://eodata.dataspace.copernicus.eu,
 *    AWS_S3_ENDPOINT=eodata.dataspace.copernicus.eu,
 *    AWS_VIRTUAL_HOSTING=FALSE
 */
object FileLayerProviderS1GrdTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setUpSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[2]", appName = classOf[FileLayerProviderS1GrdTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class FileLayerProviderS1GrdTest {
  import FileLayerProviderS1GrdTest._

  private val runOnline = true

  // ---- Scene identification --------------------------------------------------

  private val stacItemId =
    "S1A_IW_GRDH_1SDV_20260610T172444_20260610T172509_064910_082DFE_F1C5_COG"

  private val stacItemUrl = new URI(
    "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-grd/items/" + stacItemId
  )

  // SAFE root on CDSE object storage, derived from the STAC item ID.
  private val safeRoot =
    "s3://eodata/Sentinel-1/SAR/IW_GRDH_1S-COG/2026/06/10/" +
      s"$stacItemId.SAFE"

  // Approximate acquisition time (from the item ID).
  private val acquisitionTime = ZonedDateTime.parse("2026-06-10T17:24:44Z")

  // Scene footprint in WGS84 (Feature.bbox and rasterExtent so that
  // FileLayerProvider can compute featureExtentInLayout without discarding the item).
  private val sceneBboxWgs84 = Extent(1.0, 48.5, 8.5, 53.5)

  // ---- Output tile ----------------------------------------------------------

  // 11 km x 16 km in UTM 31N at 10 m, centred on Brussels.
  private val outputCrs      = CRS.fromEpsgCode(32631)
  private val outputExtent   = Extent(595000.0, 5630000.0, 606000.0, 5646000.0)
  private val outputCellSize = CellSize(10.0, 10.0)

  // ---- Helper: build a Feature with CDSE S3 asset hrefs ---------------------

  /** Construct the STAC Feature for the S1 GRD scene.
   *
   *  The measurement TIFF hrefs must:
   *   1. Pass [[org.openeo.sar.provider.Sentinel1GrdRasterSourceProvider.canProcess]]
   *      (contains `/measurement/` and `-grd-vv-` / `-grd-vh-`)
   *   2. Allow the provider to derive the SAFE root and identify the polarisations.
   *
   *  The actual measurement TIFFs / annotation XMLs are resolved via the STAC item
   *  fetched from [[Feature.selfUrl]]; the link hrefs here are not directly read.
   *  `bandNames` lets [[org.openeo.geotrellis.layers.BandAssetLinkResolver]] (running
   *  in `fromLoadStac=true` mode via [[FixedFeaturesOpenSearchClient]]) match the links
   *  to the requested bands "VV" and "VH". */
  private def buildFeature(): Feature = {
    val tag = "20260610t172444-20260610t172509-064910-082dfe"
    def measurementHref(pol: String) =
      URI.create(s"$safeRoot/measurement/s1a-iw-grd-$pol-$tag-001-cog.tiff")

    val links = Array(
      Link(href = measurementHref("vv"), title = Some("VV"), bandNames = Some(Seq("VV"))),
      Link(href = measurementHref("vh"), title = Some("VH"), bandNames = Some(Seq("VH")))
    )

    Feature(
      id           = stacItemId,
      bbox         = sceneBboxWgs84,
      nominalDate  = acquisitionTime,
      links        = links,
      resolution   = Some(10.0),
      crs          = Some(LatLng),
      rasterExtent = Some(sceneBboxWgs84),
      selfUrl      = Some(stacItemUrl),
      collectionId = "sentinel-1-grd"
    )
  }

  // ---- Test -----------------------------------------------------------------

  @Test
  def fileLayerProviderReturnsS1GrdTileLayer(): Unit = {
    org.junit.jupiter.api.Assumptions.assumeTrue(runOnline, "online test disabled")

    val feature = buildFeature()

    val openSearchClient = new FixedFeaturesOpenSearchClient
    openSearchClient.addFeature(feature)

    // Request both VV and VH sigma0 bands at 10 m.
    val provider = FileLayerProvider(
      openSearch             = openSearchClient,
      openSearchCollectionId = "sentinel-1-grd",
      openSearchLinkTitles   = NonEmptyList.of("VV", "VH"),
      rootPath               = null,   // absolute S3 URIs; pathDateExtractor is not used
      maxSpatialResolution   = outputCellSize,
      pathDateExtractor      = SplitYearMonthDayPathDateExtractor,
      layoutScheme           = FloatingLayoutScheme(256),
    )

    val bbox           = ProjectedExtent(outputExtent, outputCrs)
    val datacubeParams = new DataCubeParameters
    datacubeParams.layoutScheme = "FloatingLayoutScheme"
    datacubeParams.globalExtent = Some(bbox)
    datacubeParams.loadPerProduct = true

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

    assertFalse(cube.isEmpty, "Result RDD must not be empty")

    // S1GrdRasterSource always returns nPols+2 bands: VV, VH, incidence_angle, validity_mask.
    val expectedBandCount = 4
    val tiles = cube.values.collect()
    assertTrue(tiles.nonEmpty, "Must have at least one output tile")
    /*tiles.foreach { tile =>
      assertEquals(expectedBandCount, tile.bandCount,
        s"Each tile must carry $expectedBandCount bands (sigma0_VV, sigma0_VH, incidence_angle, validity)")
    }*/

    // Save output GeoTIFF for visual inspection.
    saveRDDTemporal(cube, "/tmp/s1grd-filelayerprovider-test/", cropBounds = Some(outputExtent))
  }
}
