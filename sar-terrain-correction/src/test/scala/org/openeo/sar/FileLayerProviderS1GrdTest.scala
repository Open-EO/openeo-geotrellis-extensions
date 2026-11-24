package org.openeo.sar

import cats.data.NonEmptyList
import geotrellis.layer.FloatingLayoutScheme
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, UShortConstantNoDataCellType}
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
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
/** A scene plus the output area to process. Add special cases (unusual annotation layouts, etc.) here. */
case class S1GrdScene(name: String, stacItemId: String, sceneBboxWgs84: Extent, areaWgs84: Extent, outputEpsg: Int) {
  def safeRoot: String = {
    val d = stacItemId.substring(17, 25)
    s"s3://eodata/Sentinel-1/SAR/IW_GRDH_1S-COG/${d.substring(0, 4)}/${d.substring(4, 6)}/${d.substring(6, 8)}/$stacItemId.SAFE"
  }
  def stacItemUrl: URI = new URI("https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-grd/items/" + stacItemId)
  def acquisitionTime: ZonedDateTime = {
    val s = stacItemId.substring(17, 32) // yyyyMMddTHHmmss
    ZonedDateTime.parse(s"${s.substring(0, 4)}-${s.substring(4, 6)}-${s.substring(6, 8)}T${s.substring(9, 11)}:${s.substring(11, 13)}:${s.substring(13, 15)}Z")
  }
  override def toString: String = name
}

object FileLayerProviderS1GrdTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setUpSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[2]", appName = classOf[FileLayerProviderS1GrdTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()

  def scenes(): java.util.stream.Stream[S1GrdScene] = java.util.stream.Stream.of(
    S1GrdScene("belgium", "S1B_IW_GRDH_1SDV_20200606T060612_20200606T060637_021909_029944_1FC2_COG",
      Extent(-0.450784, 50.25919, 3.680459, 52.16246), Extent(3.1, 51.27, 3.3, 51.37), 32631),
    // Noise annotation whose range vectors have different pixel sample positions per line
    // ("LUT column count mismatch").
    S1GrdScene("noise-varying-pixel-grid", "S1A_IW_GRDH_1SDV_20231105T122240_20231105T122305_051082_0628E6_2BCB_COG",
      Extent(83.961121, 27.41968, 86.784721, 29.354591), Extent(85.3, 28.2, 85.4, 28.3), 32645)
  )
}

class FileLayerProviderS1GrdTest {
  import FileLayerProviderS1GrdTest._

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
  private def buildFeature(scene: S1GrdScene): Feature = {
    import scene._
    def measurementHref(pol: String) =
      URI.create(s"$safeRoot/measurement/s1a-iw-grd-$pol-x-001-cog.tiff")

    val links = Array(
      Link(href = measurementHref("vv"), title = Some("vv"), bandNames = Some(Seq("vv")), datatype = Some(UShortConstantNoDataCellType)),
      Link(href = measurementHref("vh"), title = Some("vh"), bandNames = Some(Seq("vh")), datatype = Some(UShortConstantNoDataCellType))
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

  @Disabled("Requires CDSE S3 credentials")
  @ParameterizedTest(name = "{0}")
  @MethodSource(Array("scenes"))
  def fileLayerProviderReturnsS1GrdTileLayer(scene: S1GrdScene): Unit = {
    import scene._
    val outputCrs = CRS.fromEpsgCode(outputEpsg)
    val outputExtent = ProjectedExtent(areaWgs84, LatLng).reproject(outputCrs)

    val feature = buildFeature(scene)

    val openSearchClient = new FixedFeaturesOpenSearchClient
    openSearchClient.addFeature(feature)

    // Request both VV and VH sigma0 bands at 10 m.
    val provider = FileLayerProvider(
      openSearch             = openSearchClient,
      openSearchCollectionId = "sentinel-1-grd",
      openSearchLinkTitles   = NonEmptyList.of("vv"),
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

    // Default config: nPols+1 bands = VV, VH, validity (angle bands and shadow/layover
    // mask are opt-in and off by default).
    val expectedBandCount = SarProcessingConfig.default.bandCount(2)
    val tiles = cube.values.collect()
    assertTrue(tiles.nonEmpty, "Must have at least one output tile")
    /*tiles.foreach { tile =>
      assertEquals(1, tile.bandCount,
        s"Each tile must carry $expectedBandCount bands")
    }*/

    // Save output GeoTIFF for visual inspection.
    saveRDDTemporal(cube, s"/tmp/s1grd-filelayerprovider-test/$name/", cropBounds = Some(outputExtent))
  }
}
