package org.openeo.sar

import geotrellis.proj4.CRS
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{CellSize, RasterSource}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Assumptions, Test}
import org.openeo.sar.backend.nativ.NativeBackend
import org.openeo.sar.backend.onnx.OnnxBackend
import org.openeo.sar.metadata.Polarisation

import java.net.URI

/** Smoke test that exercises the full pipeline end-to-end against a real CDSE
 *  STAC item.  Requires S3 credentials for `eodata` bucket and outbound HTTP
 *  to `catalogue.dataspace.copernicus.eu`; gated by the `runOnline` flag below.
 *
 *  AWS_ACCESS_KEY_ID=xxx;AWS_DEFAULT_REGION=default;AWS_ENDPOINT_URL=https://eodata.dataspace.copernicus.eu;AWS_HTTPS=YES;AWS_S3_ENDPOINT=eodata.dataspace.copernicus.eu;AWS_SECRET_ACCESS_KEY=xxx;AWS_VIRTUAL_HOSTING=FALSE
 *
 *  */
class TerrainCorrectionTest {

  private val runOnline = true  // requires CDSE S3 + STAC access

  // A Sentinel-1 IW GRDH product over Belgium. CDSE returns object-store
  // (`s3://eodata/...`) hrefs which GeoTrellis RasterSource handles natively.
  private val stacItemUrl = new URI(
    "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-grd/items/" +
      "S1A_IW_GRDH_1SDV_20260610T172444_20260610T172509_064910_082DFE_F1C5_COG"
  )

  // Output tile: 5 km x 5 km in UTM 31N at 20 m, centred on Brussels.
  private val request = TileRequest(
    extent        = Extent(595000.0, 5630000.0, 606000.0, 5646000.0),
    cellSize      = CellSize(10.0, 10.0),
    crs           = CRS.fromEpsgCode(32631),
    polarisations = Seq(Polarisation.VV, Polarisation.VH)
  )

  // Copernicus GLO-30 DEM mosaic on AWS open data; replace with the deployment-
  // local DEM source. GeoTrellis MosaicRasterSource composes per-tile COGs.
  private def demFactory(bboxWgs84: Extent): RasterSource =
    GeoTiffRasterSource(
      "s3://eodata/auxdata/CopDEM_COG/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E004_00_DEM/Copernicus_DSM_COG_10_N50_00_E004_00_DEM.tif")

  @Test
  def tileRequestComputesColsAndRows(): Unit = {
    assertEquals(300, request.cols)
    assertEquals(300, request.rows)
  }

  @Test
  def nativeBackendProducesExpectedTile(): Unit = {
    Assumptions.assumeTrue(runOnline, "online test disabled")

    val proc = TerrainCorrectionProcessor.withDemAndGeoid(
      backend      = new NativeBackend(),
      demFactory   = demFactory,
      geoidTiffUri = new URI("file:///home/driesj/code/java/openeo-geotrellis-extensions/sar-terrain-correction/egm96.tif")
    )
    val tile = proc.computeTile(stacItemUrl, request)
    assertEquals(request.config.bandCount(request.polarisations.size), tile.bandCount)
    assertEquals(request.cols, tile.cols)
    assertEquals(request.rows, tile.rows)
    GeoTiff(tile, request.extent, request.crs).write("/tmp/terrain-correction-test-10-S1A.tif")
  }

  @Test
  def nativeBackendMultipleTiles(): Unit = {
    Assumptions.assumeTrue(runOnline, "online test disabled")

    val proc = TerrainCorrectionProcessor.withDemAndGeoid(
      backend      = new NativeBackend(),
      demFactory   = demFactory,
      geoidTiffUri = new URI("file:///home/driesj/code/java/openeo-geotrellis-extensions/sar-terrain-correction/egm96.tif")
    )

    // Open scene once (XML parsing, RasterSource construction).
    val scene = proc.openScene(stacItemUrl, request.cellSize, request.crs, request.polarisations)

    // Tile the request extent into a 2x2 grid (four sub-tiles).
    val e = request.extent
    val subExtents = Seq(
      Extent(e.xmin, e.ymin + e.height / 2, e.xmin + e.width / 2, e.ymax),  // NW
      Extent(e.xmin + e.width / 2, e.ymin + e.height / 2, e.xmax, e.ymax),  // NE
      Extent(e.xmin, e.ymin, e.xmin + e.width / 2, e.ymin + e.height / 2),  // SW
      Extent(e.xmin + e.width / 2, e.ymin, e.xmax, e.ymin + e.height / 2),  // SE
    )

    val results = proc.readExtents(scene, subExtents).toList

    assertEquals(4, results.size)
    results.zipWithIndex.foreach { case (raster, i) =>
      assertEquals(request.config.bandCount(request.polarisations.size), raster.tile.bandCount, s"bandCount tile $i")
    }
  }

  @Test
  def gamma0RtcWithShadowLayoverMask(): Unit = {
    Assumptions.assumeTrue(runOnline, "online test disabled")

    val gamma0Config = SarProcessingConfig(
      normalization     = BackscatterNormalization.Gamma0RTC,
      shadowLayoverMask = true
    )
    val gamma0Request = request.copy(config = gamma0Config)

    val proc = TerrainCorrectionProcessor.withDemAndGeoid(
      backend      = new NativeBackend(),
      demFactory   = demFactory,
      geoidTiffUri = new URI("file:///home/driesj/code/java/openeo-geotrellis-extensions/sar-terrain-correction/egm96.tif")
    )
    val tile = proc.computeTile(stacItemUrl, gamma0Request)

    val nPols = gamma0Request.polarisations.size
    // VV, VH, validity, shadow/layover = 4 (no angle bands requested)
    assertEquals(gamma0Config.bandCount(nPols), tile.bandCount)
    assertEquals(gamma0Request.cols, tile.cols)
    assertEquals(gamma0Request.rows, tile.rows)

    // Shadow/layover band values must be 0, 1 or 2.
    // Band order: backscatter..., [ellipsInc], [localInc], mask, [shadowLayover].
    // Neither angle band is requested here, so shadow/layover directly follows mask.
    val slBand = tile.band(nPols + 1)
    var allValid = true
    var c = 0; while (c < tile.cols) {
      var r = 0; while (r < tile.rows) {
        val v = slBand.getDouble(c, r)
        if (!java.lang.Double.isNaN(v) && v != 0.0 && v != 1.0 && v != 2.0) allValid = false
        r += 1
      }
      c += 1
    }
    assertTrue(allValid, "Shadow/layover band must contain only 0, 1, 2 or NaN")

    GeoTiff(tile, gamma0Request.extent, gamma0Request.crs).write("/tmp/terrain-correction-gamma0-test.tif")
  }

  @Test
  def onnxBackendProducesExpectedTile(): Unit = {
    Assumptions.assumeTrue(runOnline, "online test disabled")

    val onnx = new OnnxBackend(getClass.getResource("/sar_tc.onnx").getPath)
    try {
      val proc = new TerrainCorrectionProcessor(
        backend          = onnx,
        demSourceFactory = demFactory
      )
      val tile = proc.computeTile(stacItemUrl, request)
      assertEquals(request.config.bandCount(request.polarisations.size), tile.bandCount)
      assertEquals(request.cols, tile.cols)
      assertEquals(request.rows, tile.rows)
    } finally onnx.close()
  }
}
