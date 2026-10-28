package org.openeo.sar

import geotrellis.proj4.CRS
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{CellSize, RasterSource}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.{Assumptions, Disabled, Test}
import org.openeo.geotrelliscommon.TestConditions
import org.openeo.sar.backend.nativ.NativeBackend
import org.openeo.sar.metadata.Polarisation

import java.net.URI

@Disabled("Jenkins has no valid S3 credentials, so this test fails with 403 Forbidden")
class TerrainCorrectionTest {

  private val runOnline = false  // requires CDSE S3 + STAC access

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
    assertEquals(1100, request.cols)
    assertEquals(1600, request.rows)
  }

  @Test
  def nativeBackendProducesExpectedTile(): Unit = {
    Assumptions.assumeTrue(TestConditions.hasS3Credentials, "No S3 credentials, skipping test")

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
    Assumptions.assumeTrue(TestConditions.hasEodataData(), "No local /eodata mapped, skipping test")
    Assumptions.assumeTrue(TestConditions.hasS3Credentials, "No S3 credentials, skipping test")

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
  def openScene2018Item(): Unit = {
    Assumptions.assumeTrue(TestConditions.hasEodataData(), "No local /eodata mapped, skipping test")

    val itemUrl = new URI(
      "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-grd/items/" +
        "S1A_IW_GRDH_1SDV_20180108T204309_20180108T204334_020068_022338_8AF7_COG"
    )

    val proc = TerrainCorrectionProcessor.withDemAndGeoid(
      backend      = new NativeBackend(),
      demFactory   = demFactory,
      geoidTiffUri = new URI("file:///home/dsamaey/Downloads/us_nga_egm96_15.tif")
    )

    val scene = proc.openScene(itemUrl, request.cellSize, request.crs, request.polarisations)

    assertEquals(request.polarisations, scene.polarisations)
    assertEquals(request.cellSize, scene.cellSize)
    assertEquals(request.crs, scene.crs)
    request.polarisations.foreach { pol =>
      assertTrue(scene.sarSources.contains(pol), s"missing SAR source for $pol")
      assertTrue(scene.metadata.polarisations.contains(pol), s"missing metadata for $pol")
    }
    assertNotNull(scene.demSource)
  }

  @Test
  def gamma0RtcWithShadowLayoverMask(): Unit = {
    Assumptions.assumeTrue(TestConditions.hasEodataData(), "No local /eodata mapped, skipping test")
    Assumptions.assumeTrue(TestConditions.hasS3Credentials, "No S3 credentials, skipping test")

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
}
