package org.openeo.sar.provider

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.layers.provider.RasterSourceDefinition
import org.openeo.geotrelliscommon.resolveClasspathResource
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}
import org.openeo.sar.TerrainCorrectionProcessor
import org.openeo.sar.TerrainCorrectionProcessor.geoidFromTiff
import org.openeo.sar.backend.nativ.NativeBackend

import java.net.{HttpURLConnection, URI, URL}
import java.nio.file.Files
import java.time.ZonedDateTime

/** Test for [[Sentinel1GrdRasterSourceProvider]] using the Zeebrugge
 *  Sentinel-1 GRD testdata served over HTTP from the
 *  openeo-geopyspark-driver-testdata repo.
 *
 *  The STAC item JSON next to the SAFE folder has relative asset hrefs
 *  (resolved by [[org.openeo.sar.stac.StacItemLoader]] against the item's own
 *  location), so annotation XML and measurement data are read relative to
 *  that URL.
 *
 *  The checked-in measurement GeoTIFFs are a pixel-space (`srcWin`) crop of
 *  the full-scene COG measurements, in native SAR geometry (no warping), to
 *  keep the repository small. The crop's position in the full scene is stored
 *  in the GeoTIFF metadata as `S1_CROP_COL_OFFSET` / `S1_CROP_ROW_OFFSET`.
 *  Since the terrain-correction backend indexes measurement rasters by the
 *  full-scene SAR line/pixel grid, the measurement [[RasterSource]] is wrapped
 *  so that windowed reads are translated into the crop file's own local pixel
 *  coordinates - see [[OffsetRasterSource]]. The annotation XMLs are the
 *  unmodified ones from the same (COG) product.
 *
 *  The reference GeoTIFF is a checked-in fixture next to the item JSON in the
 *  testdata repo; this test fails with a clear message if it's missing or
 *  unreachable, and otherwise compares the freshly computed backscatter
 *  against it, pixel by pixel. */
class Sentinel1GrdRasterSourceProviderTest extends RasterMatchers {

  /** Testdata is served over HTTP from the openeo-geopyspark-driver-testdata
   *  repo rather than a locally checked-out `testdata` directory, so the test
   *  runs without requiring that repo to be cloned alongside this one.
   *  GeoTrellis's GeoTiffRasterSource / GeoTiffReader and
   *  `org.openeo.sar.io.UriIO` already dispatch on the `http(s)://` scheme,
   *  so no local-file handling is needed. */
  private val testDataRoot =
    "../testdata/"

  private val safeDir =
    testDataRoot + "Sentinel-1/zeebrugge_2020_06_06.SAFE/"
  private val stacItemPath = new URI(
    safeDir + "S1B_IW_GRDH_1SDV_20200606T060612_20200606T060637_021909_029944_1FC2_COG.json")
  private val referenceTiff =
    safeDir + "S1B_IW_GRDH_1SDV_20200606T060612_20200606T060637_021909_029944_1FC2_COG_reference.tif"
  private val demTiff = testDataRoot +
    "copernicus-dem-30m/Copernicus_DSM_COG_10_N51_00_E003_00_DEM/Copernicus_DSM_COG_10_N51_00_E003_00_DEM.tif"

  private val vvMeasurement =
    safeDir + "measurement/s1b-iw-grd-vv-20200606t060615-20200606t060640-021909-029944-001.tiff"

  @Test
  def backscatterMatchesReference(): Unit = {

    // ---- Read the crop's full-scene pixel offset ---------------------------

    // Read via GeoTiffReader: the crop is GCP-georeferenced without GCPs at its
    // corners, which GeoTrellis cannot turn into a valid extent.
    val cropTags = GeoTiffReader.readMultiband(vvMeasurement, streaming = true).tags.headTags
    def cropOffset(key: String): Long =
      cropTags.get(key).map(_.trim.toLong)
        .getOrElse(fail(s"$key metadata missing from $vvMeasurement"))
    val colOffset: Long = cropOffset("S1_CROP_COL_OFFSET")
    val rowOffset: Long = cropOffset("S1_CROP_ROW_OFFSET")

    // ---- Build the provider with an offset-translating raster source factory ----

    val demFactory: Extent => RasterSource = _ => GeoTiffRasterSource(demTiff)
    val offsetRasterSourceFactory: URI => RasterSource = uri =>
      new OffsetRasterSource(uri.toString, colOffset, rowOffset)


    val processor = new TerrainCorrectionProcessor(
      backend             = new NativeBackend,
      demSourceFactory     = demFactory,
      geoidSourceFactory   = Some(geoidFromTiff(resolveClasspathResource("org/openeo/egm96.tif"))),
      rasterSourceFactory  = offsetRasterSourceFactory
    )
    val provider = new Sentinel1GrdRasterSourceProvider(processor)

    // ---- Output AOI: fixed target extent, well within the crop's bounds ----

    val outputCrs    = CRS.fromEpsgCode(32631)
    val outputExtent = ProjectedExtent(Extent(xmin = 3.1, ymin = 51.27, xmax = 3.3, ymax = 51.37), LatLng).reproject(outputCrs)
    val cellSize = CellSize(10.0, 10.0)

    def measurementHref(pol: String, idx: String) =
      new URI(safeDir + s"measurement/s1b-iw-grd-$pol-20200606t060612-20200606t060637-021909-029944-$idx.tiff")

    val links = Array(
      Link(href = measurementHref("vv", "001"), title = Some("vv"), bandNames = Some(Seq("vv")), datatype = Some(UShortConstantNoDataCellType)),
      Link(href = measurementHref("vh", "002"), title = Some("vh"), bandNames = Some(Seq("vh")), datatype = Some(UShortConstantNoDataCellType)),
    )

    val sceneBbox = Extent(-0.460228, 50.227345, 3.666933, 52.130386) // STAC item bbox
    val feature = Feature(
      id           = "S1B_IW_GRDH_1SDV_20200606T060612_20200606T060637_021909_029944_1FC2_COG",
      bbox         = sceneBbox,
      nominalDate  = ZonedDateTime.parse("2020-06-06T06:06:12Z"),
      links        = links,
      resolution   = None,
      crs          = Some(LatLng),
      rasterExtent = None,
      selfUrl      = Some(stacItemPath),
      collectionId = "sentinel-1-grd"
    )

    val definition = RasterSourceDefinition(
      link                  = links(0),
      bandIndex             = 0,
      feature               = feature,
      rootPath              = null,
      targetCellType        = None,
      targetExtent          = ProjectedExtent(outputExtent, outputCrs),
      featureExtentInLayout = None,
      targetResolution      = Some(cellSize),
      maxResolution         = cellSize,
      datacubeParams        = None,
      experimental          = false,
      bandName              = "vv",
      softErrors            = false
    )

    assertTrue(provider.canProcess(definition), "provider must recognise the S1 GRD measurement/STAC path")

    val raster = provider.rasterSource(definition).read(outputExtent)
      .getOrElse(fail("read() returned no raster for the requested extent"))

    assertEquals(3, raster.tile.bandCount, "expected VV, VH and validity bands (default SarProcessingConfig)")
    assertTrue(raster.tile.cols > 0 && raster.tile.rows > 0)

    // ---- Store result, compare against the per-pixel reference ----

    val actualPath = Files.createTempFile("s1grd-provider-test-actual", ".tif")
    println(actualPath)
    GeoTiff(raster, outputCrs).write(actualPath.toString)

    // The reference is a checked-in fixture in the testdata repo; unlike the
    // (now removed) local-directory fallback, there is no local path to create
    // it at, so a missing reference is just a hard failure here.
    //assertTrue(urlExists(referenceTiff), s"Reference raster not found at $referenceTiff")

    try {
      val reference = GeoTiff.readMultiband(referenceTiff).raster
      val actual    = GeoTiff.readMultiband(actualPath.toString).raster

      // Per-pixel (and extent/band-count/cell-type) comparison against the reference.
      assertRastersEqual(actual, reference, 1e-6)
    } finally {

      //Files.deleteIfExists(actualPath)
    }
  }

  /** HEAD-request reachability check, used in place of `Files.exists` now that
   *  testdata is served over HTTP(S) rather than from a local directory. */
  private def urlExists(url: String): Boolean = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod("HEAD")
      conn.setConnectTimeout(10000)
      conn.setReadTimeout(10000)
      conn.getResponseCode == HttpURLConnection.HTTP_OK
    } finally conn.disconnect()
  }
}

/** Serves a pixel-space crop of a SAR measurement GeoTIFF as if it were the
 *  full scene: windowed reads addressed in *full-scene* SAR line/pixel
 *  coordinates are translated into the crop file's own local pixel coordinates.
 *
 *  The crop is read directly with [[GeoTiffReader]] (streaming), ignoring its
 *  georeferencing: it only carries GCPs, none of which lie on its corners, so
 *  GeoTrellis derives an empty extent and [[GeoTiffRasterSource]] cannot be used.
 *  [[org.openeo.sar.backend.nativ.NativeBackend]] always reads measurement
 *  rasters by [[GridBounds]] (never by geographic [[Extent]]), so only that read
 *  path is implemented; the geographic [[RasterSource]] contract is irrelevant
 *  here and intentionally unsupported. */
private final class OffsetRasterSource(
  path: String,
  colOffset: Long,
  rowOffset: Long
) extends RasterSource {

  private lazy val tiff: MultibandGeoTiff = GeoTiffReader.readMultiband(path, streaming = true)

  override def name: SourceName = StringName(path)
  override def crs: CRS = LatLng
  override def bandCount: Int = tiff.bandCount
  override def cellType: CellType = tiff.cellType
  override def resolutions: List[CellSize] = List(gridExtent.cellSize)
  override def attributes: Map[String, String] = tiff.tags.headTags
  override def attributesForBand(band: Int): Map[String, String] = tiff.tags.bandTags.lift(band).getOrElse(Map.empty)
  override def metadata: RasterMetadata = this
  // Pixel-space grid of the crop (1 unit per pixel); not a real geographic extent.
  override def gridExtent: GridExtent[Long] =
    GridExtent[Long](Extent(0, 0, tiff.cols, tiff.rows), tiff.cols.toLong, tiff.rows.toLong)
  override def targetCellType: Option[TargetCellType] = None

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    throw new UnsupportedOperationException("OffsetRasterSource only supports GridBounds reads")

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val local = GridBounds[Int](
      (bounds.colMin - colOffset).toInt, (bounds.rowMin - rowOffset).toInt,
      (bounds.colMax - colOffset).toInt, (bounds.rowMax - rowOffset).toInt
    )
    require(local.colMin >= 0 && local.rowMin >= 0 && local.colMax < tiff.cols && local.rowMax < tiff.rows,
      s"Requested full-scene window $bounds maps to $local, outside the ${tiff.cols}x${tiff.rows} crop in $path; " +
        "regenerate the test data with a larger crop margin")
    val tile = tiff.tile.crop(local).subsetBands(bands)
    Some(Raster(tile, Extent(local.colMin, local.rowMin, local.colMax + 1, local.rowMax + 1)))
  }

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget,
                                      method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    throw new UnsupportedOperationException("OffsetRasterSource does not support reprojection")

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod,
                        strategy: OverviewStrategy): RasterSource =
    throw new UnsupportedOperationException("OffsetRasterSource does not support resampling")

  override def convert(targetCellType: TargetCellType): RasterSource = this
}
