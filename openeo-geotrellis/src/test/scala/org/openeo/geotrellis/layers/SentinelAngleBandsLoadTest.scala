package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.{FloatingLayoutScheme, SpaceTimeKey}
import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.{CellSize, FloatConstantNoDataCellType, GridBounds, GridExtent, MultibandTile, Raster, RasterExtent, UShortArrayTile, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, Assumptions, BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}
import org.junitpioneer.jupiter.SetEnvironmentVariable
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.file.{FixedFeaturesOpenSearchClient, PyramidFactory}
import org.openeo.geotrellis.layers.raster_source.SentinelAngleFields.{Angle, sunField, viewField}
import org.openeo.geotrellis.layers.raster_source.SentinelAngleRasterSource.tileAngles
import org.openeo.geotrellis.layers.raster_source.SentinelDetectorMask
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses
import org.xml.sax.SAXParseException

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.ZonedDateTime
import java.util
import java.util.Comparator
import scala.jdk.CollectionConverters._

/** Angle bands loaded through FileLayerProvider, as load_stac does (issue #916). */
object SentinelAngleBandsLoadTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setUpSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[1]", classOf[SentinelAngleBandsLoadTest].getName,
      new SparkConf().set("spark.ui.enabled", "false"))

  @AfterAll
  def tearDown(): Unit = {
    if (sc != null) sc.stop()
    Files.walk(tempDir).sorted(Comparator.reverseOrder[Path]()).forEach(path => Files.delete(path))
  }

  private val layers = "/org/openeo/geotrellis/layers"
  private val l1cPath = getClass.getResource(s"$layers/MTD_TL.xml").getPath
  private val l2aPath = getClass.getResource(
    s"$layers/S2B_MSIL2A_20220502T103619_N0400_R008_T31UFS_20220502T123919.SAFE" +
      "/GRANULE/L2A_T31UFS_A026918_20220502T103700/MTD_TL.xml").getPath

  private val utm31 = CRS.fromEpsgCode(32631)
  private val l2aTile = Extent(600000, 5590200, 709800, 5700000)
  private val l1cTile = Extent(499980, 5390220, 609780, 5500020)
  /** The area of interest: 2880 m, a whole number of 10, 20 and 60 m pixels, across the detector 2/3 boundary of B05. */
  private val aoi = Extent(642000, 5649960, 644880, 5652840)

  private val angleBands = (0 to 7).map(i => s"granule_metadata##$i")

  private val tempDir: Path = Files.createTempDirectory("SentinelAngleBandsLoadTest")

  /** The real B05 detector mask, read as a JP2 band (GDAL) like the spectral bands of a product. */
  private val b05MaskJp2 = l2aPath.replace("/MTD_TL.xml", "/QI_DATA/MSK_DETFOO_B05.jp2")

  /** B04 values by pixel column and row of the area of interest; the GeoTIFF extends 20 pixels beyond it. */
  private def b04Value(col: Int, row: Int): Int = 5000 + col + 150 * row

  /** A spatially varying 10 m B04 GeoTIFF covering the area of interest and a 200 m margin. */
  private lazy val b04Tiff: String = {
    val (margin, cols, rows) = (20, 328, 328)
    val tile = UShortArrayTile(Array.tabulate[Short](cols * rows)(i => b04Value(i % cols - margin, i / cols - margin).toShort),
      cols, rows, UShortConstantNoDataCellType)
    val path = tempDir.resolve("T31UFS_20220502T103619_B04_10m.tif").toString
    GeoTiffWriter.write(SinglebandGeoTiff(tile, aoi.buffer(200), utm31), path)
    path
  }

  private def factory(bands: Seq[String], resolution: Double, xmlPath: String = l2aPath, tile: Extent = l2aTile,
                      date: String = "2022-05-02T10:36:19Z", softErrors: Boolean = false,
                      b04Path: String = b04Tiff, includeMetadata: Boolean = true,
                      includeFeatureExtent: Boolean = true): PyramidFactory = {
    val client = new FixedFeaturesOpenSearchClient
    val bbox = tile.reproject(utm31, LatLng)
    val base = OpenSearchResponses.featureBuilder()
      .withId("S2_TEST")
      .withNominalDate(date)
      .withBBox(bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax)
    val located = if (includeFeatureExtent)
      base.withCRS("EPSG:32631").withRasterExtent(tile.xmin, tile.ymin, tile.xmax, tile.ymax)
    else base
    val withLinks = located
      .withResolution(10)
      .addLink(b04Path, "B04", util.Arrays.asList("B04"))
      .addLink(b05MaskJp2, "B05_DETFOO", util.Arrays.asList("B05_DETFOO"))
    val withMetadata = if (includeMetadata) withLinks.addLink(xmlPath, "granule_metadata", angleBands.asJava) else withLinks
    val feature = withMetadata.build
    client.addFeature(feature)
    new PyramidFactory(client, "S2_TEST", bands.asJava, rootPath = null,
      maxSpatialResolution = CellSize(resolution, resolution), maxSoftErrorsRatio = if (softErrors) 0.1 else 0.0)
  }

  /** Valid tile metadata, but its referenced B05 mask is either absent or not a JP2. */
  private def metadataWithBrokenMask(corrupt: Boolean): String = {
    val granule = "L2A_T31UFS_A026918_20220502T103700"
    val dir = Files.createDirectories(tempDir.resolve(s"broken-mask-$corrupt.SAFE/GRANULE/$granule"))
    val xml = dir.resolve("MTD_TL.xml")
    if (!Files.exists(xml)) {
      Files.copy(Paths.get(l2aPath), xml)
      if (corrupt) {
        val qi = Files.createDirectories(dir.resolve("QI_DATA"))
        Files.write(qi.resolve("MSK_DETFOO_B05.jp2"), Array[Byte](1, 2, 3))
      }
    }
    xml.toString
  }

  /** Synthetic GML B05 reference on the L2A geometry, to reuse the aligned integer JP2 fixture. No GML file is read. */
  private lazy val metadataWithGmlMask: String = {
    val dir = Files.createDirectories(tempDir.resolve("gml-mask.SAFE/GRANULE/G"))
    val xml = new String(Files.readAllBytes(Paths.get(l2aPath)), UTF_8)
    assertTrue(xml.contains("MSK_DETFOO_B05.jp2"))
    Files.write(dir.resolve("MTD_TL.xml"), xml.replace("MSK_DETFOO_B05.jp2", "MSK_DETFOO_B05.gml").getBytes(UTF_8)).toString
  }

  /** Valid tile metadata whose sun-angle grid is renamed away, leaving the per-pixel sun outputs unavailable. */
  private lazy val metadataWithoutSunGrid: String = {
    val dir = Files.createDirectories(tempDir.resolve("no-sun.SAFE/GRANULE/G"))
    val xml = new String(Files.readAllBytes(Paths.get(l2aPath)), UTF_8)
    assertTrue(xml.contains("<Sun_Angles_Grid>"))
    val withoutSun = xml
      .replace("<Sun_Angles_Grid>", "<UnavailableSunGrid>")
      .replace("</Sun_Angles_Grid>", "</UnavailableSunGrid>")
    assertFalse(withoutSun.contains("Sun_Angles_Grid"))
    Files.write(dir.resolve("MTD_TL.xml"), withoutSun.getBytes(UTF_8)).toString
  }

  /** The source-building path used by load_stac, without reading or stitching layout tiles. */
  private def source(bands: Seq[String], resolution: Double, parameters: DataCubeParameters = new DataCubeParameters,
                     b04Path: String = b04Tiff, experimental: Boolean = false,
                     target: ProjectedExtent = ProjectedExtent(aoi, utm31)): BandCompositeRasterSource = {
    val pyramid = factory(bands, resolution, b04Path = b04Path)
    val provider = FileLayerProvider(pyramid.openSearchClient, "S2_TEST", NonEmptyList.fromListUnsafe(bands.toList),
      null, CellSize(resolution, resolution), SplitYearMonthDayPathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256), experimental = experimental)
    val sources = provider.loadRasterSourceRDD(target, ZonedDateTime.parse("2022-05-02T00:00:00Z"),
      ZonedDateTime.parse("2022-05-02T23:59:59Z"), 0, Some(parameters), Some(CellSize(resolution, resolution)))
    assertEquals(1, sources.size)
    sources.head._1.asInstanceOf[BandCompositeRasterSource]
  }

  /** Independent native-pixel reference for the spatially varying 10 m GeoTIFF. */
  private def assertGeoTiffValues(raster: Raster[MultibandTile], index: Int, crs: CRS): Unit = {
    val (xs, ys) = centresInTileCrs(raster, crs)
    val expected = xs.indices.map { i =>
      val col = math.floor((xs(i) - aoi.xmin) / 10).toInt
      val row = math.floor((aoi.ymax - ys(i)) / 10).toInt
      // The reprojected envelope can extend beyond the GeoTIFF's 20-pixel margin.
      if (col < -20 || row < -20 || col >= 308 || row >= 308) Float.NaN else b04Value(col, row).toFloat
    }.toArray
    assertArrayEquals(expected, band(raster, index), "every GeoTIFF value must retain its geographic position")
  }

  private def assertGeoTiffAndSunValues(raster: Raster[MultibandTile], bands: Seq[String], crs: CRS): Unit = {
    assertEquals(bands.size, raster.tile.bandCount)
    val (xs, ys) = centresInTileCrs(raster, crs)
    bands.zipWithIndex.foreach {
      case ("B04", index) => assertGeoTiffValues(raster, index, crs)
      case ("granule_metadata##4", index) =>
        assertClose(sunField(l2aSun, Angle.Azimuth, l2aTile, xs, ys), band(raster, index), "sun azimuth position")
      case ("granule_metadata##5", index) =>
        assertClose(sunField(l2aSun, Angle.Zenith, l2aTile, xs, ys), band(raster, index), "sun zenith position")
      case ("granule_metadata##0", index) =>
        assertTrue(band(raster, index).forall(_ == 159.91357727290500f), "constant sun azimuth")
      case (other, _) => fail(s"unexpected band in the sun/GeoTIFF reference: $other")
    }
  }

  /** Builds the layout-tile cube for `area` (in `crs`), without stitching or cropping it. */
  private def loadLayer(factory: PyramidFactory, area: Extent, crs: CRS, day: String = "2022-05-02",
                        loadPerProduct: Boolean = false, retainNoDataTiles: Boolean = false)
                       : MultibandTileLayerRDD[SpaceTimeKey] = {
    val polygons = ProjectedPolygons.fromExtent(area, s"EPSG:${crs.epsgCode.get}")
    val parameters = new DataCubeParameters
    parameters.layoutScheme = "FloatingLayoutScheme"
    parameters.globalExtent = Some(polygons.extent)
    parameters.loadPerProduct = loadPerProduct
    parameters.retainNoDataTiles = retainNoDataTiles
    val Seq((_, layer)) = factory.datacube_seq(polygons, s"${day}T00:00:00Z", s"${day}T23:59:59Z",
      util.Collections.emptyMap(), "", parameters)
    layer
  }

  /** Loads `area` (in `crs`) and returns it stitched, with its CRS. */
  private def load(factory: PyramidFactory, area: Extent, crs: CRS, day: String = "2022-05-02",
                   loadPerProduct: Boolean = false, retainNoDataTiles: Boolean = false): Raster[MultibandTile] = {
    // Stitch whole layout tiles first: cropping a partial tile before stitching reports a shifted extent.
    loadLayer(factory, area, crs, day, loadPerProduct, retainNoDataTiles).toSpatial().stitch().crop(area)
  }

  /** Pixel centres of `raster`, transformed from `crs` to the tile CRS. */
  private def centresInTileCrs(raster: Raster[MultibandTile], crs: CRS): (Array[Double], Array[Double]) = {
    val re = RasterExtent(raster.extent, raster.cols, raster.rows)
    val toTile = Transform(crs, utm31)
    val points = for (row <- 0 until raster.rows; col <- 0 until raster.cols) yield toTile(re.gridColToMap(col), re.gridRowToMap(row))
    (points.map(_._1).toArray, points.map(_._2).toArray)
  }

  private def band(raster: Raster[MultibandTile], index: Int): Array[Float] =
    raster.tile.band(index).toArrayDouble().map(_.toFloat)

  private def assertClose(expected: Array[Float], actual: Array[Float], what: String): Unit = {
    assertEquals(expected.length, actual.length, what)
    for (i <- expected.indices)
      assertTrue(math.abs(expected(i) - actual(i)) <= 1e-4 || (expected(i).isNaN && actual(i).isNaN),
        s"$what, pixel $i: expected ${expected(i)}, got ${actual(i)}")
  }

  private lazy val l2aSun = tileAngles(l2aPath).metadata.sunGrid.toOption.get

  private lazy val gdalAvailable: Boolean =
    try {
      com.azavea.gdal.GDALWarp.get_version_info("VERSION_NUM")
      true
    } catch {
      case _: LinkageError => false
    }
}

class SentinelAngleBandsLoadTest {

  import SentinelAngleBandsLoadTest._

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def constantAndPerPixelAnglesInRequestedOrder(loadPerProduct: Boolean): Unit = {
    val bands = Seq("granule_metadata##5", "granule_metadata##0", "granule_metadata##4", "granule_metadata##1")
    val raster = load(factory(bands, 10), aoi, utm31, loadPerProduct = loadPerProduct)
    assertEquals((288, 288, 4), (raster.cols, raster.rows, raster.tile.bandCount))

    val (xs, ys) = centresInTileCrs(raster, utm31)
    assertClose(sunField(l2aSun, Angle.Zenith, l2aTile, xs, ys), band(raster, 0), "per-pixel sun zenith")
    assertClose(sunField(l2aSun, Angle.Azimuth, l2aTile, xs, ys), band(raster, 2), "per-pixel sun azimuth")
    assertTrue(band(raster, 2).distinct.length > 100, "sun azimuth varies per pixel")

    // The constant bands keep their values.
    assertTrue(band(raster, 1).forall(_ == 159.91357727290500f), "constant sun azimuth")
    assertTrue(band(raster, 3).forall(_ == 36.85617250897f), "constant sun zenith")
  }

  @Test
  def geoTiffAngleAlignmentDoesNotOpenTheImagery(): Unit = {
    val missing = tempDir.resolve("must-not-be-opened.tif")
    assertFalse(Files.exists(missing))
    val composite = source(Seq("B04", "granule_metadata##4"), 10, b04Path = missing.toString)
    assertEquals(2, composite.bandCount)
    assertTrue(composite.sources.forall(_.gridExtent == composite.gridExtent), "the shared grid needs no GeoTIFF header")
  }

  @Test
  def experimentalGeoTiffAndAnglesShareTheGrid(): Unit = {
    Assumptions.assumeTrue(gdalAvailable, "GDAL (libgdal) is not installed here")
    val bands = Seq("B04", "granule_metadata##4", "granule_metadata##5")
    for (crs <- Seq(utm31, CRS.fromEpsgCode(32632))) {
      val area = aoi.reproject(utm31, crs)
      val composite = source(bands, 10, experimental = true, target = ProjectedExtent(area, crs))
      assertTrue(composite.sources.forall(_.gridExtent == composite.gridExtent))
      val bounds = GridBounds[Long](7, 11, 35, 38)
      val raster = composite.read(bounds).get
      assertEquals((29, 28), (raster.cols, raster.rows))
      assertEquals(composite.gridExtent.extentFor(bounds), raster.extent)
      assertGeoTiffAndSunValues(raster, bands, crs)
    }
  }

  @ParameterizedTest
  @ValueSource(doubles = Array(10.0, 20.0, 60.0))
  def geoTiffAndAnglesShareBoundsAndExtentReads(resolution: Double): Unit = {
    // Do not change the grid of reflectance-only requests as a side effect of angle alignment.
    assertEquals(aoi, source(Seq("B04"), resolution).gridExtent.extent)
    for (bands <- Seq(
      Seq("B04", "granule_metadata##5", "granule_metadata##0", "granule_metadata##4"),
      Seq("granule_metadata##4", "B04", "granule_metadata##5", "granule_metadata##0"));
      noResampleOnRead <- Seq(false, true)) {
      val parameters = new DataCubeParameters
      parameters.noResampleOnRead = noResampleOnRead
      val composite = source(bands, resolution, parameters)
      assertTrue(composite.sources.forall(_.gridExtent == composite.gridExtent), "all bands must use one pixel grid")
      val grid = composite.gridExtent
      val windows = Seq(
        GridBounds[Long](-2, -3, 6, 9),
        GridBounds[Long](7, 11, 35, 38),
        GridBounds[Long](grid.cols - 8, grid.rows - 12, grid.cols + 4, grid.rows + 5))
      val expected = windows.map { window =>
        val clipped = window.intersection(grid.dimensions).get
        val raster = composite.read(window).get
        assertEquals((clipped.width.toInt, clipped.height.toInt), (raster.cols, raster.rows))
        assertEquals(grid.extentFor(clipped), raster.extent)
        assertGeoTiffAndSunValues(raster, bands, utm31)
        val byExtent = composite.read(raster.extent).get
        assertEquals(raster.extent, byExtent.extent)
        for (index <- bands.indices) assertArrayEquals(band(raster, index), band(byExtent, index))
        val selection = Seq(3, 1, 2, 0)
        assertGeoTiffAndSunValues(composite.read(window, selection).get, selection.map(bands), utm31)
        raster
      }
      for (batched <- Seq(composite.readBounds(windows).toVector, composite.readBoundsFullTile(windows).toVector)) {
        assertEquals(windows.size, batched.size, "batched windows must not be dropped or reordered")
        expected.zip(batched).foreach { case (single, batch) =>
          assertEquals(single.extent, batch.extent)
          assertEquals((single.cols, single.rows), (batch.cols, batch.rows))
          for (index <- bands.indices) assertArrayEquals(band(single, index), band(batch, index))
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource(Array("false,10", "false,20", "false,60", "true,10", "true,20", "true,60"))
  def geoTiffAndSunAngleBandsShareTheGrid(loadPerProduct: Boolean, resolution: Double): Unit = {
    val area = Extent(aoi.xmin + 120, aoi.ymin + 180, aoi.xmax - 60, aoi.ymax - 120)
    for (bands <- Seq(
      Seq("B04", "granule_metadata##5", "granule_metadata##0", "granule_metadata##4"),
      Seq("granule_metadata##4", "B04", "granule_metadata##5", "granule_metadata##0"))) {
      val raster = load(factory(bands, resolution), area, utm31, loadPerProduct = loadPerProduct)
      assertEquals((2700 / resolution, 2580 / resolution), (raster.cols.toDouble, raster.rows.toDouble))
      assertEquals(area, raster.extent)
      assertGeoTiffAndSunValues(raster, bands, utm31)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def geoTiffAndAnglesAlignAfterReprojection(loadPerProduct: Boolean): Unit = {
    val utm32 = CRS.fromEpsgCode(32632)
    val projected = aoi.reproject(utm31, utm32)
    val area = Extent(math.ceil(projected.xmin / 20) * 20, math.ceil(projected.ymin / 20) * 20,
      math.floor(projected.xmax / 20) * 20, math.floor(projected.ymax / 20) * 20)
    for (bands <- Seq(Seq("B04", "granule_metadata##4"), Seq("granule_metadata##4", "B04"))) {
      val raster = load(factory(bands, 20), area, utm32, loadPerProduct = loadPerProduct)
      assertEquals((area.width / 20, area.height / 20), (raster.cols.toDouble, raster.rows.toDouble))
      assertEquals(area, raster.extent)
      assertGeoTiffAndSunValues(raster, bands, utm32)
    }
  }

  @ParameterizedTest
  @CsvSource(Array("false,false", "false,true", "true,false", "true,true"))
  def geoTiffAndViewAngleBandsShareTheGrid(loadPerProduct: Boolean, softErrors: Boolean): Unit = {
    Assumptions.assumeTrue(gdalAvailable, "GDAL (libgdal) is not installed here")
    val angles = tileAngles(l2aPath)
    val viewing = angles.metadata.viewing(4).toOption.get
    for (bands <- Seq(
      Seq("granule_metadata##6", "B04", "B05_DETFOO", "granule_metadata##5", "granule_metadata##7"),
      Seq("B04", "granule_metadata##7", "B05_DETFOO", "granule_metadata##6", "granule_metadata##5"))) {
      val raster = load(factory(bands, 20, softErrors = softErrors), aoi, utm31, loadPerProduct = loadPerProduct)
      assertEquals((144, 144, 5), (raster.cols, raster.rows, raster.tile.bandCount))
      assertEquals(aoi, raster.extent)
      val (xs, ys) = centresInTileCrs(raster, utm31)
      val labels = SentinelDetectorMask.detectorLabels(l2aPath, angles.metadata, 4, viewing.detectorMask, xs, ys)
      assertEquals(Set(2, 3), labels.toSet)
      bands.zipWithIndex.foreach {
        case ("B04", index) => assertGeoTiffValues(raster, index, utm31)
        case ("B05_DETFOO", index) => assertArrayEquals(labels.map(_.toFloat), band(raster, index), "JP2 detector labels")
        case ("granule_metadata##5", index) =>
          assertClose(sunField(l2aSun, Angle.Zenith, l2aTile, xs, ys), band(raster, index), "sun zenith position")
        case (name, index) =>
          val angle = if (name == "granule_metadata##6") Angle.Azimuth else Angle.Zenith
          assertClose(viewField(angles.filledViewGrids(4), angle, l2aTile, xs, ys, labels), band(raster, index), name)
      }
    }
  }

  @ParameterizedTest
  @CsvSource(Array("false,false", "false,true", "true,false", "true,true"))
  def gmlViewAnglesAndGeoTiffKeepTheirGrid(loadPerProduct: Boolean, softErrors: Boolean): Unit = {
    for (bands <- Seq(Seq("granule_metadata##7", "B04", "granule_metadata##6"),
      Seq("B04", "granule_metadata##6", "granule_metadata##7"))) {
      val raster = load(factory(bands, 20, xmlPath = metadataWithGmlMask, softErrors = softErrors),
        aoi, utm31, loadPerProduct = loadPerProduct)
      assertEquals((144, 144, 3), (raster.cols, raster.rows, raster.tile.bandCount))
      assertEquals(aoi, raster.extent)
      bands.zipWithIndex.foreach { case (name, index) =>
        assertEquals(FloatConstantNoDataCellType, raster.tile.band(index).cellType)
        if (name == "B04") assertGeoTiffValues(raster, index, utm31)
        else assertTrue(band(raster, index).forall(_.isNaN), "GML views must stay NoData")
      }
    }
  }

  /** A JP2 band read with GDAL, as the spectral bands are, in one cube with angle bands. Runs where GDAL is installed. */
  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def jp2BandAndAngleBandsShareTheGrid(loadPerProduct: Boolean): Unit = {
    Assumptions.assumeTrue(gdalAvailable, "GDAL (libgdal) is not installed here")
    val bands = Seq("B05_DETFOO", "granule_metadata##5", "granule_metadata##7", "granule_metadata##0")
    val raster = load(factory(bands, 20), aoi, utm31, loadPerProduct = loadPerProduct)

    val (xs, ys) = centresInTileCrs(raster, utm31)
    val angles = tileAngles(l2aPath)
    val labels = SentinelDetectorMask.detectorLabels(l2aPath, angles.metadata, 4,
      angles.metadata.viewing(4).toOption.get.detectorMask, xs, ys)
    assertArrayEquals(labels.map(_.toFloat), band(raster, 0), "the JP2 band holds the detector labels")
    assertClose(sunField(l2aSun, Angle.Zenith, l2aTile, xs, ys), band(raster, 1), "per-pixel sun zenith")
    assertClose(viewField(angles.filledViewGrids(4), Angle.Zenith, l2aTile, xs, ys, labels), band(raster, 2), "view zenith")
    assertTrue(band(raster, 3).forall(_ == 159.91357727290500f), "constant sun azimuth")
  }

  @ParameterizedTest
  @ValueSource(doubles = Array(10.0, 20.0, 60.0))
  def perPixelSunAnglesAlignWithTheCube(resolution: Double): Unit = {
    val raster = load(factory(Seq("granule_metadata##4", "granule_metadata##5"), resolution), aoi, utm31)
    val size = (aoi.width / resolution).toInt
    assertEquals((size, size), (raster.cols, raster.rows))
    val (xs, ys) = centresInTileCrs(raster, utm31)
    assertClose(sunField(l2aSun, Angle.Azimuth, l2aTile, xs, ys), band(raster, 0), s"sun azimuth at $resolution m")
    assertClose(sunField(l2aSun, Angle.Zenith, l2aTile, xs, ys), band(raster, 1), s"sun zenith at $resolution m")
  }

  @Test
  def perPixelSunAnglesInAnotherUtmZone(): Unit = {
    val utm32 = CRS.fromEpsgCode(32632)
    val area = {
      val e = aoi.reproject(utm31, utm32)
      Extent(math.ceil(e.xmin / 20) * 20, math.ceil(e.ymin / 20) * 20, math.floor(e.xmax / 20) * 20, math.floor(e.ymax / 20) * 20)
    }
    val raster = load(factory(Seq("granule_metadata##4", "granule_metadata##0"), 20), area, utm32)
    val (xs, ys) = centresInTileCrs(raster, utm32)
    val expected = sunField(l2aSun, Angle.Azimuth, l2aTile, xs, ys)
    assertTrue(expected.forall(!_.isNaN))
    assertClose(expected, band(raster, 0), "sun azimuth in UTM 32N")
    assertTrue(band(raster, 1).forall(_ == 159.91357727290500f), "constant sun azimuth")
  }

  @ParameterizedTest
  @CsvSource(Array("false,false", "false,true", "true,false", "true,true"))
  def gmlMasksGiveNoDataViewAnglesAndValidSunAngles(loadPerProduct: Boolean, softErrors: Boolean): Unit = {
    val area = Extent(550000, 5440000, 552560, 5442560)
    val raster = load(factory(Seq("granule_metadata##5", "granule_metadata##7", "granule_metadata##6"), 20, l1cPath, l1cTile,
      date = "2020-11-06T10:54:41Z", softErrors = softErrors), area, utm31, day = "2020-11-06", loadPerProduct = loadPerProduct)
    assertEquals((128, 128, 3), (raster.cols, raster.rows, raster.tile.bandCount))
    assertEquals(area, raster.extent)
    assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType))
    val l1cSun = tileAngles(l1cPath).metadata.sunGrid.toOption.get
    val (xs, ys) = centresInTileCrs(raster, utm31)
    assertClose(sunField(l1cSun, Angle.Zenith, l1cTile, xs, ys), band(raster, 0), "L1C sun zenith")
    for (index <- Seq(1, 2)) {
      assertTrue(raster.tile.band(index).isNoDataTile, "GML products have no per-pixel viewing angles")
      assertTrue(band(raster, index).forall(_.isNaN), "no mean-angle substitution")
    }
  }

  @ParameterizedTest
  @CsvSource(Array("false,false", "false,true", "true,false", "true,true"))
  def gmlViewOnlyCubesKeepFloatNoData(loadPerProduct: Boolean, softErrors: Boolean): Unit = {
    val area = Extent(550000, 5440000, 552560, 5442560)
    for (indices <- Seq(Seq(6), Seq(7), Seq(7, 6))) {
      val bands = indices.map(i => s"granule_metadata##$i")
      val raster = load(factory(bands, 20, l1cPath, l1cTile, date = "2020-11-06T10:54:41Z", softErrors = softErrors),
        area, utm31, day = "2020-11-06", loadPerProduct = loadPerProduct, retainNoDataTiles = true)
      assertEquals((128, 128, indices.size), (raster.cols, raster.rows, raster.tile.bandCount))
      assertEquals(area, raster.extent)
      for (index <- indices.indices) {
        assertEquals(FloatConstantNoDataCellType, raster.tile.band(index).cellType, s"view-only bands $indices")
        assertTrue(raster.tile.band(index).isNoDataTile)
        assertTrue(band(raster, index).forall(_.isNaN), "unsupported GML views must stay NaN without a sun band")
      }
    }
  }

  @ParameterizedTest
  @CsvSource(Array("false,false", "false,true", "true,false", "true,true"))
  def gmlViewAnglesKeepFloatTypeWithIntegerBands(loadPerProduct: Boolean, softErrors: Boolean): Unit = {
    Assumptions.assumeTrue(gdalAvailable, "GDAL (libgdal) is not installed here")
    val bands = Seq("granule_metadata##7", "B05_DETFOO", "granule_metadata##6")
    val raster = load(factory(bands, 20, xmlPath = metadataWithGmlMask, softErrors = softErrors),
      aoi, utm31, loadPerProduct = loadPerProduct)
    assertEquals((144, 144, 3), (raster.cols, raster.rows, raster.tile.bandCount))
    assertEquals(aoi, raster.extent)
    for (index <- bands.indices)
      assertEquals(FloatConstantNoDataCellType, raster.tile.band(index).cellType, s"mixed band $index must be float32")
    for (index <- Seq(0, 2)) {
      assertTrue(raster.tile.band(index).isNoDataTile)
      assertTrue(band(raster, index).forall(_.isNaN), "an integer band must not change the viewing NoData type")
    }
    val (xs, ys) = centresInTileCrs(raster, utm31)
    val angles = tileAngles(l2aPath)
    val labels = SentinelDetectorMask.detectorLabels(l2aPath, angles.metadata, 4,
      angles.metadata.viewing(4).toOption.get.detectorMask, xs, ys)
    assertArrayEquals(labels.map(_.toFloat), band(raster, 1), "the healthy integer JP2 band must retain every value and its position")
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def missingMetadataIsASoftErrorOrAFailure(loadPerProduct: Boolean): Unit = {
    val missing = tempDir.resolve("missing.SAFE/GRANULE/G/MTD_TL.xml").toString
    val bands = Seq("B04", "granule_metadata##5", "granule_metadata##0")

    val raster = load(factory(bands, 10, xmlPath = missing, softErrors = true), aoi, utm31,
      loadPerProduct = loadPerProduct)
    assertEquals(b04Value(5, 7), raster.tile.band(0).get(5, 7))
    assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
      "metadata soft errors must keep every angle band float32")
    assertTrue(raster.tile.band(1).isNoDataTile && raster.tile.band(2).isNoDataTile)
    assertTrue(band(raster, 1).forall(_.isNaN) && band(raster, 2).forall(_.isNaN),
      "missing metadata must give NaN angle NoData")

    val error = assertThrows(classOf[Exception], () => {
      load(factory(bands, 10, xmlPath = missing), aoi, utm31, loadPerProduct = loadPerProduct)
      ()
    })
    assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[FileNotFoundException]),
      s"expected a FileNotFoundException, got $error")
  }

  @ParameterizedTest
  @CsvSource(Array("false,false", "false,true", "true,false", "true,true"))
  def malformedXmlFollowsTheLoadingSoftErrorPolicy(loadPerProduct: Boolean, softErrors: Boolean): Unit = {
    val bands = Seq("granule_metadata##7", "B04", "granule_metadata##4", "granule_metadata##6", "granule_metadata##5")
    for (xml <- Seq("<broken>", "<broken></mismatch>")) {
      val dir = Files.createTempDirectory(tempDir, "malformed-xml-")
      val path = Files.write(dir.resolve("MTD_TL.xml"), xml.getBytes(UTF_8)).toString
      def read(): Raster[MultibandTile] =
        load(factory(bands, 10, xmlPath = path, softErrors = softErrors), aoi, utm31, loadPerProduct = loadPerProduct)

      if (softErrors) {
        val raster = read()
        assertEquals((288, 288, 5), (raster.cols, raster.rows, raster.tile.bandCount))
        assertEquals(aoi, raster.extent)
        assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
          "malformed metadata must keep every angle band float32")
        for (index <- Seq(0, 2, 3, 4)) {
          assertTrue(raster.tile.band(index).isNoDataTile, s"failed angle band $index must be NoData")
          assertTrue(band(raster, index).forall(_.isNaN), s"malformed angle band $index must be NaN")
        }
        val expected = Array.tabulate[Float](raster.cols * raster.rows)(i => b04Value(i % raster.cols, i / raster.cols).toFloat)
        assertArrayEquals(expected, band(raster, 1), "the healthy reflectance band must retain its values and position")
      } else {
        val error = assertThrows(classOf[Exception], () => { read(); () })
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toVector
        assertTrue(causes.exists(e => e.isInstanceOf[IllegalArgumentException] &&
          Option(e.getMessage).exists(_.startsWith(s"invalid Sentinel-2 tile metadata $path:"))), error.toString)
        assertTrue(causes.exists(_.isInstanceOf[SAXParseException]), error.toString)
      }
    }
  }

  @ParameterizedTest
  @CsvSource(Array("false,false", "false,true", "true,false", "true,true"))
  def malformedXmlTileMeanAnglesFollowTheLoadingPolicy(loadPerProduct: Boolean, softErrors: Boolean): Unit = {
    val dir = Files.createTempDirectory(tempDir, "malformed-mean-xml-")
    val path = Files.write(dir.resolve("MTD_TL.xml"), "<broken>".getBytes(UTF_8)).toString
    val bands = Seq("B04", "granule_metadata##1", "granule_metadata##2")
    def read(): Raster[MultibandTile] =
      load(factory(bands, 10, xmlPath = path, softErrors = softErrors), aoi, utm31, loadPerProduct = loadPerProduct)

    if (softErrors) {
      val raster = read()
      assertEquals((288, 288, 3), (raster.cols, raster.rows, raster.tile.bandCount))
      assertEquals(aoi, raster.extent)
      assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
        "malformed mean metadata must keep every angle band float32")
      val expected = Array.tabulate[Float](raster.cols * raster.rows)(i => b04Value(i % raster.cols, i / raster.cols).toFloat)
      assertArrayEquals(expected, band(raster, 0), "the healthy reflectance band must retain its values and position")
      for (index <- 1 until bands.size) {
        assertTrue(raster.tile.band(index).isNoDataTile, s"failed mean angle band $index must be NoData")
        assertTrue(band(raster, index).forall(_.isNaN), s"failed mean angle band $index must be NaN")
      }
    } else {
      val error = assertThrows(classOf[Exception], () => { read(); () })
      val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toVector
      assertTrue(causes.exists(e => e.isInstanceOf[IllegalArgumentException] &&
        Option(e.getMessage).exists(_.startsWith(s"invalid Sentinel-2 tile metadata $path:"))), error.toString)
      assertTrue(causes.exists(_.isInstanceOf[SAXParseException]), error.toString)
    }
  }

  @Test
  def angleOnlyMalformedMetadataKeepsFloatNoData(): Unit = {
    val dir = Files.createTempDirectory(tempDir, "angle-only-malformed-")
    val path = Files.write(dir.resolve("MTD_TL.xml"), "<broken>".getBytes(UTF_8)).toString
    val bands = Seq("granule_metadata##1", "granule_metadata##6")
    val raster = load(factory(bands, 10, xmlPath = path, softErrors = true), aoi, utm31, retainNoDataTiles = true)

    assertEquals((288, 288, 2), (raster.cols, raster.rows, raster.tile.bandCount))
    assertEquals(aoi, raster.extent)
    assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
      "angle-only soft metadata failures must keep float32")
    for (index <- bands.indices) {
      assertTrue(raster.tile.band(index).isNoDataTile, s"failed angle band $index must be NoData")
      assertTrue(band(raster, index).forall(_.isNaN), s"failed angle band $index must be NaN")
    }
  }

  @Test
  def metadataFallbackGridUsesItemGridOrTargetGrid(): Unit = {
    val target = GridExtent[Long](Extent(0, 0, 20, 20), CellSize(10, 10))
    val item = GridExtent[Long](Extent(0, 0, 10, 10), CellSize(10, 10))
    assertEquals(item, FileLayerProvider.metadataFallbackGrid(Some(item), target))
    assertEquals(target, FileLayerProvider.metadataFallbackGrid(None, target))
  }

  @Test
  def angleOnlyMalformedMetadataWithoutFeatureExtentUsesTargetGrid(): Unit = {
    val dir = Files.createTempDirectory(tempDir, "angle-only-no-feature-grid-")
    val path = Files.write(dir.resolve("MTD_TL.xml"), "<broken>".getBytes(UTF_8)).toString
    val bands = Seq("granule_metadata##1", "granule_metadata##6")
    val raster = load(factory(bands, 10, xmlPath = path, softErrors = true, includeFeatureExtent = false),
      aoi, utm31, retainNoDataTiles = true)

    assertEquals((288, 288, 2), (raster.cols, raster.rows, raster.tile.bandCount))
    assertEquals(aoi, raster.extent)
    assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
      "angle-only metadata without a feature grid must keep float32")
    for (index <- bands.indices) {
      assertTrue(raster.tile.band(index).isNoDataTile, s"failed angle band $index must be NoData")
      assertTrue(band(raster, index).forall(_.isNaN), s"failed angle band $index must be NaN")
    }
  }

  @Test
  def angleOnlyMalformedMetadataWithoutRetentionHasNoTiles(): Unit = {
    val dir = Files.createTempDirectory(tempDir, "angle-only-no-retention-")
    val path = Files.write(dir.resolve("MTD_TL.xml"), "<broken>".getBytes(UTF_8)).toString
    val bands = Seq("granule_metadata##1", "granule_metadata##6")
    val layer = loadLayer(factory(bands, 10, xmlPath = path, softErrors = true), aoi, utm31)

    assertTrue(layer.isEmpty, "all-NoData angle-only tiles must follow the default retention policy")
  }

  @Test
  def angleOnlyValidMetadataWithoutFeatureExtentLoads(): Unit = {
    val bands = Seq("granule_metadata##1", "granule_metadata##6")
    val raster = load(factory(bands, 10, includeFeatureExtent = false), aoi, utm31, retainNoDataTiles = true)

    assertEquals((288, 288, 2), (raster.cols, raster.rows, raster.tile.bandCount))
    assertEquals(aoi, raster.extent)
    assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
      "valid angle-only metadata without a feature grid must stay float32")
    for (index <- bands.indices) {
      assertFalse(raster.tile.band(index).isNoDataTile, s"valid angle band $index must not be NoData")
    }
  }

  @Test
  def angleOnlyValidMetadataWithoutFeatureExtentReprojects(): Unit = {
    val utm32 = CRS.fromEpsgCode(32632)
    val projected = aoi.reproject(utm31, utm32)
    val area = Extent(math.ceil(projected.xmin / 20) * 20, math.ceil(projected.ymin / 20) * 20,
      math.floor(projected.xmax / 20) * 20, math.floor(projected.ymax / 20) * 20)
    val bands = Seq("granule_metadata##1", "granule_metadata##6")
    val raster = load(factory(bands, 20, includeFeatureExtent = false), area, utm32, retainNoDataTiles = true)

    assertEquals((area.width / 20, area.height / 20), (raster.cols.toDouble, raster.rows.toDouble))
    assertEquals(area, raster.extent)
    assertFalse(raster.tile.bands.exists(_.isNoDataTile), "valid reprojected angle bands must not be NoData")
  }

  @Test
  def absentMetadataAssetsKeepFloatNoData(): Unit = {
    val bands = Seq("B04", "granule_metadata##1", "granule_metadata##6")
    val raster = load(factory(bands, 10, includeMetadata = false), aoi, utm31)

    assertEquals((288, 288, 3), (raster.cols, raster.rows, raster.tile.bandCount))
    assertEquals(aoi, raster.extent)
    assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
      "an absent metadata asset must not force integer NoData")
    val expected = Array.tabulate[Float](raster.cols * raster.rows)(i => b04Value(i % raster.cols, i / raster.cols).toFloat)
    assertArrayEquals(expected, band(raster, 0), "the healthy reflectance band must retain its values and position")
    for (index <- 1 until bands.size) {
      assertTrue(raster.tile.band(index).isNoDataTile, s"absent angle band $index must be NoData")
      assertTrue(band(raster, index).forall(_.isNaN), s"absent angle band $index must be NaN")
    }
  }

  @Test
  def angleOnlyAbsentMetadataKeepsFloatNoData(): Unit = {
    val bands = Seq("granule_metadata##1", "granule_metadata##6")
    val raster = load(factory(bands, 10, includeMetadata = false), aoi, utm31, retainNoDataTiles = true)

    assertEquals((288, 288, 2), (raster.cols, raster.rows, raster.tile.bandCount))
    assertEquals(aoi, raster.extent)
    assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
      "an absent metadata asset must keep angle-only cubes float32")
    for (index <- bands.indices) {
      assertTrue(raster.tile.band(index).isNoDataTile, s"absent angle band $index must be NoData")
      assertTrue(band(raster, index).forall(_.isNaN), s"absent angle band $index must be NaN")
    }
  }

  @Test
  def invalidMetadataPartsKeepFloatNoDataPlaceholders(): Unit = {
    val bands = Seq("B04", "granule_metadata##4", "granule_metadata##5")
    val raster = load(factory(bands, 10, xmlPath = metadataWithoutSunGrid, softErrors = true), aoi, utm31)
    assertEquals((288, 288, bands.size), (raster.cols, raster.rows, raster.tile.bandCount))
    assertEquals(aoi, raster.extent)
    assertTrue(raster.tile.bands.forall(_.cellType == FloatConstantNoDataCellType),
      "unavailable sun metadata must not force an integer cell type")
    for (index <- 1 until bands.size) {
      assertTrue(raster.tile.band(index).isNoDataTile, s"unavailable sun band $index must be NoData")
      assertTrue(band(raster, index).forall(_.isNaN), s"unavailable sun band $index must be NaN")
    }
    val expected = Array.tabulate[Float](raster.cols * raster.rows)(i => b04Value(i % raster.cols, i / raster.cols).toFloat)
    assertArrayEquals(expected, band(raster, 0), "healthy reflectance must retain its values and position")
  }

  @ParameterizedTest
  @CsvSource(Array("false,false", "false,true", "true,false", "true,true"))
  @SetEnvironmentVariable(key = "GDALREAD_MAXRETRIES", value = "1")
  def missingOrUnreadableMasksFollowSoftErrorPolicy(loadPerProduct: Boolean, softErrors: Boolean): Unit = {
    Assumptions.assumeTrue(gdalAvailable, "GDAL (libgdal) is not installed here")
    val bands = Seq("B05_DETFOO", "granule_metadata##5", "granule_metadata##7", "granule_metadata##0", "granule_metadata##6")
    for (corrupt <- Seq(false, true)) {
      val xml = metadataWithBrokenMask(corrupt)
      def read(): Raster[MultibandTile] =
        load(factory(bands, 20, xmlPath = xml, softErrors = softErrors), aoi, utm31, loadPerProduct = loadPerProduct)

      if (softErrors) {
        val raster = read()
        assertEquals(5, raster.tile.bandCount)
        val (xs, ys) = centresInTileCrs(raster, utm31)
        val angles = tileAngles(l2aPath)
        val viewing = angles.metadata.viewing(4).toOption.get
        val labels = SentinelDetectorMask.detectorLabels(l2aPath, angles.metadata, 4, viewing.detectorMask, xs, ys)
        assertArrayEquals(labels.map(_.toFloat), band(raster, 0), "the healthy JP2 band must survive mask failures")
        assertClose(sunField(l2aSun, Angle.Zenith, l2aTile, xs, ys), band(raster, 1), "healthy sun zenith")
        assertTrue(raster.tile.band(2).isNoDataTile && raster.tile.band(4).isNoDataTile, "failed view bands must be NoData")
        assertTrue(band(raster, 3).forall(_ == 159.91357727290500f), "constant angles must not replace failed view bands")
      } else {
        val error = assertThrows(classOf[Exception], () => { read(); () })
        assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
          .exists(e => Option(e.getMessage).exists(message => message.contains("viewZenith_B05") || message.contains("viewAzimuth_B05"))),
          s"expected a view-angle read failure for $xml, got $error")
      }
    }
  }

  /** Reads the real B05 detector mask with GDAL. Runs where GDAL is installed, such as CI. */
  @Test
  def perPixelViewAnglesFollowTheDetectorMask(): Unit = {
    Assumptions.assumeTrue(gdalAvailable, "GDAL (libgdal) is not installed here")
    val raster = load(factory(Seq("granule_metadata##6", "granule_metadata##7"), 20), aoi, utm31)
    val (xs, ys) = centresInTileCrs(raster, utm31)
    val angles = tileAngles(l2aPath)
    val viewing = angles.metadata.viewing(4).toOption.get
    val labels = SentinelDetectorMask.detectorLabels(l2aPath, angles.metadata, 4, viewing.detectorMask, xs, ys)
    assertEquals(Set(2, 3), labels.toSet, "the area of interest lies on the boundary between detectors 2 and 3")
    val grids = angles.filledViewGrids(4)
    assertClose(viewField(grids, Angle.Azimuth, l2aTile, xs, ys, labels), band(raster, 0), "view azimuth")
    assertClose(viewField(grids, Angle.Zenith, l2aTile, xs, ys, labels), band(raster, 1), "view zenith")
  }
}
