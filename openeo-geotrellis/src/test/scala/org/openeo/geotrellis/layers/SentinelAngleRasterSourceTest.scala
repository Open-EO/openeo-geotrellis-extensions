package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{GeoTiffOptions, SinglebandGeoTiff, Tags, Tiled}
import geotrellis.raster.{CellSize, FloatConstantNoDataCellType, GridBounds, GridExtent, MultibandTile, Raster, RasterSource, UByteArrayTile}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}
import org.junitpioneer.jupiter.SetEnvironmentVariable
import org.openeo.geotrellis.layers.provider.{RasterSourceDefinition, SentinelXmlMetadataRasterSourceProvider}
import org.openeo.geotrellis.layers.raster_source.SentinelAngleFields.{Angle, pixelCentres, sunField, viewField}
import org.openeo.geotrellis.layers.raster_source.SentinelAngleRasterSource.{TileAngles, tileAngles, viewReferenceBandId}
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource.{TileMetadata, loadTileMetadata}
import org.openeo.geotrellis.layers.raster_source._
import org.openeo.opensearch.OpenSearchResponses
import org.openeo.opensearch.OpenSearchResponses.Link
import software.amazon.awssdk.core.exception.AbortedException
import software.amazon.awssdk.services.s3.model.S3Exception
import org.xml.sax.SAXParseException

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.Comparator
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, CountDownLatch, CyclicBarrier, Executors, TimeUnit}

object SentinelAngleRasterSourceTest {
  private val layers = "/org/openeo/geotrellis/layers"
  private val l1cPath = getClass.getResource(s"$layers/MTD_TL.xml").getPath
  private val l2aPath = getClass.getResource(
    s"$layers/S2B_MSIL2A_20220502T103619_N0400_R008_T31UFS_20220502T123919.SAFE" +
      "/GRANULE/L2A_T31UFS_A026918_20220502T103700/MTD_TL.xml").getPath

  private val tileCrs = CRS.fromEpsgCode(32631)
  private val l2aTile = Extent(600000, 5590200, 709800, 5700000)
  /** The demo window: 512 x 512 pixels of 20 m across the boundary between detectors 2 and 3. */
  private val demoWindow = Extent(638720, 5644880, 648960, 5655120)
  private val demoGrid = GridExtent[Long](demoWindow, CellSize(20, 20))

  private val sunZenith = AngleOutput(Angle.Zenith, None)
  private val sunAzimuth = AngleOutput(Angle.Azimuth, None)
  private val viewZenith = AngleOutput(Angle.Zenith, Some(4))
  private val viewAzimuth = AngleOutput(Angle.Azimuth, Some(4))

  private val tempDir: Path = Files.createTempDirectory("SentinelAngleRasterSourceTest")

  @AfterAll
  def deleteTempDir(): Unit =
    Files.walk(tempDir).sorted(Comparator.reverseOrder[Path]()).forEach(path => Files.delete(path))

  /**
   * A B05 detector mask on the full 20 m grid of the L2A tile: no detector in the first 10 columns, detector 9
   * (which has no grid) in the first 10 rows, and detectors 1 to 6 in vertical stripes elsewhere.
   */
  private def maskLabel(col: Int, row: Int): Int =
    if (col < 10) 0 else if (row < 10) 9 else 1 + math.min(5, col / 915)

  private lazy val maskTiff: String = {
    val size = 5490
    val labels = Array.tabulate[Byte](size * size)(i => maskLabel(i % size, i / size).toByte)
    val geoTiff = SinglebandGeoTiff(UByteArrayTile(labels, size, size), l2aTile, tileCrs, Tags.empty,
      GeoTiffOptions(Tiled(256, 256), DeflateCompression), Nil)
    val path = tempDir.resolve("MSK_DETFOO_B05.tif").toString
    GeoTiffWriter.write(geoTiff, path)
    path
  }

  /** Opens the GeoTIFF mask instead of the JP2, which needs GDAL. */
  private def geoTiffMask(path: String): RasterSource = {
    assertTrue(path.endsWith("/QI_DATA/MSK_DETFOO_B05.jp2"), path)
    GeoTiffRasterSource(GeoTiffPath(maskTiff))
  }

  /** A B05 mask opener that either fails immediately or returns a source whose reads fail. */
  private def failingMaskOpener(angles: TileAngles, failAtOpen: Boolean): String => RasterSource = _ => {
    if (failAtOpen) throw new FileNotFoundException("missing detector mask")
    new SentinelXMLMetadataRasterSource(2, tileCrs, angles.metadata.gridExtents(20), OpenEoSourcePath("unreadable-mask")) {
      override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] =
        throw new IOException("unreadable detector mask")
    }
  }

  private def allOf(grid: GridExtent[Long]): GridBounds[Long] = GridBounds[Long](0, 0, grid.cols - 1, grid.rows - 1)

  private def readAll(source: RasterSource, bounds: GridBounds[Long]): Array[Float] =
    source.read(bounds, Seq(0)).get.tile.band(0).toArrayDouble().map(_.toFloat)

  /** The pre-R2 whole-window evaluation, with labels from the synthetic mask's defining formula. */
  private def unchunkedReference(output: AngleOutput, crs: CRS, grid: GridExtent[Long], bounds: GridBounds[Long]): Array[Float] = {
    val angles = tileAngles(l2aPath)
    val (xs, ys) = pixelCentres(grid, bounds)
    if (crs != tileCrs) {
      val transform = Transform(crs, tileCrs)
      for (k <- xs.indices) {
        val (x, y) = transform(xs(k), ys(k))
        xs(k) = x
        ys(k) = y
      }
    }
    output.viewBandId match {
      case None => sunField(angles.metadata.sunGrid.toOption.get, output.angle, l2aTile, xs, ys)
      case Some(bandId) =>
        val native = angles.metadata.gridExtents(20)
        val labels = xs.indices.map { k =>
          val col = math.floor((xs(k) - native.extent.xmin) / 20).toInt
          val row = math.floor((native.extent.ymax - ys(k)) / 20).toInt
          if (col < 0 || row < 0 || col >= native.cols || row >= native.rows) 0 else maskLabel(col, row)
        }.toArray
        viewField(angles.filledViewGrids(bandId), output.angle, l2aTile, xs, ys, labels)
    }
  }

  private def assertSameValues(expected: Array[Float], actual: Array[Float]): Unit = {
    assertEquals(expected.length, actual.length)
    for (i <- expected.indices)
      if (!expected(i).equals(actual(i))) fail(s"pixel $i: expected ${expected(i)}, got ${actual(i)}")
  }

  private def serialized[T](value: T): T = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    out.writeObject(value)
    out.close()
    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray)).readObject().asInstanceOf[T]
  }

  private def definition(path: String, bandIndex: Int, softErrors: Boolean): RasterSourceDefinition = {
    val bandNames = (0 to 7).map(i => s"granule_metadata##$i")
    val feature = OpenSearchResponses.featureBuilder()
      .withId("S2B_MSIL2A_20220502T103619_N0400_R008_T31UFS_20220502T123919")
      .withNominalDate("2022-05-02T10:36:19Z")
      .withCRS("EPSG:32631")
      .withRasterExtent(l2aTile.xmin, l2aTile.ymin, l2aTile.xmax, l2aTile.ymax)
      .withResolution(10)
      .build
    RasterSourceDefinition(Link(new File(path).toURI, Some("granule_metadata"), None, Some(bandNames)), bandIndex, feature,
      rootPath = null, targetCellType = None, targetExtent = ProjectedExtent(demoWindow, tileCrs),
      featureExtentInLayout = Some(demoGrid), targetResolution = Some(CellSize(20, 20)), maxResolution = CellSize(10, 10),
      datacubeParams = None, experimental = false, bandName = s"granule_metadata##$bandIndex", softErrors = softErrors)
  }

  private def provided(path: String, bandIndex: Int, softErrors: Boolean = false): RasterSource =
    SentinelXmlMetadataRasterSourceProvider.rasterSource(definition(path, bandIndex, softErrors))

  /** A copy of the L2A tile metadata in its own SAFE layout, with `from` replaced by `to` once after `after`. */
  private def corruptedL2a(name: String, after: String, from: String, to: String): String = {
    val xml = new String(Files.readAllBytes(Paths.get(l2aPath)), UTF_8)
    val start = xml.indexOf(after)
    assertTrue(start >= 0, after)
    val at = xml.indexOf(from, start)
    val corrupted = xml.substring(0, at) + to + xml.substring(at + from.length)
    val dir = Files.createDirectories(tempDir.resolve(s"$name.SAFE/GRANULE/G"))
    Files.write(dir.resolve("MTD_TL.xml"), corrupted.getBytes(UTF_8)).toString
  }
}

class SentinelAngleRasterSourceTest {

  import SentinelAngleRasterSourceTest._

  @Test
  def sunAnglesAtPixelCentres(): Unit = {
    val (xs, ys) = pixelCentres(demoGrid, allOf(demoGrid))
    val sunGrid = tileAngles(l2aPath).metadata.sunGrid.toOption.get
    for (output <- Seq(sunZenith, sunAzimuth)) {
      val source = SentinelAngleRasterSource(l2aPath, output, tileCrs, demoGrid)()
      assertEquals(FloatConstantNoDataCellType, source.cellType)
      assertEquals(1, source.bandCount)
      assertSameValues(sunField(sunGrid, output.angle, l2aTile, xs, ys), readAll(source, allOf(demoGrid)))
    }
    val zenith = readAll(SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, demoGrid)(), allOf(demoGrid))
    assertTrue(zenith.forall(z => z > 36 && z < 38), "sun zenith in May over Belgium")
    assertTrue(zenith.distinct.length > 1000, "sun zenith varies per pixel")
  }

  @Test
  def namesAreStablePerOutput(): Unit = {
    val ids = Seq(sunAzimuth, sunZenith, viewAzimuth, viewZenith)
      .map(output => org.openeo.geotrellis.sortableSourceName(SentinelAngleRasterSource(l2aPath, output, tileCrs, demoGrid)().name))
    assertEquals(Seq("sunAzimuth", "sunZenith", "viewAzimuth_B05", "viewZenith_B05").map(id => s"$l2aPath#$id"), ids)
  }

  @Test
  def tileMetadataIsParsedOncePerProduct(): Unit = {
    val dir = Files.createDirectories(tempDir.resolve("parsedOnce.SAFE/GRANULE/L2A_T31UFS_A026918_20220502T103700"))
    val path = Files.copy(Paths.get(l2aPath), dir.resolve("MTD_TL.xml")).toString
    val parsed = tileAngles(path)
    val sources = Seq(sunZenith, sunAzimuth, viewZenith, viewAzimuth)
      .map(output => SentinelAngleRasterSource(path, output, tileCrs, demoGrid)(geoTiffMask))

    // Without the file, every output still reads: they all use the metadata parsed above.
    Files.delete(Paths.get(path))
    for (source <- sources)
      assertTrue(readAll(source, GridBounds[Long](0, 0, 9, 9)).forall(!_.isNaN), source.name.toString)
    assertSame(parsed, tileAngles(path))
  }

  @Test
  def concurrentColdMissesLoadMetadataOnce(): Unit = {
    val dir = Files.createTempDirectory(tempDir, "concurrent-cache-")
    val path = Files.copy(Paths.get(l2aPath), dir.resolve("MTD_TL.xml")).toString
    val workers = 8
    val start = new CyclicBarrier(workers)
    val requesting = new CountDownLatch(workers)
    val loads = new AtomicInteger()
    def load(metadataPath: String): TileMetadata = {
      loads.incrementAndGet()
      assertTrue(requesting.await(10, TimeUnit.SECONDS), "all workers must request the cold key before loading")
      loadTileMetadata(metadataPath, Set(viewReferenceBandId))
    }

    val pool = Executors.newFixedThreadPool(workers)
    try {
      val requests = (0 until workers).map { _ =>
        pool.submit(new Callable[TileAngles] {
          override def call(): TileAngles = {
            start.await(10, TimeUnit.SECONDS)
            requesting.countDown()
            tileAngles(path, load)
          }
        })
      }
      val results = requests.map(_.get(20, TimeUnit.SECONDS))
      assertEquals(1, loads.get(), "concurrent cold misses must parse the metadata only once")
      val parsed = results.head
      results.foreach(result => assertSame(parsed, result, "share the filled grids and warn-once set"))
      assertEquals(l2aTile, parsed.metadata.extent)
      assertEquals((1 to 6).toSet, parsed.filledViewGrids(viewReferenceBandId).keySet)
      Files.delete(Paths.get(path))
      assertSame(parsed, tileAngles(path), "the public loader must reuse the same cached tile")
      assertEquals(1, loads.get())
    } finally {
      pool.shutdownNow()
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "metadata workers must terminate")
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("missing", "malformed", "invalid"))
  def failedMetadataLoadsAreRetriedAfterRepair(problem: String): Unit = {
    val path = Files.createTempDirectory(tempDir, "cache-retry-").resolve("MTD_TL.xml")
    if (problem != "missing")
      Files.write(path, (if (problem == "malformed") "<broken>" else "<broken/>").getBytes(UTF_8))
    var loads = 0
    def load(metadataPath: String): TileMetadata = {
      loads += 1
      loadTileMetadata(metadataPath, Set(viewReferenceBandId))
    }

    for (attempt <- 1 to 2) {
      val error = assertThrows(classOf[Exception], () => { tileAngles(path.toString, load); () })
      if (problem == "missing") assertTrue(error.isInstanceOf[FileNotFoundException], error.toString)
      else {
        assertTrue(error.isInstanceOf[IllegalArgumentException], error.toString)
        assertTrue(error.getMessage.startsWith(s"invalid Sentinel-2 tile metadata $path:"), error.getMessage)
        if (problem == "malformed") assertTrue(error.getCause.isInstanceOf[SAXParseException], error.toString)
      }
      assertEquals(attempt, loads, "a failed load must not be cached")
    }

    Files.copy(Paths.get(l2aPath), path, StandardCopyOption.REPLACE_EXISTING)
    val parsed = tileAngles(path.toString, load)
    assertEquals(3, loads)
    assertEquals(l2aTile, parsed.metadata.extent)
    Files.delete(path)
    assertSame(parsed, tileAngles(path.toString, load))
    assertSame(parsed, tileAngles(path.toString))
    assertEquals(3, loads, "a successful retry must be cached")
  }

  @Test
  def cancelledMetadataLoadsPropagateAndAreNotCached(): Unit = {
    val dir = Files.createTempDirectory(tempDir, "cache-cancel-")
    val path = Files.copy(Paths.get(l2aPath), dir.resolve("MTD_TL.xml")).toString
    val cancelled = AbortedException.builder().message("cancelled metadata load").build()
    var loads = 0
    def load(metadataPath: String): TileMetadata = {
      loads += 1
      if (loads == 1) throw cancelled
      loadTileMetadata(metadataPath, Set(viewReferenceBandId))
    }

    assertSame(cancelled, assertThrows(classOf[AbortedException], () => { tileAngles(path, load); () }))
    assertEquals(1, loads)
    val parsed = tileAngles(path, load)
    assertSame(parsed, tileAngles(path))
    assertEquals(2, loads, "cancellation must not poison the cache or prevent a later successful load")
  }

  @Test
  def readsAreClippedToTheGridAndNeedBandZero(): Unit = {
    val source = SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, demoGrid)()
    val window = GridBounds[Long](100, 200, 149, 239)
    val byBounds = source.read(window, Seq(0)).get
    val byExtent = source.read(demoGrid.extentFor(window), Seq(0)).get
    assertEquals(byBounds.extent, byExtent.extent)
    assertSameValues(byBounds.tile.band(0).toArrayDouble().map(_.toFloat), byExtent.tile.band(0).toArrayDouble().map(_.toFloat))

    val clipped = source.read(GridBounds[Long](500, 500, 600, 600), Seq(0)).get
    assertEquals((12, 12), (clipped.tile.cols, clipped.tile.rows))
    assertEquals(None, source.read(GridBounds[Long](600, 600, 700, 700), Seq(0)))
    assertThrows(classOf[IllegalArgumentException], () => {
      source.read(window, Seq(1))
      ()
    })
  }

  @Test
  def mixedAngleAndMeanExtentReadsUseTheSameGrid(): Unit = {
    val mean = provided(l2aPath, 0)
    val sun = provided(l2aPath, 4)
    val unaligned = Extent(638723, 5644883, 638763, 5644923)
    val expected = demoGrid.extentFor(demoGrid.gridBoundsFor(unaligned, clamp = true))

    for (sources <- Seq(NonEmptyList.of(mean, sun), NonEmptyList.of(sun, mean))) {
      val composite = new BandCompositeRasterSource(sources, tileCrs, parallelRead = false)
      val raster = composite.read(unaligned).get
      assertEquals(expected, raster.extent)
      assertEquals(2, raster.tile.bandCount)
    }
  }

  @ParameterizedTest
  @ValueSource(doubles = Array(10.0, 20.0, 60.0))
  def largeReadsUseBoundedMaskWindows(resolution: Double): Unit = {
    val grid = GridExtent[Long](Extent(599960, 5700040 - 517 * resolution, 599960 + 603 * resolution, 5700040),
      CellSize(resolution, resolution))
    val requested = GridBounds[Long](7, 11, 620, 540)
    val clipped = requested.intersection(grid.dimensions).get
    val maxNativeSide = math.ceil(256 * resolution / 20).toLong + 1
    var opens = 0
    var maskReads = 0
    val open: String => RasterSource = path => {
      opens += 1
      new ValueOffsetRasterSource(geoTiffMask(path), 1.0, 0.0) {
        override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
          maskReads += 1
          assertTrue(bounds.width <= maxNativeSide && bounds.height <= maxNativeSide,
            s"mask read $bounds exceeds a 256 x 256 target chunk at $resolution m")
          super.read(bounds, bands)
        }
      }
    }

    for (output <- Seq(sunZenith, sunAzimuth, viewZenith, viewAzimuth)) {
      val source = SentinelAngleRasterSource(l2aPath, output, tileCrs, grid)(open)
      val expected = unchunkedReference(output, tileCrs, grid, clipped)
      assertTrue(expected.exists(!_.isNaN), output.id)
      opens = 0
      maskReads = 0
      val raster = source.read(requested).get
      assertEquals(grid.extentFor(clipped), raster.extent)
      assertEquals((clipped.width.toInt, clipped.height.toInt), (raster.cols, raster.rows))
      assertSameValues(expected, raster.tile.band(0).toArrayDouble().map(_.toFloat))
      assertEquals(if (output.viewBandId.isDefined) 1 else 0, opens, "open at most one mask per logical read")
      if (output.viewBandId.isDefined) assertTrue(maskReads > 1, "large mask reads must be split")
      else assertEquals(0, maskReads, "sun angles must not read a mask")

      // Dense per-product loading must also bound evaluation even though it requests the whole union at once.
      opens = 0
      maskReads = 0
      val windows = Seq(GridBounds[Long](7, 11, 300, 540), GridBounds[Long](301, 11, 620, 540))
      val composite = new BandCompositeRasterSource(NonEmptyList.of(source), tileCrs,
        parallelRead = false, readFullTile = true)
      val rasters = composite.readBounds(windows).toList
      assertEquals(windows.size, rasters.size)
      for ((piece, window) <- rasters.zip(windows)) {
        val intersection = window.intersection(grid.dimensions).get
        assertEquals(grid.extentFor(intersection), piece.extent)
        assertSameValues(unchunkedReference(output, tileCrs, grid, intersection),
          piece.tile.band(0).toArrayDouble().map(_.toFloat))
      }
      assertEquals(if (output.viewBandId.isDefined) 1 else 0, opens, "reuse the mask across union chunks")
      if (output.viewBandId.isDefined) assertTrue(maskReads > 1)
    }
  }

  @Test
  def largeReprojectedReadsMatchUnchunkedReference(): Unit = {
    for (output <- Seq(sunZenith, sunAzimuth, viewZenith, viewAzimuth)) {
      val source = SentinelAngleRasterSource(l2aPath, output, tileCrs, demoGrid)(geoTiffMask)
        .reproject(LatLng).resample(603, 517)
      val window = GridBounds[Long](3, 5, 590, 500)
      assertSameValues(unchunkedReference(output, LatLng, source.gridExtent, window), readAll(source, window))
      assertSameValues(unchunkedReference(output, LatLng, source.gridExtent, window),
        source.read(source.gridExtent.extentFor(window)).get.tile.band(0).toArrayDouble().map(_.toFloat))
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  @SetEnvironmentVariable(key = "GDALREAD_MAXRETRIES", value = "1")
  def laterChunkFailuresRetainTheR1ErrorPolicy(softErrors: Boolean): Unit = {
    val native = tileAngles(l2aPath).metadata.gridExtents(20)
    var reads = 0
    val open: String => RasterSource = _ =>
      new SentinelXMLMetadataRasterSource(2, tileCrs, native, OpenEoSourcePath("later-chunk-failure")) {
        override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
          reads += 1
          if (bounds.colMin >= 2180) throw new IOException("unreadable later mask chunk")
          super.read(bounds, bands)
        }
      }
    val sun = SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, demoGrid)()
    val view = SentinelAngleRasterSource(l2aPath, viewZenith, tileCrs, demoGrid)(open)
    val composite = new BandCompositeRasterSource(NonEmptyList.of(sun, view), tileCrs,
      parallelRead = false, softErrors = softErrors)
    val window = GridBounds[Long](0, 0, 511, 31)
    if (softErrors) {
      val raster = composite.read(window).get
      assertSameValues(readAll(sun, window), raster.tile.band(0).toArrayDouble().map(_.toFloat))
      assertTrue(raster.tile.band(1).isNoDataTile, "do not return a partially evaluated view band after a soft failure")
    } else {
      val error = assertThrows(classOf[Exception], () => { composite.read(window); () })
      assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
        .exists(e => e.isInstanceOf[IOException] && Option(e.getMessage).contains("unreadable later mask chunk")), error.toString)
    }
    assertEquals(2, reads, "the failure must occur after a successful chunk")
  }

  @Test
  def largeReadSizeIsValidatedBeforeAllocating(): Unit = {
    val grid = GridExtent[Long](Extent(0, 0, 100000, 100000), CellSize(1, 1))
    val source = SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, grid)(_ => fail("unexpected mask open"))
    val error = assertThrows(classOf[IllegalArgumentException], () => { source.read(allOf(grid)); () })
    assertTrue(error.getMessage.contains("window too large"), error.getMessage)
  }

  @Test
  def reprojectedSourcesEvaluateAtTheirOwnPixelCentres(): Unit = {
    val source = SentinelAngleRasterSource(l2aPath, sunAzimuth, tileCrs, demoGrid)()
    val reprojected = source.reproject(LatLng).asInstanceOf[SentinelAngleRasterSource]
    assertEquals((l2aPath, sunAzimuth, LatLng), (reprojected.xmlPath, reprojected.output, reprojected.crs))

    val window = GridBounds[Long](10, 20, 29, 34)
    val (xs, ys) = pixelCentres(reprojected.gridExtent, window)
    val transform = Transform(LatLng, tileCrs)
    for (k <- xs.indices) {
      val (x, y) = transform(xs(k), ys(k))
      xs(k) = x
      ys(k) = y
    }
    val sunGrid = tileAngles(l2aPath).metadata.sunGrid.toOption.get
    assertSameValues(sunField(sunGrid, Angle.Azimuth, l2aTile, xs, ys), readAll(reprojected, window))

    val resampled = source.resample(100, 50).asInstanceOf[SentinelAngleRasterSource]
    assertEquals((100L, 50L), (resampled.gridExtent.cols, resampled.gridExtent.rows))
    assertEquals(source.name, resampled.name)
  }

  @Test
  def equalityIgnoresTheMaskOpenerAndSurvivesSerialization(): Unit = {
    val source = SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, demoGrid)()
    val withOtherOpener = SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, demoGrid)(geoTiffMask)
    assertEquals(source, withOtherOpener)
    assertEquals(source.hashCode(), withOtherOpener.hashCode())
    for (other <- Seq(SentinelAngleRasterSource(l2aPath, sunAzimuth, tileCrs, demoGrid)(),
      SentinelAngleRasterSource(l2aPath, sunZenith, LatLng, demoGrid)(),
      SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, GridExtent[Long](demoWindow, CellSize(10, 10)))(),
      SentinelAngleRasterSource(l1cPath, sunZenith, tileCrs, demoGrid)()))
      assertNotEquals(source, other)

    val copy = serialized(source)
    assertEquals(source, copy)
    assertEquals(source.hashCode(), copy.hashCode())
    val window = GridBounds[Long](0, 0, 63, 63)
    assertSameValues(readAll(source, window), readAll(copy, window))

    val indexed = IndexedRasterSource(source, 0)
    assertEquals(indexed, serialized(indexed))
  }

  @Test
  def viewAnglesUseTheDetectorOfEachPixel(): Unit = {
    val angles = tileAngles(l2aPath)
    val grids = angles.filledViewGrids(4)
    assertEquals((1 to 6).toSet, grids.keySet)

    // The demo window, and the north-west corner of the tile with no detector (label 0) and detector 9.
    for (grid <- Seq(demoGrid, GridExtent[Long](Extent(600000, 5698720, 601280, 5700000), CellSize(20, 20)))) {
      val bounds = allOf(grid)
      val (xs, ys) = pixelCentres(grid, bounds)
      val native = angles.metadata.gridExtents(20)
      val labels = xs.indices.map(k => maskLabel(
        ((xs(k) - native.extent.xmin) / 20).toInt, ((native.extent.ymax - ys(k)) / 20).toInt)).toArray
      for (output <- Seq(viewZenith, viewAzimuth)) {
        val source = SentinelAngleRasterSource(l2aPath, output, tileCrs, grid)(geoTiffMask)
        val values = readAll(source, bounds)
        assertSameValues(viewField(grids, output.angle, l2aTile, xs, ys, labels), values)
        for (k <- labels.indices if labels(k) == 0 || labels(k) == 9) assertTrue(values(k).isNaN)
      }
    }
    // In the demo window, detectors 2 and 3 meet; viewing zenith there is about 7 to 9 degrees.
    val demoZenith = readAll(SentinelAngleRasterSource(l2aPath, viewZenith, tileCrs, demoGrid)(geoTiffMask), allOf(demoGrid))
    assertTrue(demoZenith.forall(z => z > 1 && z < 13), "viewing zenith range")
  }

  @ParameterizedTest(name = "{argumentsWithNames}")
  @CsvSource(useHeadersInDisplayName = true, textBlock = """readFullTile, sparse, softErrors
    false,        false,  false
    false,        false,  true
    true,         false,  false
    true,         false,  true
    true,         true,   false
    true,         true,   true""")
  @SetEnvironmentVariable(key = "GDALREAD_MAXRETRIES", value = "1")
  def maskReadFailuresFollowSoftErrorPolicy(readFullTile: Boolean, sparse: Boolean, softErrors: Boolean): Unit = {
    val windows = Seq(GridBounds[Long](0, 0, 7, 7),
      if (sparse) GridBounds[Long](100, 100, 107, 107) else GridBounds[Long](8, 0, 15, 7))
    val angles = tileAngles(l2aPath)
    val sun = SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, demoGrid)()
    val constant = new SentinelXMLMetadataRasterSource(42, tileCrs, demoGrid, OpenEoSourcePath("constant"))

    for (failAtOpen <- Seq(true, false)) {
      val open = failingMaskOpener(angles, failAtOpen)
      val composite = new BandCompositeRasterSource(NonEmptyList.of(
        sun, SentinelAngleRasterSource(l2aPath, viewAzimuth, tileCrs, demoGrid)(open), constant,
        SentinelAngleRasterSource(l2aPath, viewZenith, tileCrs, demoGrid)(open)),
        tileCrs, parallelRead = false, softErrors = softErrors, readFullTile = readFullTile)
      def read(): Seq[Raster[MultibandTile]] =
        if (readFullTile) composite.readBounds(windows).toList
        else windows.map(window => composite.read(window).get)

      if (softErrors) {
        val rasters = read()
        assertEquals(windows.size, rasters.size)
        for ((raster, window) <- rasters.zip(windows)) {
          assertEquals(demoGrid.extentFor(window), raster.extent)
          assertEquals(4, raster.tile.bandCount)
          assertSameValues(readAll(sun, window), raster.tile.band(0).toArrayDouble().map(_.toFloat))
          assertTrue(raster.tile.band(1).isNoDataTile && raster.tile.band(3).isNoDataTile)
          assertTrue(raster.tile.band(2).toArrayDouble().forall(_ == 42), "healthy bands must survive mask failures")
        }
      } else {
        val error = assertThrows(classOf[Exception], () => { read(); () })
        assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
          .exists(e => Option(e.getMessage).exists(_.contains("detector mask"))), error.toString)
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def extentMaskReadFailuresFollowSoftErrorPolicy(softErrors: Boolean): Unit = {
    val extent = demoGrid.extentFor(GridBounds[Long](13, 17, 22, 25))
    val extents = Seq(extent, extent.buffer(3), demoGrid.extentFor(GridBounds[Long](-2, -3, 9, 8)))
    val angles = tileAngles(l2aPath)
    val sun = SentinelAngleRasterSource(l2aPath, sunZenith, tileCrs, demoGrid)()
    val constant = new SentinelXMLMetadataRasterSource(42, tileCrs, demoGrid, OpenEoSourcePath("constant"))

    for (failAtOpen <- Seq(true, false); parallelRead <- Seq(false, true); requested <- extents) {
      val open = failingMaskOpener(angles, failAtOpen)
      val composite = new BandCompositeRasterSource(NonEmptyList.of(
        sun, SentinelAngleRasterSource(l2aPath, viewAzimuth, tileCrs, demoGrid)(open), constant,
        SentinelAngleRasterSource(l2aPath, viewZenith, tileCrs, demoGrid)(open)),
        tileCrs, parallelRead = parallelRead, softErrors = softErrors)
      val bands = Seq(3, 0, 2, 1)
      if (softErrors) {
        val expectedSun = sun.read(requested).get
        val raster = composite.read(requested, bands).get
        assertEquals(expectedSun.extent, raster.extent)
        assertEquals((expectedSun.cols, expectedSun.rows, 4), (raster.cols, raster.rows, raster.tile.bandCount))
        assertEquals(FloatConstantNoDataCellType, raster.tile.cellType)
        assertTrue(raster.tile.band(0).toArrayDouble().forall(_.isNaN))
        assertSameValues(expectedSun.tile.band(0).toArrayDouble().map(_.toFloat),
          raster.tile.band(1).toArrayDouble().map(_.toFloat))
        assertTrue(raster.tile.band(2).toArrayDouble().forall(_ == 42), "healthy bands must survive mask failures")
        assertTrue(raster.tile.band(3).toArrayDouble().forall(_.isNaN))
      } else {
        val error = assertThrows(classOf[Exception], () => { composite.read(requested, bands); () })
        assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
          .exists(e => Option(e.getMessage).exists(_.contains("detector mask"))), error.toString)
      }
    }
  }

  @Test
  def failedMaskWindowsKeepTheirPositions(): Unit = {
    val angles = tileAngles(l2aPath)
    val open: String => RasterSource = _ =>
      new SentinelXMLMetadataRasterSource(2, tileCrs, angles.metadata.gridExtents(20), OpenEoSourcePath("partial-mask")) {
        override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
          if (bounds.colMin < 2000) throw new IOException("unreadable detector mask window")
          super.read(bounds, bands)
        }
      }
    val view = SentinelAngleRasterSource(l2aPath, viewZenith, tileCrs, demoGrid)(open)
    val windows = Seq(GridBounds[Long](0, 0, 7, 7), GridBounds[Long](100, 100, 107, 107))
    val composite = new BandCompositeRasterSource(NonEmptyList.of(view), tileCrs,
      parallelRead = false, softErrors = true, readFullTile = true)
    val rasters = composite.readBounds(windows).toList

    assertEquals(windows.map(demoGrid.extentFor(_)), rasters.map(_.extent))
    assertTrue(rasters.head.tile.band(0).isNoDataTile)
    val expected = readAll(view, windows(1))
    assertTrue(expected.forall(!_.isNaN), "the second mask window must be readable")
    assertSameValues(expected, rasters(1).tile.band(0).toArrayDouble().map(_.toFloat))
  }

  @Test
  @SetEnvironmentVariable(key = "GDALREAD_MAXRETRIES", value = "3")
  def maskReadCancellationIsNotASoftError(): Unit = {
    val cancelled = AbortedException.builder().message("cancelled mask read").build()
    val opens = new AtomicInteger()
    val view = SentinelAngleRasterSource(l2aPath, viewZenith, tileCrs, demoGrid) { _ =>
      opens.incrementAndGet()
      throw cancelled
    }
    for (sparse <- Seq(false, true)) {
      val composite = new BandCompositeRasterSource(NonEmptyList.of(view), tileCrs,
        parallelRead = false, softErrors = true, readFullTile = true)
      val windows = Seq(GridBounds[Long](0, 0, 7, 7),
        if (sparse) GridBounds[Long](100, 100, 107, 107) else GridBounds[Long](8, 0, 15, 7))
      opens.set(0)
      assertSame(cancelled, assertThrows(classOf[AbortedException], () => { composite.readBounds(windows).toList; () }))
      assertEquals(1, opens.get(), "bounds cancellation must not be retried")
      opens.set(0)
      assertSame(cancelled, assertThrows(classOf[AbortedException], () => { composite.read(demoGrid.extentFor(windows.head)); () }))
      assertEquals(1, opens.get(), "extent cancellation must not be retried")
    }
  }

  @Test
  def providerKeepsTheConstantBandsAndAddsPerPixelOnes(): Unit = {
    val constants = SentinelXMLMetadataRasterSource.forAngleBands(l2aPath).map(_.value)
    for (index <- 0 to 3) {
      val IndexedRasterSource(source: SentinelXMLMetadataRasterSource, 0) = provided(l2aPath, index)
      assertEquals(constants(index), source.value)
      assertEquals(constants(index), source.read(GridBounds[Long](0, 0, 3, 3), Seq(0)).get.tile.band(0).getDouble(0, 0).toFloat)
    }
    for ((index, output) <- Seq(4 -> sunAzimuth, 5 -> sunZenith, 6 -> viewAzimuth, 7 -> viewZenith))
      assertEquals(IndexedRasterSource(SentinelAngleRasterSource(l2aPath, output, tileCrs, demoGrid)(), 0), provided(l2aPath, index))
    for (index <- Seq(-1, 8))
      assertThrows(classOf[IllegalArgumentException], () => {
        provided(l2aPath, index)
        ()
      })
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def gmlMasksGiveFloatNoDataViewAngles(softErrors: Boolean): Unit = {
    val bounds = GridBounds[Long](3, 5, 12, 13)
    val extent = demoGrid.extentFor(bounds)
    for (index <- Seq(6, 7)) {
      val source = provided(l1cPath, index, softErrors = softErrors)
      val IndexedRasterSource(noData: NoDataRasterSource, 0) = source
      assertEquals(FloatConstantNoDataCellType, source.cellType)
      assertEquals(FloatConstantNoDataCellType, noData.cellType)
      assertEquals(demoGrid, source.gridExtent)
      assertEquals(tileCrs, source.crs)
      assertEquals(1, source.bandCount)
      for (raster <- Seq(source.read(bounds, Seq(0)).get, source.read(extent, Seq(0)).get)) {
        assertEquals(extent, raster.extent)
        assertEquals((10, 9, 1), (raster.cols, raster.rows, raster.tile.bandCount))
        val band = raster.tile.band(0)
        assertEquals(FloatConstantNoDataCellType, band.cellType)
        assertTrue(band.isNoDataTile)
        assertTrue(band.toArrayDouble().forall(_.isNaN), "unsupported GML views must contain NaN, not mean angles")
      }
    }
    for ((index, output) <- Seq(4 -> sunAzimuth, 5 -> sunZenith))
      assertEquals(IndexedRasterSource(SentinelAngleRasterSource(l1cPath, output, tileCrs, demoGrid)(), 0),
        provided(l1cPath, index, softErrors = softErrors))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("<broken>", "<broken></mismatch>"))
  def malformedXmlFollowsTheProviderSoftErrorPolicy(xml: String): Unit = {
    val dir = Files.createTempDirectory(tempDir, "malformed-xml-")
    val path = Files.write(dir.resolve("MTD_TL.xml"), xml.getBytes(UTF_8)).toString
    for (index <- 4 to 7) {
      assertNull(provided(path, index, softErrors = true), s"band $index must be unavailable, not a mean substitute")
      val error = assertThrows(classOf[IllegalArgumentException], () => { provided(path, index); () })
      assertTrue(error.getMessage.startsWith(s"invalid Sentinel-2 tile metadata $path:"), error.getMessage)
      assertTrue(error.getCause.isInstanceOf[SAXParseException], error.toString)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("<broken>", "<broken></mismatch>"))
  def malformedXmlFollowsTheMeanAngleProviderSoftErrorPolicy(xml: String): Unit = {
    val dir = Files.createTempDirectory(tempDir, "malformed-mean-xml-")
    val path = Files.write(dir.resolve("MTD_TL.xml"), xml.getBytes(UTF_8)).toString
    for (index <- 0 to 3) {
      assertNull(provided(path, index, softErrors = true), s"mean angle band $index must be unavailable")
      val error = assertThrows(classOf[IllegalArgumentException], () => { provided(path, index); () })
      assertTrue(error.getMessage.startsWith(s"invalid Sentinel-2 tile metadata $path:"), error.getMessage)
      assertTrue(error.getCause.isInstanceOf[SAXParseException], error.toString)
    }
  }

  @Test
  def invalidMeanAnglesFollowTheMeanAngleProviderSoftErrorPolicy(): Unit = {
    val path = corruptedL2a("invalid-mean", "<Mean_Sun_Angle>", "36.85617250897", "not-a-number")
    for (index <- 0 to 3) {
      assertNull(provided(path, index, softErrors = true), s"mean angle band $index must be unavailable")
      val error = assertThrows(classOf[IllegalArgumentException], () => { provided(path, index); () })
      assertTrue(error.getMessage.startsWith(s"invalid Sentinel-2 tile metadata $path:"), error.getMessage)
      assertTrue(error.getCause.isInstanceOf[NumberFormatException], error.toString)
    }
  }

  @Test
  def ordinaryMetadataIoFailureFollowsTheProviderSoftErrorPolicy(): Unit = {
    val dir = Files.createTempDirectory(tempDir, "io-error-")
    val path = Files.copy(Paths.get(l2aPath), dir.resolve("MTD_TL.xml")).toString
    val provider = new SentinelXmlMetadataRasterSourceProvider {
      override private[layers] def loadTileAngles(xmlPath: String): TileAngles =
        throw new IOException("unreadable tile metadata")
    }

    for (index <- 4 to 7) {
      assertNull(provider.rasterSource(definition(path, index, softErrors = true)),
        s"band $index must be unavailable when its metadata cannot be read, not a mean substitute")
      val error = assertThrows(classOf[IOException], () => { provider.rasterSource(definition(path, index, softErrors = false)); () })
      assertEquals("unreadable tile metadata", error.getMessage)
    }
  }

  @Test
  def ordinaryMeanAngleMetadataIoFailureFollowsTheProviderSoftErrorPolicy(): Unit = {
    val provider = new SentinelXmlMetadataRasterSourceProvider {
      override private[layers] def loadConstantAngleBand(
          xmlPath: String,
          bandIndex: Int,
          targetProjectedExtent: Option[ProjectedExtent],
          cellSize: Option[CellSize]): SentinelXMLMetadataRasterSource =
        throw new IOException("unreadable mean-angle metadata")
    }

    for (index <- 0 to 3) {
      assertNull(provider.rasterSource(definition(l1cPath, index, softErrors = true)),
        s"mean angle band $index must be unavailable when its metadata cannot be read")
      val error = assertThrows(classOf[IOException], () => {
        provider.rasterSource(definition(l1cPath, index, softErrors = false))
        ()
      })
      assertEquals("unreadable mean-angle metadata", error.getMessage)
    }
  }

  @Test
  def s3MetadataFailuresFollowTheProviderSoftErrorPolicy(): Unit = {
    val provider = new SentinelXmlMetadataRasterSourceProvider {
      override private[layers] def loadTileAngles(xmlPath: String): TileAngles =
        throw S3Exception.builder().message("unreadable S3 tile metadata").build()
    }

    for (index <- 4 to 7) {
      assertNull(provider.rasterSource(definition(l2aPath, index, softErrors = true)),
        s"band $index must be unavailable when its S3 metadata cannot be read")
      assertThrows(classOf[S3Exception], () => {
        provider.rasterSource(definition(l2aPath, index, softErrors = false))
        ()
      })
    }
  }

  @Test
  def s3MeanAngleMetadataFailuresFollowTheProviderSoftErrorPolicy(): Unit = {
    val provider = new SentinelXmlMetadataRasterSourceProvider {
      override private[layers] def loadConstantAngleBand(
          xmlPath: String,
          bandIndex: Int,
          targetProjectedExtent: Option[ProjectedExtent],
          cellSize: Option[CellSize]): SentinelXMLMetadataRasterSource =
        throw S3Exception.builder().message("unreadable S3 mean-angle metadata").build()
    }

    for (index <- 0 to 3) {
      assertNull(provider.rasterSource(definition(l1cPath, index, softErrors = true)),
        s"mean angle band $index must be unavailable when its S3 metadata cannot be read")
      assertThrows(classOf[S3Exception], () => {
        provider.rasterSource(definition(l1cPath, index, softErrors = false))
        ()
      })
    }
  }

  @Test
  def failedMetadataIoIsRetriedAfterRecovery(): Unit = {
    var failedPaths = Set.empty[String]
    val provider = new SentinelXmlMetadataRasterSourceProvider {
      override private[layers] def loadTileAngles(xmlPath: String): TileAngles =
        if (!failedPaths.contains(xmlPath)) {
          failedPaths += xmlPath
          throw new IOException("transient metadata read failure")
        } else super.loadTileAngles(xmlPath)
    }

    def copy(name: String): String = {
      val dir = Files.createTempDirectory(tempDir, name)
      Files.copy(Paths.get(l2aPath), dir.resolve("MTD_TL.xml")).toString
    }

    val softPath = copy("io-retry-soft-")
    assertNull(provider.rasterSource(definition(softPath, 4, softErrors = true)))
    assertEquals(IndexedRasterSource(SentinelAngleRasterSource(softPath, sunAzimuth, tileCrs, demoGrid)(), 0),
      provider.rasterSource(definition(softPath, 4, softErrors = true)), "a failed I/O load must not be cached")

    val strictPath = copy("io-retry-strict-")
    val error = assertThrows(classOf[IOException], () => { provider.rasterSource(definition(strictPath, 7, softErrors = false)); () })
    assertEquals("transient metadata read failure", error.getMessage)
    assertEquals(IndexedRasterSource(SentinelAngleRasterSource(strictPath, viewZenith, tileCrs, demoGrid)(), 0),
      provider.rasterSource(definition(strictPath, 7, softErrors = false)), "a failed I/O load must be retried")
  }

  @Test
  def missingMetadataIsASoftErrorOrAFailure(): Unit = {
    val missing = tempDir.resolve("missing.SAFE/GRANULE/G/MTD_TL.xml").toString
    for (index <- Seq(0, 5, 7)) {
      assertNull(provided(missing, index, softErrors = true))
      assertThrows(classOf[FileNotFoundException], () => {
        provided(missing, index)
        ()
      })
    }
  }

  @Test
  def invalidSunGridOnlyAffectsSunBands(): Unit = {
    val path = corruptedL2a("bad-sun", "<Sun_Angles_Grid>", "37.5045", "37.5O45")
    assertNull(provided(path, 5, softErrors = true))
    val error = assertThrows(classOf[IllegalArgumentException], () => {
      provided(path, 4)
      ()
    })
    assertTrue(error.getMessage.contains(s"sunAzimuth is unavailable for $path: Sun_Angles_Grid Zenith: malformed value '37.5O45'"),
      error.getMessage)
    assertEquals(IndexedRasterSource(SentinelAngleRasterSource(path, viewZenith, tileCrs, demoGrid)(), 0), provided(path, 7))
  }

  @Test
  def invalidB05GridOnlyAffectsViewBands(): Unit = {
    val path = corruptedL2a("bad-b05", """<Viewing_Incidence_Angles_Grids bandId="4" detectorId="1">""", "NaN", "NaX")
    assertNull(provided(path, 6, softErrors = true))
    val error = assertThrows(classOf[IllegalArgumentException], () => {
      provided(path, 7)
      ()
    })
    assertTrue(error.getMessage.contains(s"viewZenith_B05 is unavailable for $path"), error.getMessage)
    assertEquals(IndexedRasterSource(SentinelAngleRasterSource(path, sunZenith, tileCrs, demoGrid)(), 0), provided(path, 5))
  }
}
