package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiffOptions, SinglebandGeoTiff, Tags, Tiled}
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{ArrayTile, CellSize, CellType, GridBounds, GridExtent, MultibandTile, RasterSource, UByteCellType, UByteConstantNoDataCellType}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, Assumptions, Test}
import org.openeo.geotrellis.layers.raster_source.SentinelDetectorMask._
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource.{DetectorMaskFormat, DetectorMaskRef, loadTileMetadata}

import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

object SentinelDetectorMaskTest {
  private val l2aPath = getClass.getResource(
    "/org/openeo/geotrellis/layers/S2B_MSIL2A_20220502T103619_N0400_R008_T31UFS_20220502T123919.SAFE" +
      "/GRANULE/L2A_T31UFS_A026918_20220502T103700/MTD_TL.xml").getPath

  private val crs = CRS.fromEpsgCode(32631)

  /** A synthetic tile of 8 x 6 pixels of 20 m, with a distinct label pattern covering 0 to 12. */
  private val tileExtent = Extent(600000, 5699880, 600160, 5700000)
  private val grid = GridExtent[Long](tileExtent, CellSize(20, 20))

  private def label(col: Long, row: Long): Int = ((col + 2 * row) % 13).toInt

  private val tempDir: Path = Files.createTempDirectory("SentinelDetectorMaskTest")

  @AfterAll
  def deleteTempDir(): Unit =
    Files.walk(tempDir).sorted(Comparator.reverseOrder[Path]()).forEach(path => Files.delete(path))

  private def labelTile(cols: Int, rows: Int, cellType: CellType = UByteCellType)(value: (Int, Int) => Int) =
    ArrayTile(Array.tabulate(cols * rows)(i => value(i % cols, i / cols)), cols, rows).convert(cellType)

  /** Writes a single-band GeoTIFF mask and opens it with the JVM GeoTIFF reader. */
  private def geoTiffMask(name: String, extent: Extent = tileExtent, maskCrs: CRS = crs, cols: Int = 8, rows: Int = 6,
                          cellType: CellType = UByteCellType, overviewLabel: Option[Int] = None)
                         (value: (Int, Int) => Int = (c, r) => label(c, r)): RasterSource = {
    val overviews = overviewLabel.toList.map(ovr =>
      SinglebandGeoTiff(labelTile(cols / 2, rows / 2, cellType)((_, _) => ovr), extent, maskCrs))
    val geoTiff = SinglebandGeoTiff(labelTile(cols, rows, cellType)(value), extent, maskCrs, Tags.empty,
      GeoTiffOptions(Tiled(16, 16)), overviews)
    val path = tempDir.resolve(s"$name.tif").toString
    GeoTiffWriter.write(geoTiff, path)
    GeoTiffRasterSource(GeoTiffPath(path))
  }

  /** Pixel centres of a grid with `resolution` aligned to the tile's upper-left corner, row by row. */
  private def targetCentres(resolution: Double): (Array[Double], Array[Double]) = {
    val xs = Iterator.iterate(tileExtent.xmin + resolution / 2)(_ + resolution).takeWhile(_ < tileExtent.xmax).toArray
    val ys = Iterator.iterate(tileExtent.ymax - resolution / 2)(_ - resolution).takeWhile(_ > tileExtent.ymin).toArray
    (for (y <- ys; x <- xs) yield x, for (y <- ys; _ <- xs) yield y)
  }

  private def assertInvalid(expectedMessagePart: String)(call: => Any): Unit = {
    val error = assertThrows(classOf[IllegalArgumentException], () => {
      call
      ()
    })
    assertTrue(error.getMessage.contains(expectedMessagePart), s"unexpected message: ${error.getMessage}")
  }

  private lazy val gdalAvailable: Boolean =
    try {
      com.azavea.gdal.GDALWarp.get_version_info("VERSION_NUM")
      true
    } catch {
      case _: LinkageError => false
    }

  private def histogram(labels: Array[Int]): Map[Int, Int] = labels.groupBy(identity).map { case (k, v) => k -> v.length }
}

class SentinelDetectorMaskTest {

  import SentinelDetectorMaskTest._

  private val b05Mask = DetectorMaskRef("GRANULE/L2A_G/QI_DATA/MSK_DETFOO_B05.jp2", DetectorMaskFormat.Jp2)

  @Test
  def maskPathIsNextToTheTileMetadata(): Unit = {
    for (root <- Seq("/data/S2B_X.SAFE", "/eodata/Sentinel-2/MSI/L2A/2022/05/02/S2B_X.SAFE", "s3://eodata/S2B_X.SAFE",
      "/vsis3/eodata/S2B_X.SAFE", "https://example.com/products/S2B_X.SAFE"))
      assertEquals(s"$root/GRANULE/L2A_G/QI_DATA/MSK_DETFOO_B05.jp2", maskPath(s"$root/GRANULE/L2A_G/MTD_TL.xml", b05Mask))
  }

  @Test
  def unsupportedMaskLocations(): Unit = {
    for (xmlPath <- Seq("/data/S2B_X/GRANULE/L2A_G/MTD_TL.xml", "/data/S2B_X.SAFE/MTD_TL.xml",
      "/data/tiles/31/U/FS/2022/5/2/0/metadata.xml", "/data/S2B_X.SAFE/GRANULE/L2A_G/QI_DATA/MTD_TL.xml"))
      assertInvalid("unsupported tile metadata location")(maskPath(xmlPath, b05Mask))

    val xmlPath = "/data/S2B_X.SAFE/GRANULE/L2A_G/MTD_TL.xml"
    for (mask <- Seq("GRANULE/L2A_OTHER/QI_DATA/MSK_DETFOO_B05.jp2", "GRANULE/L2A_G/../../MSK_DETFOO_B05.jp2",
      "/GRANULE/L2A_G/QI_DATA/MSK_DETFOO_B05.jp2", "MSK_DETFOO_B05.jp2"))
      assertInvalid("is not in the granule directory GRANULE/L2A_G")(maskPath(xmlPath, DetectorMaskRef(mask, DetectorMaskFormat.Jp2)))
  }

  @Test
  def realFixtureMaskIsFound(): Unit = {
    val mask = loadTileMetadata(l2aPath, Set(4)).viewing(4).fold(message => throw new AssertionError(message), _.detectorMask)
    val path = maskPath(l2aPath, mask)
    assertTrue(path.endsWith(".SAFE/GRANULE/L2A_T31UFS_A026918_20220502T103700/QI_DATA/MSK_DETFOO_B05.jp2"), path)
    assertTrue(Files.isRegularFile(Paths.get(path)), path)
    assertEquals(path, gdalPath(path, awsDirect = true))
  }

  @Test
  def gdalPathsFollowTheBandConventions(): Unit = {
    val cases = Seq(
      // (path, AWS_DIRECT unset, AWS_DIRECT=TRUE)
      ("/eodata/S2/x.jp2", "/eodata/S2/x.jp2", "/vsis3/eodata/S2/x.jp2"),
      ("s3://eodata/S2/x.jp2", "/eodata/S2/x.jp2", "/vsis3/eodata/S2/x.jp2"),
      ("s3://bucket/S2/x.jp2", "/vsis3/bucket/S2/x.jp2", "/vsis3/bucket/S2/x.jp2"),
      ("/vsis3/EODATA/S2/x.jp2", "/vsis3/eodata/S2/x.jp2", "/vsis3/eodata/S2/x.jp2"),
      ("/vsis3/eodata/S2/x.jp2", "/vsis3/eodata/S2/x.jp2", "/vsis3/eodata/S2/x.jp2"),
      ("https://example.com/S2/x.jp2", "/vsicurl/https://example.com/S2/x.jp2", "/vsicurl/https://example.com/S2/x.jp2"),
      ("/vsicurl/https://example.com/S2/x.jp2", "/vsicurl/https://example.com/S2/x.jp2", "/vsicurl/https://example.com/S2/x.jp2"),
      ("/data/S2/x.jp2", "/data/S2/x.jp2", "/data/S2/x.jp2"))
    for ((path, plain, direct) <- cases) {
      assertEquals(plain, gdalPath(path, awsDirect = false), path)
      assertEquals(direct, gdalPath(path, awsDirect = true), path)
    }
    for (path <- Seq("http://example.com/S2/x.jp2", "gs://bucket/S2/x.jp2", "relative/S2/x.jp2"))
      assertInvalid(s"unsupported detector mask location $path")(gdalPath(path, awsDirect = false))
  }

  @Test
  def everyTargetPixelGetsItsNativeLabel(): Unit = {
    val mask = geoTiffMask("pattern")()
    for (resolution <- Seq(10.0, 20.0, 40.0, 60.0, 80.0)) {
      val (xs, ys) = targetCentres(resolution)
      val expected = xs.indices.map(k => label(nativeCol(grid, xs(k)), nativeRow(grid, ys(k)))).toArray
      assertArrayEquals(expected, labelsAt(mask, grid, crs, xs, ys), s"$resolution m")
    }
    // The first 60 m pixel centre lies 30 m into the tile, in native column and row 1.
    assertEquals(label(1, 1), labelsAt(mask, grid, crs, Array(tileExtent.xmin + 30), Array(tileExtent.ymax - 30)).head)
  }

  @Test
  def pointsOnEdgesBelongToThePixelEastOrSouth(): Unit = {
    val mask = geoTiffMask("edges")()
    val (west, north) = (tileExtent.xmin, tileExtent.ymax)
    val points = Seq(
      (west + 40, north - 10) -> label(2, 0),
      (west + 30, north - 40) -> label(1, 2),
      (west + 40, north - 40) -> label(2, 2),
      (west, north) -> label(0, 0),
      (tileExtent.xmax - 1e-6, tileExtent.ymin + 1e-6) -> label(7, 5),
      (tileExtent.xmax, north - 10) -> 0, // the east and south edges are outside the last pixels
      (west + 10, tileExtent.ymin) -> 0,
      (west - 10, north - 10) -> 0,
      (west + 10, north + 10) -> 0)
    val labels = labelsAt(mask, grid, crs, points.map(_._1._1).toArray, points.map(_._1._2).toArray)
    assertEquals(points.map(_._2), labels.toSeq)
  }

  @Test
  def onlyTheWindowHoldingThePointsIsRead(): Unit = {
    val xs = Array(tileExtent.xmin + 50, tileExtent.xmin + 110, tileExtent.xmin - 5, tileExtent.xmin + 70)
    val ys = Array(tileExtent.ymax - 30, tileExtent.ymax - 70, tileExtent.ymax - 10, tileExtent.ymax + 5)
    assertEquals(Some(GridBounds[Long](2, 1, 5, 3)), nativeWindow(grid, xs, ys))
    assertEquals(None, nativeWindow(grid, Array(tileExtent.xmin - 5), Array(tileExtent.ymax - 5)))

    // Without points inside the tile, nothing is read, so even a mask with invalid labels is not a problem.
    val invalidLabels = geoTiffMask("all-invalid")((_, _) => 200)
    assertArrayEquals(Array(0, 0), labelsAt(invalidLabels, grid, crs, Array(tileExtent.xmin - 5, 0), Array(tileExtent.ymax - 5, 0)))
  }

  @Test
  def reducedResolutionLevelsAreNeverRead(): Unit = {
    // Like the real B05 JP2, this mask has an overview with label 7, which the native data does not contain.
    val mask = geoTiffMask("overview", overviewLabel = Some(7))()
    val coarse = GridExtent[Long](tileExtent, CellSize(40, 40))
    val throughOverview = mask.resampleToGrid(coarse, NearestNeighbor, AutoHigherResolution).read().get.tile.band(0)
    assertTrue(throughOverview.toArray().contains(7), "the test mask should expose label 7 when read through its overview")

    for (resolution <- Seq(40.0, 80.0)) {
      val (xs, ys) = targetCentres(resolution)
      val labels = labelsAt(mask, grid, crs, xs, ys)
      assertArrayEquals(xs.indices.map(k => label(nativeCol(grid, xs(k)), nativeRow(grid, ys(k)))).toArray, labels)
    }
  }

  @Test
  def noDataPixelsHaveNoDetector(): Unit = {
    val mask = geoTiffMask("nodata", cellType = UByteConstantNoDataCellType)((c, _) => if (c < 4) 0 else 3)
    assertArrayEquals(Array(0, 3), labelsAt(mask, grid, crs, Array(tileExtent.xmin + 10, tileExtent.xmin + 90), Array(tileExtent.ymax - 10, tileExtent.ymax - 10)))
  }

  @Test
  def rejectsMasksThatDoNotMatchTheTile(): Unit = {
    val (x, y) = (Array(tileExtent.xmin + 10), Array(tileExtent.ymax - 10))
    assertInvalid("differs from the tile grid")(labelsAt(geoTiffMask("shifted", extent = tileExtent.translate(20, 0))(), grid, crs, x, y))
    assertInvalid("differs from the tile grid")(labelsAt(geoTiffMask("10m", cols = 16, rows = 12)(), grid, crs, x, y))
    assertInvalid("differs from the tile CRS")(labelsAt(geoTiffMask("crs", maskCrs = CRS.fromEpsgCode(32632))(), grid, crs, x, y))

    val path = tempDir.resolve("two-bands.tif").toString
    GeoTiffWriter.write(MultibandGeoTiff(MultibandTile(labelTile(8, 6)(label(_, _)), labelTile(8, 6)(label(_, _))), tileExtent, crs), path)
    assertInvalid("detector mask has 2 bands, expected 1")(labelsAt(GeoTiffRasterSource(GeoTiffPath(path)), grid, crs, x, y))
  }

  @Test
  def rejectsLabelsAboveTwelve(): Unit = {
    val mask = geoTiffMask("label-13")((c, r) => if ((c, r) == (3, 2)) 13 else 1)
    assertArrayEquals(Array(1), labelsAt(mask, grid, crs, Array(tileExtent.xmin + 10), Array(tileExtent.ymax - 10)))
    assertInvalid("detector mask has label 13, expected 0 to 12")(
      labelsAt(mask, grid, crs, Array(tileExtent.xmin + 70), Array(tileExtent.ymax - 50)))
  }

  @Test
  def detectorLabelsNeedAJp2MaskOfAKnownBand(): Unit = {
    val metadata = loadTileMetadata(l2aPath, Set(4))
    val gml = DetectorMaskRef("GRANULE/L2A_T31UFS_A026918_20220502T103700/QI_DATA/MSK_DETFOO_B05.gml", DetectorMaskFormat.Gml)
    assertInvalid("unsupported detector mask format Gml")(detectorLabels(l2aPath, metadata, 4, gml, Array(0.0), Array(0.0)))
    assertInvalid("unknown bandId 13")(detectorLabels(l2aPath, metadata, 13, b05Mask, Array(0.0), Array(0.0)))
  }

  /** Reads the real B05 JP2 with GDAL. Runs where GDAL is installed, such as CI; expected values are from rasterio. */
  @Test
  def realB05Jp2GivesNativeLabels(): Unit = {
    Assumptions.assumeTrue(gdalAvailable, "GDAL (libgdal) is not installed here")
    val metadata = loadTileMetadata(l2aPath, Set(4))
    val mask = metadata.viewing(4).fold(message => throw new AssertionError(message), _.detectorMask)
    def labels(xs: Array[Double], ys: Array[Double]): Array[Int] = detectorLabels(l2aPath, metadata, 4, mask, xs, ys)

    // 10 m pixel centres across the boundary between detectors 2 and 3, and near the west and east edges.
    val xs = Array(643765.0, 643775.0, 643785.0, 643795.0, 600005.0, 709795.0)
    assertArrayEquals(Array(2, 2, 3, 3, 1, 5), labels(xs, Array.fill(xs.length)(5649995.0)))

    def centres(resolution: Int, count: Int): (Array[Double], Array[Double]) = {
      val offsets = Array.tabulate(count)(k => resolution / 2.0 + resolution * k)
      (for (dy <- offsets; dx <- offsets) yield 600000 + dx, for (dy <- offsets; _ <- offsets) yield 5700000 - dy)
    }

    val (xs60, ys60) = centres(60, 1830)
    assertEquals(Map(0 -> 6700, 1 -> 519695, 2 -> 773221, 3 -> 774571, 4 -> 756028, 5 -> 502333, 6 -> 16352),
      histogram(labels(xs60, ys60)))
    // At 80 m, GDAL would use a reduced-resolution level that contains label 7.
    val (xs80, ys80) = centres(80, 1372)
    assertEquals(Map(0 -> 3743, 1 -> 292116, 2 -> 434780, 3 -> 435535, 4 -> 425116, 5 -> 282021, 6 -> 9073),
      histogram(labels(xs80, ys80)))
  }
}
