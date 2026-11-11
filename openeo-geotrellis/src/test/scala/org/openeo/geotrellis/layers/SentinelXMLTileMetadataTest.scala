package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.CellSize
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.xml.sax.SAXParseException
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource._

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import scala.xml.XML

object SentinelXMLTileMetadataTest {
  private val l1cPath = getClass.getResource("/org/openeo/geotrellis/layers/MTD_TL.xml").getPath
  private val l2aPath = getClass.getResource(
    "/org/openeo/geotrellis/layers/S2B_MSIL2A_20220502T103619_N0400_R008_T31UFS_20220502T123919.SAFE" +
      "/GRANULE/L2A_T31UFS_A026918_20220502T103700/MTD_TL.xml").getPath

  private val allBandIds = (0 to 12).toSet
  private val b05 = 4

  private def component(name: String, rows: Seq[String], colStep: String = "500", rowStep: String = "600",
                        unit: String = "m"): String =
    s"""<$name><COL_STEP unit="$unit">$colStep</COL_STEP><ROW_STEP unit="$unit">$rowStep</ROW_STEP>""" +
      s"""<Values_List>${rows.map(row => s"<VALUES>$row</VALUES>").mkString}</Values_List></$name>"""

  private val defaultSunZenith = component("Zenith", Seq("30 31 32", "33 34 35"))
  private val defaultSunAzimuth = component("Azimuth", Seq("150 151 152", "153 154 155"))

  private def viewingGrid(bandId: Any, detectorId: Any,
                          zenith: String = component("Zenith", Seq("NaN 5.5 INF", "-INF Infinity 6.5")),
                          azimuth: String = component("Azimuth", Seq("NaN 100.5 101", "102 103 359.5"))): String =
    s"""<Viewing_Incidence_Angles_Grids bandId="$bandId" detectorId="$detectorId">$zenith$azimuth</Viewing_Incidence_Angles_Grids>"""

  private val defaultViewingGrids = viewingGrid(4, 2) + viewingGrid(4, 3)

  private def detfoo(bandId: Any, path: String): String =
    s"""<MASK_FILENAME bandId="$bandId" type="MSK_DETFOO">$path</MASK_FILENAME>"""

  private val detfooB05 = "GRANULE/L2A_TEST/QI_DATA/MSK_DETFOO_B05.jp2"

  private val defaultGeocoding =
    """<Tile_Geocoding metadataLevel="Brief">
      |  <HORIZONTAL_CS_NAME>WGS84 / UTM zone 31N</HORIZONTAL_CS_NAME>
      |  <HORIZONTAL_CS_CODE>EPSG:32631</HORIZONTAL_CS_CODE>
      |  <Size resolution="10"><NROWS>120</NROWS><NCOLS>120</NCOLS></Size>
      |  <Size resolution="20"><NROWS>60</NROWS><NCOLS>60</NCOLS></Size>
      |  <Size resolution="60"><NROWS>20</NROWS><NCOLS>20</NCOLS></Size>
      |  <Geoposition resolution="10"><ULX>600000</ULX><ULY>5700000</ULY><XDIM>10</XDIM><YDIM>-10</YDIM></Geoposition>
      |  <Geoposition resolution="20"><ULX>600000</ULX><ULY>5700000</ULY><XDIM>20</XDIM><YDIM>-20</YDIM></Geoposition>
      |  <Geoposition resolution="60"><ULX>600000</ULX><ULY>5700000</ULY><XDIM>60</XDIM><YDIM>-60</YDIM></Geoposition>
      |</Tile_Geocoding>""".stripMargin

  /**
   * A minimal valid tile, 1.2 km wide, with a 2 x 3 sun grid and two B05 viewing grids. Each test that expects a
   * problem changes one part.
   */
  private case class SyntheticTile(root: String = "n1:Level-2A_Tile_ID",
                                   tileId: String = "S2B_OPER_MSI_L2A_TL_TEST_20220502T123919_A026918_T31UFS_N05.10",
                                   geocoding: String = defaultGeocoding,
                                   sunZenith: String = defaultSunZenith,
                                   sunAzimuth: String = defaultSunAzimuth,
                                   viewingGrids: String = defaultViewingGrids,
                                   masks: String =
                                   detfoo(4, detfooB05) +
                                     """<MASK_FILENAME bandId="4" type="MSK_QUALIT">GRANULE/L2A_TEST/QI_DATA/MSK_QUALIT_B05.jp2</MASK_FILENAME>""" +
                                     """<MASK_FILENAME type="MSK_CLASSI">GRANULE/L2A_TEST/QI_DATA/MSK_CLASSI_B00.jp2</MASK_FILENAME>""") {
    def xml: String =
      s"""<?xml version="1.0" encoding="UTF-8" standalone="no"?>
         |<$root xmlns:n1="urn:test">
         |  <n1:General_Info><TILE_ID metadataLevel="Brief">$tileId</TILE_ID></n1:General_Info>
         |  <n1:Geometric_Info>
         |    $geocoding
         |    <Tile_Angles metadataLevel="Standard">
         |      <Sun_Angles_Grid>$sunZenith$sunAzimuth</Sun_Angles_Grid>
         |      <Mean_Sun_Angle><ZENITH_ANGLE unit="deg">32.5</ZENITH_ANGLE><AZIMUTH_ANGLE unit="deg">152.5</AZIMUTH_ANGLE></Mean_Sun_Angle>
         |      $viewingGrids
         |    </Tile_Angles>
         |  </n1:Geometric_Info>
         |  <n1:Quality_Indicators_Info metadataLevel="Standard">
         |    <Pixel_Level_QI geometry="FULL_RESOLUTION">$masks</Pixel_Level_QI>
         |  </n1:Quality_Indicators_Info>
         |</$root>""".stripMargin

    def parse(viewBandIds: Set[Int] = Set(b05)): TileMetadata = parseXml(xml, viewBandIds)
  }

  private def parseXml(xml: String, viewBandIds: Set[Int] = Set(b05)): TileMetadata =
    parseTileMetadata(XML.loadString(xml), viewBandIds)

  private def right[T](result: Either[String, T]): T = result.fold(message => fail[T](s"unexpected error: $message"), identity)

  private def assertLeft(result: Either[String, _], expectedMessagePart: String): Unit = result match {
    case Left(message) => assertTrue(message.contains(expectedMessagePart), s"unexpected message: $message")
    case Right(value) => fail(s"expected an error containing '$expectedMessagePart', got $value")
  }

  /** The tile identity or geometry is invalid, so nothing can be used. */
  private def assertInvalid(tile: SyntheticTile, expectedMessagePart: String): Unit = {
    val error = assertThrows(classOf[IllegalArgumentException], () => {
      tile.parse()
      ()
    })
    assertTrue(error.getMessage.contains(expectedMessagePart), s"unexpected message: ${error.getMessage}")
  }

  /** Only the sun grid is unusable; B05 viewing angles still work. */
  private def assertSunError(tile: SyntheticTile, expectedMessagePart: String): Unit = {
    val metadata = tile.parse()
    assertLeft(metadata.sunGrid, expectedMessagePart)
    right(metadata.viewing(b05))
  }

  /** Only the B05 viewing angles are unusable; the sun grid still works. */
  private def assertViewError(tile: SyntheticTile, expectedMessagePart: String): Unit = {
    val metadata = tile.parse()
    right(metadata.sunGrid)
    assertLeft(metadata.viewing(b05), expectedMessagePart)
  }

  private def grids(metadata: TileMetadata): Map[(Int, Int), AngleGrid] =
    for ((bandId, viewing) <- metadata.viewing; (detectorId, grid) <- right(viewing).grids)
      yield (bandId, detectorId) -> grid

  private def viewingNaNCount(metadata: TileMetadata): Int =
    grids(metadata).values.map(grid => grid.zenith.count(_.isNaN) + grid.azimuth.count(_.isNaN)).sum

  /** Nodes where exactly one of zenith and azimuth is missing. */
  private def halfMissingNodes(metadata: TileMetadata): Int =
    grids(metadata).values.map(grid => grid.zenith.indices.count(i => grid.zenith(i).isNaN != grid.azimuth(i).isNaN)).sum

  private def validNodes(grid: AngleGrid): Int =
    (for (row <- 0 until grid.rows; col <- 0 until grid.cols if grid.isValid(row, col)) yield 1).size

  private def nanTokens(path: String): Int =
    "\\bNaN\\b".r.findAllIn(new String(Files.readAllBytes(Paths.get(path)), UTF_8)).size

  private def assertTileGeometry(metadata: TileMetadata, extent: Extent): Unit = {
    assertEquals(CRS.fromEpsgCode(32631), metadata.crs)
    assertEquals(extent, metadata.extent)
    assertEquals(Set(10, 20, 60), metadata.gridExtents.keySet)
    for ((resolution, size) <- Seq(10 -> 10980, 20 -> 5490, 60 -> 1830)) {
      val gridExtent = metadata.gridExtents(resolution)
      assertEquals(extent, gridExtent.extent)
      assertEquals(CellSize(resolution, resolution), gridExtent.cellSize)
      assertEquals(size.toLong, gridExtent.cols)
      assertEquals(size.toLong, gridExtent.rows)
    }
  }
}

class SentinelXMLTileMetadataTest {

  import SentinelXMLTileMetadataTest._

  @Test
  def parsesL1CFixtureWithGmlMasks(): Unit = {
    val metadata = loadTileMetadata(l1cPath, allBandIds)

    assertEquals("L1C", metadata.level)
    assertEquals("S2A_OPER_MSI_L1C_TL_VGS2_20201106T130245_A028076_T31UEQ_N02.09", metadata.tileId)
    assertEquals(Some("02.09"), metadata.processingBaseline)
    assertTileGeometry(metadata, Extent(499980, 5390220, 609780, 5500020))

    val sun = right(metadata.sunGrid)
    assertEquals((23, 23, 5000.0, 5000.0), (sun.rows, sun.cols, sun.colStep, sun.rowStep))
    assertEquals(66.2743, sun.zenithAt(0, 0))
    assertEquals(65.1413, sun.zenithAt(22, 22))
    assertEquals(171.017, sun.azimuthAt(0, 0))
    assertEquals(172.573, sun.azimuthAt(22, 22))
    assertFalse(sun.zenith.exists(_.isNaN) || sun.azimuth.exists(_.isNaN))

    assertEquals(allBandIds, metadata.viewing.keySet)
    assertEquals(91, grids(metadata).size)
    val b05Viewing = right(metadata.viewing(b05))
    assertEquals((5 to 11).toSet, b05Viewing.grids.keySet)

    // B05 detector 5 has a single valid node in this tile.
    val b05Detector5 = b05Viewing.grids(5)
    assertEquals((23, 23, 5000.0, 5000.0), (b05Detector5.rows, b05Detector5.cols, b05Detector5.colStep, b05Detector5.rowStep))
    assertEquals(2.38063, b05Detector5.zenithAt(0, 0))
    assertEquals(139.748, b05Detector5.azimuthAt(0, 0))
    assertEquals(1, validNodes(b05Detector5))

    assertEquals(81606, nanTokens(l1cPath))
    assertEquals(nanTokens(l1cPath), viewingNaNCount(metadata))
    assertEquals(0, halfMissingNodes(metadata))

    assertTrue(metadata.viewing.values.forall(viewing => right(viewing).detectorMask.format == DetectorMaskFormat.Gml))
    assertEquals(
      DetectorMaskRef("GRANULE/L1C_T31UEQ_A028076_20201106T105446/QI_DATA/MSK_DETFOO_B05.gml", DetectorMaskFormat.Gml),
      b05Viewing.detectorMask)
  }

  @Test
  def parsesL2AFixtureWithJp2Masks(): Unit = {
    val metadata = loadTileMetadata(l2aPath, allBandIds)

    assertEquals("L2A", metadata.level)
    assertEquals("S2B_OPER_MSI_L2A_TL_2BPS_20220502T123919_A026918_T31UFS_N04.00", metadata.tileId)
    assertEquals(Some("04.00"), metadata.processingBaseline)
    assertTileGeometry(metadata, Extent(600000, 5590200, 709800, 5700000))

    val sun = right(metadata.sunGrid)
    assertEquals((23, 23, 5000.0, 5000.0), (sun.rows, sun.cols, sun.colStep, sun.rowStep))
    assertEquals(37.5045, sun.zenithAt(0, 0))
    assertEquals(36.2082, sun.zenithAt(22, 22))
    assertEquals(158.915, sun.azimuthAt(0, 0))
    assertEquals(160.91, sun.azimuthAt(22, 22))
    assertFalse(sun.zenith.exists(_.isNaN) || sun.azimuth.exists(_.isNaN))

    assertEquals(78, grids(metadata).size)
    val b05Viewing = right(metadata.viewing(b05))
    assertEquals((1 to 6).toSet, b05Viewing.grids.keySet)

    val b05Detector1 = b05Viewing.grids(1)
    assertFalse(b05Detector1.isValid(0, 0))
    assertEquals(11.7529, b05Detector1.zenithAt(0, 1))
    assertEquals(110.296, b05Detector1.azimuthAt(0, 1))
    assertEquals(89, validNodes(b05Detector1))

    assertEquals(67960, nanTokens(l2aPath))
    assertEquals(nanTokens(l2aPath), viewingNaNCount(metadata))
    assertEquals(0, halfMissingNodes(metadata))

    assertTrue(metadata.viewing.values.forall(viewing => right(viewing).detectorMask.format == DetectorMaskFormat.Jp2))
    assertEquals(
      DetectorMaskRef("GRANULE/L2A_T31UFS_A026918_20220502T103700/QI_DATA/MSK_DETFOO_B05.jp2", DetectorMaskFormat.Jp2),
      b05Viewing.detectorMask)
  }

  @Test
  def onlyRequestedBandsAreParsed(): Unit = {
    assertEquals(Set(b05), loadTileMetadata(l2aPath, Set(b05)).viewing.keySet)
    assertEquals(Map.empty, loadTileMetadata(l2aPath, Set.empty).viewing)
  }

  @Test
  def parsesSyntheticTile(): Unit = {
    val metadata = SyntheticTile().parse()

    assertEquals("L2A", metadata.level)
    assertEquals(Some("05.10"), metadata.processingBaseline)
    assertEquals(Extent(600000, 5698800, 601200, 5700000), metadata.extent)
    assertEquals(Seq(10 -> 120L, 20 -> 60L, 60 -> 20L), Seq(10, 20, 60).map(r => r -> metadata.gridExtents(r).cols))

    // Columns and rows have different steps, so swapping them would be detected.
    val sun = right(metadata.sunGrid)
    assertEquals((2, 3, 500.0, 600.0), (sun.rows, sun.cols, sun.colStep, sun.rowStep))
    assertEquals(32.0, sun.zenithAt(0, 2))
    assertEquals(33.0, sun.zenithAt(1, 0))
    assertEquals(155.0, sun.azimuthAt(1, 2))

    val b05Viewing = right(metadata.viewing(b05))
    assertEquals(Set(2, 3), b05Viewing.grids.keySet)
    val grid = b05Viewing.grids(2)
    assertEquals(5.5, grid.zenithAt(0, 1))
    assertEquals(100.5, grid.azimuthAt(0, 1))
    assertEquals(359.5, grid.azimuthAt(1, 2))
    // NaN, INF, -INF and Infinity are all missing values.
    for ((row, col) <- Seq((0, 0), (0, 2), (1, 0), (1, 1))) assertTrue(grid.zenithAt(row, col).isNaN, s"($row, $col)")
    assertEquals(Seq(false, true, false, false, false, true),
      for (row <- 0 until 2; col <- 0 until 3) yield grid.isValid(row, col))

    // Only MSK_DETFOO references are used; other masks need no bandId.
    assertEquals(DetectorMaskRef(detfooB05, DetectorMaskFormat.Jp2), b05Viewing.detectorMask)
  }

  @Test
  def acceptsAnyNamespacePrefixAndBothLevels(): Unit = {
    assertEquals("L1C", SyntheticTile(root = "Level-1C_Tile_ID").parse().level)
    assertEquals("L1C", SyntheticTile(root = "n1:Level-1C_Tile_ID").parse().level)
    assertEquals("L2A", SyntheticTile(root = "Level-2A_Tile_ID").parse().level)
  }

  @Test
  def processingBaselineIsOptional(): Unit =
    assertEquals(None, SyntheticTile(tileId = "S2B_OPER_MSI_L2A_TL_TEST_20220502T123919_A026918_T31UFS").parse().processingBaseline)

  @Test
  def gmlMasksAreUsableMetadata(): Unit = {
    val tile = SyntheticTile(masks = detfoo(4, "GRANULE/X/QI_DATA/MSK_DETFOO_B05.GML"))
    assertEquals(DetectorMaskFormat.Gml, right(tile.parse().viewing(b05)).detectorMask.format)
  }

  @Test
  def otherBandsDoNotAffectB05OrSun(): Unit = {
    val brokenB01 = viewingGrid(0, 1, zenith = component("Zenith", Seq("1 2 3", "4 5")))
    val tile = SyntheticTile(
      viewingGrids = defaultViewingGrids + brokenB01,
      masks = detfoo(4, detfooB05) + detfoo(0, "GRANULE/X/QI_DATA/MSK_DETFOO_B01.tif"))

    val b05Only = tile.parse()
    right(b05Only.sunGrid)
    right(b05Only.viewing(b05))

    val withB01 = tile.parse(Set(0, b05))
    right(withB01.sunGrid)
    right(withB01.viewing(b05))
    assertLeft(withB01.viewing(0), "Viewing_Incidence_Angles_Grids bandId=0 detectorId=1 Zenith: row 1 has 2 values, expected 3")
  }

  @Test
  def missingBandIsAViewError(): Unit = {
    val metadata = SyntheticTile().parse(Set(b05, 7))
    right(metadata.viewing(b05))
    assertLeft(metadata.viewing(7), "no Viewing_Incidence_Angles_Grids for bandId=7")
  }

  @Test
  def sunGridProblemsOnlyAffectSun(): Unit = {
    assertSunError(SyntheticTile(sunZenith = component("Zenith", Seq("30 31 32", "33 34"))),
      "Sun_Angles_Grid Zenith: row 1 has 2 values, expected 3")
    assertSunError(SyntheticTile(sunAzimuth = component("Azimuth", Seq("150 151 152", "153 1S4 155"))),
      "Sun_Angles_Grid Azimuth: malformed value '1S4'")
    assertSunError(SyntheticTile(sunAzimuth = component("Azimuth", Seq("150 151 152", " "))), "malformed value ''")
    assertSunError(SyntheticTile(sunAzimuth = component("Azimuth", Seq())), "Sun_Angles_Grid Azimuth has no VALUES")
    assertSunError(SyntheticTile(sunAzimuth = component("Azimuth", Seq("150 151 152", "153 154 155", "156 157 158"))),
      "Sun_Angles_Grid: the zenith grid is 2 x 3 but the azimuth grid is 3 x 3")
    assertSunError(SyntheticTile(sunAzimuth = component("Azimuth", Seq("150 151 152", "153 154 155"), colStep = "510")),
      "Sun_Angles_Grid: zenith steps (500.0,600.0) differ from azimuth steps (510.0,600.0)")
    assertSunError(SyntheticTile(sunZenith = component("Zenith", Seq("30 31 32", "33 34 35"), unit = "km")),
      "Sun_Angles_Grid Zenith: COL_STEP unit is 'km', expected 'm'")
    assertSunError(SyntheticTile(sunZenith = component("Zenith", Seq("30 31 32", "33 34 35"), rowStep = "0")),
      "Sun_Angles_Grid Zenith: ROW_STEP must be positive")
    assertSunError(SyntheticTile(sunZenith = component("Zenith", Seq("30 31 32", "33 34 35"), colStep = "NaN")),
      "<COL_STEP> in <Zenith> is not a finite number: 'NaN'")
    assertSunError(SyntheticTile(sunZenith = ""), "expected one <Zenith> in <Sun_Angles_Grid>, found 0")
  }

  @Test
  def viewingGridProblemsOnlyAffectThatBand(): Unit = {
    assertViewError(SyntheticTile(viewingGrids = viewingGrid(4, 2,
      azimuth = component("Azimuth", Seq("NaN 100.5 1O1", "102 103 359.5")))),
      "Viewing_Incidence_Angles_Grids bandId=4 detectorId=2 Azimuth: malformed value '1O1'")
    assertViewError(SyntheticTile(viewingGrids = viewingGrid(4, 2,
      zenith = component("Zenith", Seq("1 2 3", "4 5 6", "7 8 9")))),
      "Viewing_Incidence_Angles_Grids bandId=4 detectorId=2: the zenith grid is 3 x 3 but the azimuth grid is 2 x 3")
    assertViewError(SyntheticTile(viewingGrids = viewingGrid(4, 2) + viewingGrid(4, 2)),
      "2 Viewing_Incidence_Angles_Grids for bandId=4 detectorId=2")
    assertViewError(SyntheticTile(viewingGrids = viewingGrid(4, "x")),
      "<Viewing_Incidence_Angles_Grids> has an invalid detectorId attribute: 'x'")
  }

  @Test
  def maskProblemsOnlyAffectThatBand(): Unit = {
    // An unknown format is an error, not a deliberately unsupported format like GML.
    assertViewError(SyntheticTile(masks = detfoo(4, "GRANULE/X/QI_DATA/MSK_DETFOO_B05.tif")),
      "MSK_DETFOO bandId=4: unsupported mask file 'GRANULE/X/QI_DATA/MSK_DETFOO_B05.tif', expected .jp2 or .gml")
    assertViewError(SyntheticTile(masks = detfoo(4, "")), "MSK_DETFOO bandId=4: unsupported mask file ''")
    assertViewError(SyntheticTile(masks = ""), "no MSK_DETFOO MASK_FILENAME for bandId=4")
    assertViewError(SyntheticTile(masks = detfoo(4, detfooB05) + detfoo(4, detfooB05)),
      "2 MSK_DETFOO MASK_FILENAME entries for bandId=4")
  }

  @Test
  def entriesWithoutReadableBandIdAffectAllViewing(): Unit = {
    assertViewError(SyntheticTile(viewingGrids = defaultViewingGrids + viewingGrid("x", 1)),
      "Viewing_Incidence_Angles_Grids has an invalid bandId attribute: 'x'")
    assertViewError(SyntheticTile(masks = detfoo(4, detfooB05) +
      """<MASK_FILENAME type="MSK_DETFOO">GRANULE/X/QI_DATA/MSK_DETFOO_B01.jp2</MASK_FILENAME>"""),
      "MSK_DETFOO MASK_FILENAME has an invalid bandId attribute: ''")
  }

  @Test
  def missingTileAnglesAffectsSunAndViewing(): Unit = {
    val metadata = parseXml(SyntheticTile().xml.replace("Tile_Angles", "Other_Angles"))
    assertLeft(metadata.sunGrid, "expected one <Tile_Angles> in <Geometric_Info>, found 0")
    assertLeft(metadata.viewing(b05), "expected one <Tile_Angles> in <Geometric_Info>, found 0")
  }

  @Test
  def rejectsUnexpectedRoot(): Unit =
    assertInvalid(SyntheticTile(root = "n1:Level-2A_User_Product"), "unexpected root element <Level-2A_User_Product>")

  @Test
  def rejectsInvalidTileId(): Unit =
    assertInvalid(SyntheticTile(tileId = " "), "<TILE_ID> in <General_Info> is empty")

  @Test
  def rejectsInconsistentGeometry(): Unit = {
    assertInvalid(SyntheticTile(geocoding = defaultGeocoding.replace("""<Size resolution="60"><NROWS>20</NROWS><NCOLS>20</NCOLS></Size>""", "")),
      """missing <Size resolution="60">""")
    assertInvalid(SyntheticTile(geocoding = defaultGeocoding.replace("<YDIM>-20</YDIM>", "<YDIM>20</YDIM>")),
      "the 20 m geometry is not north-up with 20 m pixels: XDIM=20.0, YDIM=20.0")
    assertInvalid(SyntheticTile(geocoding = defaultGeocoding.replace("""<Geoposition resolution="20"><ULX>600000</ULX>""", """<Geoposition resolution="20"><ULX>600020</ULX>""")),
      s"the 20 m geometry ${Extent(600020, 5698800, 601220, 5700000)} does not match the 10 m geometry")
    assertInvalid(SyntheticTile(geocoding = defaultGeocoding.replace("<NROWS>60</NROWS>", "<NROWS>61</NROWS>")),
      s"the 20 m geometry ${Extent(600000, 5698780, 601200, 5700000)} does not match the 10 m geometry")
    assertInvalid(SyntheticTile(geocoding = defaultGeocoding.replace("<NCOLS>120</NCOLS>", "<NCOLS>0</NCOLS>")),
      "<NCOLS> in <Size> is not a positive integer: '0'")
    assertInvalid(SyntheticTile(geocoding = defaultGeocoding.replace("EPSG:32631", "EPSG:none")),
      "unsupported HORIZONTAL_CS_CODE 'EPSG:none'")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("<broken>", "<broken></mismatch>"))
  def xmlSyntaxErrorsNameTheFileAndRetainTheCause(xml: String): Unit = {
    val file = Files.createTempFile("MTD_TL", ".xml")
    try {
      Files.write(file, xml.getBytes(UTF_8))
      val error = assertThrows(classOf[IllegalArgumentException], () => {
        loadTileMetadata(file.toString, Set(b05))
        ()
      })
      assertTrue(error.getCause.isInstanceOf[SAXParseException], error.toString)
      val cause = error.getCause.asInstanceOf[SAXParseException]
      assertEquals(s"invalid Sentinel-2 tile metadata $file: ${cause.getMessage}", error.getMessage)
      assertTrue(cause.getLineNumber > 0 && cause.getColumnNumber > 0, cause.toString)
    } finally Files.delete(file)
  }

  @Test
  def loadErrorsNameTheFile(): Unit = {
    val file = Files.createTempFile("MTD_TL", ".xml")
    try {
      Files.write(file, SyntheticTile(root = "n1:Level-2A_User_Product").xml.getBytes(UTF_8))
      val error = assertThrows(classOf[IllegalArgumentException], () => {
        loadTileMetadata(file.toString, Set(b05))
        ()
      })
      assertTrue(error.getMessage.startsWith(s"invalid Sentinel-2 tile metadata $file: unexpected root element"),
        error.getMessage)
    } finally Files.delete(file)
  }
}
