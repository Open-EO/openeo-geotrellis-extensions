package org.openeo.geotrellis.layers

import geotrellis.raster.{CellSize, GridBounds, GridExtent}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.layers.raster_source.SentinelAngleFields._
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource.{AngleGrid, TileMetadata, loadTileMetadata}

object SentinelAngleFieldsTest {
  private val layers = "/org/openeo/geotrellis/layers"
  private val l1cPath = getClass.getResource(s"$layers/MTD_TL.xml").getPath
  private val l2aPath = getClass.getResource(
    s"$layers/S2B_MSIL2A_20220502T103619_N0400_R008_T31UFS_20220502T123919.SAFE" +
      "/GRANULE/L2A_T31UFS_A026918_20220502T103700/MTD_TL.xml").getPath

  private val exact = 1e-9

  private def angleGrid(rows: Int, cols: Int, colStep: Double = 5000, rowStep: Double = 4000)
                       (node: (Int, Int) => Option[(Double, Double)]): AngleGrid = {
    val (zenith, azimuth) = (new Array[Double](rows * cols), new Array[Double](rows * cols))
    for (row <- 0 until rows; col <- 0 until cols) {
      val (z, a) = node(row, col).getOrElse((Double.NaN, Double.NaN))
      zenith(row * cols + col) = z
      azimuth(row * cols + col) = a
    }
    new AngleGrid(rows, cols, colStep, rowStep, zenith, azimuth)
  }

  private def normalized(azimuth: Double): Double = ((azimuth % 360) + 360) % 360

  private def anglesOf(u: Double, v: Double): (Double, Double) =
    (math.toDegrees(math.atan(math.hypot(u, v))), normalized(math.toDegrees(math.atan2(u, v))))

  /** Look-direction planes that stay away from nadir on the synthetic grids (azimuth between 110 and 150 degrees). */
  private val planeU = Plane(0.05, 0.01, -0.004)
  private val planeV = Plane(-0.10, 0.002, 0.008)

  private def planarAngles(col: Double, row: Double): (Double, Double) = anglesOf(planeU(col, row), planeV(col, row))

  /** A planar viewing grid of 7 rows and 8 columns, valid only where `valid(row, col)`. */
  private def planarGrid(valid: (Int, Int) => Boolean): AngleGrid =
    angleGrid(7, 8)((row, col) => if (valid(row, col)) Some(planarAngles(col, row)) else None)

  /** A curved field, so that bilinear interpolation does not reproduce it between nodes. */
  private val curvedGrid = angleGrid(4, 5)((row, col) => Some((3 + 0.3 * col + 0.2 * row + 0.05 * col * row, 100.0 + 2 * col - row)))
  /** The tile that `curvedGrid` covers: 4 x 5000 m wide and 3 x 4000 m high. */
  private val curvedTile = Extent(600000, 5688000, 620000, 5700000)

  private def filled(grid: AngleGrid): FilledViewGrid =
    fillViewGrid(grid).getOrElse(throw new AssertionError("fill failed"))

  private def azimuthDifference(a: Double, b: Double): Double = {
    val d = math.abs(a - b) % 360
    math.min(d, 360 - d)
  }

  private def assertView(expected: (Double, Double), grid: FilledViewGrid, col: Double, row: Double, tolerance: Double = exact): Unit = {
    assertEquals(expected._1, viewAngle(grid, Angle.Zenith, col, row), tolerance, s"zenith at ($col, $row)")
    val azimuth = viewAngle(grid, Angle.Azimuth, col, row)
    assertTrue(azimuthDifference(expected._2, azimuth) <= tolerance, s"azimuth at ($col, $row): ${expected._2} vs $azimuth")
  }

  private def assertNoView(grid: FilledViewGrid, col: Double, row: Double): Unit = {
    assertTrue(viewAngle(grid, Angle.Zenith, col, row).isNaN, s"zenith at ($col, $row)")
    assertTrue(viewAngle(grid, Angle.Azimuth, col, row).isNaN, s"azimuth at ($col, $row)")
  }

  private def direction(zenith: Double, azimuth: Double): Array[Double] = {
    val (z, a) = (math.toRadians(zenith), math.toRadians(azimuth))
    Array(math.sin(z) * math.sin(a), math.sin(z) * math.cos(a), math.cos(z))
  }

  /** The angle in degrees between two viewing directions. */
  private def angleBetween(a: (Double, Double), b: (Double, Double)): Double = {
    val (p, q) = (direction(a._1, a._2), direction(b._1, b._2))
    val cross = Array(p(1) * q(2) - p(2) * q(1), p(2) * q(0) - p(0) * q(2), p(0) * q(1) - p(1) * q(0))
    math.toDegrees(math.atan2(math.sqrt(cross.map(x => x * x).sum), p.zip(q).map { case (x, y) => x * y }.sum))
  }

  private def allViewingGrids(path: String): Iterable[AngleGrid] = {
    val metadata: TileMetadata = loadTileMetadata(path, (0 to 12).toSet)
    metadata.viewing.values.flatMap(_.fold(message => throw new AssertionError(message), _.grids.values))
  }

  private def nodes(grid: AngleGrid): Seq[(Int, Int)] = for (row <- 0 until grid.rows; col <- 0 until grid.cols) yield (row, col)

  /** Valid nodes with a missing neighbour (diagonals included) inside the grid. */
  private def outerRing(grid: AngleGrid): Set[(Int, Int)] =
    nodes(grid).filter { case (row, col) =>
      grid.isValid(row, col) && (for (r <- row - 1 to row + 1; c <- col - 1 to col + 1
                                      if r >= 0 && c >= 0 && r < grid.rows && c < grid.cols) yield grid.isValid(r, c)).contains(false)
    }.toSet

  private def without(grid: AngleGrid, hidden: Set[(Int, Int)]): AngleGrid =
    angleGrid(grid.rows, grid.cols, grid.colStep, grid.rowStep)((row, col) =>
      if (grid.isValid(row, col) && !hidden.contains((row, col))) Some((grid.zenithAt(row, col), grid.azimuthAt(row, col)))
      else None)
}

class SentinelAngleFieldsTest {

  import SentinelAngleFieldsTest._

  @Test
  def planarFieldsAreReproducedInsideAndBeyondValidNodes(): Unit = {
    // Valid nodes: rows 2-4, columns 3-5. Usable: rows 0-6, columns 1-7.
    val grid = filled(planarGrid((row, col) => row >= 2 && row <= 4 && col >= 3 && col <= 5))

    for (row <- 0 to 6; col <- 1 to 7) assertView(planarAngles(col, row), grid, col, row)
    for ((col, row) <- Seq((1.3, 0.6), (4.5, 3.25), (6.9, 5.5), (7.0, 6.0)))
      assertView(planarAngles(col, row), grid, col, row)

    // Column 0 is three rings from the nearest valid node.
    for (row <- 0 to 6) assertNoView(grid, 0, row)
    assertNoView(grid, 0.5, 3)
  }

  @Test
  def nodeValuesAreReturnedAtNodes(): Unit = {
    val view = filled(curvedGrid)
    for ((row, col) <- nodes(curvedGrid)) {
      val expected = (curvedGrid.zenithAt(row, col), curvedGrid.azimuthAt(row, col))
      assertView(expected, view, col, row)
      assertEquals(expected._1, sunAngle(curvedGrid, Angle.Zenith, col, row), exact)
      assertEquals(expected._2, sunAngle(curvedGrid, Angle.Azimuth, col, row), exact)
    }
  }

  @Test
  def nodeZeroLiesOnTheTileUpperLeftCorner(): Unit = {
    // Node (0, 0) at (ULX, ULY), and node (row 1, column 2) one row step down and two column steps right.
    val xs = Array(curvedTile.xmin, curvedTile.xmin + 2 * 5000)
    val ys = Array(curvedTile.ymax, curvedTile.ymax - 4000)
    val expectedZenith = Array(curvedGrid.zenithAt(0, 0), curvedGrid.zenithAt(1, 2)).map(_.toFloat)
    val expectedAzimuth = Array(curvedGrid.azimuthAt(0, 0), curvedGrid.azimuthAt(1, 2)).map(_.toFloat)

    assertArrayEquals(expectedZenith, sunField(curvedGrid, Angle.Zenith, curvedTile, xs, ys), 1e-5f)
    assertArrayEquals(expectedAzimuth, sunField(curvedGrid, Angle.Azimuth, curvedTile, xs, ys), 1e-4f)

    val views = Map(1 -> filled(curvedGrid))
    assertArrayEquals(expectedZenith, viewField(views, Angle.Zenith, curvedTile, xs, ys, Array(1, 1)), 1e-5f)
    assertArrayEquals(expectedAzimuth, viewField(views, Angle.Azimuth, curvedTile, xs, ys, Array(1, 1)), 1e-4f)
  }

  @Test
  def azimuthsWrapAroundNorth(): Unit = {
    val grid = angleGrid(2, 2)((_, col) => Some((5.0, if (col == 0) 359.0 else 1.0)))

    assertEquals(0.0, azimuthDifference(0, sunAngle(grid, Angle.Azimuth, 0.5, 0.5)), exact)
    assertEquals(359.5, sunAngle(grid, Angle.Azimuth, 0.25, 0.5), exact)
    assertEquals(0.5, sunAngle(grid, Angle.Azimuth, 0.75, 0.5), exact)
    assertEquals(5.0, sunAngle(grid, Angle.Zenith, 0.5, 0.5), exact)

    val view = filled(grid)
    val tanZenith = math.tan(math.toRadians(5))
    val (sin1, cos1) = (math.sin(math.toRadians(1)), math.cos(math.toRadians(1)))
    assertView(anglesOf(0, tanZenith * cos1), view, 0.5, 0.5)
    assertView(anglesOf(-0.5 * tanZenith * sin1, tanZenith * cos1), view, 0.25, 0.5)
  }

  @Test
  def viewingDirectionThroughNadir(): Unit = {
    // One degree east at column 0 and one degree west at column 1: the view passes straight down in between.
    val view = filled(angleGrid(2, 2)((_, col) => Some((1.0, if (col == 0) 90.0 else 270.0))))
    val halfTan = 0.5 * math.tan(math.toRadians(1))

    assertEquals(0.0, viewAngle(view, Angle.Zenith, 0.5, 0.5), exact)
    assertView((math.toDegrees(math.atan(halfTan)), 90.0), view, 0.25, 0.5)
    assertView((math.toDegrees(math.atan(halfTan)), 270.0), view, 0.75, 0.5)
  }

  @Test
  def thinStripWithThreeValidNodes(): Unit = {
    val grid = filled(planarGrid((row, col) => (row, col) == (0, 0) || (row, col) == (0, 1) || (row, col) == (1, 0)))

    assertView(planarAngles(2, 2), grid, 2, 2)
    assertView(planarAngles(2.5, 0.5), grid, 2.5, 0.5)
    // Column 4 is three rings from the nearest valid node (column 1).
    assertNoView(grid, 3.5, 0.5)
  }

  @Test
  def noPlaneFromTooFewOrCollinearNodes(): Unit = {
    val twoNodes = planarGrid((row, col) => row == 2 && (col == 3 || col == 4))
    val sameRow = planarGrid((row, col) => row == 2 && col >= 1 && col <= 5)
    val diagonal = planarGrid((row, col) => row == col)
    for (grid <- Seq(twoNodes, sameRow, diagonal)) {
      assertEquals(None, fitLookPlanes(grid))
      assertEquals(None, fillViewGrid(grid))
    }
    assertTrue(fillViewGrid(planarGrid((row, col) => row == col || (row, col) == (0, 1))).isDefined)
  }

  @Test
  def positionsOutsideTheGridAreNoData(): Unit = {
    val view = filled(curvedGrid)
    for ((col, row) <- Seq((-0.01, 1.0), (4.01, 1.0), (1.0, -0.01), (1.0, 3.01), (Double.NaN, 1.0))) {
      assertNoView(view, col, row)
      assertTrue(sunAngle(curvedGrid, Angle.Zenith, col, row).isNaN)
      assertTrue(sunAngle(curvedGrid, Angle.Azimuth, col, row).isNaN)
    }
    // The last column and row are inside the grid.
    assertEquals(curvedGrid.zenithAt(3, 4), sunAngle(curvedGrid, Angle.Zenith, 4, 3), exact)
  }

  @Test
  def missingSunNodesOnlyAffectTheirCells(): Unit = {
    val grid = angleGrid(3, 3)((row, col) => if ((row, col) == (0, 0)) None else Some((30.0 + col, 150.0 + row)))
    assertTrue(sunAngle(grid, Angle.Zenith, 0.5, 0.5).isNaN)
    assertTrue(sunAngle(grid, Angle.Azimuth, 0.5, 0.5).isNaN)
    assertEquals(31.5, sunAngle(grid, Angle.Zenith, 1.5, 1.5), exact)
    assertEquals(151.5, sunAngle(grid, Angle.Azimuth, 1.5, 1.5), exact)

    // On the upper edge of a cell, its lower nodes have zero weight, so a missing lower node does not matter.
    val lowerMissing = angleGrid(3, 3)((row, col) => if ((row, col) == (2, 0)) None else Some((30.0 + col, 150.0 + row)))
    assertEquals(30.5, sunAngle(lowerMissing, Angle.Zenith, 0.5, 1.0), exact)
    assertEquals(151.0, sunAngle(lowerMissing, Angle.Azimuth, 0.5, 1.0), exact)
    assertTrue(sunAngle(lowerMissing, Angle.Zenith, 0.5, 1.5).isNaN)
    assertTrue(sunAngle(lowerMissing, Angle.Azimuth, 0.5, 1.5).isNaN)
  }

  @Test
  def fieldsAreNoDataOutsideTheTileAndWithoutADetectorGrid(): Unit = {
    val xs = Array(curvedTile.xmin + 100, curvedTile.xmin - 1, curvedTile.xmin + 100, curvedTile.xmin + 100, curvedTile.xmin + 100)
    val ys = Array(curvedTile.ymax - 100, curvedTile.ymax - 100, curvedTile.ymax + 1, curvedTile.ymax - 100, curvedTile.ymax - 100)
    val detectors = Array(1, 1, 1, 0, 5)

    val sun = sunField(curvedGrid, Angle.Zenith, curvedTile, xs, ys)
    assertEquals(Seq(false, true, true, false, false), sun.toSeq.map(_.isNaN))

    val view = viewField(Map(1 -> filled(curvedGrid)), Angle.Zenith, curvedTile, xs, ys, detectors)
    assertEquals(Seq(false, true, true, true, true), view.toSeq.map(_.isNaN))
    assertTrue(viewField(Map.empty, Angle.Zenith, curvedTile, xs, ys, detectors).forall(_.isNaN))
  }

  @Test
  def croppedWindowMatchesFullEvaluation(): Unit = {
    val gridExtent = GridExtent[Long](curvedTile, CellSize(500, 500))
    val full = GridBounds[Long](0, 0, gridExtent.cols - 1, gridExtent.rows - 1)
    val crop = GridBounds[Long](7, 5, 20, 15)
    // Detector 1 on the western half of the tile, detector 2 on the eastern half.
    val views = Map(1 -> filled(curvedGrid), 2 -> filled(planarGrid((row, col) => row >= 1 && col <= 4)))

    def fields(bounds: GridBounds[Long]): Seq[Array[Float]] = {
      val (xs, ys) = pixelCentres(gridExtent, bounds)
      val detectors = xs.map(x => if (x < 610000) 1 else 2)
      Seq(sunField(curvedGrid, Angle.Zenith, curvedTile, xs, ys), sunField(curvedGrid, Angle.Azimuth, curvedTile, xs, ys),
        viewField(views, Angle.Zenith, curvedTile, xs, ys, detectors), viewField(views, Angle.Azimuth, curvedTile, xs, ys, detectors))
    }

    val (cropXs, cropYs) = pixelCentres(gridExtent, crop)
    assertEquals((curvedTile.xmin + 7.5 * 500, curvedTile.ymax - 5.5 * 500), (cropXs.head, cropYs.head))

    val fullWidth = full.width.toInt
    for ((fullField, cropField) <- fields(full).zip(fields(crop)); r <- 0 until crop.height.toInt; c <- 0 until crop.width.toInt) {
      val expected = fullField((crop.rowMin.toInt + r) * fullWidth + crop.colMin.toInt + c)
      val actual = cropField(r * crop.width.toInt + c)
      assertTrue(expected.equals(actual), s"pixel ($r, $c): $expected vs $actual")
    }
    assertTrue(fields(full).forall(_.exists(!_.isNaN)))
  }

  @Test
  def planeFitsRealDetectorsToWithinFiveThousandthsOfADegree(): Unit =
    for ((path, expectedUnfittable) <- Seq(l1cPath -> 13, l2aPath -> 0)) {
      var (unfittable, worst) = (0, 0.0)
      for (grid <- allViewingGrids(path)) fitLookPlanes(grid) match {
        case None => unfittable += 1
        case Some(planes) =>
          for ((row, col) <- nodes(grid) if grid.isValid(row, col)) {
            val fitted = anglesOf(planes.u(col, row), planes.v(col, row))
            worst = math.max(worst, angleBetween((grid.zenithAt(row, col), grid.azimuthAt(row, col)), fitted))
          }
      }
      assertEquals(expectedUnfittable, unfittable, path)
      assertTrue(worst <= 0.005, s"$path: worst plane residual $worst degrees")
    }

  @Test
  def hiddenOuterRingIsPredictedToWithinFiveThousandthsOfADegree(): Unit =
    // Expected counts are from an independent NumPy implementation of the same method.
    for ((path, expectedPoints, expectedSkipped) <- Seq((l1cPath, 3315, 13), (l2aPath, 3119, 13))) {
      var (points, skipped, worst) = (0, 0, 0.0)
      for (grid <- allViewingGrids(path)) {
        val ring = outerRing(grid)
        fillViewGrid(without(grid, ring)) match {
          case None => skipped += 1
          case Some(view) =>
            for ((row, col) <- ring) {
              val index = row * grid.cols + col
              val predicted = anglesOf(view.u(index), view.v(index))
              worst = math.max(worst, angleBetween((grid.zenithAt(row, col), grid.azimuthAt(row, col)), predicted))
              points += 1
            }
        }
      }
      assertEquals((expectedPoints, expectedSkipped), (points, skipped), path)
      assertTrue(worst <= 0.005, s"$path: worst hold-out error $worst degrees")
    }

  @Test
  def b05DetectorWithOneValidNodeCannotBeFilled(): Unit = {
    val metadata = loadTileMetadata(l1cPath, Set(4))
    val grids = metadata.viewing(4).fold(message => throw new AssertionError(message), _.grids)
    assertEquals(None, fillViewGrid(grids(5)))
    assertTrue((6 to 11).forall(detector => fillViewGrid(grids(detector)).isDefined))
  }
}
