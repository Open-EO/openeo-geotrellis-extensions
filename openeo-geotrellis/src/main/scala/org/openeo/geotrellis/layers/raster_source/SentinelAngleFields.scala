package org.openeo.geotrellis.layers.raster_source

import geotrellis.raster.{GridBounds, GridExtent}
import geotrellis.vector.Extent
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource.AngleGrid

/**
 * Per-pixel Sentinel-2 sun and viewing angles from the angle grids in the tile metadata
 * (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/916).
 *
 * Node (0, 0) of a grid lies on the tile's upper-left corner (ULX, ULY), so a point (x, y) in the tile CRS has grid
 * position col = (x - ULX) / COL_STEP and row = (ULY - y) / ROW_STEP. Angles are in degrees: zenith from the
 * vertical, azimuth clockwise from north in [0, 360). NaN means NoData.
 *
 * Viewing angles are interpolated as look-direction components u = tan(zenith) sin(azimuth) and
 * v = tan(zenith) cos(azimuth). Within one detector these lie on a plane to within a few thousandths of a degree,
 * so the nodes outside the detector's footprint are filled from a least-squares plane. Sun angles are interpolated
 * directly; their grids have no gaps.
 */
object SentinelAngleFields {

  sealed trait Angle

  object Angle {
    case object Zenith extends Angle

    case object Azimuth extends Angle
  }

  /** Filled nodes are only used up to this many rings (Chebyshev distance in nodes) from a valid node. */
  val maxFillRings = 2

  /** value(col, row) = a + b * col + c * row */
  final case class Plane(a: Double, b: Double, c: Double) {
    def apply(col: Double, row: Double): Double = a + b * col + c * row
  }

  /** Least-squares planes through the look-direction components of one detector's valid nodes. */
  final case class LookPlanes(u: Plane, v: Plane)

  /**
   * One detector's viewing grid as look-direction components, with every missing node filled from [[LookPlanes]].
   * `usable(i)` is true for nodes at most [[maxFillRings]] rings from a valid node.
   */
  final class FilledViewGrid(val rows: Int,
                             val cols: Int,
                             val colStep: Double,
                             val rowStep: Double,
                             val u: Array[Double],
                             val v: Array[Double],
                             val usable: Array[Boolean]) extends Serializable

  def lookComponents(zenith: Double, azimuth: Double): (Double, Double) = {
    val tanZenith = math.tan(math.toRadians(zenith))
    val azimuthRadians = math.toRadians(azimuth)
    (tanZenith * math.sin(azimuthRadians), tanZenith * math.cos(azimuthRadians))
  }

  /** Returns None if there are fewer than three valid nodes, or if they all lie on one line. */
  def fitLookPlanes(grid: AngleGrid): Option[LookPlanes] = {
    // Normal equations of the least-squares fit. The coordinate sums are integers, so the singularity test is exact.
    var n, sumCol, sumRow, sumColCol, sumRowRow, sumColRow = 0L
    var sumU, sumColU, sumRowU, sumV, sumColV, sumRowV = 0.0
    for (row <- 0 until grid.rows; col <- 0 until grid.cols if grid.isValid(row, col)) {
      val (u, v) = lookComponents(grid.zenithAt(row, col), grid.azimuthAt(row, col))
      n += 1
      sumCol += col
      sumRow += row
      sumColCol += col * col
      sumRowRow += row * row
      sumColRow += col * row
      sumU += u
      sumColU += col * u
      sumRowU += row * u
      sumV += v
      sumColV += col * v
      sumRowV += row * v
    }

    val exactDeterminant =
      n * (sumColCol * sumRowRow - sumColRow * sumColRow) -
        sumCol * (sumCol * sumRowRow - sumColRow * sumRow) +
        sumRow * (sumCol * sumColRow - sumColCol * sumRow)
    if (exactDeterminant == 0) None
    else {
      val normal = Array(
        Array(n, sumCol, sumRow),
        Array(sumCol, sumColCol, sumColRow),
        Array(sumRow, sumColRow, sumRowRow)).map(_.map(_.toDouble))
      Some(LookPlanes(solvePlane(normal, Array(sumU, sumColU, sumRowU)), solvePlane(normal, Array(sumV, sumColV, sumRowV))))
    }
  }

  private def determinant(m: Array[Array[Double]]): Double =
    m(0)(0) * (m(1)(1) * m(2)(2) - m(1)(2) * m(2)(1)) -
      m(0)(1) * (m(1)(0) * m(2)(2) - m(1)(2) * m(2)(0)) +
      m(0)(2) * (m(1)(0) * m(2)(1) - m(1)(1) * m(2)(0))

  /** Solves normal * (a, b, c) = rhs by Cramer's rule; `normal` is non-singular. */
  private def solvePlane(normal: Array[Array[Double]], rhs: Array[Double]): Plane = {
    def withColumn(column: Int) = Array.tabulate(3, 3)((i, j) => if (j == column) rhs(i) else normal(i)(j))

    val d = determinant(normal)
    Plane(determinant(withColumn(0)) / d, determinant(withColumn(1)) / d, determinant(withColumn(2)) / d)
  }

  /**
   * Converts one detector's viewing grid to look-direction components and fills its missing nodes from the
   * detector's [[LookPlanes]]; valid nodes keep their values. Returns None if no plane can be fitted.
   */
  def fillViewGrid(grid: AngleGrid): Option[FilledViewGrid] =
    fitLookPlanes(grid).map { planes =>
      val size = grid.rows * grid.cols
      val (u, v, usable) = (new Array[Double](size), new Array[Double](size), new Array[Boolean](size))
      for (row <- 0 until grid.rows; col <- 0 until grid.cols) {
        val index = row * grid.cols + col
        if (grid.isValid(row, col)) {
          val (nodeU, nodeV) = lookComponents(grid.zenithAt(row, col), grid.azimuthAt(row, col))
          u(index) = nodeU
          v(index) = nodeV
          for (r <- math.max(0, row - maxFillRings) to math.min(grid.rows - 1, row + maxFillRings);
               c <- math.max(0, col - maxFillRings) to math.min(grid.cols - 1, col + maxFillRings))
            usable(r * grid.cols + c) = true
        } else {
          u(index) = planes.u(col, row)
          v(index) = planes.v(col, row)
        }
      }
      new FilledViewGrid(grid.rows, grid.cols, grid.colStep, grid.rowStep, u, v, usable)
    }

  /** Bilinear interpolation needs at least 2 x 2 nodes, and the position must lie within them. */
  private def insideGrid(rows: Int, cols: Int, col: Double, row: Double): Boolean =
    rows >= 2 && cols >= 2 && col >= 0 && row >= 0 && col <= cols - 1 && row <= rows - 1

  /** The column (or row) of the cell's upper-left node; the last column (or row) uses the cell before it. */
  private def cellStart(position: Double, size: Int): Int = math.min(position.toInt, size - 2)

  /** Bilinear interpolation; nodes with zero weight are skipped, so they may be NaN. */
  private def bilinear(v00: Double, v10: Double, v01: Double, v11: Double, fx: Double, fy: Double): Double =
    weighted(v00, (1 - fx) * (1 - fy)) + weighted(v10, fx * (1 - fy)) + weighted(v01, (1 - fx) * fy) + weighted(v11, fx * fy)

  private def weighted(value: Double, weight: Double): Double = if (weight == 0) 0.0 else value * weight

  /** Whether every node with a non-zero bilinear weight is usable; nodes with zero weight do not matter. */
  private def cornersUsable(usable00: Boolean, usable10: Boolean, usable01: Boolean, usable11: Boolean,
                            fx: Double, fy: Double): Boolean =
    (usable00 || fx == 1 || fy == 1) && (usable10 || fx == 0 || fy == 1) &&
      (usable01 || fx == 1 || fy == 0) && (usable11 || fx == 0 || fy == 0)

  private def normalizedAzimuth(degrees: Double): Double = {
    val azimuth = degrees % 360
    val positive = if (azimuth < 0) azimuth + 360 else azimuth
    if (positive >= 360) 0.0 else positive
  }

  /** Viewing angle at a grid position; NaN outside the grid or if the value would use a node that is not usable. */
  def viewAngle(grid: FilledViewGrid, angle: Angle, col: Double, row: Double): Double = {
    if (!insideGrid(grid.rows, grid.cols, col, row)) return Double.NaN
    val i = cellStart(col, grid.cols)
    val j = cellStart(row, grid.rows)
    val n00 = j * grid.cols + i
    val n10 = n00 + 1
    val n01 = n00 + grid.cols
    val n11 = n01 + 1
    val fx = col - i
    val fy = row - j
    if (!cornersUsable(grid.usable(n00), grid.usable(n10), grid.usable(n01), grid.usable(n11), fx, fy)) return Double.NaN

    val u = bilinear(grid.u(n00), grid.u(n10), grid.u(n01), grid.u(n11), fx, fy)
    val v = bilinear(grid.v(n00), grid.v(n10), grid.v(n01), grid.v(n11), fx, fy)
    angle match {
      case Angle.Zenith => math.toDegrees(math.atan(math.hypot(u, v)))
      case Angle.Azimuth => normalizedAzimuth(math.toDegrees(math.atan2(u, v)))
    }
  }

  /** Sun angle at a grid position; NaN outside the grid or if the value would use a missing node. */
  def sunAngle(grid: AngleGrid, angle: Angle, col: Double, row: Double): Double = {
    if (!insideGrid(grid.rows, grid.cols, col, row)) return Double.NaN
    val i = cellStart(col, grid.cols)
    val j = cellStart(row, grid.rows)
    val fx = col - i
    val fy = row - j
    if (!cornersUsable(grid.isValid(j, i), grid.isValid(j, i + 1), grid.isValid(j + 1, i), grid.isValid(j + 1, i + 1), fx, fy))
      return Double.NaN

    angle match {
      case Angle.Zenith =>
        bilinear(grid.zenithAt(j, i), grid.zenithAt(j, i + 1), grid.zenithAt(j + 1, i), grid.zenithAt(j + 1, i + 1), fx, fy)
      case Angle.Azimuth =>
        // Unwrap the corners around a valid one, so that 359 and 1 average to 0 rather than 180.
        val reference =
          if (grid.isValid(j, i)) grid.azimuthAt(j, i)
          else if (grid.isValid(j, i + 1)) grid.azimuthAt(j, i + 1)
          else if (grid.isValid(j + 1, i)) grid.azimuthAt(j + 1, i)
          else grid.azimuthAt(j + 1, i + 1)

        def unwrapped(azimuth: Double): Double = reference + (((azimuth - reference) % 360 + 540) % 360 - 180)

        normalizedAzimuth(bilinear(unwrapped(grid.azimuthAt(j, i)), unwrapped(grid.azimuthAt(j, i + 1)),
          unwrapped(grid.azimuthAt(j + 1, i)), unwrapped(grid.azimuthAt(j + 1, i + 1)), fx, fy))
    }
  }

  def gridColumn(x: Double, tileExtent: Extent, colStep: Double): Double = (x - tileExtent.xmin) / colStep

  def gridRow(y: Double, tileExtent: Extent, rowStep: Double): Double = (tileExtent.ymax - y) / rowStep

  private def insideTile(tileExtent: Extent, x: Double, y: Double): Boolean =
    x >= tileExtent.xmin && x <= tileExtent.xmax && y >= tileExtent.ymin && y <= tileExtent.ymax

  /** The pixel-centre coordinates of `bounds` in `gridExtent`, row by row. */
  def pixelCentres(gridExtent: GridExtent[Long], bounds: GridBounds[Long]): (Array[Double], Array[Double]) = {
    val (width, height) = (bounds.width, bounds.height)
    require(width * height <= Int.MaxValue, s"window too large: $width x $height")
    val size = (width * height).toInt
    val (xs, ys) = (new Array[Double](size), new Array[Double](size))
    // Once per column and row, as GridExtent boxes its indices; Int loops, as Long ranges would box too.
    val columnXs = Array.tabulate(width.toInt)(c => gridExtent.gridColToMap(bounds.colMin + c))
    val rowYs = Array.tabulate(height.toInt)(r => gridExtent.gridRowToMap(bounds.rowMin + r))
    for (r <- 0 until height.toInt; c <- 0 until width.toInt) {
      val index = r * width.toInt + c
      xs(index) = columnXs(c)
      ys(index) = rowYs(r)
    }
    (xs, ys)
  }

  /** Sun angles at points (xs, ys) in the tile CRS; NaN outside the tile. */
  def sunField(grid: AngleGrid, angle: Angle, tileExtent: Extent, xs: Array[Double], ys: Array[Double]): Array[Float] = {
    require(xs.length == ys.length, "xs and ys differ in length")
    val values = new Array[Float](xs.length) // filled in a loop: Array.tabulate would box every value
    for (k <- xs.indices)
      values(k) =
        if (!insideTile(tileExtent, xs(k), ys(k))) Float.NaN
        else sunAngle(grid, angle, gridColumn(xs(k), tileExtent, grid.colStep), gridRow(ys(k), tileExtent, grid.rowStep)).toFloat
    values
  }

  /**
   * Viewing angles at points (xs, ys) in the tile CRS, each from the grid of its detector label. NaN outside the
   * tile, for label 0 (no detector), and for labels without a filled grid.
   */
  def viewField(grids: Map[Int, FilledViewGrid], angle: Angle, tileExtent: Extent,
                xs: Array[Double], ys: Array[Double], detectors: Array[Int]): Array[Float] = {
    require(xs.length == ys.length && xs.length == detectors.length, "xs, ys and detectors differ in length")
    val byLabel = new Array[FilledViewGrid](math.max(0, if (grids.isEmpty) 0 else grids.keys.max + 1))
    for ((label, grid) <- grids if label > 0) byLabel(label) = grid
    val values = new Array[Float](xs.length)
    for (k <- xs.indices) {
      val label = detectors(k)
      val grid = if (label > 0 && label < byLabel.length) byLabel(label) else null
      values(k) =
        if (grid == null || !insideTile(tileExtent, xs(k), ys(k))) Float.NaN
        else viewAngle(grid, angle, gridColumn(xs(k), tileExtent, grid.colStep), gridRow(ys(k), tileExtent, grid.rowStep)).toFloat
    }
    values
  }
}
