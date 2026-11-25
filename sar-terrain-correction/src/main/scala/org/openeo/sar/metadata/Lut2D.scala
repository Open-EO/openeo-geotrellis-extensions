package org.openeo.sar.metadata

/** 2-D bilinearly-interpolated lookup of `values[i][j]` defined on a sparse
 *  grid of (line, pixel) sample points. Used for both calibration vectors
 *  (sigmaNought) and noise vectors.
 *
 *  `lines`  is strictly increasing in image line indices.
 *  `pixels` is strictly increasing in image pixel indices and shared across all lines.
 *  `values(i)(j)` is the LUT value at (lines(i), pixels(j)). */
final class Lut2D(val lines: Array[Int], val pixels: Array[Int],
                  val values: Array[Array[Float]]) {

  require(values.length == lines.length, "sar_backscatter: LUT row count mismatch")
  values.foreach(r => require(r.length == pixels.length, "sar_backscatter: LUT column count mismatch"))

  /** Bilinear sample at fractional (line, pixel) coordinates. */
  def apply(line: Double, pixel: Double): Double = {
    val (i0, i1, ti) = bracket(lines, line)
    val (j0, j1, tj) = bracket(pixels, pixel)
    val v00 = values(i0)(j0); val v01 = values(i0)(j1)
    val v10 = values(i1)(j0); val v11 = values(i1)(j1)
    val a = v00 + (v01 - v00) * tj
    val b = v10 + (v11 - v10) * tj
    a + (b - a) * ti
  }

  private def bracket(grid: Array[Int], x: Double): (Int, Int, Double) = {
    if (x <= grid.head) return (0, 0, 0.0)
    if (x >= grid.last) { val k = grid.length - 1; return (k, k, 0.0) }
    var lo = 0; var hi = grid.length - 1
    while (hi - lo > 1) {
      val mid = (lo + hi) >>> 1
      if (grid(mid) <= x) lo = mid else hi = mid
    }
    val t = (x - grid(lo)).toDouble / (grid(hi) - grid(lo)).toDouble
    (lo, hi, t)
  }
}

object Lut2D {
  /** Builds a LUT from per-line vectors whose pixel sample positions may differ from line to line
   *  (as in newer thermal-noise annotations). Rows are resampled linearly onto the union of all
   *  pixel positions, clamping outside a row's own range. */
  def fromVectors(lines: Array[Int], pixelsPerLine: Seq[Array[Int]], valuesPerLine: Seq[Array[Float]]): Lut2D = {
    require(lines.length == pixelsPerLine.length && lines.length == valuesPerLine.length,
      "sar_backscatter: LUT row count mismatch")
    pixelsPerLine.zip(valuesPerLine).foreach { case (p, v) =>
      require(p.length == v.length, "sar_backscatter: LUT column count mismatch")
    }
    if (pixelsPerLine.forall(p => java.util.Arrays.equals(p, pixelsPerLine.head)))
      new Lut2D(lines, pixelsPerLine.head, valuesPerLine.toArray)
    else {
      val union = pixelsPerLine.flatten.distinct.sorted.toArray
      val values = pixelsPerLine.zip(valuesPerLine).map { case (p, v) =>
        val row = new Lut2D(Array(0), p, Array(v))
        union.map(x => row(0.0, x.toDouble).toFloat)
      }.toArray
      new Lut2D(lines, union, values)
    }
  }
}
