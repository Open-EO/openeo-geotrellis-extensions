package org.openeo.geotrelliscommon

/**
 * Direct (non-FFT) convolution with a separable kernel outer(g, g), restricted to a target
 * output window. Numerically equivalent to FFTConvolve(tile, outer(g, g)).crop(window), for the
 * odd kernel sizes produced by [[SCLConvolutionFilter.kernel1D]] (matches the offset
 * `(k - 1) / 2` used by [[FFTConvolve]]).
 */
object SeparableConvolve {

  /**
   * Zero-padded 2D convolution of `input` (row-major, cols x rows) with the separable kernel
   * outer(g, g), evaluated only for the output window [colMin..colMax] x [rowMin..rowMax]
   * (inclusive).
   */
  def convolve(input: Array[Double], cols: Int, rows: Int, g: Array[Double],
               colMin: Int, rowMin: Int, colMax: Int, rowMax: Int): Array[Double] = {
    val k = g.length
    val h = k / 2
    val outCols = colMax - colMin + 1
    val outRows = rowMax - rowMin + 1

    // Horizontal pass: only the rows needed by the vertical pass, i.e. [rowMin - h, rowMax + h],
    // clipped to the tile.
    val tmpRowMin = math.max(0, rowMin - h)
    val tmpRowMax = math.min(rows - 1, rowMax + h)
    val tmpRowSpan = tmpRowMax - tmpRowMin + 1
    val tmp = new Array[Double](tmpRowSpan * outCols)

    var r = tmpRowMin
    while (r <= tmpRowMax) {
      val rowBase = r * cols
      val tmpBase = (r - tmpRowMin) * outCols
      var c = colMin
      while (c <= colMax) {
        var s = 0.0
        val cc = c - h
        var i = 0
        while (i < k) {
          val x = cc + i
          if (x >= 0 && x < cols) s += g(i) * input(rowBase + x)
          i += 1
        }
        tmp(tmpBase + (c - colMin)) = s
        c += 1
      }
      r += 1
    }

    // Vertical pass, inner loop over columns so memory access stays contiguous.
    val out = new Array[Double](outRows * outCols)
    r = rowMin
    while (r <= rowMax) {
      val outBase = (r - rowMin) * outCols
      val rr = r - h
      var i = 0
      while (i < k) {
        val y = rr + i
        if (y >= tmpRowMin && y <= tmpRowMax) {
          val gi = g(i)
          val tmpBase = (y - tmpRowMin) * outCols
          var c = 0
          while (c < outCols) {
            out(outBase + c) += gi * tmp(tmpBase + c)
            c += 1
          }
        }
        i += 1
      }
      r += 1
    }

    out
  }
}
