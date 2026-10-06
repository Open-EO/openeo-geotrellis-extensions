package org.openeo.geotrelliscommon

import geotrellis.raster.DoubleArrayTile
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import scala.util.Random

class SeparableConvolveSpec {

  private def randomBinaryTile(n: Int, seed: Long): Array[Double] = {
    val rnd = new Random(seed)
    Array.tabulate(n * n)(_ => if (rnd.nextDouble() < 0.3) 1.0 else 0.0)
  }

  private def blobTile(n: Int, seed: Long): Array[Double] = {
    val rnd = new Random(seed)
    val scale = 4 + rnd.nextInt(20)
    val m = n / scale + 2
    val coarse = Array.tabulate(m * m)(_ => rnd.nextDouble())
    val f = Array.ofDim[Double](n * n)
    for (r <- 0 until n; c <- 0 until n) {
      val y = r.toDouble / scale; val x = c.toDouble / scale
      val y0 = y.toInt; val x0 = x.toInt
      val fy = y - y0; val fx = x - x0
      f(r * n + c) = (1 - fy) * ((1 - fx) * coarse(y0 * m + x0) + fx * coarse(y0 * m + x0 + 1)) +
        fy * ((1 - fx) * coarse((y0 + 1) * m + x0) + fx * coarse((y0 + 1) * m + x0 + 1))
    }
    val sorted = f.sorted
    val threshold = sorted(((1 - 0.2) * (sorted.length - 1)).toInt)
    f.map(v => if (v >= threshold) 1.0 else 0.0)
  }

  // reference: FFTConvolve(input, outer(g, g)) then crop to the target window
  private def referenceConvolve(input: Array[Double], n: Int, g: Array[Double],
                                 colMin: Int, rowMin: Int, colMax: Int, rowMax: Int): Array[Double] = {
    val k = g.length
    val kernel2D = Array.tabulate(k * k) { idx => g(idx / k) * g(idx % k) }
    val inputTile = DoubleArrayTile(input, n, n)
    val kernelTile = DoubleArrayTile(kernel2D, k, k)
    val convolved = FFTConvolve(inputTile, kernelTile)
    val cropped = convolved.crop(colMin, rowMin, colMax, rowMax)
    cropped.toArrayDouble()
  }

  private def check(n: Int, k: Int, img: Array[Double], colMin: Int, rowMin: Int, colMax: Int, rowMax: Int): Unit = {
    val g = SCLConvolutionFilter.kernel1D(k).get
    val expected = referenceConvolve(img, n, g, colMin, rowMin, colMax, rowMax)
    val actual = SeparableConvolve.convolve(img, n, n, g, colMin, rowMin, colMax, rowMax)
    assertTrue(expected.length == actual.length, s"length mismatch for k=$k")
    var maxDiff = 0.0
    for (i <- expected.indices) {
      maxDiff = math.max(maxDiff, math.abs(expected(i) - actual(i)))
    }
    assertTrue(maxDiff < 1e-9, s"maxDiff=$maxDiff too large for kernel=$k window=($colMin,$rowMin,$colMax,$rowMax)")
  }

  @Test def testEquivalenceVariousKernelsAndWindows(): Unit = {
    val n = 64
    val kernels = Seq(1, 3, 9, 17, 39)
    val windows: Seq[(Int, Int, Int, Int)] = Seq(
      (0, 0, n - 1, n - 1), // full tile
      (10, 10, 30, 30), // interior window
      (0, 0, 5, 5), // touching top-left edge
      (n - 6, n - 6, n - 1, n - 1) // touching bottom-right edge
    )
    for (k <- kernels; seed <- Seq(1L, 2L, 3L)) {
      val img = if (seed % 2 == 0) randomBinaryTile(n, seed) else blobTile(n, seed)
      for ((colMin, rowMin, colMax, rowMax) <- windows) {
        check(n, k, img, colMin, rowMin, colMax, rowMax)
      }
    }
  }

  @Test def testEquivalenceLargeKernel(): Unit = {
    // mirrors the production defaults; larger buffer so the kernel fits
    val n = 256
    val k = 201
    val img = blobTile(n, 42L)
    check(n, k, img, 100, 100, 155, 155)
  }
}
