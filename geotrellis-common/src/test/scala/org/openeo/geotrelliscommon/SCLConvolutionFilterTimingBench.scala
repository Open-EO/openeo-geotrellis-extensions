package org.openeo.geotrelliscommon

import geotrellis.raster.{MultibandTile, ShortArrayTile, ShortConstantNoDataCellType}
import org.junit.jupiter.api.Test

import java.util
import scala.util.Random

/**
 * One-off timing comparison of the old FFT-based createMask (LegacySCLConvolutionFilter) against
 * the new separable-convolution createMask (SCLConvolutionFilter), on realistic blob-shaped SCL
 * tiles, for the issue's own kernel sizes and the defaults. Not part of the regular test suite;
 * run manually with:
 *
 *   mvn -pl geotrellis-common test -Dtest=SCLConvolutionFilterTimingBench -DfailIfNoTests=false
 */
class SCLConvolutionFilterTimingBench {

  private val mask1Values = util.Arrays.asList(2, 4, 5, 6, 7)
  private val mask2Values = util.Arrays.asList(3, 8, 9, 10, 11)

  // Blobby binary field, mimicking cloud/shadow patches (same idea as Flips.java in the doc appendix),
  // then mapped onto realistic SCL values so both mask1 and mask2 branches actually run.
  private def blobbySclTile(n: Int, seed: Long): MultibandTile = {
    val rnd = new Random(seed)
    val scale = 4 + rnd.nextInt(40)
    val m = n / scale + 2
    val coarse = Array.fill(m * m)(rnd.nextDouble())
    val values = new Array[Short](n * n)
    var r = 0
    while (r < n) {
      var c = 0
      while (c < n) {
        val y = r.toDouble / scale
        val x = c.toDouble / scale
        val y0 = y.toInt
        val x0 = x.toInt
        val fy = y - y0
        val fx = x - x0
        val v = (1 - fy) * ((1 - fx) * coarse(y0 * m + x0) + fx * coarse(y0 * m + x0 + 1)) +
          fy * ((1 - fx) * coarse((y0 + 1) * m + x0) + fx * coarse((y0 + 1) * m + x0 + 1)) + 0.1 * rnd.nextDouble()
        val scl: Short =
          if (v > 0.6) 9 // cloud high prob -> mask2
          else if (v > 0.45) 3 // cloud shadow -> mask2
          else if (v > 0.35) 8 // cloud medium prob -> mask2
          else 4 // vegetation -> clear (mask1 branch)
        values(r * n + c) = scl
        c += 1
      }
      r += 1
    }
    MultibandTile(ShortArrayTile(values, n, n, ShortConstantNoDataCellType))
  }

  private def timeMs(iterations: Int, warmup: Int)(f: => Unit): Double = {
    var i = 0
    while (i < warmup) { f; i += 1 }
    val start = System.nanoTime()
    i = 0
    while (i < iterations) { f; i += 1 }
    (System.nanoTime() - start) / 1e6 / iterations
  }

  private def run(label: String, kernel1Size: Int, kernel2Size: Int, tileSize: Int, iterations: Int, warmup: Int): Unit = {
    val legacy = new LegacySCLConvolutionFilter(0, mask1Values, mask2Values, kernel1Size, kernel2Size)
    val optimized = new SCLConvolutionFilter(0, mask1Values, mask2Values, kernel1Size, kernel2Size)
    val buffer = optimized.bufferInPixels
    val n = tileSize + 2 * buffer

    val tiles = (0 until math.max(iterations, warmup)).map(seed => blobbySclTile(n, seed * 7919L + kernel1Size))

    var idx = 0
    val legacyMs = timeMs(iterations, warmup) {
      legacy.createMask(tiles(idx % tiles.length))
      idx += 1
    }
    idx = 0
    val newMs = timeMs(iterations, warmup) {
      optimized.createMask(tiles(idx % tiles.length))
      idx += 1
    }
    val speedup = legacyMs / newMs
    // scalastyle:off println
    println(f"$label%-45s tile=$tileSize buffer=$buffer k1=$kernel1Size k2=$kernel2Size  old(FFT)=$legacyMs%8.3f ms  new(separable)=$newMs%8.3f ms  speedup=${speedup}%5.2fx")
    // scalastyle:on println
  }

  @Test def benchmark(): Unit = {
    println("=== SCL dilation mask: old (FFT) vs new (separable convolution) ===")
    println("256px tiles, blob-shaped SCL data, single thread, 30 warmup + 30 measured iterations per config\n")
    run("Issue #859 parameters", kernel1Size = 9, kernel2Size = 39, tileSize = 256, iterations = 100, warmup = 100)
    run("Default parameters", kernel1Size = 17, kernel2Size = 201, tileSize = 256, iterations = 100, warmup = 100)
  }
}
