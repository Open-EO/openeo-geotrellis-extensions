package org.openeo.geotrelliscommon

import geotrellis.raster.{GridBounds, MultibandTile, ShortArrayTile, ShortConstantNoDataCellType, Tile}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.Test

import java.util
import scala.util.Random

// LegacySCLConvolutionFilter (the byte-for-byte reference implementation of the original,
// pre-optimization FFT-based createMask) now lives in main (CloudFilterStrategy.scala), since it's
// also used at runtime behind the useSeparableConvolution flag on
// OpenEOProcesses.toSclDilationMask. Same package, so no import needed here.

class SCLConvolutionFilterSpec {

  private val mask1Values = util.Arrays.asList(2, 4, 5, 6, 7)
  private val mask2Values = util.Arrays.asList(3, 8, 9, 10, 11)

  private def sclTile(n: Int, seed: Long, valuePool: Seq[Int], nodataFrac: Double = 0.0): MultibandTile = {
    val rnd = new Random(seed)
    val arr = Array.tabulate(n * n) { _ =>
      if (rnd.nextDouble() < nodataFrac) Short.MinValue
      else valuePool(rnd.nextInt(valuePool.length)).toShort
    }
    MultibandTile(ShortArrayTile(arr, n, n, ShortConstantNoDataCellType))
  }

  private def assertBitIdentical(expected: Tile, actual: Tile): Unit = {
    assertEquals(expected.cols, actual.cols)
    assertEquals(expected.rows, actual.rows)
    assertArrayEquals(expected.toArray(), actual.toArray())
  }

  private def compareFullTile(erosionSize: Int, kernel1Size: Int, kernel2Size: Int, tile: MultibandTile): Unit = {
    val legacy = new LegacySCLConvolutionFilter(erosionSize, mask1Values, mask2Values, kernel1Size, kernel2Size)
    val optimized = new SCLConvolutionFilter(erosionSize, mask1Values, mask2Values, kernel1Size, kernel2Size)
    assertBitIdentical(legacy.createMask(tile), optimized.createMask(tile))
  }

  @Test def testRandomTilesDefaultKernels(): Unit = {
    val allValues = 0 to 11
    for (seed <- 1L to 5L) {
      val tile = sclTile(64, seed, allValues)
      compareFullTile(0, 17, 201, tile)
    }
  }

  @Test def testRandomTilesSmallKernels(): Unit = {
    val allValues = 0 to 11
    for (seed <- 1L to 5L) {
      val tile = sclTile(64, seed, allValues)
      compareFullTile(0, 9, 39, tile)
    }
  }

  @Test def testWithNodataPixels(): Unit = {
    val allValues = 0 to 11
    for (seed <- 1L to 5L) {
      val tile = sclTile(64, seed, allValues, nodataFrac = 0.1)
      compareFullTile(0, 17, 201, tile)
    }
  }

  @Test def testKernel1SizeZero(): Unit = {
    val allValues = 0 to 11
    for (seed <- 1L to 3L) {
      val tile = sclTile(64, seed, allValues)
      compareFullTile(0, 0, 201, tile)
    }
  }

  @Test def testErosionEnabled(): Unit = {
    val allValues = 0 to 11
    for (seed <- 1L to 3L) {
      val tile = sclTile(64, seed, allValues)
      compareFullTile(5, 17, 201, tile)
    }
  }

  @Test def testAllMasked(): Unit = {
    // Every pixel is in mask1Values and mask2Values is empty of matches: forces the allMasked branch.
    val tile = sclTile(64, 1L, Seq(2, 4, 5))
    compareFullTile(0, 17, 201, tile)
  }

  @Test def testNothingMasked(): Unit = {
    // No pixel in mask1Values: forces the nothingMasked branch, still needs mask2 evaluation.
    val tile = sclTile(64, 1L, Seq(0, 1))
    compareFullTile(0, 17, 201, tile)
  }

  @Test def testNoMask2ValuesPresent(): Unit = {
    // Every pixel is in mask2Values (so binaryMask2 is uniformly 1, "allMasked" for step 2).
    val tile = sclTile(64, 1L, Seq(3, 8, 9, 10, 11))
    compareFullTile(0, 17, 201, tile)
  }

  @Test def testTargetAreaSubwindowMatchesCroppedFullTile(): Unit = {
    val allValues = 0 to 11
    val kernel1Size = 17
    val kernel2Size = 39
    val optimized = new SCLConvolutionFilter(0, mask1Values, mask2Values, kernel1Size, kernel2Size)
    val bufferSize = optimized.bufferInPixels
    val n = 64 + 2 * bufferSize
    val tile = sclTile(n, 7L, allValues)

    val fullResult = optimized.createMask(tile)
    val targetArea = GridBounds(bufferSize, bufferSize, n - bufferSize - 1, n - bufferSize - 1)
    val windowedResult = optimized.createMask(tile, targetArea)

    assertBitIdentical(fullResult.crop(targetArea.colMin, targetArea.rowMin, targetArea.colMax, targetArea.rowMax), windowedResult)
  }
}
