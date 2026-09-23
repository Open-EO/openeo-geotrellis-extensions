package org.openeo.geotrelliscommon

import geotrellis.raster.{BitCellType, GridBounds, MultibandTile, ShortArrayTile, ShortConstantNoDataCellType, Tile}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.Test
import org.openeo.geotrelliscommon.SCLConvolutionFilter.{erosion_kernel, kernel}

import java.util
import scala.util.Random

/**
 * Byte-for-byte reference implementation of the original (pre-optimization) FFT-based
 * SCLConvolutionFilter.createMask, kept here so the optimized implementation can be checked
 * against it. Do not "fix" or simplify this copy: it must stay an exact replica of the old
 * algorithm.
 */
class LegacySCLConvolutionFilter(erosion_kernal_size: Int, mask1Values: util.List[Int], mask2Values: util.List[Int], kernel1Size: Int, kernel2Size: Int) extends Serializable {
  private val erosionKernel = erosion_kernel(erosion_kernal_size)
  private val kernel1 = kernel(kernel1Size)
  private val kernel2 = kernel(kernel2Size)

  def bufferInPixels: Int = (kernel2.get.cols / 2).floor.intValue()

  def createMask(sclTile: MultibandTile): Tile = {
    var allMasked = true
    var nothingMasked = true
    val maskTile = sclTile.band(0).convert(ShortConstantNoDataCellType)

    val binaryMask1 = maskTile.map(value => {
      if (mask1Values.contains(value)) {
        allMasked = false
        0
      } else {
        nothingMasked = false
        1
      }
    })
    val convolution1 =
      if (!nothingMasked && kernel1.isDefined) {
        val eroded1 = erode(binaryMask1)
        val dilated1 = FFTConvolve(eroded1, kernel1.get)
        allMasked = true
        Some(dilated1.localIf({ d: Double => {
          val res = d > 0.057
          if (!res) {
            allMasked = false
          }
          res
        }
        }, 1.0, 0.0))
      } else {
        if (nothingMasked) {
          None
        } else {
          Some(binaryMask1)
        }
      }
    if (allMasked) {
      return convolution1.get.convert(BitCellType)
    }

    allMasked = true
    val binaryMask2 = maskTile.map(value => {
      if (mask2Values.contains(value)) {
        1
      } else {
        allMasked = false
        0
      }
    })
    val convolution2 = if (!allMasked) {
      val eroded2 = erode(binaryMask2)
      val dilated2 = FFTConvolve(eroded2, kernel2.get)
      dilated2.localIf({ d: Double => d > 0.025 }, 1.0, 0.0)
    } else {
      binaryMask2
    }

    convolution1.map(_.localOr(convolution2)).getOrElse(convolution2).convert(BitCellType)
  }

  private def erode(binaryMask2: Tile) = {
    if (erosionKernel.isDefined) {
      val maskInvert = binaryMask2.localSubtract(1).localPow(2)
      val eroded = FFTConvolve(maskInvert, erosionKernel.get)
      eroded.localIf({ d: Double => d > 0.5 }, 0.0, 1.0)
    } else {
      binaryMask2
    }
  }
}

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
