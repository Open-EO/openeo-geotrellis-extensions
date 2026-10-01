package org.openeo.geotrellis.croptype

import geotrellis.raster.{IntArrayTile, isNoData}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestMajorityVote {

  @Test
  def kernelSizeOneIsNoOp(): Unit = {
    val data = Array(0, 1, 2, 3, 4, 5, 6, 7, 8)
    val tile = IntArrayTile(data, 3, 3)
    val result = MajorityVote(tile, kernelSize = 1, excludedValues = Set.empty)
    assertArrayEquals(data, result.toArray())
  }

  @Test
  def invalidKernelSizeThrows(): Unit = {
    val tile = IntArrayTile(Array.fill(9)(0), 3, 3)
    assertThrows(classOf[IllegalArgumentException], () => MajorityVote(tile, kernelSize = 0, excludedValues = Set.empty))
    assertThrows(classOf[IllegalArgumentException], () => MajorityVote(tile, kernelSize = 26, excludedValues = Set.empty))
  }

  @Test
  def minorityPixelFlipsToSurroundingMajority(): Unit = {
    // 5x5 tile, all label 1 except a single minority pixel (label 2) in the center.
    val size = 5
    val data = Array.fill(size * size)(1)
    val centerIdx = 2 * size + 2
    data(centerIdx) = 2

    val tile = IntArrayTile(data, size, size)
    val result = MajorityVote(tile, kernelSize = 3, excludedValues = Set.empty)

    // The minority pixel should flip to the surrounding majority label.
    assertEquals(1, result.get(2, 2))
    // Pixels far from the minority pixel are unaffected.
    assertEquals(1, result.get(0, 0))
  }

  @Test
  def excludedValuesAreNeverOverwrittenOrCounted(): Unit = {
    // A 3x3 tile where the center is an excluded "no data" sentinel value, surrounded by
    // a majority label. The excluded pixel must remain untouched.
    val data = Array(
      1, 1, 1,
      1, 999, 1,
      1, 1, 1
    )
    val tile = IntArrayTile(data, 3, 3)
    val result = MajorityVote(tile, kernelSize = 3, excludedValues = Set(999))

    assertEquals(999, result.get(1, 1))
    assertEquals(1, result.get(0, 0))
  }

  @Test
  def excludedValuesDoNotContributeVotes(): Unit = {
    // Neighbourhood dominated by an excluded value (254) plus a couple of real votes for
    // label 3; the excluded value should not be able to "win" a non-excluded pixel.
    val data = Array(
      254, 254, 254,
      254, 5, 3,
      3, 3, 254
    )
    val tile = IntArrayTile(data, 3, 3)
    val result = MajorityVote(tile, kernelSize = 3, excludedValues = Set(254))

    // Pixel (1,1) originally 5 (not excluded) should flip to the majority non-excluded
    // label in its neighbourhood, which is 3 (four occurrences vs one for 5).
    assertEquals(3, result.get(1, 1))
    // Excluded pixels remain unchanged.
    assertEquals(254, result.get(0, 0))
  }

  @Test
  def allExcludedReturnsTileUnchanged(): Unit = {
    val data = Array.fill(9)(254)
    val tile = IntArrayTile(data, 3, 3)
    val result = MajorityVote(tile, kernelSize = 3, excludedValues = Set(254))
    assertArrayEquals(data, result.toArray())
  }

  @Test
  def largeKernelUsesFftPath(): Unit = {
    // Kernel size > 10 triggers the FFTConvolve branch inside OpenEOProcesses.convolveTile;
    // verify it still produces a sane majority-vote result.
    val size = 15
    val data = Array.fill(size * size)(1)
    data(size * size / 2) = 2 // single minority pixel in the middle

    val tile = IntArrayTile(data, size, size)
    val result = MajorityVote(tile, kernelSize = 11, excludedValues = Set.empty)

    assertFalse(isNoData(result.get(size / 2, size / 2)))
    assertEquals(1, result.get(size / 2, size / 2))
    assertEquals(1, result.get(0, 0))
  }
}
