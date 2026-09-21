package org.openeo.sar.metadata

import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class Lut2DTest {

  private val tolerance = 1e-9

  /** values(i)(j) = 10 * lines(i) + pixels(j), so bilinear interpolation is exact. */
  private def linearLut: Lut2D = new Lut2D(
    lines = Array(0, 10),
    pixels = Array(0, 100, 200),
    values = Array(
      Array(0.0f, 100.0f, 200.0f),
      Array(100.0f, 200.0f, 300.0f)
    )
  )

  @Test
  def applyReturnsExactValuesAtSamplePoints(): Unit = {
    val lut = linearLut
    assertEquals(0.0, lut(0, 0), tolerance)
    assertEquals(200.0, lut(0, 200), tolerance)
    assertEquals(100.0, lut(10, 0), tolerance)
    assertEquals(300.0, lut(10, 200), tolerance)
  }

  @Test
  def applyInterpolatesBilinearly(): Unit = {
    val lut = linearLut
    assertEquals(50.0, lut(0, 50), tolerance)
    assertEquals(50.0, lut(5, 0), tolerance)
    assertEquals(200.0, lut(5, 150), tolerance)
    assertEquals(175.0, lut(2.5, 150), tolerance)
  }

  @Test
  def applyClampsOutsideTheGrid(): Unit = {
    val lut = linearLut
    assertEquals(0.0, lut(-100, -100), tolerance)
    assertEquals(300.0, lut(1000, 1000), tolerance)
    assertEquals(lut(0, 100), lut(-5, 100), tolerance)
    assertEquals(lut(10, 100), lut(50, 100), tolerance)
  }

  @Test
  def applySupportsSingleRowAndColumnGrids(): Unit = {
    val lut = new Lut2D(Array(5), Array(7), Array(Array(42.0f)))
    assertEquals(42.0, lut(5, 7), tolerance)
    assertEquals(42.0, lut(-1, 1000), tolerance)
  }

  @Test
  def constructorRejectsMismatchedDimensions(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => new Lut2D(Array(0, 1), Array(0, 1), Array(Array(0.0f, 1.0f))))
    assertThrows(classOf[IllegalArgumentException],
      () => new Lut2D(Array(0), Array(0, 1), Array(Array(0.0f))))
  }
}
