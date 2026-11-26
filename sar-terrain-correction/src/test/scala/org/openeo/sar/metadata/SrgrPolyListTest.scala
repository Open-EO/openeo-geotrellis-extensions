package org.openeo.sar.metadata

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SrgrPolyListTest {
  private val list = new SrgrPolyList(IndexedSeq(
    SrgrPoly(0.0, 800000.0, 0.0, Array(800000.0, 0.50)),
    SrgrPoly(1.0, 800000.0, 0.0, Array(800000.0, 0.60)),
    SrgrPoly(2.0, 800000.0, 0.0, Array(800000.0, 0.70))
  ))

  @Test
  def groundRangeIsContinuousAcrossRecordMidpoints(): Unit = {
    val r = 800000.0 + 60000.0
    // slant offset = slope(t) * gr, slope interpolated linearly in time
    def expected(t: Double): Double = 60000.0 / (0.5 + 0.1 * t)
    Seq(0.0, 0.49, 0.5, 0.51, 1.0, 1.5, 2.0).foreach { t =>
      assertEquals(expected(t), list.groundRangeFromSlant(t, r), 1e-2, s"t=$t")
    }
  }

  @Test
  def clampsOutsideRecords(): Unit = {
    val r = 800000.0 + 60000.0
    assertEquals(60000.0 / 0.5, list.groundRangeFromSlant(-5.0, r), 1e-2)
    assertEquals(60000.0 / 0.7, list.groundRangeFromSlant(9.0, r), 1e-2)
  }

  @Test
  def derivativeIsInterpolated(): Unit =
    assertEquals(0.55, list.dSlantRange_dGround(0.5, 1000.0), 1e-9)
}
