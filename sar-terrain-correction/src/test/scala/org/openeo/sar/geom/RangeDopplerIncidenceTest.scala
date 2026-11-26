package org.openeo.sar.geom

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class RangeDopplerIncidenceTest {
  // Target on the equator at lon 0, sensor 700 km up and 400 km to the north: look in the (north, up) plane.
  private val ground = Vec3(6378137.0, 0.0, 0.0)
  private val up = Vec3(1.0, 0.0, 0.0)
  private val north = Vec3(0.0, 0.0, 1.0)
  private val sat = ground + up * 700000.0 + north * 400000.0
  private val thetaEl = math.atan2(400000.0, 700000.0)

  // normal of a slope that is tilted by `alpha` towards the sensor (north)
  private def tilted(alpha: Double) = up * math.cos(alpha) + north * math.sin(alpha)

  @Test
  def flatTerrainHasEllipsoidIncidence(): Unit = {
    assertEquals(thetaEl, RangeDoppler.localIncidence(ground, sat, up), 1e-9)
    assertEquals(thetaEl, RangeDoppler.localIncidenceInRangePlane(ground, sat, up, up), 1e-9)
  }

  @Test
  def slopeFacingSensorReducesIncidence(): Unit = {
    val alpha = math.toRadians(10)
    assertEquals(thetaEl - alpha, RangeDoppler.localIncidenceInRangePlane(ground, sat, tilted(alpha), up), 1e-9)
    assertEquals(thetaEl - alpha, RangeDoppler.localIncidence(ground, sat, tilted(alpha)), 1e-9)
  }

  @Test
  def steepSlopeFacingSensorIsLayover(): Unit =
    assertTrue(RangeDoppler.localIncidenceInRangePlane(ground, sat, tilted(thetaEl + math.toRadians(5)), up) < 0)

  @Test
  def slopeFacingAwayBeyondGrazingIsShadow(): Unit = {
    val n = tilted(-(math.Pi / 2 - thetaEl + math.toRadians(5)))
    assertTrue(RangeDoppler.localIncidence(ground, sat, n) > math.Pi / 2)
    assertTrue(RangeDoppler.localIncidenceInRangePlane(ground, sat, n, up) > math.Pi / 2)
  }
}
