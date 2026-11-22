package org.openeo.sar.backend.nativ.rtc

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.openeo.sar.geom.{Ecef, RangeDoppler, Vec3}
import org.openeo.sar.orbit.{OrbitInterpolator, StateVector}

class AreaRtcTest {

  // Circular equatorial orbit; the target is at lon 0, so zero-Doppler time is 0.
  private val radius = 7.0e6
  private val omega = 2 * math.Pi / 5900.0
  private val orbit = new OrbitInterpolator(IndexedSeq.tabulate(11) { i =>
    val t = (i - 5) * 10.0 + 3.0 // no node at the zero-Doppler time
    StateVector(t,
      Vec3(radius * math.cos(omega * t), radius * math.sin(omega * t), 0.0),
      Vec3(-radius * omega * math.sin(omega * t), radius * omega * math.cos(omega * t), 0.0))
  })

  private val lat0 = math.toRadians(-3.5)
  private val spacingDeg = 3.3e-5
  private val n = 400

  /** 1 / ANF at the scene centre for terrain that rises with `slope` (m/m) towards the south (away from the sensor, so the slope faces it), and the
   *  ellipsoid incidence angle there. */
  private def inverseAnfAtCentre(slope: Double): (Double, Double) = {
    val lonLat = (row: Int, col: Int) =>
      (math.toRadians((col - n / 2) * spacingDeg), lat0 - math.toRadians((row - n / 2) * spacingDeg))
    val heights = Array.tabulate(n, n) { (row, _) =>
      // metres south of the centre
      val south = math.toRadians((row - n / 2) * spacingDeg) * 6.36e6
      (slope * south).toFloat
    }

    val pCentre = Ecef.fromGeodetic(0.0, lat0, 0.0)
    val tCentre = RangeDoppler.zeroDopplerTime(pCentre, orbit, 1.0)
    val pSat = orbit.positionAt(tCentre)
    val rCentre = (pSat - pCentre).norm
    val dt = 0.002
    val dr = 5.0
    val grid = AreaRtc.RadarGrid(azLine0 = tCentre / dt - 100, nAz = 200, rMin = rCentre - 100 * dr, dr = dr,
      nRg = 200, lineTimeInterval = dt)

    val anf = AreaRtc.computeRtcBilinearDistribution(heights, lonLat, grid, orbit, tSeed = 1.0, nChunks = 3)
    val vals = for (a <- 90 to 110; r <- 90 to 110) yield anf.sample(a, r, 0.0)
    // the facets are of similar size as the radar pixels, which gives a ripple of a few % per pixel: average
    val value = vals.sum / vals.size

    val up = Ecef.ellipsoidalNormal(pCentre)
    val thetaEl = math.acos((pSat - pCentre).normalize.dot(up))
    (1.0 / value, thetaEl)
  }

  @Test
  def flatTerrainMatchesTanOfIncidenceAngle(): Unit = {
    val (inverse, thetaEl) = inverseAnfAtCentre(0.0)
    assertTrue(thetaEl > math.toRadians(20) && thetaEl < math.toRadians(60), s"incidence angle ${math.toDegrees(thetaEl)}")
    assertEquals(math.tan(thetaEl), inverse, 0.03 * math.tan(thetaEl))
  }

  @Test
  def slopeFacingSensorMatchesTanOfLocalIncidenceAngle(): Unit = {
    val alpha = math.toRadians(10)
    val (inverse, thetaEl) = inverseAnfAtCentre(math.tan(alpha))
    val expected = math.tan(thetaEl - alpha)
    assertEquals(expected, inverse, 0.04 * expected)
  }

  @Test
  def slopeFacingAwayMatchesTanOfLocalIncidenceAngle(): Unit = {
    val alpha = math.toRadians(10)
    val (inverse, thetaEl) = inverseAnfAtCentre(-math.tan(alpha))
    val expected = math.tan(thetaEl + alpha)
    assertEquals(expected, inverse, 0.04 * expected)
  }
}
