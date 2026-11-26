package org.openeo.sar.backend

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class Gamma0RtcFactorTest {

  private def db(x: Double) = 10 * math.log10(x)

  @Test
  def flatTerrainEqualsSigma0OverCosTheta(): Unit = {
    for (deg <- Seq(30.0, 35.0, 40.0, 45.0)) {
      val t = math.toRadians(deg)
      val f = TerrainCorrectionBackend.gamma0RtcFactor(t, t)
      assertEquals(1.0 / math.cos(t), f, 1e-12)
      assertEquals(-db(math.cos(t)), db(f), 1e-9)
    }
  }

  @Test
  def foreslopeIsDarkenedBackslopeIsBrightened(): Unit = {
    val el = math.toRadians(38.0)
    val flat = TerrainCorrectionBackend.gamma0RtcFactor(el, el)
    assertTrue(TerrainCorrectionBackend.gamma0RtcFactor(el, math.toRadians(20.0)) < flat)
    assertTrue(TerrainCorrectionBackend.gamma0RtcFactor(el, math.toRadians(60.0)) > flat)
  }

  @Test
  def degenerateGeometryIsNaN(): Unit = {
    assertTrue(TerrainCorrectionBackend.gamma0RtcFactor(math.toRadians(38.0), math.Pi / 2).isNaN)
    assertTrue(TerrainCorrectionBackend.gamma0RtcFactor(0.0, math.toRadians(38.0)).isNaN)
  }
}
