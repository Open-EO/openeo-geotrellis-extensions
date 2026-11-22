package org.openeo.sar

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.util

class RtcAlgorithmOptionsTest {

  private def configFor(options: (String, Object)*): SarProcessingConfig = {
    val params = new SarBackscatterParameters
    val map = new util.HashMap[String, Object]()
    options.foreach { case (k, v) => map.put(k, v) }
    params.options = map
    params.toSarProcessingConfig()
  }

  @Test
  def defaultsToAuto(): Unit = {
    val config = configFor()
    assertEquals(RtcAlgorithm.Auto, config.rtcAlgorithm)
    assertEquals(config.bandCount(1), SarProcessingConfig().bandCount(1))
  }

  @Test
  def parsesOptions(): Unit = {
    val config = configFor(
      "rtc_algorithm" -> "Bilinear-Distribution", "rtc_dem_upsampling" -> Integer.valueOf(3),
      "rtc_min_value_db" -> java.lang.Double.valueOf(-30.0), "rtc_method_band" -> java.lang.Boolean.TRUE)
    assertEquals(RtcAlgorithm.BilinearDistribution, config.rtcAlgorithm)
    assertEquals(3, config.rtcDemUpsampling)
    assertEquals(Some(-30.0), config.rtcMinValueDb)
    assertEquals(SarProcessingConfig().bandCount(1) + 1, config.bandCount(1))
  }

  @Test
  def rejectsUnknownAlgorithm(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => configFor("rtc_algorithm" -> "foo"))
}
