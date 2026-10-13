package org.openeo.sar.provider

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.openeo.sar.metadata.Polarisation.{VH, VV}
import org.openeo.sar.provider.Sentinel1GrdRasterSourceProvider.polarisationBandIndex

import java.net.URI

class Sentinel1GrdRasterSourceProviderTest {
  private val safeRoot = URI.create("s3://eodata/S1B_IW_GRDH_1SDV_TEST.SAFE")
  private def measurement(pol: String) = s"$safeRoot/measurement/s1b-iw-grd-$pol-20191010t060611-001-cog.tiff"

  @Test
  def resolvesPolarisationBandIndex(): Unit = {
    assertEquals(0, polarisationBandIndex(measurement("vh"), Seq(VH, VV), safeRoot))
    assertEquals(1, polarisationBandIndex(measurement("vv"), Seq(VH, VV), safeRoot))
    assertEquals(1, polarisationBandIndex(measurement("VV").toUpperCase, Seq(VH, VV), safeRoot))
  }

  @Test
  def failsOnUnrecognisedPath(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => polarisationBandIndex(s"$safeRoot/measurement/unknown.tiff", Seq(VH, VV), safeRoot))

  @Test
  def failsOnPolarisationMissingFromScene(): Unit =
    assertThrows(classOf[IllegalStateException], () => polarisationBandIndex(measurement("vh"), Seq(VV), safeRoot))
}
