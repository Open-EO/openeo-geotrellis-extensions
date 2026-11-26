package org.openeo.sar

import geotrellis.proj4.LatLng
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.openeo.sar.backend.nativ.NativeBackend
import org.openeo.sar.provider.Sentinel1GrdRasterSourceProvider

class DefaultGeoidTest {
  @Test
  def bundledGeoidCanBeRead(): Unit = {
    val extent = Extent(3.1, 51.27, 3.3, 51.37)
    val source = TerrainCorrectionProcessor.defaultGeoidSourceFactory(extent)
    assertEquals(LatLng, source.crs)
    val raster = source.read(extent).getOrElse(fail("Bundled EGM96 does not cover the test extent"))
    assertTrue(java.lang.Double.isFinite(raster.tile.band(0).getDouble(0, 0)))
  }

  @Test
  def processorAndProviderAcceptDefaultGeoid(): Unit = {
    assertNotNull(new TerrainCorrectionProcessor(new NativeBackend))
    assertNotNull(Sentinel1GrdRasterSourceProvider.withGeoid())
  }
}
