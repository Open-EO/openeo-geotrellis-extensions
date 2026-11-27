package org.openeo.geotrellis

import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.geotiff.GTiffOptions
import org.openeo.geotrellis.netcdf.NetCDFOptions
import org.openeo.geotrellis.png.PngOptions
import org.openeo.geotrellis.zarr.ZarrOptions

class FormatOptionsTest {

  @Test
  def hasDefaults(): Unit = {
    for (options <- Seq[FormatOptions](new GTiffOptions, new NetCDFOptions, new PngOptions, new ZarrOptions)) {
      assertEquals(None, options.temporalExtent)
      assertNull(options.startDatetime)
      assertNull(options.endDatetime)
      assertEquals(None, options.cropBounds)
      assertTrue(options.addBandStatistics)
      assertFalse(options.retainNoDataTiles)
    }
  }

  @Test
  def setTemporalExtentWithBothEndsNullClearsIt(): Unit = {
    val options = new PngOptions
    options.setTemporalExtent("2020-01-01T00:00:00Z", null)
    assertEquals("2020-01-01T00:00:00Z", options.startDatetime)
    assertNull(options.endDatetime)

    options.setTemporalExtent(null, null)
    assertEquals(None, options.temporalExtent)
  }

  @Test
  def deepCloneKeepsInheritedOptions(): Unit = {
    val options = new GTiffOptions
    options.setTemporalExtent("2020-01-01T00:00:00Z", "2020-01-10T00:00:00Z")
    options.setCropBounds(Extent(0, 0, 1, 1))
    options.setAddBandStatistics(true)
    options.setRetainNoDataTiles(true)

    val clone = options.deepClone()
    assertEquals(Some(("2020-01-01T00:00:00Z", "2020-01-10T00:00:00Z")), clone.temporalExtent)
    assertEquals(Some(Extent(0, 0, 1, 1)), clone.cropBounds)
    assertTrue(clone.addBandStatistics)
    assertTrue(clone.retainNoDataTiles)
  }
}

