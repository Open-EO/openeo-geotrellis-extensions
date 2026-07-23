package org.openeo.geotrellis.layers

import geotrellis.raster.{GridBounds, RasterSource}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.layers.raster_source.NetCDFRasterSource

class NetCDFRasterSourceTest {

  private def resourcePath(path: String): String =
    Thread.currentThread().getContextClassLoader.getResource(path).getPath

  private def assertNearlyEqual(a: Double, b: Double, delta: Double = 1e-6): Unit =
    assertTrue(math.abs(a - b) <= delta, s"Expected $a ~= $b")

  @Test
  def readSingleBandNetcdfVariableLikeGdal(): Unit = {
    val path = resourcePath("org/openeo/geotrellis/cgls_fapar_2009/c_gls_FAPAR_200907100000_GLOBE_VGT_V2.0.1.nc")
    val source = s"""NETCDF:"$path":FAPAR"""

    val ucar: RasterSource = NetCDFRasterSource.fromSource(source)

    assertEquals(1, ucar.bandCount)
    assertEquals(493, ucar.cols)
    assertEquals(266, ucar.rows)
    assertNearlyEqual(1.8973214288275528, ucar.extent.xmin, 1e-6)
    assertNearlyEqual(49.352678585684544, ucar.extent.ymin, 1e-6)
    assertNearlyEqual(6.299107143119464, ucar.extent.xmax, 1e-6)
    assertNearlyEqual(51.7276785856839, ucar.extent.ymax, 1e-6)

    val bounds = GridBounds[Long](100, 20, 140, 50)
    val ucarRaster = ucar.read(bounds, Seq(0)).get

    assertEquals(41, ucarRaster.cols)
    assertEquals(31, ucarRaster.rows)
    assertEquals(1, ucarRaster.tile.bandCount)
    assertEquals(38, ucarRaster.tile.band(0).get(35, 26))
  }

  @Test
  def readTemporalBandsFrom3dVariable(): Unit = {
    val path = resourcePath("org/openeo/geotrellis/netcdfCollection/openEO_0.nc")
    val source = s"""NETCDF:"$path":B01"""

    val ucar: RasterSource = NetCDFRasterSource.fromSource(source)

    assertEquals(7, ucar.bandCount)
    assertEquals(1370, ucar.cols)
    assertEquals(929, ucar.rows)
    assertNearlyEqual(615050.0, ucar.extent.xmin, 1e-6)
    assertNearlyEqual(5677250.0, ucar.extent.ymin, 1e-6)
    assertNearlyEqual(628750.0, ucar.extent.xmax, 1e-6)
    assertNearlyEqual(5686540.0, ucar.extent.ymax, 1e-6)

    val bounds = GridBounds[Long](250, 300, 290, 340)
    val ucarRaster = ucar.read(bounds, Seq(0, 6)).get

    assertEquals(2, ucarRaster.tile.bandCount)
    assertEquals(626, ucarRaster.tile.band(0).get(10, 10))
    assertEquals(789, ucarRaster.tile.band(1).get(15, 5))
  }
}
