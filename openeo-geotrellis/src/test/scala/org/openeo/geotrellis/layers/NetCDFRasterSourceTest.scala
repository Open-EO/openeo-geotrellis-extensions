package org.openeo.geotrellis.layers

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{CellSize, DefaultTarget, GridBounds, RasterSource, TargetCellSize}
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

  @Test
  def resampleChangesResolution(): Unit = {
    val path = resourcePath("org/openeo/geotrellis/cgls_fapar_2009/c_gls_FAPAR_200907100000_GLOBE_VGT_V2.0.1.nc")
    val source = NetCDFRasterSource.fromSource(s"""NETCDF:"$path":FAPAR""")

    val originalCellSize = source.cellSize
    val targetCellSize = CellSize(originalCellSize.width * 2, originalCellSize.height * 2)
    val resampled = source.resample(TargetCellSize(targetCellSize), NearestNeighbor, OverviewStrategy.DEFAULT)

    assertEquals(LatLng, resampled.crs)
    assertEquals(source.extent, resampled.extent)
    assertTrue(resampled.cols < source.cols, s"Resampled cols ${resampled.cols} should be less than source cols ${source.cols}")
    assertTrue(resampled.rows < source.rows, s"Resampled rows ${resampled.rows} should be less than source rows ${source.rows}")
    assertNearlyEqual(targetCellSize.width, resampled.cellSize.width, 1e-9)
    assertNearlyEqual(targetCellSize.height, resampled.cellSize.height, 1e-9)

    val raster = resampled.read(resampled.extent, Seq(0)).get
    assertEquals(resampled.cols, raster.cols)
    assertEquals(resampled.rows, raster.rows)
  }

  @Test
  def resamplePreservesDataValues(): Unit = {
    val path = resourcePath("org/openeo/geotrellis/cgls_fapar_2009/c_gls_FAPAR_200907100000_GLOBE_VGT_V2.0.1.nc")
    val source = NetCDFRasterSource.fromSource(s"""NETCDF:"$path":FAPAR""")

    // Read a pixel from the original source
    val bounds = GridBounds[Long](100, 20, 100, 20)
    val originalValue = source.read(bounds, Seq(0)).get.tile.band(0).get(0, 0)

    // Resample without changing resolution (identity), value should be the same
    val resampled = source.resample(DefaultTarget, NearestNeighbor, OverviewStrategy.DEFAULT)
    val resampledValue = resampled.read(bounds, Seq(0)).get.tile.band(0).get(0, 0)

    assertEquals(originalValue, resampledValue)
  }

  @Test
  def reprojectChangesCoordinateSystem(): Unit = {
    val path = resourcePath("org/openeo/geotrellis/cgls_fapar_2009/c_gls_FAPAR_200907100000_GLOBE_VGT_V2.0.1.nc")
    val source = NetCDFRasterSource.fromSource(s"""NETCDF:"$path":FAPAR""")
    val utm32n = CRS.fromEpsgCode(32632)

    val reprojected = source.reproject(utm32n)

    assertEquals(utm32n, reprojected.crs)
    assertEquals(1, reprojected.bandCount)
    // Extent should now be in metres (UTM), not degrees — y values north of equator are > 5,000,000
    assertTrue(reprojected.extent.ymin > 5000000, s"Expected UTM ymin in metres north of equator, got ${reprojected.extent.ymin}")
    assertTrue(reprojected.extent.ymax < 6000000, s"Expected UTM ymax < 6,000,000m, got ${reprojected.extent.ymax}")

    val raster = reprojected.read(reprojected.extent, Seq(0)).get
    assertEquals(reprojected.cols, raster.cols)
    assertEquals(reprojected.rows, raster.rows)
    assertTrue(raster.tile.band(0).toArrayDouble().exists(!_.isNaN), "Reprojected tile should contain non-NaN values")
  }

  @Test
  def reprojectAndResampleReturnCorrectCrs(): Unit = {
    val path = resourcePath("org/openeo/geotrellis/netcdfCollection/openEO_0.nc")
    val source = NetCDFRasterSource.fromSource(s"""NETCDF:"$path":B01""")

    val sourceCrs = source.crs
    assertTrue(sourceCrs != LatLng, s"Expected a projected (non-geographic) CRS, got $sourceCrs")

    // Reproject to LatLng
    val reprojected = source.reproject(LatLng)
    assertEquals(LatLng, reprojected.crs)
    assertTrue(reprojected.extent.xmin >= -180 && reprojected.extent.xmax <= 180, "Reprojected extent should be in degrees")
    assertTrue(reprojected.extent.ymin >= -90 && reprojected.extent.ymax <= 90, "Reprojected extent should be in degrees")

    // Resample the reprojected result to a coarser resolution
    val coarser = reprojected.resample(TargetCellSize(CellSize(reprojected.cellSize.width * 2, reprojected.cellSize.height * 2)), NearestNeighbor, OverviewStrategy.DEFAULT)
    assertEquals(LatLng, coarser.crs)
    assertTrue(coarser.cols < reprojected.cols)
    assertTrue(coarser.rows < reprojected.rows)
  }
}
