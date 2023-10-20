package org.openeo.geotrellis.layers

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{CellSize, FloatConstantNoDataCellType, GridBounds, GridExtent, RasterSource}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class NoDataRasterSourceTest {

  private val noDataRasterSource = NoDataRasterSource.instance

  @Test
  def testGlobal(): Unit = {
    assertEquals(LatLng, noDataRasterSource.crs)
    val Some(noDataRaster) = noDataRasterSource.read()
    assertEquals(1, noDataRaster.tile.bandCount)
    assertTrue(noDataRaster.tile.band(0).isNoDataTile)
  }

  @Test
  def readExtent(): Unit = {
    val Some(rasterCroppedByExtent) = noDataRasterSource.read(Extent(0.0, 50.0, 5.0, 55.0))
    assertEquals(Extent(0.0, 50.0, 5.0, 55.0), rasterCroppedByExtent.extent)
  }

  @Test
  def readGridBounds(): Unit = {
    val Some(rasterCroppedByGridBounds) = noDataRasterSource.read(GridBounds[Long](0, 0, 0, 0))
    assertEquals(1, rasterCroppedByGridBounds.cols)
    assertEquals(1, rasterCroppedByGridBounds.rows)
  }

  @Test
  def convert(): Unit = {
    val converted = noDataRasterSource.convert(FloatConstantNoDataCellType)
    assertEquals(LatLng, converted.crs)
    val Some(convertedRaster) = converted.read()
    assertEquals(FloatConstantNoDataCellType, convertedRaster.cellType)
    assertTrue(convertedRaster.tile.band(0).isNoDataTile)
  }

  @Test
  def reproject(): Unit = {
    val reprojected = noDataRasterSource.reproject(targetCRS = WebMercator)
    assertEquals(WebMercator, reprojected.crs)
    val Some(reprojectedRaster) = reprojected.read()
  }

  @Test
  def resample(): Unit = {
    val resampled = noDataRasterSource.resample(targetCols = 100, targetRows = 100)
    assertEquals(LatLng, resampled.crs)
    val Some(resampledRaster) = resampled.read()
    assertEquals(100, resampledRaster.cols)
    assertEquals(100, resampledRaster.rows)
  }

  @Test
  def testReadResampledByGridBounds(): Unit = {
    val resampled = noDataRasterSource.resample(targetCols = 100, targetRows = 100)
    val Some(resampledRasterCroppedByGridBounds) = resampled.read(GridBounds[Long](50, 50, 52, 51))
    assertEquals(3, resampledRasterCroppedByGridBounds.cols)
    assertEquals(2, resampledRasterCroppedByGridBounds.rows)
  }

  @Test
  def testExistingGridExtent(): Unit = {
    val crs = LatLng
    val gridExtent = GridExtent[Long](Extent(0.0, 50.0, 5.0, 55.0), cols = 100, rows = 100)
    val noDataRasterSource: RasterSource = NoDataRasterSource.instance(gridExtent, crs)

    assertEquals(Extent(0.0, 50.0, 5.0, 55.0), noDataRasterSource.extent)
    assertEquals(100, noDataRasterSource.cols)
    assertEquals(100, noDataRasterSource.rows)
    assertEquals(List(CellSize(0.05, 0.05)), noDataRasterSource.resolutions)

    val Some(raster) = noDataRasterSource.read(Extent(1.0, 51.0, 4.0, 54.0))
    assertEquals(1, raster.tile.bandCount)
    assertTrue(raster.tile.band(0).isNoDataTile)

    assertEquals(60, raster.cols)
    assertEquals(60, raster.rows)
  }

  @Test
  def testReprojectExistingGridExtent(): Unit = {
    val crs = LatLng
    val gridExtent = GridExtent[Long](Extent(0.0, 50.0, 5.0, 55.0), cols = 100, rows = 100)
    val noDataRasterSource: RasterSource = NoDataRasterSource.instance(gridExtent, crs)

    val reprojected = noDataRasterSource.reproject(WebMercator)
    val Some(reprojectedRaster) = reprojected.read()
    assertTrue(gridExtent.extent.equalsExact(reprojectedRaster.extent.reproject(reprojected.crs, crs), 0.1))
  }
}
