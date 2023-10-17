package org.openeo.geotrellis.layers

import geotrellis.proj4.WebMercator
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.{FloatConstantNoDataCellType, GridBounds}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class NoDataRasterSourceTest {

  @Test
  def test(): Unit = {
    val noDataRasterSource = NoDataRasterSource.get
    val Some(noDataRaster) = noDataRasterSource.read()
    MultibandGeoTiff(noDataRaster, noDataRasterSource.crs).write("/tmp/noDataRaster.tif")
    assertEquals(1, noDataRaster.tile.bandCount)
    assertTrue(noDataRaster.tile.band(0).isNoDataTile)

    val converted = noDataRasterSource.convert(FloatConstantNoDataCellType)
    val Some(convertedRaster) = converted.read()
    MultibandGeoTiff(convertedRaster, converted.crs).write("/tmp/convertedRaster.tif")
    assertEquals(FloatConstantNoDataCellType, convertedRaster.cellType)
    assertTrue(convertedRaster.tile.band(0).isNoDataTile)

    val reprojected = noDataRasterSource.reproject(targetCRS = WebMercator)
    val Some(reprojectedRaster) = reprojected.read()
    MultibandGeoTiff(reprojectedRaster, reprojected.crs).write("/tmp/reprojectedRaster.tif")
    assertEquals(WebMercator, reprojected.crs)

    val resampled = noDataRasterSource.resample(targetCols = 100, targetRows = 100)
    val Some(resampledRaster) = resampled.read()
    MultibandGeoTiff(resampledRaster, resampled.crs).write("/tmp/resampledRaster.tif")
    assertEquals(100, resampledRaster.cols)
    assertEquals(100, resampledRaster.rows)

    val Some(rasterCroppedByExtent) = noDataRasterSource.read(Extent(0.0, 50.0, 5.0, 55.0))
    MultibandGeoTiff(rasterCroppedByExtent, noDataRasterSource.crs).write("/tmp/rasterCroppedByExtent.tif")

    val Some(rasterCroppedByGridBounds) = noDataRasterSource.read(GridBounds[Long](5, 5, 5, 5))
    MultibandGeoTiff(rasterCroppedByGridBounds, noDataRasterSource.crs).write("/tmp/rasterCroppedByGridBounds.tif")
    assertEquals(1, rasterCroppedByGridBounds.cols)
    assertEquals(1, rasterCroppedByGridBounds.rows)
  }
}
