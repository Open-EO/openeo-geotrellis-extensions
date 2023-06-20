package org.openeo.geotrellis.layers

import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{ConvertTargetCellType, DefaultTarget, DoubleConstantNoDataCellType, RasterSource, resample}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ValueOffsetRasterSourceTest {
  def getCornerPixelValue(rs: RasterSource): Int = rs.read().get._1.toArrayTile().band(0).get(5, 5)

  @Test
  def testOffset(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val tiffRs = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(tiffRs)
    val rs = new ValueOffsetRasterSource(tiffRs, -1000)
    val newValue = getCornerPixelValue(rs)
    assertEquals(originalValue - 1000, newValue)
  }

  @Test
  def testOffsetConvert(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val tiffRs = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(tiffRs)
    val rs = new ValueOffsetRasterSource(tiffRs, -1000)
    val newValue = getCornerPixelValue(rs.convert(ConvertTargetCellType(DoubleConstantNoDataCellType)))
    assertEquals(originalValue - 1000, newValue)
  }

  @Test
  def testOffsetResample(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val tiffRs = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(tiffRs)
    val rs = new ValueOffsetRasterSource(tiffRs, -1000)

    val newValue = getCornerPixelValue(rs.resample(
      DefaultTarget,
      resample.NearestNeighbor,
      OverviewStrategy.DEFAULT,
    ))
    assertEquals(originalValue - 1000, newValue)
  }

  @Test
  def testOffsetConvertAndResample(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val tiffRs = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(tiffRs)
    val rs = new ValueOffsetRasterSource(tiffRs, -1000)

    val newValue = getCornerPixelValue(rs
      .resample(
        DefaultTarget,
        resample.NearestNeighbor,
        OverviewStrategy.DEFAULT,
      )
      .convert(ConvertTargetCellType(DoubleConstantNoDataCellType))
    )
    assertEquals(originalValue - 1000, newValue)
  }
}
