package org.openeo.geotrellis.layers

import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{ConvertTargetCellType, DefaultTarget, FloatConstantNoDataCellType, RasterSource, ShortConstantNoDataCellType, UShortConstantNoDataCellType, resample}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.layers.raster_source.ValueOffsetRasterSource

class ValueOffsetRasterSourceTest {
  def getCornerPixelValue(rs: RasterSource): Int = rs.read().get._1.toArrayTile().band(0).get(5, 5)
  def getCornerPixelValueDouble(rs: RasterSource): Double = rs.read().get._1.toArrayTile().band(0).getDouble(5, 5)

  @Test
  def testOffset(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val tiffRs = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(tiffRs)
    val rs = new ValueOffsetRasterSource(tiffRs, 1, -1000)
    val newValue = getCornerPixelValue(rs)
    assertEquals(originalValue - 1000, newValue)
  }

  @Test
  def testOffsetConvert(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val tiffRs = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(tiffRs)
    val rs = new ValueOffsetRasterSource(tiffRs, 1, -1000)
    val newValue = getCornerPixelValue(rs.convert(ConvertTargetCellType(FloatConstantNoDataCellType)))
    assertEquals(originalValue - 1000, newValue)
  }

  @Test
  def testOffsetResample(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val tiffRs = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(tiffRs)
    val rs = new ValueOffsetRasterSource(tiffRs, 1, -1000)

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
    val rs = new ValueOffsetRasterSource(tiffRs, 1, -1000)

    val newValue = getCornerPixelValue(rs
      .resample(
        DefaultTarget,
        resample.NearestNeighbor,
        OverviewStrategy.DEFAULT,
      )
      .convert(ConvertTargetCellType(FloatConstantNoDataCellType))
    )
    assertEquals(originalValue - 1000, newValue)
  }

  @Test
  def testScaleOffsetConvertAndResample(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val tiffRs = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(tiffRs)
    val rs = new ValueOffsetRasterSource(tiffRs, 2, -1000)

    val newValue = getCornerPixelValue(rs
      .resample(
        DefaultTarget,
        resample.NearestNeighbor,
        OverviewStrategy.DEFAULT,
      )
      .convert(ConvertTargetCellType(FloatConstantNoDataCellType))
    )
    assertEquals(originalValue * 2 - 1000, newValue)
  }

  @Test
  def testScaleTypeConversion(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val originalRasterSource = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(originalRasterSource)
    val offsetRasterSource = new ValueOffsetRasterSource(originalRasterSource, 0.2, 0)
    val offsetValue = getCornerPixelValueDouble(offsetRasterSource)

    assertEquals(FloatConstantNoDataCellType, offsetRasterSource.cellType)
    assertEquals((originalValue * 0.2), offsetValue, 0.0001)
  }

  @Test
  def testBigOffsetTypeConversion(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val originalRasterSource = GeoTiffRasterSource(file.toString)

    val originalValue = getCornerPixelValue(originalRasterSource)
    val offsetRasterSource = new ValueOffsetRasterSource(originalRasterSource, 1, 1E13)
    val offsetValue = getCornerPixelValueDouble(offsetRasterSource)

    assertEquals(FloatConstantNoDataCellType, offsetRasterSource.cellType)
    assertEquals((originalValue + 1E13), offsetValue, 1000000)
  }

  @Test
  def testNegativeOffsetTypeConversion(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val unsignedRasterSource = GeoTiffRasterSource(file.toString).convert(UShortConstantNoDataCellType)

    val originalValue = getCornerPixelValue(unsignedRasterSource)
    val offsetRasterSource = new ValueOffsetRasterSource(unsignedRasterSource, 1, -10)
    val offsetValue = getCornerPixelValueDouble(offsetRasterSource)

    assertEquals(ShortConstantNoDataCellType, offsetRasterSource.cellType)
    assertEquals((originalValue - 10), offsetValue)
  }

  @Test
  def testScaleTypeFloatNoConversion(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val originalRasterSource = GeoTiffRasterSource(file.toString).convert(FloatConstantNoDataCellType)

    val originalValue = getCornerPixelValue(originalRasterSource)
    val offsetRasterSource = new ValueOffsetRasterSource(originalRasterSource, 2, -2000)
    val offsetValue = getCornerPixelValueDouble(offsetRasterSource)

    assertEquals(FloatConstantNoDataCellType, offsetRasterSource.cellType)
    assertEquals(originalValue * 2 - 2000, offsetValue)
  }

  @Test
  def testBigOffsetTypeFloatNoConversion(): Unit = {
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.tiff")
    val originalRasterSource = GeoTiffRasterSource(file.toString).convert(FloatConstantNoDataCellType)

    val originalValue = getCornerPixelValue(originalRasterSource)
    val offsetRasterSource = new ValueOffsetRasterSource(originalRasterSource, 1, 1E12)
    val offsetValue = getCornerPixelValueDouble(offsetRasterSource)

    assertEquals(FloatConstantNoDataCellType, offsetRasterSource.cellType)
    assertEquals(originalValue + 1E12, offsetValue, 10000)
  }
}
