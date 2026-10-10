package org.openeo.geotrellis.layers

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffReprojectRasterSource}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.{ConvertTargetCellType, IntConstantNoDataCellType, MultibandTile, NoDataHandling, RasterExtent, TargetRegion, UShortArrayTile, UShortCellType, UShortCells, UShortConstantNoDataCellType, isData, isNoData}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.openeo.geotrellis.layers.raster_source.{NoDataPreservingGeoTiffReprojectRasterSource, NoDataPreservingGeoTiffResampleRasterSource}

import java.nio.file.Path

class NoDataPreservingGeoTiffRasterSourcesTest {
  private val crs = CRS.fromEpsgCode(32631)
  private val extent = Extent(700000, 5600000, 701000, 5601000)
  private val (cols, rows) = (100, 100)

  private def writeGeoTiff(dir: Path, values: Array[Short], cellType: UShortCells with NoDataHandling = UShortCellType): GeoTiffPath = {
    val path = dir.resolve(s"raw_${cellType.name}.tif").toString
    val tile = UShortArrayTile(values, cols, rows, cellType)
    MultibandGeoTiff(MultibandTile(tile), extent, crs).write(path)
    GeoTiffPath(path)
  }

  private def allZeros(dir: Path): GeoTiffPath = writeGeoTiff(dir, Array.fill[Short](cols * rows)(0))

  @Test
  def resampleWidensRawCellTypeAndPreservesValues(@TempDir dir: Path): Unit = {
    val values = Array.tabulate[Short](cols * rows)(i => if (i % 2 == 0) 0 else -1) // 0 and 65535
    val rasterSource = new NoDataPreservingGeoTiffResampleRasterSource(writeGeoTiff(dir, values), TargetRegion(RasterExtent(extent, cols, rows)))

    assertEquals(IntConstantNoDataCellType, rasterSource.cellType)

    val Some(raster) = rasterSource.read()
    assertEquals(IntConstantNoDataCellType, raster.cellType)
    assertEquals(values.map(_ & 0xFFFF).toSeq, raster.tile.band(0).toArray().toSeq)
  }

  @Test
  def resampleFillOutsideOfGeoTiffIsNoData(@TempDir dir: Path): Unit = {
    // target grid is shifted by half a pixel so border cells sample outside of the GeoTiff
    val shiftedExtent = Extent(extent.xmin - 5, extent.ymin - 5, extent.xmax + 5, extent.ymax + 5)
    val rasterSource = new NoDataPreservingGeoTiffResampleRasterSource(allZeros(dir), TargetRegion(RasterExtent(shiftedExtent, cols + 1, rows + 1)))

    val Some(raster) = rasterSource.read()
    val band = raster.tile.band(0)

    assertTrue(band.toArray().forall(v => isNoData(v) || v == 0))
    assertEquals(0, band.get(cols / 2, rows / 2))
  }

  @Test
  def reprojectFillOutsideOfFootprintIsNoData(@TempDir dir: Path): Unit = {
    val geoTiffPath = allZeros(dir)

    val rasterSource = new NoDataPreservingGeoTiffReprojectRasterSource(geoTiffPath, LatLng)
    assertEquals(IntConstantNoDataCellType, rasterSource.cellType)

    val Some(raster) = rasterSource.read()
    val values = raster.tile.band(0).toArray()

    assertEquals(IntConstantNoDataCellType, raster.cellType)
    assertTrue(values.forall(v => isNoData(v) || v == 0), "valid 0s should be retained")
    assertTrue(values.exists(v => isData(v)), "expected data within the footprint")
    assertTrue(values.exists(v => isNoData(v)), "expected NODATA outside of the (rotated) footprint")

    // a GeoTrellis GeoTiffReprojectRasterSource fills these cells with 0 instead
    val Some(rawRaster) = GeoTiffReprojectRasterSource(geoTiffPath, LatLng).read()
    assertTrue(rawRaster.tile.band(0).toArray().forall(_ == 0))
  }

  @Test
  def reprojectWithRawTargetCellTypeIsWidened(@TempDir dir: Path): Unit = {
    val rasterSource = new NoDataPreservingGeoTiffReprojectRasterSource(allZeros(dir), LatLng)
      .convert(ConvertTargetCellType(UShortCellType))

    assertEquals(IntConstantNoDataCellType, rasterSource.cellType)

    val Some(raster) = rasterSource.read()
    assertEquals(IntConstantNoDataCellType, raster.cellType)
    assertTrue(raster.tile.band(0).toArray().exists(v => isNoData(v)))
  }

  @Test
  def cellTypeWithNoDataIsLeftAlone(@TempDir dir: Path): Unit = {
    val values = Array.tabulate[Short](cols * rows)(i => (i % 3).toShort)
    val geoTiffPath = writeGeoTiff(dir, values, UShortConstantNoDataCellType)

    val resampleRasterSource = new NoDataPreservingGeoTiffResampleRasterSource(geoTiffPath, TargetRegion(RasterExtent(extent, cols, rows)))
    assertEquals(UShortConstantNoDataCellType, resampleRasterSource.cellType)
    val Some(resampled) = resampleRasterSource.read()
    assertEquals(UShortConstantNoDataCellType, resampled.cellType)

    val reprojectRasterSource = new NoDataPreservingGeoTiffReprojectRasterSource(geoTiffPath, LatLng)
    assertEquals(UShortConstantNoDataCellType, reprojectRasterSource.cellType)
    val Some(reprojected) = reprojectRasterSource.read()
    assertEquals(UShortConstantNoDataCellType, reprojected.cellType)
  }
}
