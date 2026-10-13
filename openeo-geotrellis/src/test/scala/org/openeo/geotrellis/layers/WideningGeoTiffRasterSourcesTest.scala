package org.openeo.geotrellis.layers

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffReprojectRasterSource, GeoTiffResampleRasterSource}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.{ConvertTargetCellType, IntConstantNoDataCellType, MultibandTile, NoDataHandling, RasterExtent, TargetRegion, UShortArrayTile, UShortCellType, UShortCells, UShortConstantNoDataCellType, isData, isNoData}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.openeo.geotrellis.layers.raster_source.{WideningGeoTiffReprojectRasterSource, WideningGeoTiffResampleRasterSource}

import java.nio.file.Path

class WideningGeoTiffRasterSourcesTest {
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

  /**
   * A raw uint16 GeoTiff should be read as int32 with a NODATA value, with all of its original values remaining valid
   * data. The values are the extremes of the uint16 range: 0 (formerly treated as NODATA) and 65535 (stored as -1 in a
   * Short array; would come back as -1 if the conversion did not treat it as unsigned).
   *
   * The target grid is the GeoTiff's own grid, so no cells fall outside of it: this only tests the widening itself; see
   * resampleFillOutsideOfGeoTiffIsNoData for the NODATA fill.
   */
  @Test
  def resampleWidensRawCellTypeAndPreservesValues(@TempDir dir: Path): Unit = {
    val values = Array.tabulate[Short](cols * rows)(i => if (i % 2 == 0) 0 else -1) // 0 and 65535
    val rasterSource = new WideningGeoTiffResampleRasterSource(writeGeoTiff(dir, values), TargetRegion(RasterExtent(extent, cols, rows)))

    // the widened cell type is known before reading (FileLayerProvider.determineCelltype relies on this)

    assertEquals(IntConstantNoDataCellType, rasterSource.cellType)

    val Some(raster) = rasterSource.read()
    assertEquals(IntConstantNoDataCellType, raster.cellType)
    // original values, interpreted as unsigned
    assertEquals(values.map(_ & 0xFFFF).toSeq, raster.tile.band(0).toArray().toSeq)
  }

  /**
   * Target cells that lie outside of a raw GeoTiff should become NODATA rather than 0.
   *
   * The GeoTiff consists of valid 0s only and the target grid extends one cell beyond it on each side, so the centres
   * of the border cells lie outside of the GeoTiff (a centre exactly on its edge would still be considered inside).
   * The border should be NODATA and the interior should retain its valid 0s. A GeoTrellis GeoTiffResampleRasterSource
   * fills the border with 0 instead, making it indistinguishable from the interior.
   */
  @Test
  def resampleFillOutsideOfGeoTiffIsNoData(@TempDir dir: Path): Unit = {
    val geoTiffPath = allZeros(dir)
    val targetRegion = TargetRegion(RasterExtent(extent.buffer(10), cols + 2, rows + 2))

    def isBorder(col: Int, row: Int): Boolean = col == 0 || row == 0 || col == cols + 1 || row == rows + 1

    val Some(raster) = new WideningGeoTiffResampleRasterSource(geoTiffPath, targetRegion).read()
    val band = raster.tile.band(0)

    assertEquals(IntConstantNoDataCellType, raster.cellType)
    for (col <- 0 until band.cols; row <- 0 until band.rows) {
      if (isBorder(col, row)) assertTrue(isNoData(band.get(col, row)), s"expected NODATA at border ($col, $row)")
      else assertEquals(0, band.get(col, row), s"expected valid 0 in interior ($col, $row)")
    }

    val Some(rawRaster) = GeoTiffResampleRasterSource(geoTiffPath, targetRegion).read()
    assertTrue(rawRaster.tile.band(0).toArray().forall(_ == 0))
  }

  /**
   * Target cells that lie outside of the footprint of a reprojected raw GeoTiff should become NODATA rather than 0.
   *
   * Reprojecting from UTM to LatLng rotates the GeoTiff's footprint, so the corners of the target grid lie outside of
   * it. The GeoTiff consists of valid 0s only, so the result should consist of 0s (within the footprint) and NODATA
   * (outside of it). A GeoTrellis GeoTiffReprojectRasterSource fills the corners with 0 instead, making them
   * indistinguishable from the footprint.
   */
  @Test
  def reprojectFillOutsideOfFootprintIsNoData(@TempDir dir: Path): Unit = {
    val geoTiffPath = allZeros(dir)

    val rasterSource = new WideningGeoTiffReprojectRasterSource(geoTiffPath, LatLng)
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

  /**
   * Explicitly converting to a raw target cell type should not undo the widening: the source should still report and
   * read a widened cell type with NODATA, otherwise the cells outside of the footprint would become 0 again.
   */
  @Test
  def reprojectWithRawTargetCellTypeIsWidened(@TempDir dir: Path): Unit = {
    val rasterSource = new WideningGeoTiffReprojectRasterSource(allZeros(dir), LatLng)
      .convert(ConvertTargetCellType(UShortCellType))

    assertEquals(IntConstantNoDataCellType, rasterSource.cellType)

    val Some(raster) = rasterSource.read()
    assertEquals(IntConstantNoDataCellType, raster.cellType)
    assertTrue(raster.tile.band(0).toArray().exists(v => isNoData(v)))
  }

  /**
   * A GeoTiff that already has a NODATA value doesn't need widening: both sources should keep its cell type, so they
   * behave like their GeoTrellis counterparts.
   */
  @Test
  def cellTypeWithNoDataIsLeftAlone(@TempDir dir: Path): Unit = {
    val values = Array.tabulate[Short](cols * rows)(i => (i % 3).toShort)
    val geoTiffPath = writeGeoTiff(dir, values, UShortConstantNoDataCellType)

    val resampleRasterSource = new WideningGeoTiffResampleRasterSource(geoTiffPath, TargetRegion(RasterExtent(extent, cols, rows)))
    assertEquals(UShortConstantNoDataCellType, resampleRasterSource.cellType)
    val Some(resampled) = resampleRasterSource.read()
    assertEquals(UShortConstantNoDataCellType, resampled.cellType)

    val reprojectRasterSource = new WideningGeoTiffReprojectRasterSource(geoTiffPath, LatLng)
    assertEquals(UShortConstantNoDataCellType, reprojectRasterSource.cellType)
    val Some(reprojected) = reprojectRasterSource.read()
    assertEquals(UShortConstantNoDataCellType, reprojected.cellType)
  }
}
