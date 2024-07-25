package org.openeo.geotrellis.layers

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{CellSize, FloatConstantNoDataCellType, GridBounds, GridExtent, RasterSource}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeAll, Test}

import java.nio.file.{Files, Paths}


object SentinelXMLMetadataRasterSourceTest {
  val outDir = "tmp/SentinelXMLMetadataRasterSourceTest/"
  @BeforeAll
  def beforeAll(): Unit = {
    Files.createDirectories(Paths.get(outDir))
    // After the tests run, one can manually compare the exported tiff files in QGIS
  }
}

class SentinelXMLMetadataRasterSourceTest {

  import SentinelXMLMetadataRasterSourceTest._

  @Test
  def testReadAngles(): Unit = {
    val path = getClass.getResource("/org/openeo/geotrellis/layers/MTD_TL.xml").getPath
    val source: Seq[SentinelXMLMetadataRasterSource] = SentinelXMLMetadataRasterSource.forAngleBands(path)
    assertEquals( 171.800, source(0).read().get.tile.band(0).getDouble(0,0),0.001)
    assertEquals(  65.707, source(1).read().get.tile.band(0).getDouble(0,0),0.001)
    assertEquals( 251.333, source(2).read().get.tile.band(0).getDouble(0,0),0.001)
    assertEquals(source.head.name.toString, path.toString + "_171800")
    GeoTiff(source.head.read().get, source.head.crs).write(outDir + "testReadAngles.tif")
  }

  val path = getClass.getResource("/org/openeo/geotrellis/layers/MTD_TL.xml").getPath
  private val noDataRasterSource = SentinelXMLMetadataRasterSource.forAngleBands(path).head

  @Test
  def testGlobal(): Unit = {
    assertEquals(geotrellis.proj4.CRS.fromEpsgCode(32631), noDataRasterSource.crs)
    val Some(noDataRaster) = noDataRasterSource.read()
    assertEquals(1, noDataRaster.tile.bandCount)
    assertEquals(noDataRaster.tile.band(0).getDouble(0, 0), 171.80014, 0.0001)
  }

  @Test
  def readExtent(): Unit = {
    val extent = Extent(555000.0, 5390220.0, 609780.0, 5500020.0)
    val Some(rasterCroppedByExtent) = noDataRasterSource.read(extent)
    assertEquals(extent, rasterCroppedByExtent.extent)
    GeoTiff(rasterCroppedByExtent, noDataRasterSource.crs).write(outDir + "readExtent.tif")
  }

  @Test
  def readGridBounds(): Unit = {
    val Some(rasterCroppedByGridBounds) = noDataRasterSource.read(GridBounds[Long](0, 0, 1023, 1023))
    GeoTiff(rasterCroppedByGridBounds, noDataRasterSource.crs).write(outDir + "readGridBounds.tif")
    assertEquals(1024, rasterCroppedByGridBounds.cols)
    assertEquals(1024, rasterCroppedByGridBounds.rows)
  }

  @Test
  def convert(): Unit = {
    val converted = noDataRasterSource.convert(FloatConstantNoDataCellType)
    assertEquals(geotrellis.proj4.CRS.fromEpsgCode(32631), converted.crs)
    val Some(convertedRaster) = converted.read()
    assertEquals(FloatConstantNoDataCellType, convertedRaster.cellType)
    assertEquals(convertedRaster.tile.band(0).getDouble(0, 0), 171.80014, 0.0001)
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
    assertEquals(geotrellis.proj4.CRS.fromEpsgCode(32631), resampled.crs)
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
