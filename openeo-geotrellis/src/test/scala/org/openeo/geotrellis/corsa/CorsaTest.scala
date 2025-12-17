package org.openeo.geotrellis.corsa

import geotrellis.proj4.CRS
import geotrellis.raster.{FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, Tile, isData}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.testkit.RasterMatchers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.openeo.geotrellis.corsa

import java.nio.file.{Files, Path, Paths}
import scala.jdk.StreamConverters._

object CorsaTest {
  private val CorsaHome = Paths.get("/home/bossie/Documents/VITO/openeo-geotrellis-extensions/CORSA encode process #563")

  private val TileSize = 120
  private val Bands = Seq("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
}

class CorsaTest extends RasterMatchers {
  import CorsaTest._

  @Test
  def encode(@TempDir tempDir: Path): Unit = {
    val (Raster(cubeArray, extent), crs) = sentinel2Tile
    cubeArray foreach { (_, value) => require(isData(value)) } // sanity check

    val cubeArrayFile = tempDir.resolve("cubeArray.tif")
    MultibandGeoTiff(cubeArray, extent, crs).write(cubeArrayFile.toString)

    val (level0, level1) = {
      val Vector(level0, level1) = corsa.compress(tile = cubeArray).bands
      (level0, level1.resample(extent, targetCols = 30, targetRows = 30))
    }

    SinglebandGeoTiff(level0, extent, crs).write(f"/tmp/level0_20m.tif")
    SinglebandGeoTiff(level1, extent, crs).write(f"/tmp/level1_40m.tif")

    assertRastersEqual(
      actual = Raster(level0.convert(FloatConstantNoDataCellType), extent),
      expected = MultibandGeoTiff(s"$CorsaHome/level0_20m_2021-09-07Z_ref.tif").raster
    )

    assertRastersEqual(
      actual = Raster(level1.convert(FloatConstantNoDataCellType), extent),
      expected = MultibandGeoTiff(s"$CorsaHome/level1_40m_2021-09-07Z_ref.tif").raster
    )
  }

  private def sentinel2Tile: (Raster[MultibandTile], CRS) = {
    val files = Files.list(Paths.get("/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2021/09/07/S2B_20210907T104619_31UFS_TOC_V210")).toScala(Seq)

    val bandRasterSources = for {
      band <- Bands
      bandFile <- files.find(_.toString contains band)
    } yield GeoTiffRasterSource(bandFile.toString)

    val crs = bandRasterSources.head.crs
    val cols = bandRasterSources.map(_.cols).max
    val rows = bandRasterSources.map(_.rows).max

    val rasters = for {
      rs <- bandRasterSources
      resampledRs = rs.resample(targetCols = cols, targetRows = rows)
      Some(raster) = resampledRs.read(GridBounds(cols / 2, rows / 2, cols / 2 + TileSize - 1, rows / 2 + TileSize - 1)) // center of tile (arbitrary)
    } yield raster

    val extent = rasters.head.extent
    val multibandTile = MultibandTile(bands = rasters.map(_.tile.band(0)))

    (Raster(multibandTile, extent), crs)
  }

  @Test
  def decode(): Unit = {
    val nanTo0 = (value: Int) => if (isData(value)) value else 0

    val level0Tiff = SinglebandGeoTiff(s"$CorsaHome/level0_20m_2021-09-07Z_ref.tif")
    val level1Tiff = SinglebandGeoTiff(s"$CorsaHome/level1_40m_2021-09-07Z_ref.tif")

    val level0 = level0Tiff.raster.mapTile(_.map(nanTo0))
    val level1 = level1Tiff.raster.mapTile(_.map(nanTo0))

    require(level0.dimensions.cols == 60)
    require(level0.dimensions.rows == 60)
    require(level1.dimensions.cols == 30)
    require(level1.dimensions.rows == 30)

    val sentinel2Tile = corsa.decompress(tile = MultibandTile(
      level0.tile,
      level1.resample(targetCols = level0.cols, targetRows = level0.rows).tile
    ))

    assertEquals(Bands.size, sentinel2Tile.bandCount)
    assertEquals(TileSize, sentinel2Tile.cols)
    assertEquals(TileSize, sentinel2Tile.rows)

    MultibandGeoTiff(sentinel2Tile, level0Tiff.extent, level0Tiff.crs).write("/tmp/reconstructed.tif")
    // TODO: compare with original?
  }
}
