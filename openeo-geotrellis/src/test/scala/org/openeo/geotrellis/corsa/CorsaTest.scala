package org.openeo.geotrellis.corsa

import ai.onnxruntime.OrtUtil
import geotrellis.proj4.CRS
import geotrellis.raster.{DoubleArrayTile, FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, Tile, isData}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.testkit.RasterMatchers
import org.apache.commons.math3.linear.MatrixUtils
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.api.io.TempDir
import org.openeo.geotrellis.corsa

import java.nio.file.{Files, Path, Paths}
import scala.jdk.StreamConverters._

object CorsaTest {
  private val TileSize = 120
  private val Bands = Seq("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
  private val n: Double = Double.NaN
}

@EnabledIfEnvironmentVariable(named = "CORSA_MODEL_DIR", matches=".+")
class CorsaTest extends RasterMatchers {
  import CorsaTest._

  private def testResourcePath(filename: String): String =
    getClass.getResource(s"/org/openeo/geotrellis/corsa/$filename").getPath

  @Test
  def encode(@TempDir tempDir: Path): Unit = {
    val (Raster(cubeArray, extent), crs) = sentinel2Tile
    cubeArray foreach { (_, value) => require(isData(value)) } // sanity check

    val cubeArrayFile = tempDir.resolve("cubeArray.tif")
    MultibandGeoTiff(cubeArray, extent, crs).write(cubeArrayFile.toString)

    val (level0, level1) = {
      val Vector(level0, level1) = corsa.compress(modelDir, tile = cubeArray).bands
      (level0, level1.resample(extent, targetCols = 30, targetRows = 30))
    }

    SinglebandGeoTiff(level0, extent, crs).write(f"/tmp/level0_20m.tif")
    SinglebandGeoTiff(level1, extent, crs).write(f"/tmp/level1_40m.tif")

    assertRastersEqual(
      actual = Raster(level0.convert(FloatConstantNoDataCellType), extent),
      expected = MultibandGeoTiff(testResourcePath("level0_20m_2021-09-07Z_ref.tif")).raster
    )

    assertRastersEqual(
      actual = Raster(level1.convert(FloatConstantNoDataCellType), extent),
      expected = MultibandGeoTiff(testResourcePath("level1_40m_2021-09-07Z_ref.tif")).raster
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
    val level0Tiff = SinglebandGeoTiff(testResourcePath("level0_20m_2021-09-07Z_ref.tif"))
    val level1Tiff = SinglebandGeoTiff(testResourcePath("level1_40m_2021-09-07Z_ref.tif"))

    val level0 = level0Tiff.raster
    val level1 = level1Tiff.raster

    require(level0.dimensions.cols == 60)
    require(level0.dimensions.rows == 60)
    require(level1.dimensions.cols == 30)
    require(level1.dimensions.rows == 30)

    val sentinel2Tile = corsa.decompress(modelDir, tile = MultibandTile(
      level0.tile,
      level1.resample(targetCols = level0.cols, targetRows = level0.rows).tile
    ))

    assertEquals(Bands.size, sentinel2Tile.bandCount)
    assertEquals(TileSize, sentinel2Tile.cols)
    assertEquals(TileSize, sentinel2Tile.rows)

    assertRastersEqual(
      actual = Raster(sentinel2Tile, level0Tiff.extent),
      expected = MultibandGeoTiff(testResourcePath("reconstructed_2021-09-07Z_ref.tif")).raster
    )
  }

  @Test
  def interpolate(): Unit = {
    val row = Array(n, 1, n, n, n, 5, n, 7, n)

    interpolate(row, limit = 2)

    assertArrayEquals(Array(n, 1, 2, 3, n, 5, 6, 7, n), row)
  }

  @Test
  def interpolateTileRows(): Unit = {
    val tile = DoubleArrayTile(Array(
      1, 2, 3, 4, 5,
      1, n, n, 4, n,
      n, n, 3, 4, 5,
      1, n, n, n, 5,
    ), cols = 5, rows = 4)

    val interpolated = interpolateTileRows(tile, limit = 2)

    print(interpolated.toArrayDouble().mkString("Array(", ", ", ")"))

    assertArrayEquals(Array(
      1, 2, 3, 4, 5,
      1, 2, 3, 4, n,
      n, n, 3, 4, 5,
      1, 2, 3, n, 5,
    ), interpolated.toArrayDouble())
  }

  @Test
  def replaceNaNsWith0(): Unit = {
    val bandTile = DoubleArrayTile(Array(
      1, 2, 3, 4, 5,
      1, n, n, 4, n,
      n, n, 3, 4, 5,
      1, n, n, n, 5,
    ), cols = 5, rows = 4)

    println(bandTile.asciiDraw())

    // add interpolateTile(bandTile) to transpose(interpolateTile(transpose(bandTile))), divide by 2 and replace remaining NaNs with 0
    val mRows = OrtUtil.reshape(bandTile.array, Array(bandTile.rows, bandTile.cols)).asInstanceOf[Array[Array[Double]]]

    val tRows = MatrixUtils.createRealMatrix(mRows).copy().transpose().getData

    mRows.foreach(row => interpolate(row, limit = 2))
    println("interpolated m:\n" + dump(mRows))

    println("original t:\n" + dump(tRows))
    tRows.foreach(row => interpolate(row, limit = 2))
    println("interpolated t:\n" + dump(tRows))

    val interpolated = (MatrixUtils.createRealMatrix(mRows) add MatrixUtils.createRealMatrix(tRows).transpose()).scalarMultiply(0.5)
    val interpolatedTile = DoubleArrayTile(interpolated.getData.flatten, cols = bandTile.cols, rows = bandTile.rows)

    println(interpolatedTile.asciiDraw())

    val tileWithoutNaN = interpolatedTile.mapDouble((x: Double) => if (isData(x)) x else 0)

    println(tileWithoutNaN.asciiDraw())
  }

  private def dump(matrix: Array[Array[Double]]): String = {
    def dumpRow(row: Array[Double]): String = row mkString " "
    matrix.map(dumpRow) mkString "\n"
  }

  def replaceNaNsWith0(bandTile: Tile): Tile = {
    val mRows = OrtUtil.reshape(bandTile.toArrayDouble(), Array(bandTile.rows, bandTile.cols)).asInstanceOf[Array[Array[Double]]]
    val tRows = MatrixUtils.createRealMatrix(mRows).copy().transpose().getData

    mRows.foreach(row => interpolate(row, limit = 2))
    tRows.foreach(row => interpolate(row, limit = 2))

    val interpolated = (MatrixUtils.createRealMatrix(mRows) add MatrixUtils.createRealMatrix(tRows).transpose())
      .scalarMultiply(0.5)

    DoubleArrayTile(interpolated.getData.flatten, cols = bandTile.cols, rows = bandTile.rows)
      .mapDouble((x: Double) => if (isData(x)) x else 0)
      .convert(FloatConstantNoDataCellType)
  }

  @Test
  def replaceNaNsWith0IsBackwardsCompatible(): Unit = {
    val original = MultibandGeoTiff(testResourcePath("reconstructed_2021-09-07Z_ref.tif")).tile

    val interpolatedBands = for {
      bandTile <- original.bands
    } yield replaceNaNsWith0(bandTile)

    assertTilesEqual(actual = MultibandTile(interpolatedBands), expected = original)
  }

  def interpolateTileRows(tile: Tile, limit: Int): Tile = {
    val interpolatedRows = for {
      row <- OrtUtil.reshape(tile.toArrayDouble(), Array(tile.rows, tile.cols)).asInstanceOf[Array[Array[Double]]] // allocates a new array
    } yield {
      interpolate(row, limit)
      row
    }

    DoubleArrayTile(interpolatedRows.flatten, tile.cols, tile.rows)
  }

  def interpolate(row: Array[Double], limit: Int): Unit = { // modifies row in-place
    def gapIndicesFrom(index: Int): (Int, Int) = {
      var lower = -1
      var upper = -1

      for (i <- index until row.length if lower == -1) {
        if (row(i).isNaN) {
          lower = i
        }
      }

      if (lower == -1) return null

      for (i <- (lower + 1) until row.length if upper == -1) {
        if (!row(i).isNaN) {
          upper = i
        }
      }

      if (upper == -1) null else (lower, upper) // upper is exclusive
    }

    def interpolate(lower: Int, upper: Int, limit: Int): Unit = { // gap indices
      if (lower <= 0 || upper >= row.length) return // row starts or ends with NaN; do not interpolate

      val deltaX = upper - (lower - 1)
      val deltaY = row(upper) - row(lower - 1)
      val delta = deltaY / deltaX

      for (i <- lower until upper if i - lower < limit) {
        val interpolated = row(lower - 1) + (i - lower + 1) * delta
        row(i) = interpolated
      }
    }

    var from = 0
    var gapIndices: (Int, Int) = null

    do {
      gapIndices = gapIndicesFrom(from)

      if (gapIndices != null) {
        val (lower, upper) = gapIndices
        interpolate(lower, upper, limit)
        from = upper
      }
    } while (from < row.length && gapIndices != null)
  }
}
