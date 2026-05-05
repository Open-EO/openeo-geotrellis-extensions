package org.openeo.geotrellis.corsa

import geotrellis.proj4.CRS
import geotrellis.raster.{FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, isData}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.testkit.RasterMatchers
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.openeo.geotrellis.corsa

import java.nio.file.{Files, Path, Paths}
import scala.jdk.StreamConverters._

object CorsaTest {
  private final val V1PatchSize = 120
  private val Bands = Seq("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
  private final val n: Double = Double.NaN
}

class CorsaTest extends RasterMatchers {
  import CorsaTest._

  private def testResourcePath(filename: String): String =
    getClass.getResource(s"/org/openeo/geotrellis/corsa/$filename").getPath

  @EnabledIfEnvironmentVariable(named = "CORSA_MODEL_DIR", matches=".+")
  @Test
  def encode(@TempDir tempDir: Path): Unit = {
    val (Raster(cubeArray, extent), crs) = sentinel2Tile()
    cubeArray foreach { (_, value) => require(isData(value)) } // sanity check

    val cubeArrayFile = tempDir.resolve("cubeArray.tif")
    MultibandGeoTiff(cubeArray, extent, crs).write(cubeArrayFile.toString)

    val (level0, level1) = {
      val Vector(level0, level1) = corsa.compress(modelDir, tile = cubeArray).bands
      (level0, level1.resample(extent, targetCols = level0.cols / 2, targetRows = level0.rows / 2))
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

  private def sentinel2Tile(tileSize: Int = V1PatchSize): (Raster[MultibandTile], CRS) = {
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
      Some(raster) = resampledRs.read(GridBounds(cols / 2, rows / 2, cols / 2 + tileSize - 1, rows / 2 + tileSize - 1)) // center of tile (arbitrary)
    } yield raster

    val extent = rasters.head.extent
    val multibandTile = MultibandTile(bands = rasters.map(_.tile.band(0)))

    (Raster(multibandTile, extent), crs)
  }

  @EnabledIfEnvironmentVariable(named = "CORSA_MODEL_DIR", matches=".+")
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
    assertEquals(V1PatchSize, sentinel2Tile.cols)
    assertEquals(V1PatchSize, sentinel2Tile.rows)

    assertRastersEqual(
      actual = Raster(sentinel2Tile, level0Tiff.extent),
      expected = MultibandGeoTiff(testResourcePath("reconstructed_2021-09-07Z_ref.tif")).raster
    )
  }

  @Test
  def interpolateNaN(): Unit = {
    val row = Array(n, n, n, 4, n, 6, n, n, n, 10, n)
    corsa.interpolateNaN(row, limit = 2)

    assertArrayEquals(Array(n, n, n, 4, 5, 6, 7, 8, n, 10, n), row, 0.0)
  }

  @EnabledIfEnvironmentVariable(named = "USER", matches="bossie",
    disabledReason = "models are not yet available on the cluster")
  @ParameterizedTest
  @ValueSource(ints = Array(256, 512, 1024))
  def compressV2(patchSize: Int): Unit = {
    val tempDir = Paths.get("/tmp/compressV2") // TODO: remove

    val (Raster(original, extent), crs) = sentinel2Tile(patchSize)
    MultibandGeoTiff(original, extent, crs).write(tempDir.resolve(s"original_$patchSize.tif").toString)

    val compressed = corsa.compressV2(original)
    assertEquals(2, compressed.bandCount)
    assertEquals(patchSize / 2, compressed.cols)
    assertEquals(patchSize / 2, compressed.rows)

    MultibandGeoTiff(compressed, extent, crs).write(tempDir.resolve(s"compressed_$patchSize.tif").toString)

    val reconstructed = corsa.decompressV2(compressed)
    assertEquals(original.bandCount, reconstructed.bandCount)
    assertEquals(patchSize, reconstructed.cols)
    assertEquals(patchSize, reconstructed.rows)

    MultibandGeoTiff(reconstructed, extent, crs).write(tempDir.resolve(s"reconstructed_$patchSize.tif").toString)
  }

  @EnabledIfEnvironmentVariable(named = "USER", matches="bossie",
    disabledReason = "models are not yet available on the cluster")
  @Test
  def compressV2(): Unit = {
    val patchSize = 256

    val (Raster(original, extent), _) = sentinel2Tile(patchSize)

    val (level0, level1) = {
      val Vector(level0, level1) = corsa.compressV2(original).bands
      (level0, level1.resample(extent, targetCols = patchSize / 4, targetRows = patchSize / 4))
    }

    assertRastersEqual(
      actual = Raster(level0, extent),
      expected = MultibandGeoTiff(testResourcePath("level0_20m_2021-09-07Z_p256_v2_ref.tif")).raster
    )

    assertRastersEqual(
      actual = Raster(level1, extent),
      expected = MultibandGeoTiff(testResourcePath("level1_40m_2021-09-07Z_p256_v2_ref.tif")).raster
    )
  }

  @EnabledIfEnvironmentVariable(named = "USER", matches="bossie",
    disabledReason = "models are not yet available on the cluster")
  @Test
  def decompressV2(): Unit = {
    val patchSize = 256

    val level0 = SinglebandGeoTiff(testResourcePath("level0_20m_2021-09-07Z_p256_v2_ref.tif"))
    val level1 = SinglebandGeoTiff(testResourcePath("level1_40m_2021-09-07Z_p256_v2_ref.tif"))

    assertEquals(patchSize / 2, level0.cols)
    assertEquals(patchSize / 2, level0.rows)
    assertEquals(patchSize / 4, level1.cols)
    assertEquals(patchSize / 4, level1.rows)

    val compressed = MultibandTile(
      level0.tile,
      level1.tile.resample(targetCols = level0.cols, targetRows = level0.rows)
    )

    val decompressed = corsa.decompressV2(compressed)

    assertRastersEqual(
      actual = Raster(decompressed, level0.extent),
      expected = MultibandGeoTiff(testResourcePath("reconstructed_2021-09-07Z_p256_v2_ref.tif")).raster
    )
  }
}
