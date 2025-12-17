package org.openeo.geotrellis.corsa

import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtUtil}
import ai.onnxruntime.OrtSession.SessionOptions
import geotrellis.proj4.CRS
import geotrellis.raster.{FloatArrayTile, FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, Tile, isData}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.testkit.RasterMatchers
import io.circe.generic.auto._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.openeo.geotrellis.corsa
import org.openeo.geotrelliscommon.CirceException

import java.nio.file.{Files, Path, Paths}
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._
import scala.util.{Failure, Success, Using}

object CorsaTest {
  private val CorsaHome = Paths.get("/home/bossie/Documents/VITO/openeo-geotrellis-extensions/CORSA encode process #563")
  private val ModelDir = {
    // copied from /data/users/Public/luytsa/corsa-compression/pretrain_BEN_10-20mbands_bicubic_512-128_onnx
    CorsaHome.resolve("pretrain_BEN_10-20mbands_bicubic_512-128_onnx")
  }

  private val TileSize = 120
  private val Bands = Seq("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
  private val BandPowerTransformerParams = Bands.map { band =>
    parsePowerTransformerParams(CorsaHome.resolve(s"scalers/scaler2024_power_${band}_info.json"))
  }

  private case class PowerTransformerParams(lambda: Double, mean: Double, scale: Double)

  private def parsePowerTransformerParams(configFile: Path): PowerTransformerParams = {
    case class PtKwargs(method: String, standardize: Boolean)
    case class ScKwargs(with_mean: Boolean, with_std: Boolean)
    case class Params(pt_kwargs: PtKwargs, scaler_mean: Seq[Double], scaler_var: Seq[Double],
                                      lambdas_ : Seq[Double], sc_kwargs: ScKwargs)

    val config = for {
      json <- Using(Source.fromFile(configFile.toFile)) { source => source.mkString }
      config <- CirceException.decode[Params](json).toTry
    } yield config

    config match {
      case Success(config) =>
        // assumptions that simplify scaling implementation
        require(config.pt_kwargs.method == "yeo-johnson")
        require(config.pt_kwargs.standardize)
        require(config.scaler_mean.size == 1)
        require(config.scaler_var.size == 1)
        require(config.lambdas_.size == 1)
        require(config.sc_kwargs.with_mean)
        require(config.sc_kwargs.with_std)

        PowerTransformerParams(config.lambdas_.head, config.scaler_mean.head, config.scaler_var.head)
      case Failure(e) => throw e
    }
  }
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
  def pocDecode(): Unit = {
    val modelPath = ModelDir.resolve("decoder.onnx")

    val nanTo0 = (value: Int) => if (isData(value)) value else 0

    val level0Tiff = SinglebandGeoTiff(s"$CorsaHome/level0_20m_2021-09-07Z_ref.tif")
    val level1Tiff = SinglebandGeoTiff(s"$CorsaHome/level1_40m_2021-09-07Z_ref.tif")

    val level0 = level0Tiff.raster.mapTile(_.map(nanTo0))
    val level1 = level1Tiff.raster.mapTile(_.map(nanTo0))

    require(level0.dimensions.cols == 60)
    require(level0.dimensions.rows == 60)
    require(level1.dimensions.cols == 30)
    require(level1.dimensions.rows == 30)

    val env = OrtEnvironment.getEnvironment
    val options = new SessionOptions
    options.setIntraOpNumThreads(1)

    val recon = Using(env.createSession(modelPath.toString, options)) { session =>
      // note: original code loops over several patches of size 60; in this case there is only one

      // inputs are sorted
      val inputNames = session.getInputInfo.keySet().iterator()
      val level0InputName = inputNames.next()
      val level1InputName = inputNames.next()

      val patchLevel0Data = OnnxTensor.createTensor(env,
        OrtUtil.reshape(Array[Long](level0.tile.toArray().map(_.toLong): _*), Array(1, 60, 60)))
      val patchLevel1Data = OnnxTensor.createTensor(env,
        OrtUtil.reshape(Array[Long](level1.tile.toArray().map(_.toLong): _*), Array(1, 30, 30)))

      val ortInputs = Map(
        level0InputName -> patchLevel0Data,
        level1InputName -> patchLevel1Data,
      ).asJava

      val result = session.run(ortInputs) // 1 x 10 x 120 x 120

      val recon = result.get(0).getValue.asInstanceOf[Array[Array[Array[Array[Float]]]]](0)

      val bandTiles = for {
        band <- recon
      } yield FloatArrayTile(band.flatten, cols = TileSize, rows = TileSize)

      MultibandTile(bandTiles)
    }

    val sentinel2Tile = unscale(recon.get)
    assertEquals(Bands.size, sentinel2Tile.bandCount)
    assertEquals(TileSize, sentinel2Tile.cols)
    assertEquals(TileSize, sentinel2Tile.rows)

    MultibandGeoTiff(sentinel2Tile, level0Tiff.extent, level0Tiff.crs).write("/tmp/reconstructed.tif")
    // TODO: compare with original?
  }

  private def inverseYeoJohnsonTransform(tile: Tile, λ: Double): Tile =
    tile.mapDouble { x =>
      if (x >= 0) {
        if (λ == 0) math.exp(x) - 1
        else math.pow(x * λ + 1, 1 / λ) - 1
      } else {
        if (λ == 2) 1 - math.exp(-x)
        else 1 - math.pow(-(2 - λ) * x + 1, 1 / (2 - λ))
      }
    }

  private def inverseStandardScalerTransform(tile: Tile, mean: Double, scale: Double): Tile =
    tile.mapDouble { x => x * scale + mean }

  private def unscale(recon: MultibandTile): MultibandTile =
    recon.mapBands { case (i, bandTile) =>
      val PowerTransformerParams(lambda, mean, scale) = BandPowerTransformerParams(i)

      inverseYeoJohnsonTransform(
        inverseStandardScalerTransform(
          bandTile,
          mean,
          scale
        ),
        lambda
      )
    }
}
