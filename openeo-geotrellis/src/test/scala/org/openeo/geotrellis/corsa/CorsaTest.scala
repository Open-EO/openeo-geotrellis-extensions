package org.openeo.geotrellis.corsa

import ai.onnxruntime.{OnnxJavaType, OnnxTensor, OrtEnvironment, OrtSession, OrtUtil, TensorInfo}
import ai.onnxruntime.OrtSession.SessionOptions
import geotrellis.proj4.CRS
import geotrellis.raster.{FloatArrayTile, FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, Tile, UShortArrayTile, isData}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.testkit.RasterMatchers
import io.circe.generic.auto._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.openeo.geotrelliscommon.CirceException

import java.nio.file.{Files, Path, Paths}
import java.util
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._
import scala.sys.process._
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
  def pocEncode(@TempDir tempDir: Path): Unit = {
    val scaleByPython = false
    val suffix = if (scaleByPython) "python" else "scala"

    val modelPath = ModelDir.resolve("encoder.onnx")
    val scalerDir = ModelDir.resolve("scalers")

    val (Raster(cubeArray, extent), crs) = sentinel2Tile
    val cubeArrayWithoutNaNs = cubeArray.map { (_, value) => if (isData(value)) value else 0 } // TODO: interpolate

    val cubeArrayFile = tempDir.resolve("cubeArray.tif")
    MultibandGeoTiff(cubeArrayWithoutNaNs, extent, crs).write(cubeArrayFile.toString)

    require(cubeArrayWithoutNaNs.bandCount == Bands.size)
    require(cubeArrayWithoutNaNs.dimensions.cols == TileSize)
    require(cubeArrayWithoutNaNs.dimensions.rows == TileSize)

    val cubeArrayNormalized =
      if (scaleByPython) preprocessDataCubeInPython(cubeArrayFile, scalerDir)
      else preprocessDataCubeInScala(cubeArrayWithoutNaNs)

    val (level0, level1) = processWindowOnnx(cubeArrayNormalized, modelPath)

    // already 20m and 40m resolution, see comment below
    SinglebandGeoTiff(level0, extent, crs).write(f"/tmp/level0_20m_$suffix.tif")
    SinglebandGeoTiff(level1, extent, crs).write(f"/tmp/level1_40m_$suffix.tif")

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

  private def preprocessDataCubeInPython(cubeArrayFile: Path, scalerDir: Path): MultibandTile = {
    val applyScalersScript = getClass.getClassLoader.getResource("org/openeo/geotrellis/corsa/apply_scalers.py").getPath
    val applyScalers = Seq("/home/bossie/PycharmProjects/openeo/venv38/bin/python", applyScalersScript, cubeArrayFile.toAbsolutePath.toString, scalerDir.toAbsolutePath.toString) ++ Bands
    if (applyScalers.! != 0) throw new IllegalStateException(s"${applyScalers mkString " "} returned non-zero exit status")

    val scaled = MultibandGeoTiff(s"${cubeArrayFile}_scaled.tif").tile

    scaled.convert(FloatConstantNoDataCellType)
    // UDF adds a new dimension in addition to bands/y/x; this is done processWindowOnnx instead
  }

  private def preprocessDataCubeInScala(cubeArray: MultibandTile): MultibandTile =
    cubeArray
      .convert(FloatConstantNoDataCellType)
      .mapBands { case (i, bandTile) =>
        val PowerTransformerParams(lambda, mean, scale) = BandPowerTransformerParams(i)

        clip(
          standardScalerTransform(
            yeoJohnsonTransform(bandTile, lambda),
            mean,
            scale
          ),
          min = -100,
          max = 18000
        )
      }

  private def yeoJohnsonTransform(tile: Tile, λ: Double): Tile =
    tile.mapDouble { x =>
      if (x >= 0) {
        if (λ != 0) (math.pow(x + 1, λ) - 1) / λ
        else math.log1p(x)
      } else {
        if (λ != 2) -(math.pow(-x + 1, 2 - λ) - 1) / (2 - λ)
        else -math.log1p(-x)
      }
    }

  private def standardScalerTransform(tile: Tile, mean: Double, scale: Double): Tile =
    tile.mapDouble { x => (x - mean) / scale }

  private def clip(tile: Tile, min: Double, max: Double): Tile = {
    require(min <= max)
    tile.mapDouble { x => if (x < min) min else if (x > max) max else x }
  }

  private def processWindowOnnx(cubeArrayNormalized: MultibandTile, modelPath: Path): (Tile, Tile)  = {
    require(Files.exists(modelPath))

    val data = reshape(cubeArrayNormalized)
    require(data.length == 1)
    require(data.head.length == Bands.size)
    require(data.head.head.length == 120)
    require(data.head.head.head.length == 120)

    val env = OrtEnvironment.getEnvironment
    val (ortSession, ortInputName) = loadOrtSession(modelPath, env)

    val tensor = OnnxTensor.createTensor(env, data)
    val ortInputs = Map(ortInputName -> tensor).asJava

    val result = ortSession.run(ortInputs)

    val ortL0Ids = OrtUtil.reshape(result.get(2).getValue.asInstanceOf[Array[Array[Long]]].flatten, Array(1, 1, 60, 60)).asInstanceOf[Array[Array[Array[Array[Long]]]]]
    val ortL1Ids = OrtUtil.reshape(result.get(3).getValue.asInstanceOf[Array[Array[Long]]].flatten, Array(1, 1, 30, 30)).asInstanceOf[Array[Array[Array[Array[Long]]]]]

    // resample by means of "repeat" in the UDF's process_window_onnx() is undone by the resample_spatial in the UDP
    val level0 = UShortArrayTile(ortL0Ids.flatten.flatten.flatten.map(_.toShort), cols = 60, rows = 60, noDataValue = None)
    val level1 = UShortArrayTile(ortL1Ids.flatten.flatten.flatten.map(_.toShort), cols = 30, rows = 30, noDataValue = None)

    (level0, level1)
  }

  private def reshape(cubeArrayNormalized: MultibandTile): Array[Array[Array[Array[Float]]]] = {
    require(cubeArrayNormalized.bandCount == Bands.size)
    require(cubeArrayNormalized.dimensions.cols == TileSize)
    require(cubeArrayNormalized.dimensions.rows == TileSize)

    def unflattenRaster(floats: Array[Float]): Array[Array[Float]] =
      floats.sliding(size = TileSize, step = TileSize).toArray // 1D -> 2D

    val bands = for {
      bandTile <- cubeArrayNormalized.bands.toArray
      yx = unflattenRaster(bandTile.toArrayTile().asInstanceOf[FloatArrayTile].array)
    } yield yx

    Array(bands) // some additional dimension
  }

  // TODO: eventually cache this
  private def loadOrtSession(modelPath: Path, env: OrtEnvironment): (OrtSession, String) = {
    val options = new SessionOptions
    options.setIntraOpNumThreads(1)
    val session = env.createSession(modelPath.toString, options)

    require(session.getNumInputs == 1)
    val inputName = session.getInputNames.iterator().next()

    val inputInfo: TensorInfo = session.getInputInfo.get(inputName).getInfo.asInstanceOf[TensorInfo]
    require(inputInfo.`type` == OnnxJavaType.FLOAT)
    require(util.Arrays.equals(inputInfo.getShape, Array(1L, Bands.size, TileSize, TileSize))) // [???, bands, y, x]

    (session, inputName)
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
