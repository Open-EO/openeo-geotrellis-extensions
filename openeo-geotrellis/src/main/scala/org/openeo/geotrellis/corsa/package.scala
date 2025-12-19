package org.openeo.geotrellis

import ai.onnxruntime.OrtSession.SessionOptions
import ai.onnxruntime.{OnnxJavaType, OnnxTensor, OrtEnvironment, OrtSession, OrtUtil, TensorInfo}
import geotrellis.raster.{FloatArrayTile, FloatConstantNoDataCellType, MultibandTile, Tile, UShortArrayTile, isData}
import io.circe.generic.auto._
import org.openeo.geotrelliscommon.{CirceException, ResampledTile}

import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Using}

package object corsa {
  def modelDir: String = {
    val modelDirEnvar = "CORSA_MODEL_DIR"

    Option(System.getenv(modelDirEnvar))
      .getOrElse(throw new IllegalStateException(s"$modelDirEnvar is not set"))
  }

  private case class EncodeSessionDetails(session: OrtSession, inputName: String)
  private val encodeSessions = new ConcurrentHashMap[Path, EncodeSessionDetails]

  private case class DecodeSessionDetails(session: OrtSession, level0InputName: String, level1InputName: String)
  private val decodeSessions = new ConcurrentHashMap[Path, DecodeSessionDetails]

  private lazy val sessionOptions = {
    val options = new SessionOptions
    options.setIntraOpNumThreads(1)
    options
  }

  private lazy val Env = OrtEnvironment.getEnvironment

  private val TileSize = 120
  private val Bands = Seq("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
  private val BandPowerTransformerParams = Bands.map { band =>
    val configClasspathResource =  s"org/openeo/geotrellis/corsa/scalers/scaler2024_power_${band}_info.json"
    parsePowerTransformerParams(configClasspathResource)
  }

  private case class PowerTransformerParams(lambda: Double, mean: Double, scale: Double)

  private def parsePowerTransformerParams(configClasspathResource: String): PowerTransformerParams = {
    case class PtKwargs(method: String, standardize: Boolean)
    case class ScKwargs(with_mean: Boolean, with_std: Boolean)
    case class Params(pt_kwargs: PtKwargs, scaler_mean: Seq[Double], scaler_var: Seq[Double],
                      lambdas_ : Seq[Double], sc_kwargs: ScKwargs)

    val config = for {
      json <- Using(Source.fromResource(configClasspathResource)) { source => source.mkString }
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

  def compress(modelDir: String, tile: MultibandTile): MultibandTile = {
    // assumes tiled to 120x120 (e.g. with featureflags: tilesize: 120) with the expected 10 bands
    require(tile.cols == TileSize, tile.cols.toString)
    require(tile.rows == TileSize, tile.rows.toString)
    require(tile.bandCount == Bands.size, s"expected bands: ${Bands mkString ", "}")

    // TODO: replace NaNs with 0 using interpolation

    val (level0, level1) = processWindowOnnx(preprocessDataCubeInScala(tile), Paths.get(modelDir).resolve("encoder.onnx"))

    assert(level0.cols == 60)
    assert(level0.rows == 60)
    assert(level1.cols == 30)
    assert(level1.rows == 30)

    MultibandTile(
      level0,
      ResampledTile(level1, sourceCols = level1.cols, sourceRows = level1.rows, targetCols = level0.cols, targetRows = level0.rows)
    )
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

    val EncodeSessionDetails(encodeSession, encodeInputName) = encodeSessions.computeIfAbsent(modelPath, path => {
      val session = Env.createSession(path.toString, sessionOptions)

      require(session.getNumInputs == 1)
      val inputName = session.getInputNames.iterator().next()

      val inputInfo: TensorInfo = session.getInputInfo.get(inputName).getInfo.asInstanceOf[TensorInfo]
      require(inputInfo.`type` == OnnxJavaType.FLOAT)
      require(util.Arrays.equals(inputInfo.getShape, Array(1L, Bands.size, TileSize, TileSize))) // [???, bands, y, x]

      EncodeSessionDetails(session, inputName)
    })

    val tensor = OnnxTensor.createTensor(Env, data)
    val ortInputs = Map(encodeInputName -> tensor).asJava

    val result = encodeSession.run(ortInputs)

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

  def decompress(modelDir: String, tile: MultibandTile): MultibandTile = {
    require(tile.bandCount == 2, tile.bandCount.toString)
    require(tile.dimensions.cols == 60, tile.dimensions.cols.toString)
    require(tile.dimensions.rows == 60, tile.dimensions.rows.toString)

    val nanTo0 = (value: Int) => if (isData(value)) value else 0

    val level0 = tile.band(0).map(nanTo0)
    val level1 = ResampledTile(tile.band(1).map(nanTo0), sourceCols = 60, sourceRows = 60, targetCols = 30, targetRows = 30)

    val patchLevel0Data = OnnxTensor.createTensor(Env,
      OrtUtil.reshape(Array[Long](level0.toArray().map(_.toLong): _*), Array(1, 60, 60)))
    val patchLevel1Data = OnnxTensor.createTensor(Env,
      OrtUtil.reshape(Array[Long](level1.toArray().map(_.toLong): _*), Array(1, 30, 30)))

    val DecodeSessionDetails(decodeSession, decodeLevel0InputName, decodeLevel1InputName) =
      decodeSessions.computeIfAbsent(Paths.get(modelDir).resolve("decoder.onnx"), path => {
        val decodeSession = Env.createSession(path.toString, sessionOptions)

        // inputs are sorted
        val inputNames = decodeSession.getInputInfo.keySet().iterator()
        val level0InputName = inputNames.next()
        val level1InputName = inputNames.next()

        DecodeSessionDetails(decodeSession, level0InputName, level1InputName)
      })

    val ortInputs = Map(
      decodeLevel0InputName -> patchLevel0Data,
      decodeLevel1InputName -> patchLevel1Data,
    ).asJava

    val result = decodeSession.run(ortInputs) // 1 x 10 x 120 x 120

    val recon = result.get(0).getValue.asInstanceOf[Array[Array[Array[Array[Float]]]]](0)

    val bandTiles = for {
      band <- recon
    } yield FloatArrayTile(band.flatten, cols = TileSize, rows = TileSize)

    val scaledTile = MultibandTile(bandTiles)
    unscale(scaledTile)
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
