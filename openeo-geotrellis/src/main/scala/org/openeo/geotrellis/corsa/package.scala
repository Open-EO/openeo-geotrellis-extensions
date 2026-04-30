package org.openeo.geotrellis

import ai.onnxruntime.OrtSession.SessionOptions
import ai.onnxruntime.{OnnxJavaType, OnnxTensor, OrtEnvironment, OrtSession, OrtUtil, TensorInfo}
import geotrellis.raster.{DoubleArrayTile, FloatArrayTile, FloatConstantNoDataCellType, MultibandTile, Tile, UShortArrayTile, isData}
import io.circe.generic.auto._
import org.apache.commons.math3.linear.MatrixUtils
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

    val normalizedTile = tile.mapBands { case (_, bandTile) => replaceNaNsWith0(bandTile) }

    val (level0, level1) = processWindowOnnx(
      preprocessDataCubeInScala(normalizedTile), Paths.get(modelDir).resolve("encoder.onnx")
    )

    assert(level0.cols == 60)
    assert(level0.rows == 60)
    assert(level1.cols == 30)
    assert(level1.rows == 30)

    MultibandTile(
      level0,
      ResampledTile(level1, sourceCols = level1.cols, sourceRows = level1.rows, targetCols = level0.cols, targetRows = level0.rows)
    )
  }

  private def replaceNaNsWith0(bandTile: Tile): Tile = {
    val mRows = OrtUtil.reshape(bandTile.toArrayDouble(), Array(bandTile.rows, bandTile.cols)).asInstanceOf[Array[Array[Double]]]
    val tRows = MatrixUtils.createRealMatrix(mRows).copy().transpose().getData

    val limit = 2

    mRows.foreach(row => interpolateNaN(row, limit))
    tRows.foreach(row => interpolateNaN(row, limit))

    val interpolated = (MatrixUtils.createRealMatrix(mRows) add MatrixUtils.createRealMatrix(tRows).transpose())
      .scalarMultiply(0.5)

    DoubleArrayTile(interpolated.getData.flatten, cols = bandTile.cols, rows = bandTile.rows)
      .mapDouble((x: Double) => if (isData(x)) x else 0)
      .convert(FloatConstantNoDataCellType)
  }

  def interpolateNaN(row: Array[Double], limit: Int): Unit = { // modifies row in-place
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
    require(cubeArrayNormalized.cols == cubeArrayNormalized.rows)

    def unflattenRaster(floats: Array[Float]): Array[Array[Float]] =
      floats.sliding(size = cubeArrayNormalized.cols, step = cubeArrayNormalized.rows).toArray // 1D -> 2D

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

    def nanTo0(value: Int): Int = if (isData(value)) value else 0

    val level0 = tile.band(0).map(nanTo0 _)
    val level1 = ResampledTile(tile.band(1).map(nanTo0 _), sourceCols = 60, sourceRows = 60, targetCols = 30, targetRows = 30)

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

  def compressImproved(tile: MultibandTile): MultibandTile = {
    require(tile.cols == tile.rows)

    val tileSize = tile.cols

    val modelPath =
      Paths.get(s"/home/bossie/Documents/VITO/openeo-geotrellis-extensions/CORSA improvements #702/onnx/corsa_mtc_160k_64b_${tileSize}p/encoder.onnx")

    require(Files.exists(modelPath))

    val session = Env.createSession(modelPath.toString, sessionOptions)

    // session.getInputInfo.forEach { (key, value) => println(s"$key: $value")} // x: NodeInfo(name=x,info=TensorInfo(javaType=FLOAT,onnxType=ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT,shape=[1, 10, 256, 256]))
    session.getOutputInfo.forEach { (key, value) => println(s"$key: $value")}

    val normalizedTile = tile.mapBands { case (_, bandTile) => replaceNaNsWith0(bandTile) }
    val data = reshape(normalizedTile)

    val tensor = OnnxTensor.createTensor(Env, data)
    val ortInputs = Map("x" -> tensor).asJava

    val result = session.run(ortInputs)

    val ortL0Ids = OrtUtil.reshape(result.get(2).getValue.asInstanceOf[Array[Array[Long]]].flatten, Array(1, 1, tileSize / 2, tileSize / 2)).asInstanceOf[Array[Array[Array[Array[Long]]]]]
    val ortL1Ids = OrtUtil.reshape(result.get(3).getValue.asInstanceOf[Array[Array[Long]]].flatten, Array(1, 1, tileSize / 4, tileSize / 4)).asInstanceOf[Array[Array[Array[Array[Long]]]]]

    val level0 = UShortArrayTile(ortL0Ids.flatten.flatten.flatten.map(_.toShort), cols = tileSize / 2, rows = tileSize / 2, noDataValue = None)
    val level1 = UShortArrayTile(ortL1Ids.flatten.flatten.flatten.map(_.toShort), cols = tileSize / 4, rows = tileSize / 4, noDataValue = None)

    MultibandTile(
      level0,
      ResampledTile(level1, sourceCols = level1.cols, sourceRows = level1.rows, targetCols = level0.cols, targetRows = level0.rows)
    )
  }
}
