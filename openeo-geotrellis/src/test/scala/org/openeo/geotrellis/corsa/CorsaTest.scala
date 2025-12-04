package org.openeo.geotrellis.corsa

import ai.onnxruntime.{OnnxJavaType, OnnxTensor, OrtEnvironment, OrtSession, OrtUtil, TensorInfo}
import ai.onnxruntime.OrtSession.SessionOptions
import geotrellis.proj4.CRS
import geotrellis.raster.{FloatArrayTile, FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, Tile, UShortArrayTile, isData}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.nio.file.{Files, Path, Paths}
import java.util
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._
import scala.sys.process._

object CorsaTest {
  private val TileSize = 120
  private val Bands = Seq("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
}

class CorsaTest {
  import CorsaTest._

  @Test
  def poc_encode(@TempDir tempDir: Path): Unit = {
    val modelDir = {
      // copied from /data/users/Public/luytsa/corsa-compression/pretrain_BEN_10-20mbands_bicubic_512-128_onnx
      Paths.get("/home/bossie/Documents/VITO/openeo-geotrellis-extensions/CORSA encode process #563/pretrain_BEN_10-20mbands_bicubic_512-128_onnx")
    }
    val modelPath = modelDir.resolve("encoder.onnx")
    val scalerDir = modelDir.resolve("scalers")

    val (Raster(cubeArray, extent), crs) = sentinel2Tile

    val cubeArrayFile = tempDir.resolve("cubeArray.tif")
    MultibandGeoTiff(cubeArray, extent, crs).write(cubeArrayFile.toString)
    require(cubeArray.bandCount == Bands.size)
    require(cubeArray.dimensions.cols == TileSize)
    require(cubeArray.dimensions.rows == TileSize)

    // TODO: replace NaNs with 0; in this case there are none
    cubeArray foreach { (_, value) => require(isData(value)) }

    val cubeArrayNormalized = preprocessDataCube(cubeArrayFile, scalerDir)

    val (level0, level1) = processWindowOnnx(cubeArrayNormalized, modelPath)

    // already 20m and 40m resolution, see comment below
    SinglebandGeoTiff(level0, extent, crs).write("/tmp/level0_20m.tif")
    SinglebandGeoTiff(level1, extent, crs).write("/tmp/level1_40m.tif")
  }

  private def sentinel2Tile: (Raster[MultibandTile], CRS) = {
    val files = Files.list(Paths.get("/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2021/09/07/S2B_20210907T104619_31UFS_TOC_V210")).toScala(Seq)

    val bandRasterSources = for {
      band <- Bands
      bandFile <- files.find(_.toString contains band)
    } yield GeoTiffRasterSource(bandFile.toString)

    val crs = bandRasterSources.head.crs

    val rasters = for {
      rs <- bandRasterSources
      raster <- rs.read(GridBounds(0L, 0L, TileSize - 1, TileSize - 1))
    } yield raster

    val extent = rasters.head.extent
    val multibandTile = MultibandTile(bands = rasters.map(_.tile.band(0)))

    (Raster(multibandTile, extent), crs)
  }

  private def preprocessDataCube(cubeArrayFile: Path, scalerDir: Path): MultibandTile = {
    // TODO: scale; original does this by unpickling some objects from disk and applying them to the input

    val applyScalersScript = getClass.getClassLoader.getResource("org/openeo/geotrellis/corsa/apply_scalers.py").getPath
    val applyScalers = Seq("/home/bossie/PycharmProjects/openeo/venv38/bin/python", applyScalersScript, cubeArrayFile.toAbsolutePath.toString, scalerDir.toAbsolutePath.toString) ++ Bands
    if (applyScalers.! != 0) throw new IllegalStateException(s"${applyScalers mkString " "} returned non-zero exit status")

    val scaled = MultibandGeoTiff(s"${cubeArrayFile}_scaled.tif").tile

    scaled.convert(FloatConstantNoDataCellType)
    // UDF adds a new dimension in addition to bands/y/x; this is done processWindowOnnx instead
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
    val level0 = UShortArrayTile(ortL0Ids.flatten.flatten.flatten.map(_.toShort), cols = 60, rows = 60)//.resample(120, 120)
    val level1 = UShortArrayTile(ortL1Ids.flatten.flatten.flatten.map(_.toShort), cols = 30, rows = 30)//.resample(120, 120)

    (level0, level1)
  }

  private def reshape(cubeArrayNormalized: MultibandTile): Array[Array[Array[Array[Float]]]] = {
    require(cubeArrayNormalized.bandCount == Bands.size)
    require(cubeArrayNormalized.dimensions.cols == TileSize)
    require(cubeArrayNormalized.dimensions.rows == TileSize)

    def unflatten(floats: Array[Float]): Array[Array[Float]] =
      floats.sliding(size = TileSize, step = TileSize).toArray // 1D -> 2D

    val yxBands = for {
      bandTile <- cubeArrayNormalized.bands.toArray
      xy = unflatten(bandTile.toArrayTile().asInstanceOf[FloatArrayTile].array)
      yx = xy // TODO: .transpose unnecessary?
    } yield yx

    Array(yxBands) // some additional dimension
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
}
