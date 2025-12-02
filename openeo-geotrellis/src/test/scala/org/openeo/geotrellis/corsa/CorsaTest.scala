package org.openeo.geotrellis.corsa

import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtSession}
import ai.onnxruntime.OrtSession.SessionOptions
import geotrellis.proj4.CRS
import geotrellis.raster.{FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, Tile, isData}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._

class CorsaTest {

  @Test
  def test(): Unit = {
    val bands = Seq("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
    val modelDir = {
      // copied from /data/users/Public/luytsa/corsa-compression/pretrain_BEN_10-20mbands_bicubic_512-128_onnx
      Paths.get("/home/bossie/Documents/VITO/openeo-geotrellis-extensions/CORSA encode process #563/pretrain_BEN_10-20mbands_bicubic_512-128_onnx")
    }
    val modelPath = modelDir.resolve("encoder.onnx")

    // TODO: set up ONNX dependencies (~ load_onnx_deps)

    val (Raster(cubeArray, extent), crs) = sentinel2Tile(bands)
    // MultibandGeoTiff(cubeArray, extent, crs).write("/tmp/sample.tif")

    // TODO: replace NaNs with 0; in this case there are none
    cubeArray foreach { (_, value) => assertTrue(isData(value)) }

    // TODO: normalize data
    val cubeArrayNormalized = preprocessDataCube(cubeArray, bands)

    val (level0, level1) = processWindowOnnx(cubeArrayNormalized, modelPath)

    // TODO: resample to 20m and 40m respectively
    SinglebandGeoTiff(level0, extent, crs).write("/tmp/level0_20m.tif")
    SinglebandGeoTiff(level1, extent, crs).write("/tmp/level1_40m.tif")
  }

  private def sentinel2Tile(bands: Seq[String]): (Raster[MultibandTile], CRS) = {
    val files = Files.list(Paths.get("/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2021/09/07/S2B_20210907T104619_31UFS_TOC_V210")).toScala(Seq)

    val bandRasterSources = for {
      band <- bands
      bandFile <- files.find(_.toString contains band)
    } yield GeoTiffRasterSource(bandFile.toString)

    val crs = bandRasterSources.head.crs

    val tileSize: Long = 120

    val rasters = for {
      rs <- bandRasterSources
      raster <- rs.read(GridBounds(0, 0, tileSize - 1, tileSize - 1))
    } yield raster

    val extent = rasters.head.extent
    val multibandTile = MultibandTile(bands = rasters.map(_.tile.band(0)))

    (Raster(multibandTile, extent), crs)
  }

  private def preprocessDataCube(cubeArray: MultibandTile, orderedBands: Seq[String]): MultibandTile = {
    // TODO: scale; original does this by unpickling some objects from disk and applying them to the input
    cubeArray.convert(FloatConstantNoDataCellType)
    // TODO: original adds a new axis in addition to bands/y/x; do it in processWindowOnnx instead?
  }

  private def processWindowOnnx(cubeArrayNormalized: MultibandTile, modelPath: Path): (Tile, Tile)  = {
    // TODO: apply model encoder.onnx and return level0 and level1
    assert(Files.exists(modelPath))

    val env = OrtEnvironment.getEnvironment()
    val tensor = OnnxTensor.createTensor(env, cubeArrayNormalized) // TODO: load cubeArrayNormalized to tensor
    val (ortSession, ortInputName) = loadOrtSession(modelPath, env)
    val ortInputs = Map(ortInputName -> tensor).asJava

    val result = ortSession.run(ortInputs)
    // TODO: get stuff from result

    ???
  }

  private def loadOrtSession(modelPath: Path, env: OrtEnvironment): (OrtSession, String) = {
    // TODO: start ONNX runtime inference session from model
    // TODO: set intra_op_num_threads to 1

    val options = new SessionOptions
    options.setIntraOpNumThreads(1)
    val session = env.createSession(modelPath.toString, options)

    (session, session.getInputNames.iterator().next())
  }
}
