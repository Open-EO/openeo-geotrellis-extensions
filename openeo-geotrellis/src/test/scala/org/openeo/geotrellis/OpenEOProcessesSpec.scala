package org.openeo.geotrellis

import geotrellis.raster.mapalgebra.focal.{Convolve, Kernel, TargetCell}
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ArrayMultibandTile, DoubleArrayTile, Tile, TileLayout}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.spark._
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Test}

object OpenEOProcessesSpec{

  private var sc: SparkContext = _

  @BeforeClass
  def setUpSpark(): Unit = {
    sc = {
      val config = new HdfsConfiguration
      //config.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(config)

      val conf = new SparkConf().setMaster("local[2]")//.set("spark.driver.bindAddress", "127.0.0.1")
      //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, conf)
    }
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    sc.stop()
  }

}

class OpenEOProcessesSpec extends RasterMatchers {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  @Test
  def applyMaskFFT(): Unit = {
    val tile: Tile = DoubleArrayTile.fill(1.0,1280, 1280)
    val tileSize = 256
    val datacube = TileLayerRDDBuilders.createMultibandTileLayerRDD(OpenEOProcessesSpec.sc, new ArrayMultibandTile(Array[Tile](tile)), new TileLayout(1 + tile.cols / tileSize, 1 + tile.rows / tileSize, tileSize, tileSize))
    val kernel: Tile = DoubleArrayTile.fill(1.0,61, 61)

    val resultCube = new OpenEOProcesses().apply_kernel_spatial(datacube, kernel)

    val theResultTile = time{ resultCube.stitch().tile.band(0) }
    val expectedConvolution = time{Convolve.apply(tile, new Kernel(kernel), Option.empty, TargetCell.All)}
    assertEqual(expectedConvolution,theResultTile)
  }
}
