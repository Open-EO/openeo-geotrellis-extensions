package org.openeo.geotrellis.udf

import geotrellis.layer.{Bounds, KeyBounds, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.raster.render.ColorRamp
import geotrellis.raster.{ArrayMultibandTile, FloatArrayTile, MultibandTile, MutableArrayTile, Raster, Tile, TileLayout}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, _}
import geotrellis.vector.{Extent, MultiPolygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.geotiff.saveRDD
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}

import java.time.ZonedDateTime
import java.util
import scala.collection.mutable.ListBuffer
import scala.io.Source

object UdfTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    sc = SparkUtils.createLocalSparkContext("local[*]", classOf[UdfTest].getName, sparkConf)
  }

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

/*
Note: Ensure that the python environment has all the required modules installed.
Numpy should be installed before Jep for off-heap memory tiles to work!

Note: In order to run these tests you need to set several environment variables.
If you use the virtual environment venv (with JEP and Numpy installed):
1. LD_LIBRARY_PATH = .../venv/lib/python3.6/site-packages/jep
  This will look for the shared library 'jep.so'. This is the compiled C code that binds Java and Python.
2. PATH = .../venv/bin:$PATH
  This will ensure your virtual environment is used instead of your default python interpreter.
*/
class UdfTest {

  /*
    Supported CellTypes: Float
    Unsupported CellTypes: Bit, Byte, Ubyte, Short, UShort, Int, Float, Double
   */
  @Test
  def testSimpleDatacubeOperationsFloat(): Unit = {
    val filename = "/org/openeo/geotrellis/udf/simple_datacube_operations.py"
    val source = Source.fromURL(getClass.getResource(filename))
    val code = source.getLines.mkString("\n")
    source.close()

    val zeroTile: MutableArrayTile = FloatArrayTile.fill(0, 256, 256)
    val multibandTile: MultibandTile = new ArrayMultibandTile(Array(zeroTile).asInstanceOf[Array[Tile]])
    val extent: Extent = new Extent(0,0,10,10)
    val tileLayout = new TileLayout(1, 1, zeroTile.cols.asInstanceOf[Integer], zeroTile.rows.asInstanceOf[Integer])

    val tileLayerRDD: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, multibandTile, tileLayout).asInstanceOf[ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]]

    tileLayerRDD.values.first().bands(0).foreach(e => assert(e == 0))
    val resultRDD = Udf.runUserCode(code, tileLayerRDD, new util.ArrayList[String](), new util.HashMap[String, Any]())
    resultRDD.values.first().bands(0).foreach(e => assert(e == 60))
  }
}

