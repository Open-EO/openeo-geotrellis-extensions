package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis._
import geopyspark.geotrellis.testkit._
import geopyspark.util._
import geotrellis.raster._
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import protos.tileMessages._

object DoubleArrayTileWrapper extends Wrapper2[Tile, ProtoTile]{
  def testOut(sc: SparkContext): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[Tile, ProtoTile](testRdd(sc))

  def testIn(rdd: RDD[Array[Byte]]) =
    PythonTranslator.fromPython[Tile, ProtoTile](rdd, ProtoTile.parseFrom)

  def testRdd(sc: SparkContext): RDD[Tile] = {
    val arr = Array(
      DoubleArrayTile(Array[Double](0, 0, 1, 1), 2, 2),
      DoubleArrayTile(Array[Double](1, 2, 3, 4), 2, 2),
      DoubleArrayTile(Array[Double](5, 6, 7, 8), 2, 2))
    sc.parallelize(arr)
  }
}
