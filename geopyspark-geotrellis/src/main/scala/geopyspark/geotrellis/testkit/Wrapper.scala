package geopyspark.geotrellis.testkit

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage


abstract class Wrapper2[T, M <: GeneratedMessage] {
  def testOut(sc: SparkContext): JavaRDD[Array[Byte]]
  def testIn(rdd: RDD[Array[Byte]]): Unit
  def testRdd(sc: SparkContext): RDD[T]
}
