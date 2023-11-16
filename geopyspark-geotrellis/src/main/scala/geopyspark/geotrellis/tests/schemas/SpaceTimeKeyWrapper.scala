package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis._
import geopyspark.geotrellis.testkit._
import geopyspark.util._
import geotrellis.layer._
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import protos.keyMessages._

import java.time.Instant


object SpaceTimeKeyWrapper extends Wrapper2[SpaceTimeKey, ProtoSpaceTimeKey] {
  def testOut(sc: SparkContext): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[SpaceTimeKey, ProtoSpaceTimeKey](testRdd(sc))
  def testIn(rdd: RDD[Array[Byte]]) =
    PythonTranslator.fromPython[SpaceTimeKey, ProtoSpaceTimeKey](rdd, ProtoSpaceTimeKey.parseFrom)

  def testRdd(sc: SparkContext): RDD[SpaceTimeKey] = {
    val arr = Array(
      SpaceTimeKey(7, 3,Instant.parse("2016-08-24T09:00:00Z").toEpochMilli),
      SpaceTimeKey(9, 4,Instant.parse("2016-08-24T09:00:00Z").toEpochMilli),
      SpaceTimeKey(11, 5, Instant.parse("2016-08-24T09:00:00Z").toEpochMilli))
    sc.parallelize(arr)
  }
}
