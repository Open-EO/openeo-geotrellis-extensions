package org.openeo.geotrellis

import geotrellis.spark.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.ClassRule
import org.junit.rules.ExternalResource

trait LocalSparkContext {
  def appName: String = getClass.getName
  def sparkConf: SparkConf = new SparkConf
  def sparkMaster: String = "local[*]"

  var sc: SparkContext = _

  @ClassRule
  def sparkContext: ExternalResource = new ExternalResource {
    override def before(): Unit =
      sc = SparkUtils.createLocalSparkContext(sparkMaster, appName, sparkConf)

    override def after(): Unit =
      sc.stop()
  }
}
