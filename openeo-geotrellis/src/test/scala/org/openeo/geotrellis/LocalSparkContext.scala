package org.openeo.geotrellis

import geotrellis.spark.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.ClassRule
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.rules.ExternalResource

protected trait LocalSparkContextBase {
  protected var _sc: Option[SparkContext] = None

  implicit def sc: SparkContext = {
    if (_sc.isEmpty) {
      val conf = new SparkConf()
        .set("spark.kryoserializer.buffer.max", "512m")
        .set("spark.rdd.compress", "true")
      _sc = Some(SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, conf))
    }
    _sc.get
  }

  def tearDownSpark(): Unit = {
    if (_sc.isDefined) {
      _sc.get.stop()
      _sc = None
    }
  }
}

//noinspection JUnitMalformedDeclaration
trait LocalSparkContext extends LocalSparkContextBase {
  @ClassRule
  def sparkContext: ExternalResource = new ExternalResource {
    override def before(): Unit = sc

    override def after(): Unit = tearDownSpark()
  }
}

//noinspection JUnitMalformedDeclaration
trait LocalSparkContextJupyter extends LocalSparkContextBase {

  @BeforeAll
  def setupSpark(): Unit = sc

  @AfterAll
  def after(): Unit = tearDownSpark()
}
