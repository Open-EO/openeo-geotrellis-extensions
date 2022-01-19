package org.openeo.geotrellis

import geotrellis.spark.util.SparkUtils
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.ComputeStatsGeotrellisAdapterTest.{getClass, sc}
import org.openeo.geotrellis.aggregate_polygon.SparkAggregateScriptBuilder

import java.util

object AggregateSpatialTest {

  private var sc: SparkContext = _

  @BeforeClass
  def setUpSpark(): Unit = {
    sc = {
      val config = new HdfsConfiguration
      config.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(config)

      val conf = new SparkConf().set("spark.driver.bindAddress", "127.0.0.1")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, conf)
    }
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    sc.stop()
  }
}

class AggregateSpatialTest {

  import AggregateSpatialTest._

  private val computeStatsGeotrellisAdapter = new ComputeStatsGeotrellisAdapter(
    zookeepers = "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181",
    accumuloInstanceName = "hdp-accumulo-instance"
  )

  @Test def computeVectorCube_on_datacube_from_polygons(): Unit = {
    val cube = LayerFixtures.sentinel2B04Layer
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube("max",cube,LayerFixtures.b04Polygons,"/tmp/csvoutput")
  }

  @Test def multiple_statistics(): Unit = {
    val cube = LayerFixtures.sentinel2B04Layer
    val builder= new SparkAggregateScriptBuilder
    val emptyMap = new util.HashMap[String,Object]()
    builder.expressionEnd("min",emptyMap)
    builder.expressionEnd("median",emptyMap)
    builder.expressionEnd("mean",emptyMap)
    builder.expressionEnd("max",emptyMap)
    builder.expressionEnd("sd",emptyMap)
    builder.expressionEnd("sum",emptyMap)
    builder.expressionEnd("count",emptyMap)
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube(builder,cube,LayerFixtures.b04Polygons,"/tmp/csvoutput")
  }
}
