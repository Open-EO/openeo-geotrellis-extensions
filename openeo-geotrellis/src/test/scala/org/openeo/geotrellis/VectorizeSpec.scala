package org.openeo.geotrellis

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import geotrellis.spark.util.SparkUtils
import geotrellis.vector.PolygonFeature
import geotrellis.vector.io.json.JsonFeatureCollection
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{BeforeClass, Test}
import org.openeo.geotrellis.OpenEOProcessesSpec.{getClass, sc}

object VectorizeSpec{

  var sc: SparkContext = _

  @BeforeClass
  def setupSpark() = {
    sc = {
      val conf = new SparkConf().setMaster("local[2]")//.set("spark.driver.bindAddress", "127.0.0.1")
      //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, conf)
    }
  }
}

class VectorizeSpec {



  //TODO better test would be a round-trip: rasterize then vectorize

  @Test
  def vectorize() = {
    val layer = LayerFixtures.ClearNDVILayerForSingleDate()(VectorizeSpec.sc)
    val polygons: Array[PolygonFeature[Int]] = new OpenEOProcesses().vectorize(layer)
    print(polygons)
    val json = JsonFeatureCollection(polygons).asJson

    Files.write(Paths.get("polygons.geojson"), json.toString().getBytes(StandardCharsets.UTF_8))
    print(json)
  }
}
