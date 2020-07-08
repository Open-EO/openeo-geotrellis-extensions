package org.openeo.geotrellis

import java.nio.file.Paths

import geotrellis.spark.util.SparkUtils
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{BeforeClass, Test}

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
    val outputPath = Paths.get("polygons.geojson")
    new OpenEOProcesses().vectorize(layer,outputPath.toString)

    val json: JsonFeatureCollection = GeoJson.fromFile[JsonFeatureCollection](outputPath.toString)
    print(json)
  }
}
