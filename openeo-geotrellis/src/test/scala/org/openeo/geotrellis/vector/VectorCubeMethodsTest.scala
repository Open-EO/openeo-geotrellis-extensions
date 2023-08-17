package org.openeo.geotrellis.vector

import geotrellis.layer.{KeyBounds, SpatialKey}
import geotrellis.proj4.CRS
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Extent
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Test}

object VectorCubeMethodsTest {

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


class VectorCubeMethodsTest {
  import VectorCubeMethodsTest._

  @Test def testVectorToRaster(): Unit = {
    val path = getClass.getResource("/org/openeo/geotrellis/geometries/input_vector_cube.geojson").getPath
    val target_resolution = 1
    val target_crs = "EPSG:4326"
    val cube = VectorCubeMethods.vectorToRaster(path, target_resolution, target_crs)
    assert(cube.metadata.crs == CRS.fromEpsgCode(4326))
    assert(cube.metadata.layout.cellheight == 1.0)
    assert(cube.metadata.layout.cellwidth == 1.0)
    assert(cube.metadata.layout.cols == 256)
    assert(cube.metadata.layout.rows == 256)
    assert(cube.metadata.bounds == KeyBounds(SpatialKey(0, 0), SpatialKey(0, 0)))
    assert(cube.metadata.layout.extent == Extent(1.0, -252.0, 257.0, 4.0))
  }

}
