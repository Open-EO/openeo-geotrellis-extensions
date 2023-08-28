package org.openeo.geotrellis.vector

import geotrellis.layer.{KeyBounds, LayoutDefinition, SpatialKey}
import geotrellis.proj4.CRS
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Extent
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertTrue}
import org.openeo.geotrellis.LayerFixtures.sentinel2B04Layer

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
    val targetDataCube = sentinel2B04Layer
    val cube = VectorCubeMethods.vectorToRaster(path, targetDataCube)
    assertEquals(targetDataCube.metadata.crs, cube.metadata.crs)
    assertEquals(targetDataCube.metadata.cellheight.toInt, cube.metadata.cellheight.toInt)
    assertEquals(targetDataCube.metadata.cellwidth.toInt, cube.metadata.cellwidth.toInt)
    val expectedBounds = KeyBounds(SpatialKey(0,0),SpatialKey(173,129))
    assertEquals(expectedBounds, cube.metadata.bounds)
    val expectedExtent = Extent(277438.26352113695, 110530.15880237566, 722056.3830076031, 442397.8228986237)
    assertEquals(expectedExtent, cube.metadata.extent)
  }

}
