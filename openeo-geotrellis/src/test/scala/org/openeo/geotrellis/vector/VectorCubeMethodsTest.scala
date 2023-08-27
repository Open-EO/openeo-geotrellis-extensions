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
    assertEquals(cube.metadata.crs, targetDataCube.metadata.crs)
    assertTrue(cube.metadata.layout.cellheight == targetDataCube.metadata.layout.cellheight)
    assertTrue(cube.metadata.layout.cellheight == targetDataCube.metadata.layout.cellheight)
    assertEquals(cube.metadata.layout.cols, targetDataCube.metadata.layout.cols)
    assertEquals(cube.metadata.layout.rows, targetDataCube.metadata.layout.rows)
    assertEquals(cube.metadata.bounds, KeyBounds(SpatialKey(0, 0), SpatialKey(0, 0)))
    val trueExtent = Extent(1.0, 100.0, 500.0, 400.0)
    assertTrue(cube.metadata.extent == trueExtent)
  }

}
