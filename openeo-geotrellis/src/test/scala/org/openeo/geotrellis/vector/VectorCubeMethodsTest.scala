package org.openeo.geotrellis.vector

import geotrellis.layer.{SpatialKey}
import geotrellis.raster.{MultibandTile}
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Feature, Geometry}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.Assert.{assertEquals}
import org.openeo.geotrellis.LayerFixtures.sentinel2B04Layer
import org.openeo.geotrellis.vector.VectorCubeMethods.extractFeatures
import geotrellis.spark.{MultibandTileLayerRDD}

import java.util

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
    val cube: MultibandTileLayerRDD[SpatialKey] = VectorCubeMethods.vectorToRaster(path, targetDataCube)
    assertEquals(targetDataCube.metadata.crs, cube.metadata.crs)
    assertEquals(targetDataCube.metadata.cellheight.toInt, cube.metadata.cellheight.toInt)
    assertEquals(targetDataCube.metadata.cellwidth.toInt, cube.metadata.cellwidth.toInt)
    assertEquals(targetDataCube.metadata.bounds.get.toSpatial, cube.metadata.bounds)
    assertEquals(targetDataCube.metadata.extent, cube.metadata.extent)

    val cubeTiles: Array[MultibandTile] = cube.collect().map(_._2)
    val features: Seq[Feature[Geometry, Double]] = extractFeatures(path, targetDataCube.metadata.crs, targetDataCube.metadata.layout)
    val actualValues: Set[Int] = cubeTiles.map(_.band(0).toArray().toSet).toSet.flatten.filter(_ != -2147483648)
    val expectedValues = features.map(_.data).toSet
    assertEquals(expectedValues, actualValues)
    // saveSingleNetCDFSpatial(cube, "/tmp/testVectorToRaster.nc", new util.ArrayList(util.Arrays.asList("Band1")), null, null, 1)
  }

}
