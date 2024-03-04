package org.openeo.geotrellis.vector

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.MultibandTile
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Feature, Geometry}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.LayerFixtures
import org.openeo.geotrellis.LayerFixtures.sentinel2B04Layer
import org.openeo.geotrellis.netcdf.NetCDFRDDWriter.{saveSingleNetCDFSpatial}
import org.openeo.geotrellis.vector.VectorCubeMethods.extractFeatures

import java.time.ZonedDateTime
import scala.collection.mutable

object VectorCubeMethodsTest {

  private var _sc: Option[SparkContext] = None

  private def sc: SparkContext = {
    if (_sc.isEmpty) {
      val config = new HdfsConfiguration
      //config.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(config)

      val conf = new SparkConf().setMaster("local[2]") //.set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.kryoserializer.buffer.max", "512m")
        .set("spark.rdd.compress", "true")
      //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      _sc = Some(SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, conf))
    }
    _sc.get
  }

  @BeforeClass
  def setUpSpark_BeforeClass(): Unit = sc

  @BeforeAll
  def setUpSpark_BeforeAll(): Unit = sc

  var gotAfterAll = false

  @AfterAll
  def tearDownSpark_AfterAll(): Unit = {
    gotAfterAll = true
    maybeStopSpark()
  }

  var gotAfterClass = false

  @AfterClass
  def tearDownSpark_AfterClass(): Unit = {
    gotAfterClass = true;
    maybeStopSpark()
  }

  def maybeStopSpark(): Unit = {
    if (gotAfterAll && gotAfterClass) {
      if (_sc.isDefined) {
        _sc.get.stop()
        _sc = None
      }
    }
  }
}


class VectorCubeMethodsTest {

  @Test def testVectorToRaster(): Unit = {
    val path = getClass.getResource("/org/openeo/geotrellis/geometries/input_vector_cube.geojson").getPath
    val targetDataCube = sentinel2B04Layer
    val cube: MultibandTileLayerRDD[SpatialKey] = VectorCubeMethods.vectorToRasterSpatial(path, targetDataCube)
    assertEquals(targetDataCube.metadata.crs, cube.metadata.crs)
    assertEquals(targetDataCube.metadata.cellheight.toInt, cube.metadata.cellheight.toInt)
    assertEquals(targetDataCube.metadata.cellwidth.toInt, cube.metadata.cellwidth.toInt)
    assertEquals(targetDataCube.metadata.bounds.get.toSpatial, cube.metadata.bounds)
    assertEquals(targetDataCube.metadata.extent, cube.metadata.extent)

    val cubeTiles: Array[MultibandTile] = cube.collect().map(_._2)
    val features: Map[String, Seq[(String, mutable.Buffer[Feature[Geometry, Double]])]] = extractFeatures(path, targetDataCube.metadata.crs, targetDataCube.metadata.layout)

    assert(features.keys.size == 1)
    assert(features.keys.head == "")
    val featureBands: Seq[(String, mutable.Buffer[Feature[Geometry, Double]])] = features("")
    for (tile <- cubeTiles) {
      val featureBandValues: Seq[Set[Double]] = featureBands.map(_._2.map(_.data).toSet.filter(!_.isNaN)).toSeq
      val cubeBandValues: Seq[Set[Int]] = tile.bands.map(_.toArray().toSet.filter(_ != -2147483648))
      assertEquals(featureBandValues, cubeBandValues)
    }

    // saveSingleNetCDFSpatial(cube, "/tmp/testVectorToRasterSpatial.nc", new java.util.ArrayList(java.util.Arrays.asList("Band1")), null, null, 1)
  }

  @Test
  def testVectorToRasterTemporal(): Unit = {
    val path = getClass.getResource("/org/openeo/geotrellis/geometries/input_vector_cube_temporal.geojson").getPath
    val targetDataCube = LayerFixtures.sentinel2B04Layer
    val cube: MultibandTileLayerRDD[SpaceTimeKey] = VectorCubeMethods.vectorToRasterTemporal(path, targetDataCube)
    assertEquals(targetDataCube.metadata.crs, cube.metadata.crs)
    assertEquals(targetDataCube.metadata.cellheight.toInt, cube.metadata.cellheight.toInt)
    assertEquals(targetDataCube.metadata.cellwidth.toInt, cube.metadata.cellwidth.toInt)
    assertEquals(targetDataCube.metadata.bounds.get.toSpatial, cube.metadata.bounds.get.toSpatial)
    assertEquals(targetDataCube.metadata.extent, cube.metadata.extent)

    val cubeTiles: Array[(SpaceTimeKey, MultibandTile)] = cube.collect()
    val features: Map[String, Seq[(String, mutable.Buffer[Feature[Geometry, Double]])]] = extractFeatures(path, targetDataCube.metadata.crs, targetDataCube.metadata.layout)
    val featureDates = features.keys
    val format = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val cubeDates = cubeTiles.map(_._1.time.format(format))
    assertEquals(featureDates.toSet, cubeDates.toSet)

    for (tile <- cubeTiles) {
      val date: ZonedDateTime = tile._1.time
      val featureBands: Seq[(String, mutable.Buffer[Feature[Geometry, Double]])] = features(date.format(format))
      val featureBandValues: Seq[Set[Double]] = featureBands.map(_._2.map(_.data).toSet.filter(!_.isNaN)).toSeq
      val cubeBandValues: Seq[Set[Int]] = tile._2.bands.map(_.toArray().toSet.filter(_ != -2147483648))
      assertEquals(featureBandValues, cubeBandValues)
    }

    // saveSingleNetCDF(cube, "/tmp/testVectorToRasterTemporal.nc", new java.util.ArrayList(java.util.Arrays.asList("Band1", "Band2")), null, null, 1)
  }
}
