package org.openeo.geotrellis

import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.{ArrayMultibandTile, DoubleArrayTile, Tile}
import geotrellis.spark.MultibandTileLayerRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrelliscommon.CubeProcessRegistry

import java.util.Collections

object CubeProcessRegistryTest {

  private var _sc: Option[SparkContext] = None

  def sc: SparkContext = _sc.getOrElse(throw new IllegalStateException("SparkContext not initialised"))

  @BeforeAll
  def startSpark(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName(getClass.getSimpleName)
      .set("spark.driver.bindAddress", "127.0.0.1")
    _sc = Some(new SparkContext(conf))
  }

  @AfterAll
  def stopSpark(): Unit = {
    _sc.foreach(_.stop())
    _sc = None
  }
}

class CubeProcessRegistryTest {

  /** Build a minimal SpaceTimeKey datacube from a ramp tile (value = col index). */
  private def demCube(): MultibandTileLayerRDD[SpaceTimeKey] = {
    val tile: Tile = DoubleArrayTile.fill(0.0, 128, 128).mapDouble((c, _, _) => c.toDouble)
    val multiband = new ArrayMultibandTile(Array[Tile](tile))
    LayerFixtures.buildSpatioTemporalDataCube(
      java.util.Arrays.asList(tile),
      Seq("2021-01-01T00:00:00Z")
    )
  }

  @Test
  def aspectIsRegistered(): Unit = {
    CubeProcessRegistry.clear()
    CubeProcessRegistry.register(new OpenEOProcesses())

    assertTrue(CubeProcessRegistry.hasProcess("aspect"),
      "CubeProcessRegistry should have 'aspect' after registering OpenEOProcesses")
  }

  @Test
  def aspectIsListedInProcesses(): Unit = {
    CubeProcessRegistry.clear()
    CubeProcessRegistry.register(new OpenEOProcesses())

    val ids = CubeProcessRegistry.processIds()
    assertTrue(ids.contains("aspect"), s"processIds() should contain 'aspect', got: $ids")
  }

  @Test
  def aspectInvokeReturnsNonNullResult(): Unit = {
    CubeProcessRegistry.clear()
    CubeProcessRegistry.register(new OpenEOProcesses())

    val cube = demCube()
    val result = CubeProcessRegistry.invoke(cube, "aspect", Collections.emptyMap[String, AnyRef]())

    assertNotNull(result, "invoke('aspect') should return a non-null result")
  }

  @Test
  def aspectInvokeReturnsDatacube(): Unit = {
    CubeProcessRegistry.clear()
    CubeProcessRegistry.register(new OpenEOProcesses())

    val cube = demCube()
    val result = CubeProcessRegistry.invoke(cube, "aspect", Collections.emptyMap[String, AnyRef]())

    assertTrue(result.isInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]],
      "invoke('aspect') result should be a MultibandTileLayerRDD")
  }
}
