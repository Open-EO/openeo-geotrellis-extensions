package org.openeo.geotrellis

import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.{ArrayMultibandTile, DoubleArrayTile, Tile}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.{SparkConf, SparkContext, SparkTestHelper}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrelliscommon.CubeProcessRegistry

import java.util.Collections
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

object CubeProcessRegistryTest {

  private var _sc: Option[SparkContext] = None

  def sc: SparkContext = _sc.getOrElse(throw new IllegalStateException("SparkContext not initialised"))

  @BeforeAll
  def startSpark(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1,2]")
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
  def failOnceIsRegisteredAndPassesDataThrough(): Unit = {
    CubeProcessRegistry.clear()
    // Registering the provider's singleton mirrors what the SPI loader does on first use.
    CubeProcessRegistry.register(new FaultInjectionProcessesProvider().getInstance())
    assertTrue(CubeProcessRegistry.hasProcess("fail_once"))

    val cube = demCube()
    val result = CubeProcessRegistry.invoke(cube, "fail_once", Collections.emptyMap[String, AnyRef]())
      .asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]]
    // The first stage attempt fails, Spark retries it and the data passes through unchanged.
    assertEquals(cube.count(), result.count())
    assertEquals(cube.metadata, result.metadata)
  }

  @Test
  def failOnceFailsTheGivenPartition(): Unit = {
    CubeProcessRegistry.clear()
    CubeProcessRegistry.register(new FaultInjectionProcessesProvider().getInstance())

    val demo = demCube()
    val cube = ContextRDD(demo.repartition(3), demo.metadata)
    val failedTasks = new ConcurrentLinkedQueue[(Int, Int)]()
    val listener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
        if (!taskEnd.taskInfo.successful) failedTasks.add((taskEnd.taskInfo.partitionId, taskEnd.taskInfo.attemptNumber))
    }

    // A Python int arrives as a java.lang.Integer or Long through py4j.
    val args = Map[String, AnyRef]("partition" -> java.lang.Long.valueOf(2)).asJava
    val result = CubeProcessRegistry.invoke(cube, "fail_once", args).asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]]
    CubeProcessRegistryTest.sc.addSparkListener(listener)
    try {
      assertEquals(cube.count(), result.count())
      SparkTestHelper.waitUntilListenerBusEmpty(CubeProcessRegistryTest.sc)
    } finally {
      CubeProcessRegistryTest.sc.removeSparkListener(listener)
    }
    assertEquals(Seq((2, 0)), failedTasks.asScala.toSeq, "Only the first attempt of the task for partition 2 should fail")
  }

  @Test
  def failOnceRejectsPartitionOutOfRange(): Unit = {
    CubeProcessRegistry.clear()
    CubeProcessRegistry.register(new FaultInjectionProcessesProvider().getInstance())

    val cube = demCube()
    val args = Map[String, AnyRef]("partition" -> Integer.valueOf(cube.getNumPartitions)).asJava
    val exception = assertThrows(classOf[java.lang.reflect.InvocationTargetException],
      () => CubeProcessRegistry.invoke(cube, "fail_once", args))
    assertTrue(exception.getCause.isInstanceOf[IllegalArgumentException])
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
