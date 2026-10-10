package org.openeo.geotrellis

import geotrellis.layer.{KeyBounds, SpaceTimeKey, TemporalKey}
import geotrellis.raster.{DoubleArrayTile, DoubleCellType, DoubleConstantNoDataCellType, MultibandTile, Tile, isNoData}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrelliscommon.CubeProcessRegistry

import java.lang.reflect.InvocationTargetException
import java.time.{ZoneOffset, ZonedDateTime}
import scala.jdk.CollectionConverters._

object ResampleCubeTemporalTest {

  private var _sc: Option[SparkContext] = None

  @BeforeAll
  def startSpark(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
      .set("spark.driver.bindAddress", "127.0.0.1")
    _sc = Some(SparkContext.getOrCreate(conf))
  }

  @AfterAll
  def stopSpark(): Unit = {
    _sc.foreach(_.stop())
    _sc = None
  }
}

class ResampleCubeTemporalTest {

  private val size = 16

  private def constant(value: Double): Tile = DoubleArrayTile.fill(value, size, size)

  /** value in the left half of the tile, no-data in the right half */
  private def leftHalf(value: Double): Tile =
    DoubleArrayTile.empty(size, size).mapDouble((col, _, _) => if (col < size / 2) value else Double.NaN)

  private def date(day: Int): String = f"2021-01-$day%02dT00:00:00Z"

  private def cube(tilesByDate: (String, Tile)*): MultibandTileLayerRDD[SpaceTimeKey] = {
    val cubes = tilesByDate.map { case (d, tile) => LayerFixtures.buildSpatioTemporalDataCube(java.util.Arrays.asList(tile), Seq(d)) }
    val keys = cubes.map(_.metadata.bounds.get)
    val bounds = KeyBounds(keys.map(_.minKey).minBy(_.instant), keys.map(_.maxKey).maxBy(_.instant))
    ContextRDD(cubes.map(_.rdd).reduce(_ union _), cubes.head.metadata.copy(bounds = bounds))
  }

  private def byDay(result: MultibandTileLayerRDD[SpaceTimeKey]): Map[Int, MultibandTile] =
    result.collect().map { case (key, tile) => (key.time.withZoneSameInstant(ZoneOffset.UTC).getDayOfMonth, tile) }.toMap

  private def targetCube(days: Int*): MultibandTileLayerRDD[SpaceTimeKey] = cube(days.map(d => (date(d), constant(0))): _*)

  @Test
  def nearestNeighborWithoutValidWithin(): Unit = {
    val data = cube(date(1) -> constant(1), date(5) -> constant(5), date(10) -> constant(10))
    // day 3 is as close to day 1 as to day 5: the earlier one wins
    val result = new OpenEOProcesses().resampleCubeTemporal(data, targetCube(2, 3, 8, 20))

    val tiles = byDay(result)
    assertEquals(Set(2, 3, 8, 20), tiles.keySet)
    assertEquals(Map(2 -> 1.0, 3 -> 1.0, 8 -> 10.0, 20 -> 10.0), tiles.map { case (d, t) => (d, t.band(0).getDouble(0, 0)) })
    assertEquals(ZonedDateTime.parse(date(2)).toInstant.toEpochMilli, result.metadata.bounds.get.minKey.instant)
    assertEquals(ZonedDateTime.parse(date(20)).toInstant.toEpochMilli, result.metadata.bounds.get.maxKey.instant)
    assertEquals(data.metadata.layout, result.metadata.layout)
    assertEquals(data.metadata.cellType, result.metadata.cellType)
  }

  @Test
  def nearestNeighborIgnoresValuesWithoutValidWithin(): Unit = {
    val data = cube(date(1) -> constant(1), date(4) -> leftHalf(4))
    val tile = byDay(new OpenEOProcesses().resampleCubeTemporal(data, targetCube(5)))(5).band(0)

    assertEquals(4.0, tile.getDouble(0, 0))
    assertTrue(isNoData(tile.getDouble(size - 1, 0)))
  }

  @Test
  def validWithinTakesNearestValidValuePerPixel(): Unit = {
    val data = cube(date(1) -> constant(1), date(4) -> leftHalf(4), date(30) -> constant(30))
    val processes = new OpenEOProcesses()

    val within5Days = byDay(processes.resampleCubeTemporal(data, targetCube(5, 15), Some(5)))
    assertEquals(Set(5, 15), within5Days.keySet)
    // nothing within 5 days of day 15
    val day15 = within5Days(15)
    assertEquals(1, day15.bandCount)
    assertTrue(day15.band(0).isNoDataTile)
    val tile = within5Days(5).band(0)
    assertEquals(4.0, tile.getDouble(0, 0))
    assertEquals(1.0, tile.getDouble(size - 1, 0))

    val within2Days = byDay(processes.resampleCubeTemporal(data, targetCube(5), Some(2)))(5).band(0)
    assertEquals(4.0, within2Days.getDouble(0, 0))
    assertTrue(isNoData(within2Days.getDouble(size - 1, 0)))
  }

  @Test
  def validWithinSwitchesToNoDataCellTypeForMissingTargets(): Unit = {
    val raw = cube(date(1) -> constant(1).convert(DoubleCellType), date(30) -> constant(0).convert(DoubleCellType))
    val result = new OpenEOProcesses().resampleCubeTemporal(raw, targetCube(1, 15), Some(5))

    assertEquals(DoubleConstantNoDataCellType, result.metadata.cellType)
    val tiles = byDay(result)
    assertEquals(Set(1, 15), tiles.keySet)
    assertEquals(1.0, tiles(1).band(0).getDouble(0, 0))
    assertTrue(tiles(15).band(0).isNoDataTile)
  }

  @Test
  def isExportedThroughCubeProcessRegistry(): Unit = {
    CubeProcessRegistry.clear()
    // Registering the provider's singleton mirrors what the SPI loader does on first use.
    CubeProcessRegistry.register(new ResampleProcessesProvider().getInstance())
    assertTrue(CubeProcessRegistry.hasProcess("resample_cube_temporal"))

    val data = cube(date(1) -> constant(1), date(4) -> leftHalf(4))
    // A Python int arrives as a java.lang.Integer or Long through py4j, None as null.
    val args = new java.util.HashMap[String, AnyRef](Map[String, AnyRef](
      "target" -> targetCube(5), "valid_within" -> java.lang.Long.valueOf(5), "dimension" -> null).asJava)
    val result = CubeProcessRegistry.invoke(data, "resample_cube_temporal", args).asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]]

    val tile = byDay(result)(5).band(0)
    assertEquals(4.0, tile.getDouble(0, 0))
    assertEquals(1.0, tile.getDouble(size - 1, 0))
  }

  @Test
  def rejectsMissingOrInvalidTarget(): Unit = {
    CubeProcessRegistry.clear()
    CubeProcessRegistry.register(new ResampleProcessesProvider().getInstance())
    val data = cube(date(1) -> constant(1))

    for (args <- Seq(Map[String, AnyRef](), Map[String, AnyRef]("target" -> "2021-01-05"))) {
      val exception = assertThrows(classOf[InvocationTargetException],
        () => CubeProcessRegistry.invoke(data, "resample_cube_temporal", args.asJava))
      assertTrue(exception.getCause.isInstanceOf[IllegalArgumentException])
    }
  }
}
