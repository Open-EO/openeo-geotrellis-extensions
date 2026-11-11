package org.openeo.geotrellis.reproject

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark._
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Extent
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.OpenEOProcesses
import org.openeo.geotrelliscommon.{ConfigurableSpaceTimePartitioner, SparseSpaceTimePartitioner}

import java.time.ZonedDateTime

object TileRDDReprojectTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[2]", classOf[TileRDDReprojectTest].getSimpleName, new SparkConf())

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class TileRDDReprojectTest {
  import TileRDDReprojectTest._

  private val crs = CRS.fromEpsgCode(32631)
  private val tileSize = 256
  private val layoutCols = 16
  private val sourceCellSize = 20.0
  private val extent = {
    val size = layoutCols * tileSize * sourceCellSize
    Extent(500000, 5600000, 500000 + size, 5600000 + size)
  }
  private val sourceLayout = LayoutDefinition(extent, TileLayout(layoutCols, layoutCols, tileSize, tileSize))
  // 16x coarser: 20m to 320m
  private val downsampledLayout = LayoutDefinition(extent, TileLayout(1, 1, tileSize, tileSize))
  private val dates = Seq("2025-08-01T00:00:00Z", "2025-08-02T00:00:00Z", "2025-08-03T00:00:00Z").map(ZonedDateTime.parse)

  private def spaceTimeKeys: Seq[SpaceTimeKey] = for {
    date <- dates
    col <- 0 until layoutCols
    row <- 0 until layoutCols
  } yield SpaceTimeKey(col, row, date)

  private def cube(index: PartitionerIndex[SpaceTimeKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
    val keyBounds = KeyBounds(spaceTimeKeys.min, spaceTimeKeys.max)
    val metadata = TileLayerMetadata(ByteConstantNoDataCellType, sourceLayout, extent, crs, keyBounds)
    val partitioner = SpacePartitioner(keyBounds)(implicitly, implicitly, index)
    val dates = this.dates
    val tileSize = this.tileSize

    val tiles = sc.parallelize(spaceTimeKeys)
      .map(key => key -> MultibandTile(ByteArrayTile.fill((dates.indexOf(key.time) + 1).toByte, tileSize, tileSize)))
      .partitionBy(partitioner)

    ContextRDD(tiles, metadata)
  }

  private def assertValuesPreserved(resampled: MultibandTileLayerRDD[SpaceTimeKey]): Unit = {
    val tiles = resampled.collect()
    assertEquals(dates.map(SpaceTimeKey(0, 0, _)).toSet, tiles.map(_._1).toSet)

    for ((key, tile) <- tiles) {
      val expected = dates.indexOf(key.time) + 1
      assertEquals((expected, expected), tile.band(0).findMinMax)
    }
  }

  @Test
  def downsamplingReducesSourceBoundsSpacePartitioner(): Unit = {
    val input = cube(new ConfigurableSpaceTimePartitioner(indexReduction = 0))
    val inputNumPartitions = input.getNumPartitions

    val (_, resampled) = new OpenEOProcesses().resampleCubeSpatial_spacetime(input, crs, downsampledLayout, NearestNeighbor, null)

    assertTrue(resampled.partitioner.get.isInstanceOf[SpacePartitioner[_]])
    assertTrue(resampled.getNumPartitions * 10 < inputNumPartitions,
      s"expected far fewer than $inputNumPartitions partitions but got ${resampled.getNumPartitions}")
    assertValuesPreserved(resampled)
  }

  @Test
  def downsamplingRetargetsSparseSpaceTimePartitioner(): Unit = {
    val keys = spaceTimeKeys.toArray
    val indices = keys.map(SparseSpaceTimePartitioner.toIndex(_, indexReduction = 0)).distinct.sorted
    val input = cube(new SparseSpaceTimePartitioner(indices, 0, Some(keys)))

    val (_, resampled) = TileRDDReproject(input, crs, Right(downsampledLayout), 16, NearestNeighbor, None)

    val index = resampled.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index
    assertTrue(index.isInstanceOf[SparseSpaceTimePartitioner])
    assertEquals(dates.map(SpaceTimeKey(0, 0, _)).toSet, index.asInstanceOf[SparseSpaceTimePartitioner].theKeys.get.toSet)
    assertEquals(dates.size, resampled.getNumPartitions)
    assertValuesPreserved(resampled)
  }

  @Test
  def partitionerIsKeptWithoutSignificantDownsampling(): Unit = {
    val input = cube(new ConfigurableSpaceTimePartitioner(indexReduction = 0))
    val partitioner = input.partitioner

    // same resolution, other tile size
    val retiledLayout = LayoutDefinition(extent, TileLayout(layoutCols * 2, layoutCols * 2, tileSize / 2, tileSize / 2))
    val (_, resampled) = TileRDDReproject(input, crs, Right(retiledLayout), 0, NearestNeighbor, partitioner)

    assertEquals(partitioner, resampled.partitioner)
  }

  @Test
  def downsamplingWithoutSpacePartitionerFallsBackToHashPartitioner(): Unit = {
    val adapted = TileRDDReproject.adaptPartitionerToDownsampling[SpaceTimeKey](
      partitioner = None, sourceNumPartitions = 1000, tileCountRatio = 1.0 / 256, targetBounds = EmptyBounds,
      remapSpatialKey = key => Seq(key))

    assertEquals(Some(new HashPartitioner(4)), adapted)
  }
}
