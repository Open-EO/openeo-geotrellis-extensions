package org.openeo.geotrellis.geotiff

import geotrellis.layer._
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.SparkUtils
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}

object RasterRegionPyramidFactoryTest {
  private implicit var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = {
    val conf = SparkUtils.createSparkConf.set("spark.kryoserializer.buffer.max", "512m")
    sc = SparkUtils.createLocalSparkContext("local[2]", getClass.getName, conf)
  }

  @AfterAll
  def tearDownSpark(): Unit = {
    sc.stop()
  }

  private class LazyObject extends Serializable {
    lazy val get: Array[Int] = { println(s"${Thread.currentThread()} called LazyObject#get"); new Array[Int](1024 * 1024) }
  }
}

@Disabled("no need to run automatically as it's a POC")
@Test
class RasterRegionPyramidFactoryTest {
  import RasterRegionPyramidFactoryTest._

  @Test
  def layer(): Unit = {
    val pyramidFactory = new RasterRegionPyramidFactory
    val zoom = 14

    val data = pyramidFactory.data(zoom)

    println(data.toDebugString)
    assertTrue(data.partitioner.get.isInstanceOf[SpacePartitioner[_]], data.partitioner.toString)

    val mask = pyramidFactory.mask(zoom)
    assertTrue(mask.partitioner == data.partitioner)

    val joined =
      data.spatialJoin(mask)
        // map() doesn't preserve partitioning, and mapValues() doesn't give you the SpaceTimeKey
        .mapPartitions(pairs => pairs.map { case (key @ SpaceTimeKey(col, row, _), (dataTile, maskTile)) => (key, if ((col + row) % 2 == 0) dataTile else maskTile.map { (_, value) => value * 255 }) }, preservesPartitioning = true)

    val joinedLayer = ContextRDD(joined, data.metadata).cache()
    assertTrue(joinedLayer.partitioner == data.partitioner)

    joinedLayer
      .map { case (key, tile) => (key, tile.dimensions) }
      .foreach(println)

    val Raster(tile, extent) = joinedLayer.toSpatial().stitch()

    GeoTiff(tile, extent, joinedLayer.metadata.crs).write("/tmp/checkered.tif")
  }

  @Test
  def lazyValues(): Unit = {
    val lazies = sc.parallelize(0 until 100)
      .map(i => (i, new LazyObject))
      .mapValues(_.get)
      .repartition(8)

    println(lazies.toDebugString)

    println(lazies.count())
  }
}
