package org.openeo.geotrellis

import geotrellis.layer.SpatialKey
import geotrellis.raster.IntArrayTile
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SparkRuntimeCompatibilityTest {
  @Test
  def sparkSqlAndRasterShuffle(): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkRuntimeCompatibilityTest")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      assertEquals(10L, spark.range(0, 20).where("id % 2 = 0").count())

      val tiles = spark.sparkContext.parallelize(Seq(
        (SpatialKey(0, 0), IntArrayTile.fill(1, 1, 1)),
        (SpatialKey(0, 0), IntArrayTile.fill(2, 1, 1)),
        (SpatialKey(1, 0), IntArrayTile.fill(3, 1, 1))
      ), 2)
      val sums = tiles.mapValues(_.get(0, 0)).reduceByKey(_ + _).collect().toMap
      assertEquals(Map(SpatialKey(0, 0) -> 3, SpatialKey(1, 0) -> 3), sums)
    } finally {
      spark.stop()
    }
  }
}
