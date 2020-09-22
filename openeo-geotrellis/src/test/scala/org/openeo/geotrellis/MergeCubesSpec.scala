package org.openeo.geotrellis

import java.time.ZonedDateTime
import java.util

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{BeforeClass, Test}
import org.openeo.geotrellis.LayerFixtures._


object MergeCubesSpec{

  var sc: SparkContext = _

  @BeforeClass
  def setupSpark() = {
    sc = {
      val conf = new SparkConf().setMaster("local[2]").setAppName(getClass.getSimpleName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName)
      SparkContext.getOrCreate(conf)
    }
  }
}

class MergeCubesSpec {
  @Test def testMergeCubesBasic(): Unit = {
    val celltype: DataType = CellType.fromName("int8raw").withDefaultNoData
    val zeroTile: MutableArrayTile = ByteArrayTile.fill(0.toByte, 256, 256)
    zeroTile.set(0, 0, 1)
    zeroTile.set(0, 1, ByteConstantNoDataCellType.noDataValue)
    val tileLayerRDD: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = tileToSpaceTimeDataCube(zeroTile)
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(tileLayerRDD, tileLayerRDD, null)
    val firstTile: MultibandTile = merged.toJavaRDD.take(1).get(0)._2
    System.out.println("firstTile = " + firstTile)
    assertEquals(4, firstTile.bandCount)
    assertEquals(zeroTile, firstTile.band(0))
    assertEquals(zeroTile, firstTile.band(2))
  }

  @Test def testMergeCubesSumOperator(): Unit = {
    val celltype: DataType = CellType.fromName("int8raw").withDefaultNoData
    val zeroTile: MutableArrayTile = ByteArrayTile.fill(0.toByte, 256, 256)
    zeroTile.set(0, 0, 1)
    zeroTile.set(0, 1, ByteConstantNoDataCellType.noDataValue)
    val tileLayerRDD: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = tileToSpaceTimeDataCube(zeroTile)
    val mergedOr: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(tileLayerRDD, tileLayerRDD, "sum")
    val firstTileSum: MultibandTile = mergedOr.toJavaRDD.take(1).get(0)._2
    System.out.println("firstTileOr = " + firstTileSum)
    assertEquals(2, firstTileSum.bandCount)
    assertEquals(2, firstTileSum.band(0).get(0, 0))
    assertTrue(firstTileSum.band(1).isNoDataTile)
  }

  @Test def testMergeCubeConcat(): Unit = { // Set up
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band3: ArrayTile = ByteArrayTile.fill(5.toByte, 256, 256).convert(CellType.fromName("uint16"))
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band3, band3, band3), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    // Do merge
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, null)
    // Check result
    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect
    assertEquals(2, mergedTimes.size)
    import scala.collection.JavaConversions._
    for (item <- merged.toJavaRDD.collect) {
      assertEquals(5, item._2.bandCount)
      assertEquals(2, item._2.band(0).get(0, 0))
      assertEquals(3, item._2.band(1).get(0, 0))
      assertEquals(5, item._2.band(2).get(0, 0))
      assertEquals(5, item._2.band(3).get(0, 0))
      assertEquals(5, item._2.band(4).get(0, 0))
    }
    assertEquals(CellType.fromName("uint16"), merged.metadata.cellType)
  }


  @Test def testMergeCubeTemporalDisjointNoOp(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(5.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(8.toByte, 256, 256)
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = LayerFixtures.buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z", "2020-03-03T00:00:00Z"))
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = LayerFixtures.buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-11-11T00:00:00Z", "2020-12-12T00:00:00Z"))
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, null)
    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect
    assertEquals(5, mergedTimes.size)
    import scala.collection.JavaConversions._
    for (item <- merged.toJavaRDD.collect) {
      assertEquals(4, item._2.bandCount)
      if (item._1.temporalKey.time.isBefore(ZonedDateTime.parse("2020-10-01T00:00:00Z"))) { // time range with left part bands
        assertEquals(2, item._2.band(0).get(0, 0))
        assertEquals(3, item._2.band(1).get(0, 0))
        assertTrue(item._2.band(2).isNoDataTile)
        assertTrue(item._2.band(3).isNoDataTile)
      }
      else { // time range with right part bands
        assertTrue(item._2.band(0).isNoDataTile)
        assertTrue(item._2.band(1).isNoDataTile)
        assertEquals(5, item._2.band(2).get(0, 0))
        assertEquals(8, item._2.band(3).get(0, 0))
      }
    }
  }

  @Test def testMergeCubePartialOverlapDifference(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(5.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(8.toByte, 256, 256)
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = LayerFixtures.buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = LayerFixtures.buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-02-02T00:00:00Z", "2020-03-03T00:00:00Z"))
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, "subtract")
    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect
    assertEquals(3, mergedTimes.size)
    import scala.collection.JavaConversions._
    for (item <- merged.toJavaRDD.collect) {
      assertEquals(2, item._2.bandCount)
      val month: Int = item._1.temporalKey.time.getMonthValue
      if (month == 1) {
        assertEquals(2, item._2.band(0).get(0, 0))
        assertEquals(3, item._2.band(1).get(0, 0))
      }
      else {
        if (month == 2) {
          assertEquals(-(3), item._2.band(0).get(0, 0))
          assertEquals(-(5), item._2.band(1).get(0, 0))
        }
        else {
          assertEquals(5, item._2.band(0).get(0, 0))
          assertEquals(8, item._2.band(1).get(0, 0))
        }
      }
    }
  }

  @Test def testMergeCubeDifference_SpatialSpatial(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(5.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(8.toByte, 256, 256)
    val cube1: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(MergeCubesSpec.sc,MultibandTile(band1,band2),TileLayout(4,4,256,256))
    val cube2: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(MergeCubesSpec.sc,MultibandTile(band3,band4),cube1.metadata.tileLayout)
    val processes = new OpenEOProcesses()
    val merged: MultibandTileLayerRDD[SpatialKey] = processes.mergeSpatialCubes(cube1, cube2, "subtract")

    import scala.collection.JavaConversions._
    for (item <- merged.toJavaRDD.collect) {
      assertEquals(2, item._2.bandCount)
      assertEquals(-3, item._2.band(0).get(0, 0))
      assertEquals(-5, item._2.band(1).get(0, 0))
    }
  }


  @Test def testMergeCubeDifference_SpatialSpaceTime(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(5.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(8.toByte, 256, 256)
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = LayerFixtures.buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val cube2: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(MergeCubesSpec.sc,MultibandTile(band3,band4),cube1.metadata.tileLayout)
    val processes = new OpenEOProcesses()
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = processes.mergeCubes_SpaceTime_Spatial(ContextRDD(processes.applySpacePartitioner(cube1,cube1.metadata.bounds.get),cube1.metadata), cube2, "subtract",true)
    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect
    assertEquals(2, mergedTimes.size)
    import scala.collection.JavaConversions._
    for (item <- merged.toJavaRDD.collect) {
      assertEquals(2, item._2.bandCount)
      val month: Int = item._1.temporalKey.time.getMonthValue
      if (month == 1) {
        assertEquals(3, item._2.band(0).get(0, 0))
        assertEquals(5, item._2.band(1).get(0, 0))
      }
      else {
        if (month == 2) {
          assertEquals(3, item._2.band(0).get(0, 0))
          assertEquals(5, item._2.band(1).get(0, 0))
        }

      }
    }
  }

  @Test def testMergeCubeFullOverlapNoOp(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(1.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(4.toByte, 256, 256)
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = LayerFixtures.buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = LayerFixtures.buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, null)
    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect
    assertEquals(2, mergedTimes.size)
    import scala.collection.JavaConversions._
    for (item <- merged.toJavaRDD.collect) {
      assertEquals(4, item._2.bandCount)
      assertEquals(1, item._2.band(0).get(0, 0))
      assertEquals(2, item._2.band(1).get(0, 0))
      assertEquals(3, item._2.band(2).get(0, 0))
      assertEquals(4, item._2.band(3).get(0, 0))
    }
  }

  @Test def testMergeCubeFullOverlapDifference(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(5.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(8.toByte, 256, 256)
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, "subtract")
    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect
    assertEquals(2, mergedTimes.size)
    import scala.collection.JavaConversions._
    for (item <- merged.toJavaRDD.collect) {
      assertEquals(2, item._2.bandCount)
      assertEquals(-(3), item._2.band(0).get(0, 0))
      assertEquals(-(5), item._2.band(1).get(0, 0))
    }
  }

  @Test def testMergeCubeOverlapBandMismatch(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(5.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(8.toByte, 256, 256)
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, "subtract")
    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect
    assertEquals(2, mergedTimes.size)
    import scala.collection.JavaConversions._
    for (item <- merged.toJavaRDD.collect) {
      assertEquals(2, item._2.bandCount)
      assertEquals(-(3), item._2.band(0).get(0, 0))
      assertEquals(-(5), item._2.band(1).get(0, 0))
    }
  }

  @Test def testMergeCubesBadOperator(): Unit = {

    val zeroTile: MutableArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    zeroTile.set(0, 0, 1)
    zeroTile.set(0, 1, ByteConstantNoDataCellType.noDataValue)
    val tileLayerRDD: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = tileToSpaceTimeDataCube(zeroTile)
    try {
      new OpenEOProcesses().mergeCubes(tileLayerRDD, tileLayerRDD, "unsupported")
      fail("Should have thrown an exception.")
    } catch {
      case e: UnsupportedOperationException =>

    }
  }
}
