package org.openeo.geotrellis

import geotrellis.layer.{SpaceTimeKey, _}
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.testkit.TileLayerRDDBuilders
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.geotrellis.LayerFixtures._
import org.openeo.geotrellis.geotiff.saveRDD
import org.openeo.geotrelliscommon.{OpenEORasterCube, OpenEORasterCubeMetadata, SparseSpaceTimePartitioner}

import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

object MergeCubesSpec {

  var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = {
    sc = {
      val conf = new SparkConf().setMaster("local[2]").setAppName(getClass.getSimpleName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName)
      SparkContext.getOrCreate(conf)
    }
  }

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()

  private def getDebugTile: MutableArrayTile = {
    val size = 256
    val arr = ListBuffer[Byte]()
    for {
      row <- 1 to size
      col <- 1 to size
    } {
      // Make a small shape to make it easier to debug:
      arr += {
        if (row < size / 2) 100.toByte else (if (col < size / 3) 200.toByte else 0.toByte)
      }
    }

    val tile = ByteConstantNoDataArrayTile.apply(arr.toArray, size, size)
    tile.set(0, 0, 1)
    tile.set(0, 1, tile.cellType.noDataValue)
    tile
  }

  def simpleMeanSquaredError(tileA: Tile, tileB: Tile): Double = {
    val diff = tileA.convert(DoubleConstantNoDataCellType).localSubtract(tileB.convert(DoubleConstantNoDataCellType))
    // The geotrellis .map() fills the tile with '0' values instead of 'noDataValue', so avoid.
    val diffArr = diff.toArrayDouble().filter(!isNoData(_))
    val squared = diffArr.map(v => v * v)
    squared.sum / squared.length
  }

  object AggregationType extends Enumeration {
    case class Val(fileMarker: String) extends super.Val

    implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

    val no: Val = Val("noAggregate")
    val simple: Val = Val("simpleAggregate")
    val extraNoData: Val = Val("extraNoDataAggregate")
  }

  def testMergeCubesTiledNoDataArguments: java.util.stream.Stream[Arguments] = util.Arrays.stream((
    for {
      r <- Seq(AggregationType.no, AggregationType.simple, AggregationType.extraNoData)
      g <- Seq(AggregationType.no, AggregationType.simple, AggregationType.extraNoData)
      b <- Seq(AggregationType.no) // No need to run all combinations to test all what is needed
    } yield Arguments.of(r, g, b)
    ).toArray)
}

class MergeCubesSpec {

  import MergeCubesSpec._

  @Test def testMergeCubesCrsResample(): Unit = {
    val path = "/tmp/testMergeCubesCrsResample/"
    new Directory(Paths.get(path).toFile).deleteRecursively()
    Files.createDirectories(Paths.get(path))
    val specialTile = MergeCubesSpec.getDebugTile
    // Avoid global extent to avoid errors when reprojecting
    val tileLayerRDD = tileToSpaceTimeDataCube(specialTile, Some(LayerFixtures.defaultExtent))
    saveRDD(tileLayerRDD.toSpatial(tileLayerRDD.keys.collect().head.time), 1, path + "tileLayerRDD.tiff")
    val newCrs = CRS.fromEpsgCode(32631)
    val extend = tileLayerRDD.metadata.layout.extent
    val extend_reproject = extend.reproject(tileLayerRDD.metadata.crs, newCrs)
    val ld = LayoutDefinition(RasterExtent(extend_reproject, CellSize(extend_reproject.width / specialTile.cols, extend_reproject.height / specialTile.rows)), specialTile.cols, specialTile.rows)
    val tileLayerRDD_tiled = tileLayerRDD.reproject(newCrs, ld)._2
    saveRDD(tileLayerRDD_tiled.toSpatial(tileLayerRDD_tiled.keys.collect().head.time), 1, path + "tileLayerRDD_tiled.tiff")

    val wrappedRDD = new OpenEORasterCube[SpaceTimeKey](tileLayerRDD.rdd, tileLayerRDD.metadata, new OpenEORasterCubeMetadata(Seq("B01", "B02")))
    val merged = new OpenEOProcesses().mergeCubes(wrappedRDD, tileLayerRDD_tiled, null)
    saveRDD(wrappedRDD.toSpatial(wrappedRDD.keys.collect().head.time), 1, path + "wrappedRDD.tiff")
    saveRDD(merged.toSpatial(merged.keys.collect().head.time), 1, path + "merged.tiff")

    val firstTile: MultibandTile = merged.toJavaRDD.take(1).get(0)._2
    assertEquals(4, firstTile.bandCount)
    assertEquals(specialTile, firstTile.band(0))

    // Due to resampling with interpolation, some artifacts may occur. So use fuzzy compare with MSE:
    val mse = MergeCubesSpec.simpleMeanSquaredError(specialTile, firstTile.band(2))
    println("MSE = " + mse)
    assertTrue(mse < 0.1)
  }

  @Test def testMergeCubesTiledResample(): Unit = {
    val path = "/tmp/testMergeCubesTiledResample/"
    Files.createDirectories(Paths.get(path))
    val specialTile = MergeCubesSpec.getDebugTile
    // Avoid global extent to avoid errors when reprojecting
    val tileLayerRDD = tileToSpaceTimeDataCube(specialTile, Some(LayerFixtures.defaultExtent))
    val tileLayerRDD_tiled = tileToSpaceTimeDataCube(specialTile, Some(LayerFixtures.defaultExtent), 2)

    val wrappedRDD = new OpenEORasterCube[SpaceTimeKey](tileLayerRDD.rdd, tileLayerRDD.metadata, new OpenEORasterCubeMetadata(Seq("B01", "B02")))
    val merged = new OpenEOProcesses().mergeCubes(wrappedRDD, tileLayerRDD_tiled, null)
    saveRDD(wrappedRDD.toSpatial(wrappedRDD.keys.collect().head.time), 1, path + "wrappedRDD.tiff")
    saveRDD(merged.toSpatial(merged.keys.collect().head.time), 1, path + "merged.tiff")

    val firstTile: MultibandTile = merged.toJavaRDD.take(1).get(0)._2
    assertEquals(4, firstTile.bandCount)
    assertEquals(specialTile, firstTile.band(0))

    // Due to resampling with interpolation, some artifacts may occur. So use fuzzy compare with MSE:
    val mse = MergeCubesSpec.simpleMeanSquaredError(specialTile, firstTile.band(2))
    println("MSE = " + mse)
    assertTrue(mse < 0.1)
  }


  /**
   * Combining aggregate_temporal and merge_cubes can leave the tiles RDD in a bad state.
   * This only causes problems when an other merge_cubes is called.
   * To trigger 2 different errors, and to make sure no errors remain, we iterate all possible combinations of those.
   * 3 layers, called R, G and B are merged. They exists out of some tiles that will make 8 different ways of overlapping.
   * Plus, this test is parameterized to get all combinations of aggregate_temporal
   */
  @ParameterizedTest
  @MethodSource(Array("testMergeCubesTiledNoDataArguments"))
  def testMergeCubesTiledNoData(aggregateR: AggregationType.Value,
                                aggregateG: AggregationType.Value,
                                aggregateB: AggregationType.Value,
                               ): Unit = {
    val path = "tmp/testMergeCubesTiledNoData/" + aggregateR + aggregateG + aggregateB + "/"
    Files.createDirectories(Paths.get(path))
    val p = new OpenEOProcesses()

    def aggregate(rdd: MultibandTileLayerRDD[SpaceTimeKey],
            aggregationType: AggregationType.Value,
           ): MultibandTileLayerRDD[SpaceTimeKey] = {
      val startDate = rdd.keys.collect().head.time
      if (aggregationType == AggregationType.no) {
        rdd
      } else {
        val intervals = if (aggregationType == AggregationType.extraNoData) {
          List(startDate, startDate, startDate.plusMonths(1), startDate.plusMonths(1)).map(DateTimeFormatter.ISO_INSTANT.format(_)).asJava
        } else {
          List(startDate, startDate).map(DateTimeFormatter.ISO_INSTANT.format(_)).asJava
        }

        val labels = if (aggregationType == AggregationType.extraNoData) {
          List(startDate, startDate.plusMonths(1)).map(DateTimeFormatter.ISO_INSTANT.format(_)).asJava
        } else {
          List(startDate).map(DateTimeFormatter.ISO_INSTANT.format(_)).asJava
        }

        val composite = p.aggregateTemporal(
          rdd,
          intervals,
          labels,
          TestOpenEOProcessScriptBuilder.createMedian(true, rdd.metadata.cellType),
          java.util.Collections.emptyMap()
        )
        val tmp2 = new ContextRDD(composite, composite.metadata)
        tmp2
      }
    }

    val tileLayerRDD_R = aggregate(buildSpatioTemporalDataCubePattern(), aggregateR)
    val tileLayerRDD_G = aggregate(buildSpatioTemporalDataCubePattern(patternScale = 2), aggregateG)
    val tileLayerRDD_B = aggregate(buildSpatioTemporalDataCubePattern(patternScale = 4), aggregateB)

    assertEquals(DoubleConstantNoDataCellType,tileLayerRDD_R.metadata.cellType)

    val tileLayerRDD_RG = new OpenEOProcesses().mergeCubes(tileLayerRDD_R, tileLayerRDD_G, null)
    val tileLayerRDD_RGB = new OpenEOProcesses().mergeCubes(tileLayerRDD_RG, tileLayerRDD_B, null)
    saveRDD(tileLayerRDD_R.toSpatial(tileLayerRDD_R.keys.collect().head.time), -1, path + "tileLayerRDD_R.tiff")
    saveRDD(tileLayerRDD_G.toSpatial(tileLayerRDD_G.keys.collect().head.time), -1, path + "tileLayerRDD_G.tiff")
    saveRDD(tileLayerRDD_B.toSpatial(tileLayerRDD_B.keys.collect().head.time), -1, path + "tileLayerRDD_B.tiff")
    saveRDD(tileLayerRDD_RG.toSpatial(tileLayerRDD_RG.keys.collect().head.time), -1, path + "tileLayerRDD_RG.tiff")
    saveRDD(tileLayerRDD_RGB.toSpatial(tileLayerRDD_RGB.keys.collect().head.time), -1, path + "tileLayerRDD_RGB.tiff")
    // No error should pop up when saving the images.
  }

  @Test def testSimpleMeanSquaredError(): Unit = {
    val size = 8
    val arr = ListBuffer[Byte]()
    for {
      row <- 1 to size
      col <- 1 to size
    } {
      arr += {
        if (row < size / 2 && col < size / 2) ByteConstantNoDataCellType.noDataValue else 100.toByte
      }
    }
    val specialTile: MutableArrayTile = ByteArrayTile.apply(arr.toArray, size, size)
    specialTile.set(0, 0, 99)

    val plainTile: MutableArrayTile = ByteArrayTile.fill(100.toByte, size, size)
    val mse = MergeCubesSpec.simpleMeanSquaredError(specialTile, plainTile)
    println("MSE = " + mse)
    assertTrue(mse < 0.1)
    assertTrue(mse > 0) // MSE could be 0, but here we expect something changed.
  }

  @Test def testMergeCubesBasic(): Unit = {
    val celltype: DataType = CellType.fromName("int8raw").withDefaultNoData
    val zeroTile: MutableArrayTile = ByteArrayTile.fill(0.toByte, 256, 256)
    zeroTile.set(0, 0, 1)
    zeroTile.set(0, 1, ByteConstantNoDataCellType.noDataValue)
    val tileLayerRDD: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = tileToSpaceTimeDataCube(zeroTile)
    val wrappedRDD = new OpenEORasterCube[SpaceTimeKey](tileLayerRDD.rdd,tileLayerRDD.metadata,new OpenEORasterCubeMetadata(Seq("B01","B02")))
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(wrappedRDD, tileLayerRDD, null)
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

  @Test def testMergeSparseRDD(): Unit = {
    val idx1 = Seq( SpatialKey(3, 1), SpatialKey(7, 2))
    val sparseLayer1 = LayerFixtures.aSparseSpacetimeTileLayerRdd(idx1)
    val c1Keys = sparseLayer1.map(_._1.spatialKey).distinct().collect()
    print(c1Keys)
    val idx2 = Seq( SpatialKey(3, 1), SpatialKey(6, 2), SpatialKey(1, 3))
    val sparseLayer2 = LayerFixtures.aSparseSpacetimeTileLayerRdd(idx2)
    val merged = new OpenEOProcesses().mergeCubes(sparseLayer1,sparseLayer2,operator=null)
    val localTiles = merged.collect()
    assertTrue(merged.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
    assertTrue(merged.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.isInstanceOf[SparseSpaceTimePartitioner])
    assertEquals((idx1++idx2).toSet,localTiles.map(_._1.spatialKey).toSet)
  }

  @Test def testMergeComposites(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(5.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(8.toByte, 256, 256)
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-03T00:00:00Z", "2020-02-02T00:00:00Z"))
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-01-02T00:00:00Z", "2020-02-02T00:00:00Z"))

    val startDate = ZonedDateTime.parse("2020-01-01T00:00:00Z")
    val intervals = Range(0, 3).flatMap { r => Seq(startDate.plusDays(10L * r), startDate.plusDays(10L * (r + 1))) }.map(DateTimeFormatter.ISO_INSTANT.format(_))
    val labels = Range(0, 3).map { r => DateTimeFormatter.ISO_INSTANT.format(startDate.plusDays(10L * r)) }

    val p = new OpenEOProcesses()
    val medianProcess = TestOpenEOProcessScriptBuilder.createMedian(true,cube1.metadata.cellType)
    assertEquals(cube1.metadata.cellType,medianProcess.getOutputCellType())
    val composite1 = p.aggregateTemporal(cube1,intervals.asJava,labels.asJava,medianProcess, java.util.Collections.emptyMap())
    val composite2 = p.aggregateTemporal(cube2,intervals.asJava,labels.asJava,medianProcess, java.util.Collections.emptyMap())
    val merged = p.mergeCubes(p.filterEmptyTile(composite1), p.filterEmptyTile(composite2), operator = null)
    val expectedKey = SpaceTimeKey(0,0,1577836800000L)
    val localTiles = merged.filter(_._1==expectedKey).collect()
    val c1Tiles = composite1.filter(_._1==expectedKey).collect()
    val c2Tiles = composite2.filter(_._1==expectedKey).collect()
    assertEquals(1,localTiles.length)
    assertEquals(1,c1Tiles.length)
    assertEquals(1,c2Tiles.length)
    assertEquals(localTiles(0)._2, MultibandTile(c1Tiles(0)._2.bands ++ c2Tiles(0)._2.bands))


  }
}
