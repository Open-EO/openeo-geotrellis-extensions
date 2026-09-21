package org.openeo.geotrellis

import geotrellis.layer.{SpaceTimeKey, _}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.util.withGetComponentMethods
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{NarrowDependency, OneToOneDependency, ShuffleDependency, SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.geotrellis.GeneralUtils.safeConvert
import org.openeo.geotrellis.LayerFixtures._
import org.openeo.geotrellis.geotiff.saveRDD
import org.openeo.geotrelliscommon.{ByTileSpacetimePartitioner, ConfigurableSpaceTimePartitioner, OpenEORasterCube, OpenEORasterCubeMetadata, SparseSpaceTimePartitioner, SpatialKeysProvider}

import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
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
    val diff = safeConvert(tileA, DoubleConstantNoDataCellType).localSubtract(safeConvert(tileB,DoubleConstantNoDataCellType))
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

  /**
   * Partitioner index variants that mergeCubes_SpaceTime_Spatial (and its underlying
   * leftJoinSpacetimeSpatial join implementation) needs to support on the left (SpaceTimeKey) side.
   */
  def mergeCubesSpaceTimeSpatialPartitionerVariants: java.util.stream.Stream[Arguments] = util.Arrays.stream(Array(
    Arguments.of("configurable"),
    Arguments.of("byTile"),
    Arguments.of("sparseWithKeys"),
    Arguments.of("sparseNoKeys"),
  ))
}

class MergeCubesSpec {

  import MergeCubesSpec._

  @Test def testMergeCubesCrsResample(): Unit = {
    val path = "/tmp/testMergeCubesCrsResample/"
    new Directory(Paths.get(path).toFile).deleteRecursively()
    Files.createDirectories(Paths.get(path))
    val specialTile = MergeCubesSpec.getDebugTile
    // Avoid global extent to avoid errors when reprojecting
    val extentEpsg32631 = defaultExtentEpsg32631
    val otherCrs = CRS.fromEpsgCode(32631)
    val extentLatLng = extentEpsg32631.reproject(otherCrs, LatLng)
    val tileLayerRDD = tileToSpaceTimeDataCube(specialTile, Some(extentLatLng))
    saveRDD(tileLayerRDD.toSpatial(tileLayerRDD.keys.collect().head.time), 1, path + "tileLayerRDD.tiff")
    val extend = tileLayerRDD.metadata.layout.extent
    val ld = LayoutDefinition(RasterExtent(extentEpsg32631, CellSize(extentEpsg32631.width / specialTile.cols, extentEpsg32631.height / specialTile.rows)), specialTile.cols, specialTile.rows)
    val tileLayerRDD_tiled = tileLayerRDD.reproject(otherCrs, ld)._2
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
    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
      assertEquals(5, item._2.bandCount)
      assertEquals(2, item._2.band(0).get(0, 0))
      assertEquals(3, item._2.band(1).get(0, 0))
      assertEquals(5, item._2.band(2).get(0, 0))
      assertEquals(5, item._2.band(3).get(0, 0))
      assertEquals(5, item._2.band(4).get(0, 0))
    }
    assertEquals(CellType.fromName("int32ud-128"), merged.metadata.cellType)
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
    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
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
    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
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

    for (item: (SpatialKey, MultibandTile) <- merged.collect) {
      assertEquals(2, item._2.bandCount)
      assertEquals(-3, item._2.band(0).get(0, 0))
      assertEquals(-5, item._2.band(1).get(0, 0))
    }
  }

  @Test def testMergeCubeDifferenceCRS_SpatialSpatial(): Unit = {
    val band1 = ByteArrayTile.fill(5.toByte, 32, 32).withNoData(Some(0.toByte))
    val band2 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
    val band3 = ByteArrayTile.fill(9.toByte, 32, 32).withNoData(Some(0.toByte))
    val band4 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))

    val tileLayout = TileLayout(2,2,16,16)
    val tile1 = MultibandTile(band1,band2)
    val tile2 = MultibandTile(band3,band4)

    val crs = CRS.fromEpsgCode(32631)
    val extent = Extent(500000.00, 5650000.00, 507000.00, 5660950.00)
    val extentLatLng = extent.reproject(crs,LatLng)

    val cube1: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(sc, Raster(tile1, extentLatLng), tileLayout, LatLng)
    val cube2: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(sc, Raster(tile2, extent), tileLayout, crs)
    val processes = new OpenEOProcesses()
    val merged: MultibandTileLayerRDD[SpatialKey] = processes.mergeSpatialCubes(cube1, cube2, "mean")

    val difference = extent.compare(merged.metadata.layoutExtent)
    assertEquals(merged.metadata.extent, extentLatLng)
    assertEquals(merged.metadata.crs, LatLng)

    for (item: (SpatialKey, MultibandTile) <- merged.collect) {
      assertEquals(2, item._2.bandCount)
      assertEquals(7, item._2.band(0).get(0, 0))
      assertEquals(3, item._2.band(1).get(0, 0))
    }
  }

//  @Test def testMergeCubeDifferenceExtent_SpatialSpatial(): Unit = {
//    val band1 = ByteArrayTile.fill(5.toByte, 32, 32).withNoData(Some(0.toByte))
//    val band2 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
//
//    val band3 = ByteArrayTile.fill(9.toByte, 32, 32).withNoData(Some(0.toByte))
//    val band4 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
//
//    val tileLayout1 = TileLayout(2,2,16,16)
//    val tileLayout2 = TileLayout(1,1,32,32)
//    val tile1 = MultibandTile(band1,band2)
//    val tile2 = MultibandTile(band3,band4)
//
//    val extent1 = Extent(3.00, 51.00, 3.10, 51.10)
//    val extent2 = Extent(3.05, 51.05, 3.15, 51.15)
//
//    val cube1: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(sc, Raster(tile1, extent1), tileLayout1, LatLng)
//    val cube2: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(sc, Raster(tile2, extent2), tileLayout2, LatLng)
//
//    val processes = new OpenEOProcesses()
//    val merged: MultibandTileLayerRDD[SpatialKey] = processes.mergeSpatialCubes(cube1, cube2, "mean")
//
//    assertEquals(merged.metadata.layout, LayoutDefinition(extent1.combine(extent2),TileLayout(3,3,16,16)))
//    val collected = merged.collect()
//
//    for (item: (SpatialKey, MultibandTile) <- merged.collect) {
//      assertEquals(2, item._2.bandCount)
//      item match {
//        case (SpatialKey(0,0), tile) =>
//          assertEquals(0, tile.band(0).get(0, 0).toByte)
//          assertEquals(0, tile.band(1).get(0, 0).toByte)
//        case (SpatialKey(0,1), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpatialKey(0,2), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpatialKey(1,0), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpatialKey(1,1), tile) =>
//          assertEquals(7, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpatialKey(1,2), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpatialKey(2,0), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpatialKey(2,1), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpatialKey(2,2), tile) =>
//          assertEquals(0, tile.band(0).get(0, 0).toByte)
//          assertEquals(0, tile.band(1).get(0, 0).toByte)
//        case _ => fail("Unexpected spatial key")
//      }
//    }
//  }


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
    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
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

//  @Test def testMergeCubeDifferenceExtent_SpatialSpaceTime(): Unit = {
//
//    val band1 = ByteArrayTile.fill(5.toByte, 32, 32).withNoData(Some(0.toByte))
//    val band2 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
//    val band3 = ByteArrayTile.fill(9.toByte, 32, 32).withNoData(Some(0.toByte))
//    val band4 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
//
//    val tileLayout1 = TileLayout(2,2,16,16)
//    val tileLayout2 = TileLayout(1,1,32,32)
//    val tile1 = MultibandTile(band1,band2)
//    val tile2 = MultibandTile(band3,band4)
//
//    val extent1 = Extent(3.00, 51.00, 3.10, 51.10)
//    val extent2 = Extent(3.05, 51.05, 3.15, 51.15)
//
//    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"), Some(extent1), tilingFactor = 2)
//    val cube2: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(sc, Raster(tile2, extent2), tileLayout2, LatLng)
//    val processes = new OpenEOProcesses()
//    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = processes.mergeCubes_SpaceTime_Spatial(ContextRDD(processes.applySpacePartitioner(cube1,cube1.metadata.bounds.get),cube1.metadata), cube2, "mean",swapOperands = false)
//    val mergedTimes: Array[TemporalKey] = merged.map((p: (SpaceTimeKey, MultibandTile)) => p._1.temporalKey).collect.distinct
//    assertEquals(2, mergedTimes.length)
//    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
//      assertEquals(2, item._2.bandCount)
//      item match {
//        case (SpaceTimeKey(0,0,t), tile) =>
//          assertEquals(0, tile.band(0).get(0, 0).toByte)
//          assertEquals(0, tile.band(1).get(0, 0).toByte)
//        case (SpaceTimeKey(0,1,t), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(0,2,t), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(1,0,t), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(1,1,t), tile) =>
//          assertEquals(7, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(1,2,t), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(2,0,t), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(2,1,t), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(2,2,t), tile) =>
//          assertEquals(0, tile.band(0).get(0, 0).toByte)
//          assertEquals(0, tile.band(1).get(0, 0).toByte)
//        case _ => fail("Unexpected spatial key")
//      }
//    }
//  }

  @Test def testMergeCubeDifferenceCRS_SpatialSpaceTime(): Unit = {
    val band1 = ByteArrayTile.fill(5.toByte, 32, 32).withNoData(Some(0.toByte))
    val band2 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
    val band3 = ByteArrayTile.fill(9.toByte, 32, 32).withNoData(Some(0.toByte))
    val band4 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))

    val tileLayout = TileLayout(2,2,16,16)
    val tile1 = MultibandTile(band1,band2)
    val tile2 = MultibandTile(band3,band4)

    val crs = CRS.fromEpsgCode(32631)
    val extent = Extent(500000.00, 5650000.00, 507000.00, 5660950.00)
    val extentLatLng = extent.reproject(crs,LatLng)

    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"), Some(extentLatLng), tilingFactor = 2)
    val cube2: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(sc, Raster(tile2, extent), tileLayout, crs)
    val processes = new OpenEOProcesses()
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = processes.mergeCubes_SpaceTime_Spatial(ContextRDD(processes.applySpacePartitioner(cube1,cube1.metadata.bounds.get),cube1.metadata), cube2, "mean",swapOperands = false)

    val difference = extent.compare(merged.metadata.layoutExtent)
    assertEquals(merged.metadata.extent, extentLatLng)
    assertEquals(merged.metadata.crs, LatLng)

    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
      assertEquals(2, item._2.bandCount)
      assertEquals(7, item._2.band(0).get(0, 0))
      assertEquals(3, item._2.band(1).get(0, 0))
    }
  }

  /**
   * Wraps a SpaceTimeKey cube with a fresh partitioner using the given index, so we can
   * exercise mergeCubes_SpaceTime_Spatial with different partitioner index implementations
   * on the left (SpaceTimeKey) input.
   */
  private def repartitionWithIndex(cube: MultibandTileLayerRDD[SpaceTimeKey], index: geotrellis.spark.partition.PartitionerIndex[SpaceTimeKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
    val kb: Bounds[SpaceTimeKey] = cube.metadata.getComponent[Bounds[SpaceTimeKey]]
    val partitioner = SpacePartitioner[SpaceTimeKey](kb)(implicitly, implicitly, index)
    ContextRDD(cube.partitionBy(partitioner), cube.metadata)
  }

  private def withPartitionerVariant(cube: MultibandTileLayerRDD[SpaceTimeKey], variant: String): MultibandTileLayerRDD[SpaceTimeKey] = {
    val allKeys = cube.map(_._1).distinct().collect()
    variant match {
      case "configurable" =>
        repartitionWithIndex(cube, new ConfigurableSpaceTimePartitioner(indexReduction = 0))
      case "byTile" =>
        repartitionByTile(cube, allKeys.map(_.spatialKey).distinct.toSeq)
      case "sparseWithKeys" =>
        val indices = allKeys.map(SparseSpaceTimePartitioner.toIndex(_, indexReduction = 0)).distinct.sorted
        repartitionWithIndex(cube, new SparseSpaceTimePartitioner(indices, 0, theKeys = Some(allKeys)))
      case "sparseNoKeys" =>
        val indices = allKeys.map(SparseSpaceTimePartitioner.toIndex(_, indexReduction = 0)).distinct.sorted
        repartitionWithIndex(cube, new SparseSpaceTimePartitioner(indices, 0, theKeys = None))
      case other =>
        fail(s"Unknown partitioner variant: $other")
    }
  }

  /** All RDD ids reachable from `rdd` (its full lineage, including itself). */
  private def lineageIds(rdd: RDD[_]): Set[Int] = {
    val visited = mutable.Set[Int]()
    def go(r: RDD[_]): Unit = if (visited.add(r.id)) r.dependencies.foreach(d => go(d.rdd))
    go(rdd)
    visited.toSet
  }

  /** Depth-first search of `rdd`'s lineage for a SpatialToSpacetimeJoinRdd instance, if any. */
  private def findJoinRdd(rdd: RDD[_]): Option[SpatialToSpacetimeJoinRdd[_]] = {
    val visited = mutable.Set[Int]()
    val stack = mutable.Stack[RDD[_]](rdd)
    while (stack.nonEmpty) {
      val r = stack.pop()
      if (visited.add(r.id)) {
        r match {
          case j: SpatialToSpacetimeJoinRdd[_] => return Some(j)
          case _ => r.dependencies.foreach(d => stack.push(d.rdd))
        }
      }
    }
    None
  }

  /**
   * Asserts the shuffle/lineage characteristics that mergeCubes_SpaceTime_Spatial is expected
   * to produce for a given left-cube partitioner `variant`:
   *
   *  - "configurable": the left cube's index is already a ConfigurableSpaceTimePartitioner, so
   *    SpatialToSpacetimeJoinRdd re-uses the left cube unchanged (no re-partition/shuffle of the
   *    left side) and joins it to the right cube using its custom narrow Dependency machinery.
   *  - "sparseNoKeys": SparseSpaceTimePartitioner without known keys can't be reused, so
   *    SpatialToSpacetimeJoinRdd is still used, but the left cube first needs an extra shuffle to
   *    a ConfigurableSpaceTimePartitioner.
   *  - "byTile" / "sparseWithKeys": leftJoinSpacetimeSpatial takes the plain-join fast path; no
   *    SpatialToSpacetimeJoinRdd is created at all, and the left cube's own partitioner/index is
   *    preserved as-is (only the right, spatial cube gets shuffled to align with it).
   */
  private def assertShuffleBehaviour(variant: String, leftCube: MultibandTileLayerRDD[SpaceTimeKey], merged: RDD[_]): Unit = {
    val leftLineage = lineageIds(leftCube)
    val leftIndexClass = leftCube.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.getClass
    val mergedIndexClass = merged.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.getClass

    val joinRddOpt = findJoinRdd(merged)

    variant match {
      case "configurable" =>
        val j = joinRddOpt.getOrElse(fail("expected a SpatialToSpacetimeJoinRdd in the lineage"))
        assertTrue(leftLineage.contains(j.spatiallyPartitionedRdd.id),
          "left cube should be reused as-is (no extra shuffle) when its index is already Configurable")
        assertEquals(leftIndexClass, mergedIndexClass, "left partitioner index class should be preserved")

        val deps = j.dependencies
        assertEquals(2, deps.size, "SpatialToSpacetimeJoinRdd should have exactly 2 dependencies")
        assertTrue(deps.forall(_.isInstanceOf[NarrowDependency[_]]),
          "both dependencies of SpatialToSpacetimeJoinRdd must be narrow: it should never shuffle the left cube itself")
        assertFalse(deps.exists(_.isInstanceOf[ShuffleDependency[_, _, _]]),
          "no ShuffleDependency should be directly attached to SpatialToSpacetimeJoinRdd")
        assertTrue(deps.exists(_.isInstanceOf[OneToOneDependency[_]]),
          "the (already-partitioned) left cube must be wired in via a OneToOneDependency")

        val spatialDep = deps.find(_.getClass.getSimpleName == "SpatialDependency")
          .getOrElse(fail("expected the custom SpatialDependency among SpatialToSpacetimeJoinRdd's dependencies"))
          .asInstanceOf[NarrowDependency[_]]
        // Every output partition of the join must map to exactly one partition of the (broadcast-like)
        // spatially-reshuffled right-hand side: this is the core narrow-dependency contract.
        for (p <- j.partitions.indices) {
          assertEquals(1, spatialDep.getParents(p).size,
            s"SpatialDependency.getParents($p) should resolve to exactly one parent partition")
        }

      case "sparseNoKeys" =>
        val j = joinRddOpt.getOrElse(fail("expected a SpatialToSpacetimeJoinRdd in the lineage"))
        assertFalse(leftLineage.contains(j.spatiallyPartitionedRdd.id),
          "left cube should have been re-partitioned (extra shuffle) since its index isn't reusable")
        assertNotEquals(leftIndexClass, mergedIndexClass,
          "the re-partition should have replaced the original index with a ConfigurableSpaceTimePartitioner")
        assertEquals(classOf[ConfigurableSpaceTimePartitioner], mergedIndexClass)

      case "byTile" | "sparseWithKeys" =>
        assertTrue(joinRddOpt.isEmpty,
          "the plain-join fast path should not construct a SpatialToSpacetimeJoinRdd")
        assertEquals(leftIndexClass, mergedIndexClass,
          "left partitioner index class should be preserved by the plain-join fast path")

      case other =>
        fail(s"Unknown partitioner variant: $other")
    }
  }

  /**
   * Covers mergeCubes_SpaceTime_Spatial (and the underlying leftJoinSpacetimeSpatial join)
   * across the different SpacePartitioner index implementations that can occur on the
   * left (SpaceTimeKey) cube: ConfigurableSpaceTimePartitioner, ByTileSpacetimePartitioner,
   * and SparseSpaceTimePartitioner with and without known keys.
   *
   * The left cube is a dense/continuous 4x4 grid (16 spatial keys), while the right
   * (spatial) cube only has values for a sparse, non-adjacent subset of those spatial keys.
   * This verifies that all left spacetime keys are preserved in the result (left outer join
   * semantics): keys with a matching right spatial key get the overlap resolver applied, while
   * keys without a match on the right simply keep their left-hand values unchanged, regardless
   * of the left partitioner implementation.
   */
  @ParameterizedTest
  @MethodSource(Array("mergeCubesSpaceTimeSpatialPartitionerVariants"))
  def testMergeCubeSpaceTimeSpatialPartialOverlap(variant: String): Unit = {
    val leftBand1: ByteArrayTile = ByteArrayTile.fill(10.toByte, 256, 256)
    val leftBand2: ByteArrayTile = ByteArrayTile.fill(20.toByte, 256, 256)
    val dates = Seq("2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z")
    val leftCubeBase: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] =
      buildSpatioTemporalDataCube(util.Arrays.asList(leftBand1, leftBand2), dates, tilingFactor = 4)

    val allLeftSpatialKeys = leftCubeBase.map(_._1.spatialKey).distinct().collect().toSet
    assertEquals(16, allLeftSpatialKeys.size) // sanity check: dense/continuous 4x4 grid

    val rightBand1: ByteArrayTile = ByteArrayTile.fill(3.toByte, 256, 256)
    val rightBand2: ByteArrayTile = ByteArrayTile.fill(4.toByte, 256, 256)
    val fullRight: MultibandTileLayerRDD[SpatialKey] =
      TileLayerRDDBuilders.createMultibandTileLayerRDD(sc, MultibandTile(rightBand1, rightBand2), leftCubeBase.metadata.tileLayout)

    // Sparse, non-adjacent subset of the left grid: the right cube does NOT fully overlap the left cube.
    val desiredRightKeys = Set(SpatialKey(0, 0), SpatialKey(1, 1), SpatialKey(2, 3), SpatialKey(3, 0))
    assertTrue(desiredRightKeys.subsetOf(allLeftSpatialKeys))
    val sparseRight: MultibandTileLayerRDD[SpatialKey] = fullRight.withContext(_.filter { case (k, _) => desiredRightKeys.contains(k) })

    val leftCube = withPartitionerVariant(leftCubeBase, variant)

    val processes = new OpenEOProcesses()
    val merged = processes.mergeCubes_SpaceTime_Spatial(leftCube, sparseRight, "subtract", swapOperands = false)

    // Strategy 1: statically inspect the RDD lineage/dependency chain to verify that
    // shuffle-avoiding partitioner variants really do avoid shuffling the left cube, and that
    // SpatialToSpacetimeJoinRdd's custom narrow Dependency is wired up correctly when it's used.
    assertShuffleBehaviour(variant, leftCube, merged)

    // Strategy 2: explicitly verify the overarching partitioner-index invariant across all
    // variants: the left cube's index class must be retained in the merged result, EXCEPT for
    // "sparseNoKeys", whose index (SparseSpaceTimePartitioner without known keys) can't be
    // reused and is therefore replaced (with a ConfigurableSpaceTimePartitioner).
    val leftIndexClass = leftCube.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.getClass
    val mergedIndexClass = merged.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.getClass
    if (variant == "sparseNoKeys") {
      assertNotEquals(leftIndexClass, mergedIndexClass,
        s"variant '$variant': partitioner index class should NOT be retained (its index isn't reusable)")
    } else {
      assertEquals(leftIndexClass, mergedIndexClass,
        s"variant '$variant': partitioner index class should be retained as-is")
    }

    val collected = merged.collect()

    // All left spacetime keys must be preserved in the result, regardless of right-side overlap.
    assertEquals(allLeftSpatialKeys.size * dates.size, collected.length)
    assertEquals(allLeftSpatialKeys, collected.map(_._1.spatialKey).toSet)

    for ((key, tile) <- collected) {
      assertEquals(2, tile.bandCount)
      if (desiredRightKeys.contains(key.spatialKey)) {
        // Overlapping keys: the overlap resolver ("subtract") is applied.
        assertEquals(10 - 3, tile.band(0).get(0, 0))
        assertEquals(20 - 4, tile.band(1).get(0, 0))
      } else {
        // Non-overlapping keys: the left-hand values are preserved unchanged.
        assertEquals(10, tile.band(0).get(0, 0))
        assertEquals(20, tile.band(1).get(0, 0))
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
    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
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
    assertEquals(2, mergedTimes.length)
    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
      assertEquals(2, item._2.bandCount)
      assertEquals(-(3), item._2.band(0).get(0, 0))
      assertEquals(-(5), item._2.band(1).get(0, 0))
    }
  }

  @Test def testMergeCubeFullOverlapMean(): Unit = {
    val band1: ByteArrayTile = ByteArrayTile.fill(2.toByte, 256, 256)
    val band2: ByteArrayTile = ByteArrayTile.fill(4.toByte, 256, 256)
    val band3: ByteArrayTile = ByteArrayTile.fill(6.toByte, 256, 256)
    val band4: ByteArrayTile = ByteArrayTile.fill(8.toByte, 256, 256)
    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"))
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, "mean")
    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect
    assertEquals(2, mergedTimes.length)
    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
      assertEquals(2, item._2.bandCount)
      // mean(2, 6) = 4, mean(4, 8) = 6
      assertEquals(4, item._2.band(0).get(0, 0))
      assertEquals(4, item._2.band(0).get(128, 128))
      assertEquals(6, item._2.band(1).get(0, 0))
      assertEquals(6, item._2.band(1).get(128, 128))
    }
  }

//  @Test def testMergeCubeDifferenceExtentSpaceTimeSpaceTime(): Unit = {
//    val band1 = ByteArrayTile.fill(5.toByte, 32, 32).withNoData(Some(0.toByte))
//    val band2 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
//    val band3 = ByteArrayTile.fill(9.toByte, 32, 32).withNoData(Some(0.toByte))
//    val band4 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
//
//    val extent1 = Extent(3.00, 51.00, 3.10, 51.10)
//    val extent2 = Extent(3.05, 51.05, 3.15, 51.15)
//
//    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"), Some(extent1), tilingFactor = 2)
//    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"), Some(extent2), tilingFactor = 2)
//    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, "mean")
//    val mergedTimes: Array[TemporalKey] = merged.map((p: Tuple2[SpaceTimeKey, MultibandTile]) => p._1.temporalKey).collect.distinct
//    assertEquals(2, mergedTimes.length)
//    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
//      assertEquals(2, item._2.bandCount)
//      item match {
//        case (SpaceTimeKey(0,0,t), tile) =>
//          assertEquals(0, tile.band(0).get(0, 0).toByte)
//          assertEquals(0, tile.band(1).get(0, 0).toByte)
//        case (SpaceTimeKey(0,1,t), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(0,2,t), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(1,0,t), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(1,1,t), tile) =>
//          assertEquals(7, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(1,2,t), tile) =>
//          assertEquals(5, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(2,0,t), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(2,1,t), tile) =>
//          assertEquals(9, tile.band(0).get(0, 0))
//          assertEquals(3, tile.band(1).get(0, 0))
//        case (SpaceTimeKey(2,2,t), tile) =>
//          assertEquals(0, tile.band(0).get(0, 0).toByte)
//          assertEquals(0, tile.band(1).get(0, 0).toByte)
//        case _ => fail("Unexpected spatial key")
//      }
//    }
//  }

  @Test def testMergeCubeDifferenceCRS_SpaceTimeSpaceTime(): Unit = {
    val band1 = ByteArrayTile.fill(5.toByte, 32, 32).withNoData(Some(0.toByte))
    val band2 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))
    val band3 = ByteArrayTile.fill(9.toByte, 32, 32).withNoData(Some(0.toByte))
    val band4 = ByteArrayTile.fill(3.toByte, 32, 32).withNoData(Some(0.toByte))

    val tileLayout = TileLayout(2,2,16,16)
    val tile1 = MultibandTile(band1,band2)
    val tile2 = MultibandTile(band3,band4)

    val crs = CRS.fromEpsgCode(32631)
    val extent = Extent(500000.00, 5650000.00, 507000.00, 5660950.00)
    val extentLatLng = extent.reproject(crs,LatLng)

    val cube1: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band1, band2), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"), Some(extentLatLng), tilingFactor = 2)
    val cube2: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = buildSpatioTemporalDataCube(util.Arrays.asList(band3, band4), Seq("2020-01-01T00:00:00Z", "2020-02-02T00:00:00Z"), Some(extent), tilingFactor = 2, crs)
    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(cube1, cube2, "mean")

    val difference = extent.compare(merged.metadata.layoutExtent)
    assertEquals(merged.metadata.extent, extentLatLng)
    assertEquals(merged.metadata.crs, LatLng)

    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
      assertEquals(2, item._2.bandCount)
      assertEquals(7, item._2.band(0).get(0, 0))
      assertEquals(3, item._2.band(1).get(0, 0))
    }
  }

  @Test def testMergeCubeMeanNoDataHandling(): Unit = {
    // tile with NoData at (0, 1)
    val tileWithNoData: MutableArrayTile = ByteArrayTile.fill(4.toByte, 256, 256)
    tileWithNoData.set(0, 1, ByteConstantNoDataCellType.noDataValue)
    val tileLayerRDD: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = tileToSpaceTimeDataCube(tileWithNoData)

    val plainTile: MutableArrayTile = ByteArrayTile.fill(6.toByte, 256, 256)
    val plainLayerRDD: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = tileToSpaceTimeDataCube(plainTile)

    val merged: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new OpenEOProcesses().mergeCubes(tileLayerRDD, plainLayerRDD, "mean")
    val firstTile: MultibandTile = merged.toJavaRDD.take(1).get(0)._2
    // mean(4, 6) = 5 for normal cells
    assertEquals(5, firstTile.band(0).get(0, 0))
    // when one value is NoData, return the other
    assertEquals(6, firstTile.band(0).get(0, 1))
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
    for (item: (SpaceTimeKey, MultibandTile) <- merged.collect) {
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

  @Test def testMergeSparseRDDByTile(): Unit = {
    val idx1 = Seq( SpatialKey(3, 1), SpatialKey(7, 2))
    val sparseLayer1 = LayerFixtures.aSparseSpacetimeTileLayerRdd(idx1)
    val c1Keys = sparseLayer1.map(_._1.spatialKey).distinct().collect()
    val idx2 = Seq( SpatialKey(3, 1), SpatialKey(6, 2), SpatialKey(1, 3))
    val sparseLayer2 = LayerFixtures.aSparseSpacetimeTileLayerRdd(idx2)

    val repart1 = repartitionByTile(sparseLayer1, idx1)
    assertTrue(repart1.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.isInstanceOf[ByTileSpacetimePartitioner])
    val merged = new OpenEOProcesses().mergeCubes(repart1,repartitionByTile(sparseLayer2,idx2),operator=null)
    val localTiles = merged.collect()
    assertTrue(merged.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
    val index = merged.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index
    assertTrue(index.isInstanceOf[ByTileSpacetimePartitioner])
    assertEquals((idx1++idx2).toSet,localTiles.map(_._1.spatialKey).toSet)
    assertEquals((idx1++idx2).distinct.sorted.toList,index.asInstanceOf[SpatialKeysProvider].spatialKeys.get.toList)
  }

  private def repartitionByTile(cube:MultibandTileLayerRDD[SpaceTimeKey], keys: Seq[SpatialKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
    val kb: Bounds[SpaceTimeKey] = cube.metadata.getComponent[Bounds[SpaceTimeKey]]
    val p = SpacePartitioner[SpaceTimeKey](kb)(implicitly, implicitly, new ByTileSpacetimePartitioner(Some(keys.toArray)))
    ContextRDD(p(cube),cube.metadata)
  }

  @Test def testMergeSparseRDDDifferentCrs(): Unit = {
    val idx1 = Seq( SpatialKey(3, 1), SpatialKey(7, 2))
    val sparseLayer1 = LayerFixtures.aSparseSpacetimeTileLayerRdd(idx1)
    val c1Keys = sparseLayer1.map(_._1.spatialKey).distinct().collect()
    print(c1Keys)
    val sparseLayer2 = LayerFixtures.sentinel2B04LayerSparse
    assertNotEquals(sparseLayer1.metadata.crs,sparseLayer2.metadata.crs)
    val merged = new OpenEOProcesses().mergeCubes(sparseLayer1,sparseLayer2,operator=null)
    val localTiles = merged.collect()
    assertTrue(merged.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
    assertTrue(merged.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.isInstanceOf[SparseSpaceTimePartitioner])
    assertEquals(Set(SpatialKey(3,1), SpatialKey(7,2), SpatialKey(4,0)),localTiles.map(_._1.spatialKey).toSet)
  }

  @Test def testMergeSparseRDDWithPartitionerNone(): Unit = {
    val idx1 = Seq( SpatialKey(3, 1), SpatialKey(7, 2))

    val sparseLayer1 = LayerFixtures.aSparseSpacetimeTileLayerRdd(idx1)
    val c1Keys = sparseLayer1.map(_._1.spatialKey).distinct().collect()
    print(c1Keys)
    val idx2 = Seq( SpatialKey(3, 1), SpatialKey(6, 2), SpatialKey(1, 3))
    val collection = aSpacetimeTileLayerRdd(8,4,4,crs = CRS.fromName("EPSG:4087"))
    val sparseLayer2 = collection._1.withContext{_.filter(t => idx2.contains(t._1.spatialKey))}
    assertTrue(sparseLayer2.partitioner.isEmpty)
    val merged = new OpenEOProcesses().mergeCubes(sparseLayer1,sparseLayer2,operator=null)
    assertTrue(merged.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
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
