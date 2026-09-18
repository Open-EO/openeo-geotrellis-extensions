package org.openeo.geotrellis

import geotrellis.layer.{KeyBounds, SpaceTimeKey, SpatialKey}
import geotrellis.raster.{ArrayMultibandTile, IntConstantNoDataArrayTile, MultibandTile, NODATA, TileLayout, isNoData}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import java.util.stream.{Stream => JStream}
import scala.jdk.CollectionConverters._

object ReduceSpatialTest {
  private implicit var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()

  def reduceSpatialDataCubeParams(): JStream[Arguments] = JStream.of(
    Arguments.of("max", Seq(5.0, 9.0)),
    Arguments.of("min", Seq(1.0, 6.0)),
    Arguments.of("sum", Seq(27.0, 90.0)),
    Arguments.of("count", Seq(9.0, 12.0)),
    Arguments.of("mean", Seq(3.0, 7.5)),
  )

  def reduceSpatiotemporalDataCubeParams(): JStream[Arguments] = JStream.of(
    Arguments.of("max", Seq(5.0, 9.0), Seq(6.0, 10.0)),
    Arguments.of("min", Seq(1.0, 6.0), Seq(2.0, 7.0)),
    Arguments.of("sum", Seq(27.0, 90.0), Seq(36.0, 102.0)),
    Arguments.of("count", Seq(9.0, 12.0), Seq(9.0, 12.0)),
    Arguments.of("mean", Seq(3.0, 7.5), Seq(4.0, 8.5)),
  )
}

class ReduceSpatialTest extends TileLayerRDDBuilders {
  import ReduceSpatialTest._

  private val n = NODATA

  private val multibandTile: MultibandTile = {
    val band0 = IntConstantNoDataArrayTile(
      Array(
        1, 1, 2, 2,
        n, n, n, 3,
        4, n, 4, 5,
        5, n, n, n,
      ), cols = 4, rows = 4)

    val band1 = IntConstantNoDataArrayTile(
      Array(
        6, 6, 6, n,
        7, 7, 7, 7,
        8, n, 9, 9,
        9, n, n, 9,
      ), cols = 4, rows = 4)

    ArrayMultibandTile(band0, band1)
  }

  private val tileLayout = TileLayout(
    layoutCols = 2, layoutRows = 2, tileCols = multibandTile.cols / 2, tileRows = multibandTile.rows / 2
  )

  @ParameterizedTest
  @MethodSource(Array("reduceSpatialDataCubeParams"))
  def reduceSpatialDataCube(reducer: String, expectedBandValues: Seq[Double]): Unit = {
    val spatialCube = createMultibandTileLayerRDD(sc, multibandTile, tileLayout)
    val actualBandValues = new ComputeStatsGeotrellisAdapter().compute_reduction_from_spatial_datacube(spatialCube, reducer)

    assertEquals(expectedBandValues, actualBandValues.asScala)
  }

  @ParameterizedTest
  @MethodSource(Array("reduceSpatiotemporalDataCubeParams"))
  def reduceSpatiotemporalDataCube(reducer: String, expectedTimestamp0BandValues: Seq[Double], expectedTimestamp1BandValues: Seq[Double]): Unit = {
    val timestamp0 = LocalDate.of(1981, 4, 24).atStartOfDay(ZoneOffset.UTC)
    val timestamp1 = timestamp0.plusDays(1)

    val spaceTimeCube = this.spaceTimeCube(timestamp0, timestamp1)
    val actualBandValues = new ComputeStatsGeotrellisAdapter().compute_reduction_timeseries_from_spatiotemporal_datacube(spaceTimeCube, reducer)

    assertEquals(
      expectedTimestamp0BandValues,
      actualBandValues.get(timestamp0.toInstant.toEpochMilli.toString).asScala
    )

    assertEquals(
      expectedTimestamp1BandValues,
      actualBandValues.get(timestamp1.toInstant.toEpochMilli.toString).asScala
    )
  }

  private def spaceTimeCube(timestamp0: ZonedDateTime, timestamp1: ZonedDateTime): MultibandTileLayerRDD[SpaceTimeKey] = {
    val spatialCube = createMultibandTileLayerRDD(sc, multibandTile, tileLayout)

    val spaceTimeRdd = spatialCube.flatMap { case (SpatialKey(col, row), multibandTile) =>
      Seq(
        SpaceTimeKey(col, row, timestamp0) -> multibandTile,
        SpaceTimeKey(col, row, timestamp1) -> multibandTile.mapBands { case (_, tile) => tile.mapIfSet(_ + 1) }
      )
    }

    val metadata = spatialCube.metadata.copy(bounds = spatialCube.metadata.bounds.flatMap { case KeyBounds(SpatialKey(minCol, minRow), SpatialKey(maxCol, maxRow)) =>
      KeyBounds(minKey = SpaceTimeKey(minCol, minRow, timestamp0), maxKey = SpaceTimeKey(maxCol, maxRow, timestamp1))
    })

    ContextRDD(spaceTimeRdd, metadata)
  }

  @ParameterizedTest
  @MethodSource(Array("reduceSpatiotemporalDataCubeParams"))
  def reduceSpatialDataCubeWithDataFrames(reducer: String, expectedTimestamp0BandValues: Seq[Double], expectedTimestamp1BandValues: Seq[Double]): Unit = {
    val timestamp0 = LocalDate.of(1981, 4, 24).atStartOfDay(ZoneOffset.UTC)
    val timestamp1 = timestamp0.plusDays(1)

    val spaceTimeCube = this.spaceTimeCube(timestamp0, timestamp1)

    val isFloatingPoint = spaceTimeCube.metadata.cellType.isFloatingPoint

    val pixelRdd: RDD[Row] = for {
      keyedMultibandTile <- spaceTimeCube
      (spaceTimeKey, multibandTile) = keyedMultibandTile // odd that this doesn't work directly
      date = java.sql.Timestamp.from(spaceTimeKey.time.toInstant)
      row <- 0 until multibandTile.rows
      col <- 0 until multibandTile.cols
      bandValues = multibandTile.bands.map { tile =>
        if (isFloatingPoint) {
          val value = tile.getDouble(col, row)
          if (isNoData(value)) null else value
        } else {
          val value = tile.get(col, row)
          if (isNoData(value)) null else value
        }
      }
    } yield Row.fromSeq(date +: bandValues)

    val dataType = if (isFloatingPoint) DoubleType else IntegerType
    val bandColumns = (0 until multibandTile.bandCount).map(bandIndex => s"band_$bandIndex")

    val bandStructs = bandColumns.map(StructField(_, dataType))
    val dateStruct = StructField("date", TimestampType)

    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val df = spark.createDataFrame(pixelRdd, schema = StructType(dateStruct +: bandStructs))

    val actualBandValues = df
      .groupBy("date")
      .agg(bandColumns.map((_, reducer)).toMap)
      .sort("date")
      .drop("date")
      .collect()

    for ((expectedBandValue, i) <- expectedTimestamp0BandValues.zipWithIndex) {
      val actualBandValue = actualBandValues.head.getAs[Number](i).doubleValue()
      assertEquals(expectedBandValue, actualBandValue)
    }

    for ((expectedBandValue, i) <- expectedTimestamp1BandValues.zipWithIndex) {
      val actualBandValue = actualBandValues.last.getAs[Number](i).doubleValue()
      assertEquals(expectedBandValue, actualBandValue)
    }
  }
}
