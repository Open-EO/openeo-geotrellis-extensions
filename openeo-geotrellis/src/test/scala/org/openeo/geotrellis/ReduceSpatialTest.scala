package org.openeo.geotrellis

import geotrellis.layer.{KeyBounds, SpaceTimeKey, SpatialKey}
import geotrellis.raster.{ArrayMultibandTile, IntConstantNoDataArrayTile, MultibandTile, NODATA, TileLayout}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import org.apache.spark._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.geotrellis.aggregate_polygon.SparkAggregateScriptBuilder

import java.nio.file.Path
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import java.util
import java.util.stream.{Stream => JStream}
import scala.io.Source
import scala.jdk.StreamConverters._
import scala.util.Using

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

  private def csvLines(dir: Path): Seq[String] = {
    for {
      csvFile <- java.nio.file.Files.list(dir).toScala(Seq) if csvFile.toString.endsWith(".csv")
      lines = Using.resource(Source.fromFile(csvFile.toFile)) { resource => resource.getLines().toSeq }
      line <- lines.drop(1)
    } yield line
  }

  private def scriptBuilder(operator: String): SparkAggregateScriptBuilder = {
    val scriptBuilder = new SparkAggregateScriptBuilder
    scriptBuilder.expressionEnd(operator, arguments = util.Collections.emptyMap())
    scriptBuilder
  }
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
  def reduceSpatialDataCube(reducer: String, expectedBandValues: Seq[Double], @TempDir tempDir: Path): Unit = {
    val spatialCube = createMultibandTileLayerRDD(sc, multibandTile, tileLayout)
    new ComputeStatsGeotrellisAdapter().reduce_spatial_spatial_cube(spatialCube, scriptBuilder(reducer), outputDir = tempDir.toString)

    val Seq(csvLine) = csvLines(tempDir)

    assertEquals(expectedBandValues, csvLine.split(",").toSeq.map(_.toDouble))
  }

  @ParameterizedTest
  @MethodSource(Array("reduceSpatiotemporalDataCubeParams"))
  def reduceSpatiotemporalDataCube(reducer: String, expectedTimestamp0BandValues: Seq[Double], expectedTimestamp1BandValues: Seq[Double], @TempDir tempDir: Path): Unit = {
    val timestamp0 = LocalDate.of(1981, 4, 24).atStartOfDay(ZoneOffset.UTC)
    val timestamp1 = timestamp0.plusDays(1)

    val spaceTimeCube = this.spaceTimeCube(timestamp0, timestamp1)

    new ComputeStatsGeotrellisAdapter().reduce_spatial(spaceTimeCube, scriptBuilder(reducer), outputDir = tempDir.toString)

    val linesSortedByDate = csvLines(tempDir).sorted

    assertEquals(expectedTimestamp0BandValues, linesSortedByDate.head.split(",").toSeq.drop(1).map(_.toDouble))
    assertEquals(expectedTimestamp1BandValues, linesSortedByDate.last.split(",").toSeq.drop(1).map(_.toDouble))
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
}
