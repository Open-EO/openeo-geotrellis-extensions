package org.openeo.geotrellis

import geotrellis.raster.{ArrayMultibandTile, IntConstantNoDataArrayTile, MultibandTile, NODATA, Raster, TileLayout}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.stream.{Stream => JStream}
import scala.jdk.CollectionConverters._

object ReduceSpatialTest {
  private implicit var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()

  def testParams(): JStream[Arguments] = JStream.of(
    Arguments.of("max", Seq(5.0, 9.0)),
    Arguments.of("min", Seq(1.0, 6.0)),
    Arguments.of("sum", Seq(27.0, 90.0)),
    Arguments.of("count", Seq(9, 12))
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
  @MethodSource(Array("testParams"))
  def test(reducer: String, bandValues: Seq[Double]): Unit = {
    val cube = createMultibandTileLayerRDD(sc, multibandTile, tileLayout)
    val bandMaxes = new ComputeStatsGeotrellisAdapter().compute_reduction_timeseries_from_spatial_datacube(cube, reducer)

    assertEquals(bandValues, bandMaxes.asScala)
  }
}
