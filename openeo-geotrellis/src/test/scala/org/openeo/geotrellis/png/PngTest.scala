package org.openeo.geotrellis.png

import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchClient
import cats.data.NonEmptyList
import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteArrayTile, CellSize, ColorMaps, ColorRamps, MultibandTile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel.DISK_ONLY
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.LayerFixtures
import org.openeo.geotrellis.geotiff.WriteRDDToGeotiffTest
import org.openeo.geotrellis.layers.{FileLayerProvider, SplitYearMonthDayPathDateExtractor}

import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util

object PngTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    // originally geotrellis.spark.util.SparkUtils.createLocalSparkContext
    val conf = SparkUtils.createSparkConf
      .setMaster("local[2]")
      .setAppName(PngTest.getClass.getName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .set("spark.kryo.registrationRequired", "true") // this requires e.g. RasterSource to be registered too
      .set("spark.kryo.registrator", Seq(
        classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName,
        classOf[org.openeo.geotrellis.png.KryoRegistrator].getName) mkString ","
      )

    sc = new SparkContext(conf)
  }

  @AfterClass
  def tearDownSpark(): Unit =
    sc.stop()
}

class PngTest {
  import PngTest._

  @Test
  def testSaveStitched(): Unit = {

    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)
    val tileLayerRDD =  LayerFixtures.aSpacetimeTileLayerRdd( 8,4)

    val spatialLayer = tileLayerRDD._1.toSpatial()

    val singleBand = spatialLayer.withContext{_.mapValues(_.subsetBands(0))}
    val opts = new PngOptions
    opts.setColorMap(new util.ArrayList(java.util.Arrays.asList(10,20,584854)))
    saveStitched(singleBand, "/tmp/testSaveStitchedColormap.png",null,opts)
  }

  @Test
  def testSaveStitchedColormap(): Unit = {
    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)
    val tileLayerRDD =  LayerFixtures.aSpacetimeTileLayerRdd( 8,4)

    val spatialLayer = tileLayerRDD._1.toSpatial()

    val singleBand = spatialLayer.withContext{_.mapValues(_.subsetBands(0))}
    val opts = new PngOptions

    opts.setColorMap(ColorRamps.BlueToRed.toColorMap(Range(0,256).toArray))
    saveStitched(singleBand, "/tmp/testSaveStitchedColormap.png",null,opts)
  }

}
