package org.openeo.geotrellis.zarr

import better.files.File.apply
import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{ByteArrayTile, ColorMaps, MultibandTile, TileLayout}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.io.TempDir
import org.junit.rules.TemporaryFolder
import org.junit.{AfterClass, Rule}
import org.junit.jupiter.api.{BeforeAll, Test}
import org.openeo.geotrellis.{LayerFixtures, zarr}
import org.openeo.geotrellis.geotiff.{GTiffOptions, WriteRDDToGeotiffTest}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Path
import java.time.ZonedDateTime
import scala.annotation.meta.getter

object ZarrWriterTest{
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[WriteRDDToGeotiffTest])

  var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = {
    sc = {
      val conf = new SparkConf().setMaster("local[8]").setAppName(getClass.getSimpleName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName)
        .set("spark.ui.enabled", "true")
      SparkContext.getOrCreate(conf)
    }
    if (sc.uiWebUrl.isDefined) logger.info("Spark uiWebUrl: " + sc.uiWebUrl.get)
  }

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class ZarrWriterTest {

  @(Rule @getter)
  val temporaryFolder = new TemporaryFolder

  val allOverviewOptions = {
    val opts = new GTiffOptions()
    opts.setColorMap(ColorMaps.IGBP)
    opts.addHeadTag("Copyright", "The unit test.")
    opts.addBandTag(0, "BAND", "Band Name")
    opts.overviews = "ALL"
    opts
  }

  @Test
  def testWriteSingleBandRDD(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(ZarrWriterTest.sc,MultibandTile(imageTile),TileLayout(layoutCols,layoutRows,256,256),LatLng)

    val filename = (tempDir / "out.zarr").toString()
    zarr.ZarrWriter.saveZarr(tileLayerRDD,filename,1)
  }


  @Test
  def testWriteMultiBandRDD(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val secondBand = imageTile.map{x => if(x >= 5 ) 10 else 100 }
    val thirdBand = imageTile.map{x => if(x >= 5 ) 50 else 200 }

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(ZarrWriterTest.sc,MultibandTile(imageTile,secondBand,thirdBand),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val filename = (tempDir / "outMultiBand.zarr").toString()
    zarr.ZarrWriter.saveZarr(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},filename,3)
  }

  @Test
  def testWriteSpaceTime(@TempDir tempDir:Path):Unit ={
    val date1 = ZonedDateTime.parse("1990-01-02T00:00:00Z")
    val extentTAP3857 = Extent(564389 - 10, 6659413 - 10, 565503 + 10, 6660301 + 10)

    for {
      date2 <- List(date1.plusDays(2), date1.plusHours(2))
    } {
      val dates = List(date1, date2)
      val dataCubeContextRDD: MultibandTileLayerRDD[SpaceTimeKey] = LayerFixtures.randomNoiseLayer(
        extent = extentTAP3857,
        crs = CRS.fromName("EPSG:3857"),
        dates = Some(dates)
      )
      val filename = (tempDir / "outMultiBand.zarr").toString()
      zarr.ZarrWriter.saveZarr(dataCubeContextRDD,filename,1)
    }
  }
}
