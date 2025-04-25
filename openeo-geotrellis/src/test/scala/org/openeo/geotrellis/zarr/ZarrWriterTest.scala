package org.openeo.geotrellis.zarr

import better.files.File.apply
import com.bc.zarr.storage.FileSystemStore
import com.bc.zarr.ZarrUtils
import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{ByteArrayTile, ColorMaps, MultibandTile, TileLayout}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.rules.TemporaryFolder
import org.junit.{AfterClass, Rule}
import org.junit.jupiter.api.{BeforeAll, Test}
import org.openeo.geotrellis.{LayerFixtures, zarr}
import org.openeo.geotrellis.geotiff.{GTiffOptions, WriteRDDToGeotiffTest}
import org.slf4j.{Logger, LoggerFactory}

import java.io.InputStreamReader
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
    val zarrOptions = new ZarrOptions

    val filename = (tempDir / "out.zarr").toString()
    zarr.ZarrWriter.saveZarrGeneric(tileLayerRDD,filename,zarrOptions)

    val store = new FileSystemStore(filename,null)
    val inputStream = store.getInputStream("out/.zattrs")
    assertTrue(inputStream!=null)
    val attr = ZarrUtils.fromJson(new InputStreamReader(inputStream), classOf[java.util.Map[_, _]])
    assertTrue(attr.containsKey("_ARRAY_DIMENSIONS"))
    val dim = attr.get("_ARRAY_DIMENSIONS")
    assertTrue(dim.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(2,dim.asInstanceOf[java.util.ArrayList[String]].size())
    assertTrue(dim.asInstanceOf[java.util.ArrayList[String]].contains("x"))
    assertTrue(dim.asInstanceOf[java.util.ArrayList[String]].contains("y"))
    assertTrue(attr.containsKey("_CRS"))
    val crs = attr.get("_CRS")
    assertTrue(crs.isInstanceOf[java.util.Map[_,_]])
    assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("wkt"))
    assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("code"))
    assertEquals("EPSG:4326",crs.asInstanceOf[java.util.Map[_,_]].get("code"))
    assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("proj:shape"))
    val shape = crs.asInstanceOf[java.util.Map[_,_]].get("proj:shape")
    assertEquals(java.util.Arrays.asList(1024,2048),shape)
    assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("proj:bbox"))
    val bbox = crs.asInstanceOf[java.util.Map[_,_]].get("proj:bbox")
    val expectedBbox = java.util.Arrays.asList(tileLayerRDD.metadata.extent.xmin,tileLayerRDD.metadata.extent.ymin,tileLayerRDD.metadata.extent.xmax,tileLayerRDD.metadata.extent.ymax)
    assertEquals(expectedBbox,bbox)

    assertTrue(attr.containsKey("extent"))
    val extent = attr.get("extent")
    assertTrue(extent.isInstanceOf[java.util.Map[_,_]])
    assertTrue(extent.asInstanceOf[java.util.Map[_,_]].containsKey("spatial"))


    val inputStreamX = store.getInputStream("x/.zattrs")
    assertTrue(inputStreamX!=null)
    val attrX = ZarrUtils.fromJson(new InputStreamReader(inputStreamX), classOf[java.util.Map[_, _]])
    assertTrue(attrX.containsKey("_ARRAY_DIMENSIONS"))
    val dimX = attrX.get("_ARRAY_DIMENSIONS")
    assertTrue(dimX.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(1,dimX.asInstanceOf[java.util.ArrayList[String]].size())

    val inputStreamY = store.getInputStream("y/.zattrs")
    assertTrue(inputStreamY!=null)
    val attrY = ZarrUtils.fromJson(new InputStreamReader(inputStreamY), classOf[java.util.Map[_, _]])
    assertTrue(attrY.containsKey("_ARRAY_DIMENSIONS"))
    val dimY = attrY.get("_ARRAY_DIMENSIONS")
    assertTrue(dimY.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(1,dimY.asInstanceOf[java.util.ArrayList[String]].size())

    val inputStreamZarray = store.getInputStream("out/.zarray")
    assertTrue(inputStreamZarray!=null)
    val zarray = ZarrUtils.fromJson(new InputStreamReader(inputStreamZarray), classOf[java.util.Map[_, _]])
    assertTrue(zarray.containsKey("shape"))
    assertEquals(java.util.Arrays.asList(1024,2048),zarray.get("shape"))
    assertTrue(zarray.containsKey("chunks"))
  }


  @Test
  def testWriteMultiBandRDD(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val secondBand = imageTile.map{x => if(x >= 5 ) 10 else 100 }
    val thirdBand = imageTile.map{x => if(x >= 5 ) 50 else 200 }
    val zarrOptions = new ZarrOptions
    zarrOptions.setBands(3, new java.util.ArrayList(java.util.Arrays.asList("B01","B02","B04")))

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(ZarrWriterTest.sc,MultibandTile(imageTile,secondBand,thirdBand),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val filename = (tempDir / "out.zarr").toString()
    zarr.ZarrWriter.saveZarrGeneric(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},filename,zarrOptions)

    val store = new FileSystemStore(filename,null)
    val inputStream = store.getInputStream("out/.zattrs")
    assertTrue(inputStream!=null)
    val attr = ZarrUtils.fromJson(new InputStreamReader(inputStream), classOf[java.util.Map[_, _]])
    assertTrue(attr.containsKey("_ARRAY_DIMENSIONS"))
    val dim = attr.get("_ARRAY_DIMENSIONS")
    assertTrue(dim.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(3,dim.asInstanceOf[java.util.ArrayList[String]].size())
    assertTrue(dim.asInstanceOf[java.util.ArrayList[String]].contains("x"))
    assertTrue(dim.asInstanceOf[java.util.ArrayList[String]].contains("y"))
    assertTrue(dim.asInstanceOf[java.util.ArrayList[String]].contains("Band"))
    assertTrue(attr.containsKey("COLOR_INTERPRETATION"))
    assertEquals(new java.util.ArrayList(java.util.Arrays.asList("B01","B02","B04")),attr.get("COLOR_INTERPRETATION"))
    assertTrue(attr.containsKey("_CRS"))
    val crs = attr.get("_CRS")
    assertTrue(crs.isInstanceOf[java.util.Map[_,_]])
    assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("wkt"))
    assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("code"))
    assertEquals("EPSG:4326",crs.asInstanceOf[java.util.Map[_,_]].get("code"))
    assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("proj:shape"))
    val shape = crs.asInstanceOf[java.util.Map[_,_]].get("proj:shape")
    assertEquals(java.util.Arrays.asList(1024,2048),shape)
    assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("proj:bbox"))
    val bbox = crs.asInstanceOf[java.util.Map[_,_]].get("proj:bbox")
    val expectedBbox = java.util.Arrays.asList(tileLayerRDD.metadata.extent.xmin,tileLayerRDD.metadata.extent.ymin,tileLayerRDD.metadata.extent.xmax,tileLayerRDD.metadata.extent.ymax)
    assertEquals(expectedBbox,bbox)

    assertTrue(attr.containsKey("extent"))
    val extent = attr.get("extent")
    assertTrue(extent.isInstanceOf[java.util.Map[_,_]])
    assertTrue(extent.asInstanceOf[java.util.Map[_,_]].containsKey("spatial"))


    val inputStreamX = store.getInputStream("x/.zattrs")
    assertTrue(inputStreamX!=null)
    val attrX = ZarrUtils.fromJson(new InputStreamReader(inputStreamX), classOf[java.util.Map[_, _]])
    assertTrue(attrX.containsKey("_ARRAY_DIMENSIONS"))
    val dimX = attrX.get("_ARRAY_DIMENSIONS")
    assertTrue(dimX.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(1,dimX.asInstanceOf[java.util.ArrayList[String]].size())

    val inputStreamY = store.getInputStream("y/.zattrs")
    assertTrue(inputStreamY!=null)
    val attrY = ZarrUtils.fromJson(new InputStreamReader(inputStreamY), classOf[java.util.Map[_, _]])
    assertTrue(attrY.containsKey("_ARRAY_DIMENSIONS"))
    val dimY = attrY.get("_ARRAY_DIMENSIONS")
    assertTrue(dimY.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(1,dimY.asInstanceOf[java.util.ArrayList[String]].size())

    val inputStreamZarray = store.getInputStream("out/.zarray")
    assertTrue(inputStreamZarray!=null)
    val zarray = ZarrUtils.fromJson(new InputStreamReader(inputStreamZarray), classOf[java.util.Map[_, _]])
    assertTrue(zarray.containsKey("shape"))
    assertEquals(java.util.Arrays.asList(3,1024,2048),zarray.get("shape"))
    assertTrue(zarray.containsKey("chunks"))
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
      val filename = (tempDir / "out.zarr").toString()
      val zarrOptions = new ZarrOptions
      zarr.ZarrWriter.saveZarr(dataCubeContextRDD,filename,zarrOptions)

      val store = new FileSystemStore(filename,null)
      val inputStream = store.getInputStream("out/.zattrs")
      assertTrue(inputStream!=null)
      val attr = ZarrUtils.fromJson(new InputStreamReader(inputStream), classOf[java.util.Map[_, _]])
      assertTrue(attr.containsKey("_ARRAY_DIMENSIONS"))
      val dim = attr.get("_ARRAY_DIMENSIONS")
      assertTrue(dim.isInstanceOf[java.util.ArrayList[String]])
      assertEquals(3,dim.asInstanceOf[java.util.ArrayList[String]].size())
      assertTrue(dim.asInstanceOf[java.util.ArrayList[String]].contains("x"))
      assertTrue(dim.asInstanceOf[java.util.ArrayList[String]].contains("y"))
      assertTrue(dim.asInstanceOf[java.util.ArrayList[String]].contains("time"))
      assertTrue(attr.containsKey("_CRS"))
      val crs = attr.get("_CRS")
      assertTrue(crs.isInstanceOf[java.util.Map[_,_]])
      assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("wkt"))
      assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("code"))
      assertEquals("EPSG:3857",crs.asInstanceOf[java.util.Map[_,_]].get("code"))
      assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("proj:shape"))
      val shape = crs.asInstanceOf[java.util.Map[_,_]].get("proj:shape")
      assertEquals(java.util.Arrays.asList(256,256),shape)
      assertTrue(crs.asInstanceOf[java.util.Map[_,_]].containsKey("proj:bbox"))
      val bbox = crs.asInstanceOf[java.util.Map[_,_]].get("proj:bbox")
      val expectedBbox = java.util.Arrays.asList(extentTAP3857.xmin,extentTAP3857.ymin,extentTAP3857.xmax,extentTAP3857.ymax)
      assertEquals(expectedBbox,bbox)

      assertTrue(attr.containsKey("extent"))
      val extent = attr.get("extent")
      assertTrue(extent.isInstanceOf[java.util.Map[_,_]])
      assertTrue(extent.asInstanceOf[java.util.Map[_,_]].containsKey("spatial"))
      assertTrue(extent.asInstanceOf[java.util.Map[_,_]].containsKey("temporal"))


      val inputStreamX = store.getInputStream("x/.zattrs")
      assertTrue(inputStreamX!=null)
      val attrX = ZarrUtils.fromJson(new InputStreamReader(inputStreamX), classOf[java.util.Map[_, _]])
      assertTrue(attrX.containsKey("_ARRAY_DIMENSIONS"))
      val dimX = attrX.get("_ARRAY_DIMENSIONS")
      assertTrue(dimX.isInstanceOf[java.util.ArrayList[String]])
      assertEquals(1,dimX.asInstanceOf[java.util.ArrayList[String]].size())

      val inputStreamY = store.getInputStream("y/.zattrs")
      assertTrue(inputStreamY!=null)
      val attrY = ZarrUtils.fromJson(new InputStreamReader(inputStreamY), classOf[java.util.Map[_, _]])
      assertTrue(attrY.containsKey("_ARRAY_DIMENSIONS"))
      val dimY = attrY.get("_ARRAY_DIMENSIONS")
      assertTrue(dimY.isInstanceOf[java.util.ArrayList[String]])
      assertEquals(1,dimY.asInstanceOf[java.util.ArrayList[String]].size())

      val inputStreamTime = store.getInputStream("time/.zattrs")
      assertTrue(inputStreamTime!=null)
      val attrTime = ZarrUtils.fromJson(new InputStreamReader(inputStreamTime), classOf[java.util.Map[_, _]])
      assertTrue(attrTime.containsKey("_ARRAY_DIMENSIONS"))
      val dimTime = attrTime.get("_ARRAY_DIMENSIONS")
      assertTrue(dimTime.isInstanceOf[java.util.ArrayList[String]])
      assertEquals(1,dimTime.asInstanceOf[java.util.ArrayList[String]].size())

      val inputStreamZarray = store.getInputStream("out/.zarray")
      assertTrue(inputStreamZarray!=null)
      val zarray = ZarrUtils.fromJson(new InputStreamReader(inputStreamZarray), classOf[java.util.Map[_, _]])
      assertTrue(zarray.containsKey("shape"))
      assertEquals(java.util.Arrays.asList(2,256,256),zarray.get("shape"))
      assertTrue(zarray.containsKey("chunks"))
    }
  }
}
