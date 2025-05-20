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

    val expectedBbox = java.util.Arrays.asList(tileLayerRDD.metadata.extent.xmin,tileLayerRDD.metadata.extent.ymin,tileLayerRDD.metadata.extent.xmax,tileLayerRDD.metadata.extent.ymax)
    val store = new FileSystemStore(filename,null)

    val inputStreamB01 = store.getInputStream("B01/.zattrs")
    assertTrue(inputStreamB01!=null)
    val attrB01 = ZarrUtils.fromJson(new InputStreamReader(inputStreamB01), classOf[java.util.Map[_, _]])
    assertTrue(attrB01.containsKey("_ARRAY_DIMENSIONS"))
    val dimB01 = attrB01.get("_ARRAY_DIMENSIONS")
    assertTrue(dimB01.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(2,dimB01.asInstanceOf[java.util.ArrayList[String]].size())
    assertTrue(dimB01.asInstanceOf[java.util.ArrayList[String]].contains("x"))
    assertTrue(dimB01.asInstanceOf[java.util.ArrayList[String]].contains("y"))
    assertTrue(attrB01.containsKey("_CRS"))
    val crsB01 = attrB01.get("_CRS")
    assertTrue(crsB01.isInstanceOf[java.util.Map[_,_]])
    assertTrue(crsB01.asInstanceOf[java.util.Map[_,_]].containsKey("wkt"))
    assertTrue(crsB01.asInstanceOf[java.util.Map[_,_]].containsKey("code"))
    assertEquals("EPSG:4326",crsB01.asInstanceOf[java.util.Map[_,_]].get("code"))
    assertTrue(crsB01.asInstanceOf[java.util.Map[_,_]].containsKey("proj:shape"))
    val shapeB01 = crsB01.asInstanceOf[java.util.Map[_,_]].get("proj:shape")
    assertEquals(java.util.Arrays.asList(1024,2048),shapeB01)
    assertTrue(crsB01.asInstanceOf[java.util.Map[_,_]].containsKey("proj:bbox"))
    val bboxB01 = crsB01.asInstanceOf[java.util.Map[_,_]].get("proj:bbox")
    assertEquals(expectedBbox,bboxB01)
    assertTrue(attrB01.containsKey("extent"))
    val extentB01 = attrB01.get("extent")
    assertTrue(extentB01.isInstanceOf[java.util.Map[_,_]])
    assertTrue(extentB01.asInstanceOf[java.util.Map[_,_]].containsKey("spatial"))

    val inputStreamZarrayB1 = store.getInputStream("B01/.zarray")
    assertTrue(inputStreamZarrayB1!=null)
    val zarrayB01 = ZarrUtils.fromJson(new InputStreamReader(inputStreamZarrayB1), classOf[java.util.Map[_, _]])
    assertTrue(zarrayB01.containsKey("shape"))
    assertEquals(java.util.Arrays.asList(1024,2048),zarrayB01.get("shape"))
    assertTrue(zarrayB01.containsKey("chunks"))
    assertEquals(java.util.Arrays.asList(256,256),zarrayB01.get("chunks"))

    val inputStreamZgroupB1 = store.getInputStream("B01/.zgroup")
    assertTrue(inputStreamZgroupB1!=null)
    val zgroupB01 = ZarrUtils.fromJson(new InputStreamReader(inputStreamZgroupB1), classOf[java.util.Map[_, _]])
    assertTrue(zgroupB01.containsKey("zarr_format"))
    assertEquals(2,zgroupB01.get("zarr_format"))

    val inputStreamB02 = store.getInputStream("B02/.zattrs")
    assertTrue(inputStreamB02!=null)
    val attrB02 = ZarrUtils.fromJson(new InputStreamReader(inputStreamB02), classOf[java.util.Map[_, _]])
    assertTrue(attrB02.containsKey("_ARRAY_DIMENSIONS"))
    val dimB02 = attrB02.get("_ARRAY_DIMENSIONS")
    assertTrue(dimB02.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(2,dimB02.asInstanceOf[java.util.ArrayList[String]].size())
    assertTrue(dimB02.asInstanceOf[java.util.ArrayList[String]].contains("x"))
    assertTrue(dimB02.asInstanceOf[java.util.ArrayList[String]].contains("y"))
    assertTrue(attrB02.containsKey("_CRS"))
    val crsB02 = attrB02.get("_CRS")
    assertTrue(crsB02.isInstanceOf[java.util.Map[_,_]])
    assertTrue(crsB02.asInstanceOf[java.util.Map[_,_]].containsKey("wkt"))
    assertTrue(crsB02.asInstanceOf[java.util.Map[_,_]].containsKey("code"))
    assertEquals("EPSG:4326",crsB02.asInstanceOf[java.util.Map[_,_]].get("code"))
    assertTrue(crsB02.asInstanceOf[java.util.Map[_,_]].containsKey("proj:shape"))
    val shapeB02 = crsB02.asInstanceOf[java.util.Map[_,_]].get("proj:shape")
    assertEquals(java.util.Arrays.asList(1024,2048),shapeB02)
    assertTrue(crsB02.asInstanceOf[java.util.Map[_,_]].containsKey("proj:bbox"))
    val bboxB02 = crsB02.asInstanceOf[java.util.Map[_,_]].get("proj:bbox")
    assertEquals(expectedBbox,bboxB02)
    assertTrue(attrB02.containsKey("extent"))
    val extentB02 = attrB02.get("extent")
    assertTrue(extentB02.isInstanceOf[java.util.Map[_,_]])
    assertTrue(extentB02.asInstanceOf[java.util.Map[_,_]].containsKey("spatial"))

    val inputStreamZarrayB2 = store.getInputStream("B02/.zarray")
    assertTrue(inputStreamZarrayB2!=null)
    val zarrayB02 = ZarrUtils.fromJson(new InputStreamReader(inputStreamZarrayB2), classOf[java.util.Map[_, _]])
    assertTrue(zarrayB02.containsKey("shape"))
    assertEquals(java.util.Arrays.asList(1024,2048),zarrayB02.get("shape"))
    assertTrue(zarrayB02.containsKey("chunks"))
    assertEquals(java.util.Arrays.asList(256,256),zarrayB02.get("chunks"))


    val inputStreamZgroupB2 = store.getInputStream("B02/.zgroup")
    assertTrue(inputStreamZgroupB2!=null)
    val zgroupB02 = ZarrUtils.fromJson(new InputStreamReader(inputStreamZgroupB2), classOf[java.util.Map[_, _]])
    assertTrue(zgroupB02.containsKey("zarr_format"))
    assertEquals(2,zgroupB02.get("zarr_format"))

    val inputStreamB04 = store.getInputStream("B04/.zattrs")
    assertTrue(inputStreamB04!=null)
    val attrB04 = ZarrUtils.fromJson(new InputStreamReader(inputStreamB04), classOf[java.util.Map[_, _]])
    assertTrue(attrB04.containsKey("_ARRAY_DIMENSIONS"))
    val dimB04 = attrB04.get("_ARRAY_DIMENSIONS")
    assertTrue(dimB04.isInstanceOf[java.util.ArrayList[String]])
    assertEquals(2,dimB04.asInstanceOf[java.util.ArrayList[String]].size())
    assertTrue(dimB04.asInstanceOf[java.util.ArrayList[String]].contains("x"))
    assertTrue(dimB04.asInstanceOf[java.util.ArrayList[String]].contains("y"))
    assertTrue(attrB04.containsKey("_CRS"))
    val crsB04 = attrB04.get("_CRS")
    assertTrue(crsB04.isInstanceOf[java.util.Map[_,_]])
    assertTrue(crsB04.asInstanceOf[java.util.Map[_,_]].containsKey("wkt"))
    assertTrue(crsB04.asInstanceOf[java.util.Map[_,_]].containsKey("code"))
    assertEquals("EPSG:4326",crsB04.asInstanceOf[java.util.Map[_,_]].get("code"))
    assertTrue(crsB04.asInstanceOf[java.util.Map[_,_]].containsKey("proj:shape"))
    val shapeB04 = crsB04.asInstanceOf[java.util.Map[_,_]].get("proj:shape")
    assertEquals(java.util.Arrays.asList(1024,2048),shapeB04)
    assertTrue(crsB04.asInstanceOf[java.util.Map[_,_]].containsKey("proj:bbox"))
    val bboxB04 = crsB04.asInstanceOf[java.util.Map[_,_]].get("proj:bbox")
    assertEquals(expectedBbox,bboxB04)
    assertTrue(attrB04.containsKey("extent"))
    val extentB04 = attrB04.get("extent")
    assertTrue(extentB04.isInstanceOf[java.util.Map[_,_]])
    assertTrue(extentB04.asInstanceOf[java.util.Map[_,_]].containsKey("spatial"))

    val inputStreamZarrayB4 = store.getInputStream("B04/.zarray")
    assertTrue(inputStreamZarrayB4!=null)
    val zarrayB04 = ZarrUtils.fromJson(new InputStreamReader(inputStreamZarrayB4), classOf[java.util.Map[_, _]])
    assertTrue(zarrayB04.containsKey("shape"))
    assertEquals(java.util.Arrays.asList(1024,2048),zarrayB04.get("shape"))
    assertTrue(zarrayB04.containsKey("chunks"))
    assertEquals(java.util.Arrays.asList(256,256),zarrayB04.get("chunks"))

    val inputStreamZgroupB4 = store.getInputStream("B04/.zgroup")
    assertTrue(inputStreamZgroupB4!=null)
    val zgroupB04 = ZarrUtils.fromJson(new InputStreamReader(inputStreamZgroupB4), classOf[java.util.Map[_, _]])
    assertTrue(zgroupB04.containsKey("zarr_format"))
    assertEquals(2,zgroupB04.get("zarr_format"))


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

    val inputStreamZgroup = store.getInputStream(".zgroup")
    assertTrue(inputStreamZgroup!=null)
    val zgroup = ZarrUtils.fromJson(new InputStreamReader(inputStreamZgroup), classOf[java.util.Map[_, _]])
    assertTrue(zgroup.containsKey("zarr_format"))
    assertEquals(2,zgroup.get("zarr_format"))
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
