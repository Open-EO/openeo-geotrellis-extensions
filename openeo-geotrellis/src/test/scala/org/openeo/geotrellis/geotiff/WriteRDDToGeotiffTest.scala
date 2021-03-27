package org.openeo.geotrellis.geotiff

import java.awt.image.DataBufferByte

import geotrellis.layer.{CRSWorldExtent, KeyBounds, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ByteArrayTile, ByteConstantTile, MultibandTile, Raster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._



object WriteRDDToGeotiffTest{

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

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class WriteRDDToGeotiffTest {

  def createTextImage(width:Int,height:Int, fontSize:Int = 500) = {
    import java.awt.Font
    import java.awt.image.BufferedImage

    val font = new Font("Arial", Font.PLAIN, fontSize)
    val text = "openEO"

    val img = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val g2d = img.createGraphics

    g2d.setFont(font)
    val fm = g2d.getFontMetrics
    g2d.setColor(java.awt.Color.WHITE)
    g2d.translate(20,400)
    g2d.drawString(text, 0, fm.getAscent)
    g2d.dispose()

    img.getData().getDataBuffer().asInstanceOf[DataBufferByte].getData()


  }


  @Test
  def testWriteRDD(): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val filename = "out.tif"
    saveRDD(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},1,filename)

    val output = GeoTiff.readSingleband(filename).raster.tile
    assertArrayEquals(imageTile.toArray(),output.toArray())
  }

  @Test
  def testWriteMultibandRDD(): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val secondBand = imageTile.map{x => if(x >= 5 ) 10 else 100 }
    val thirdBand = imageTile.map{x => if(x >= 5 ) 50 else 200 }

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile,secondBand,thirdBand),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val filename = "outRGB.tif"
    saveRDD(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},3,filename)
    val result = GeoTiff.readMultiband(filename).raster.tile
    assertArrayEquals(imageTile.toArray(),result.band(0).toArray())
    assertArrayEquals(secondBand.toArray(),result.band(1).toArray())
    assertArrayEquals(thirdBand.toArray(),result.band(2).toArray())
  }


  @Test
  def testWriteCroppedRDD(): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val secondBand = imageTile.map{x => if(x >= 5 ) 10 else 100 }
    val thirdBand = imageTile.map{x => if(x >= 5 ) 50 else 200 }
    //,secondBand,thirdBand

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile,secondBand,thirdBand),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val currentExtent = tileLayerRDD.metadata.extent
    val cropBounds = Extent(-115, -65, 5.0, 56)

    val croppedRaster: Raster[MultibandTile] = tileLayerRDD.stitch().crop(cropBounds)
    val referenceFile = "croppedRaster.tif"
    GeoTiff(croppedRaster,LatLng).write(referenceFile)
    val filename = "outRGBCropped3.tif"
    saveRDD(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},3,filename,cropBounds = Some(cropBounds))
    val result = GeoTiff.readMultiband(filename).raster
    val reference = GeoTiff.readMultiband(referenceFile).raster

    assertEquals(result.extent,reference.extent)
    assertArrayEquals(reference.tile.band(0).toArray(),result.tile.band(0).toArray())

  }

  @Test
  def testWriteRDDGlobalLayout(): Unit ={
    val layoutCols = 8
    val layoutRows = 8

    val intImage = createTextImage( layoutCols*256, layoutRows*256,500)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)
    val secondBand = imageTile.map{x => if(x >= 5 ) 10 else 100 }
    val thirdBand = imageTile.map{x => if(x >= 5 ) 50 else 200 }


    val level = ZoomedLayoutScheme(LatLng).levelForZoom(3)

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile,secondBand,thirdBand),level.layout.tileLayout,LatLng)

    val cropBounds = Extent(0, -90, 180, 90)
    val croppedRaster: Raster[MultibandTile] = tileLayerRDD.stitch().crop(cropBounds)
    val referenceFile = "croppedRasterGlobalLayout.tif"
    GeoTiff(croppedRaster,LatLng).write(referenceFile)

    val filename = "outCropped.tif"
    saveRDD(tileLayerRDD.withContext{_.repartition(tileLayerRDD.count().toInt)},3,filename,cropBounds = Some(cropBounds))
    val resultRaster = GeoTiff.readMultiband(filename).raster


    val reference = GeoTiff.readMultiband(referenceFile).raster

    assertEquals(resultRaster.extent,reference.extent)
    assertArrayEquals(reference.tile.band(0).toArray(),resultRaster.tile.band(0).toArray())
  }

  @Test
  def testWriteEmptyRdd(): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256,256.toByte)

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val empty = tileLayerRDD.withContext{_.filter(_ => false)}
    val filename = "outEmpty.tif"
    val cropBounds = Extent(-115, -65, 5.0, 56)
    saveRDD(empty,-1,filename,cropBounds = Some(cropBounds))

    val emptyTile = ByteConstantTile.apply(256.toByte, 2048, 1024)
    val croppedReference: Raster[Tile] = new Raster(emptyTile,LatLng.worldExtent).crop(cropBounds)

    val result = GeoTiff.readMultiband(filename).raster.tile
    val croppedOutput = result.band(0).toArrayTile()
    assertArrayEquals(croppedReference.tile.toBytes(),croppedOutput.toBytes())

  }

  private def createLayerWithGaps(layoutCols:Int,layoutRows:Int) = {

    val intImage = createTextImage(layoutCols * 256, layoutRows * 256)
    val imageTile = ByteArrayTile(intImage, layoutCols * 256, layoutRows * 256)

    val secondBand = imageTile.map { x => if (x >= 5) 10 else 100 }
    val thirdBand = imageTile.map { x => if (x >= 5) 50 else 200 }

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc, MultibandTile(imageTile, secondBand, thirdBand), TileLayout(layoutCols, layoutRows, 256, 256), LatLng)
    print(tileLayerRDD.keys.collect())
    val filtered: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = tileLayerRDD.withContext {
      _.filter { case (key, tile) => (key.col > 0 && (key.col != 1 || key.row != 1)) }
    }
    (imageTile, filtered)
  }

  @Test
  def testWriteMultibandRDDWithGaps(): Unit ={
    val layoutCols = 8
    val layoutRows = 4
    val ( imageTile:ByteArrayTile, filtered:MultibandTileLayerRDD[SpatialKey]) = createLayerWithGaps(layoutCols,layoutRows)

    val filename = "outFiltered.tif"
    saveRDD(filtered.withContext{_.repartition(layoutCols*layoutRows)},3,filename)
    val result = GeoTiff.readMultiband(filename).raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val croppedOutput = result.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(),croppedOutput.toArray())
  }

  @Test
  def testWriteMultibandTemporalRDDWithGaps(): Unit ={
    val layoutCols = 8
    val layoutRows = 4
    val ( imageTile:ByteArrayTile, filtered:MultibandTileLayerRDD[SpatialKey]) = createLayerWithGaps(layoutCols,layoutRows)
    val temporal: RDD[(SpaceTimeKey, MultibandTile)] = filtered.flatMap(tuple=>{
      Seq((SpaceTimeKey(tuple._1,new TemporalKey(110000L)),tuple._2),(SpaceTimeKey(tuple._1,new TemporalKey(10000*110000L)),tuple._2))
    }).repartition(layoutCols*layoutRows)

    val spatialM = filtered.metadata
    val newBounds = KeyBounds[SpaceTimeKey](SpaceTimeKey(spatialM.bounds.get._1,TemporalKey(0L)),SpaceTimeKey(spatialM.bounds.get._2,TemporalKey(0L)))
    val temporalMetadata = new TileLayerMetadata[SpaceTimeKey](spatialM.cellType,spatialM.layout,spatialM.extent,spatialM.crs,newBounds)
    saveRDDTemporal(ContextRDD(temporal,temporalMetadata),"./")
    val result = GeoTiff.readMultiband("./openEO_1970-01-01Z.tif").raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val croppedOutput = result.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(),croppedOutput.toArray())
    val result2 = GeoTiff.readMultiband("./openEO_1970-01-13Z.tif").raster.tile
    assertArrayEquals(croppedReference.toArray(),result2.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArray())

  }


}
