package org.openeo.geotrellis.geotiff

import geotrellis.layer.{CRSWorldExtent, SpaceTimeKey, SpatialKey, ZoomedLayoutScheme}
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.{ByteArrayTile, ByteConstantNoDataCellType, ByteConstantTile, ColorMaps, MultibandTile, Raster, Tile, TileLayout, isData}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.vector._
import geotrellis.vector.io.json.GeoJson
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._
import org.junit.rules.TemporaryFolder
import org.openeo.geotrellis.{LayerFixtures, OpenEOProcesses, ProjectedPolygons}

import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime
import java.util
import java.util.zip.Deflater._
import scala.annotation.meta.getter
import scala.collection.JavaConverters._
import scala.io.Source


object WriteRDDToGeotiffTest{

  var sc: SparkContext = _

  @BeforeClass
  def setupSpark() = {
    sc = {
      val conf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName)
      SparkContext.getOrCreate(conf)
    }
  }

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class WriteRDDToGeotiffTest {

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
  def testWriteRDD(): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val filename = "out.tif"

    saveRDD(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},1,filename,formatOptions = allOverviewOptions)

    val tiff = GeoTiff.readSingleband(filename)
    assertTrue(tiff.options.colorMap.isDefined)
    assertEquals("Band Name",tiff.tags.bandTags(0).get("BAND").get)
    assertEquals(3,tiff.overviews.size)
    val output = tiff.raster.tile
    assertArrayEquals(imageTile.toArray(),output.toArray())
  }


  @Test
  def testWriteRDD_apply_neighborhood(): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val tileLayerRDD = LayerFixtures.buildSingleBandSpatioTemporalDataCube(util.Arrays.asList(imageTile),Seq("2017-03-01T00:00:00Z"))

    val filename = "openEO_2017-03-01Z.tif"
    val p = new OpenEOProcesses()
    val buffered: MultibandTileLayerRDD[SpaceTimeKey] = p.remove_overlap(p.retile(tileLayerRDD,224,224,16,16),224,224,16,16)

    val cropBounds = Extent(-115, -65, 5.0, 56)
    saveRDDTemporal(buffered,"./",cropBounds = Some(cropBounds))

    val croppedRaster: Raster[MultibandTile] = tileLayerRDD.toSpatial().stitch().crop(cropBounds)
    val referenceFile = "croppedRaster.tif"
    GeoTiff(croppedRaster,LatLng).write(referenceFile)

    val result = GeoTiff.readMultiband(filename).raster
    val reference = GeoTiff.readMultiband(referenceFile).raster

    assertArrayEquals(reference.tile.band(0).toArray(),result.tile.band(0).toArray())

  }

  @Test
  def testWriteMultibandRDD(): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
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

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
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

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256,500)
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

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
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



  @Test
  def testWriteMultibandRDDWithGaps(): Unit ={
    val layoutCols = 8
    val layoutRows = 4
    val ( imageTile:ByteArrayTile, filtered:MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(layoutCols,layoutRows)

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
    val (layer,imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)


    saveRDDTemporal(layer,"./")
    val result = GeoTiff.readMultiband("./openEO_2017-01-02Z.tif").raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val croppedOutput = result.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(),croppedOutput.toArray())
    val result2 = GeoTiff.readMultiband("./openEO_2017-01-03Z.tif").raster.tile
    assertArrayEquals(croppedReference.toArray(),result2.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArray())

  }

  @Test
  def testSaveSamplesOnlyConsidersPixelsWithinGeometry(): Unit = {
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage(layoutCols * 256, layoutRows * 256)
    val imageTile = ByteArrayTile(intImage, layoutCols * 256, layoutRows * 256)

    val date = ZonedDateTime.now()

    val tileLayerRDD = TileLayerRDDBuilders
      .createSpaceTimeTileLayerRDD(Seq((imageTile, date)), TileLayout(layoutCols, layoutRows, 256, 256),
        ByteConstantNoDataCellType)(WriteRDDToGeotiffTest.sc)
      .withContext(_.mapValues(MultibandTile(_)))

    val geometriesPath = getClass.getResource("/org/openeo/geotrellis/geotiff/ll_ur_polygon.geojson").getPath

    // its extent differs substantially from its shape
    val tiltedRectangle = ProjectedPolygons.fromVectorFile(geometriesPath)

    val sampleNames = tiltedRectangle.polygons.indices
      .map(_.toString)
      .asJava

    val targetDir = temporaryFolder.getRoot.toString

    saveSamples(tileLayerRDD, targetDir, tiltedRectangle, sampleNames,
      DeflateCompression(BEST_COMPRESSION))

    val Array(geoTiffPath) = Files.list(Paths.get(targetDir)).iterator().asScala.toArray // 1 date, 1 polygon
    val raster = GeoTiff.readMultiband(geoTiffPath.toString).raster.mapTile(_.band(0))

    val geometries = {
      val in = Source.fromFile(geometriesPath)
      try GeoJson.parse[Geometry](in.mkString)
      finally in.close()
    }

    // raster extent should be the same as the extent of the input geometries
    assertTrue(raster.extent.equalsExact(geometries.extent, 1.0))

    def rasterValueAt(point: Point): Int = {
      val (col, row) = raster.rasterExtent.mapToGrid(point)
      raster.tile.get(col, row)
    }

    // pixels within input geometries should carry data
    val pointWithinGeometries = geometries.getCentroid
    assertTrue(isData(rasterValueAt(pointWithinGeometries)))

    // pixels outside of geometries should not carry data
    val pointOutsideOfGeometries = {
      val point = LineString(geometries.getCentroid, geometries.extent.southEast).getCentroid
      // sanity checks
      assertTrue(geometries.extent contains point)
      assertFalse(geometries.union() contains point)
      point
    }

    assertFalse(isData(rasterValueAt(pointOutsideOfGeometries)))
  }
}
