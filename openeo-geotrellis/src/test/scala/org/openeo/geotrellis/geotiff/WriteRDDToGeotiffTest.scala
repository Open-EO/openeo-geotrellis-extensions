package org.openeo.geotrellis.geotiff

import geotrellis.layer.{CRSWorldExtent, SpaceTimeKey, SpatialKey, ZoomedLayoutScheme}
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.render.ColorMap.Options
import geotrellis.raster.render.DoubleColorMap
import geotrellis.raster.{ByteArrayTile, ByteConstantNoDataCellType, ByteConstantTile, ColorMaps, MultibandTile, Raster, Tile, TileLayout, isData}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.vector._
import geotrellis.vector.io.json.GeoJson
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.junit.Assert._
import org.junit._
import org.junit.rules.TemporaryFolder
import org.openeo.geotrellis.{LayerFixtures, OpenEOProcesses, ProjectedPolygons}
import org.openeo.sparklisteners.GetInfoSparkListener
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Paths}
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util
import java.util.zip.Deflater._
import scala.annotation.meta.getter
import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.io.Directory


object WriteRDDToGeotiffTest{
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[WriteRDDToGeotiffTest])

  var sc: SparkContext = _

  @BeforeClass
  def setupSpark() = {
    sc = {
      val conf = new SparkConf().setMaster("local[2]").setAppName(getClass.getSimpleName)
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

class WriteRDDToGeotiffTest {

  import WriteRDDToGeotiffTest._

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
    assertEquals(layoutCols * layoutRows,tiff.imageData.segmentBytes.length)
    assertEquals(8*256,tiff.imageData.segmentLayout.totalCols)
    assertEquals(3,tiff.overviews.size)
    assertEquals(2,tiff.overviews(1).imageData.segmentBytes.length)
    assertEquals(2*256,tiff.overviews(1).imageData.segmentLayout.totalCols)
    val output = tiff.raster.tile
    assertArrayEquals(imageTile.toArray(),output.toArray())
  }

  @Test
  def testTiffOptionsSerializable(): Unit = {
    // This test is dependent on scala version
    println("Scala versionString: " + scala.util.Properties.versionString)

    val m = Map(0.0 -> 100, 1.0 -> 101, 2.0 -> -10, 3.0 -> 0).mapValues(_ * 3) //.map(identity)
    val colormap = new DoubleColorMap(m, new Options(noDataColor = 42))

    val opts = new GTiffOptions()
    opts.setColorMap(colormap)
    SparkEnv.get.closureSerializer.newInstance().serialize(opts)
    assertEquals(colormap.options.noDataColor, opts.colorMap.get.options.noDataColor)
    assertEquals(colormap.mapDouble(0.5), opts.colorMap.get.mapDouble(0.5))
    assertEquals(-30, colormap.mapDouble(2.0))
    assertEquals(-30, opts.colorMap.get.mapDouble(2.0))
    assertEquals(colormap.breaksString, opts.colorMap.get.breaksString)
  }

  @Test
  def testTiffOptionsSerializableMax(): Unit = {
    // This test is dependent on scala version
    println("Scala versionString: " + scala.util.Properties.versionString)

    val m = Map(
      0.0 -> 10,
      2.0 -> Int.MinValue,
      1.0 -> Int.MaxValue,
      1.0 -> Int.MaxValue,
      Double.NaN -> 20,
      Double.NegativeInfinity -> 30,
      Double.PositiveInfinity -> 40,
      Double.MinPositiveValue -> 50,
      -1.0 -> 60,
    )
    val colormap = new DoubleColorMap(m, new Options(noDataColor = 42424242))

    val opts = new GTiffOptions()
    opts.setColorMap(colormap)
    SparkEnv.get.closureSerializer.newInstance().serialize(opts)
    assertEquals(colormap.options.noDataColor, opts.colorMap.get.options.noDataColor)
    assertEquals(colormap.mapDouble(-1), opts.colorMap.get.mapDouble(-1))
    assertEquals(colormap.mapDouble(0.5), opts.colorMap.get.mapDouble(0.5))
    assertEquals(colormap.mapDouble(1.0), opts.colorMap.get.mapDouble(1.0))
    assertEquals(colormap.mapDouble(2.0), opts.colorMap.get.mapDouble(2.0))
    assertEquals(colormap.mapDouble(Double.NaN), opts.colorMap.get.mapDouble(Double.NaN))
    assertEquals(colormap.mapDouble(Double.NegativeInfinity), opts.colorMap.get.mapDouble(Double.NegativeInfinity))
    assertEquals(colormap.mapDouble(Double.PositiveInfinity), opts.colorMap.get.mapDouble(Double.PositiveInfinity))
    assertEquals(colormap.mapDouble(Double.MinPositiveValue), opts.colorMap.get.mapDouble(Double.MinPositiveValue))
    assertEquals(colormap.breaksString, opts.colorMap.get.breaksString)
    assertEquals(10, opts.colorMap.get.mapDouble(0))
    assertEquals(Int.MaxValue, opts.colorMap.get.mapDouble(1))
    assertEquals(Int.MinValue, opts.colorMap.get.mapDouble(2))
    assertEquals(60, colormap.mapDouble(-1.0))
    assertEquals(42424242, colormap.mapDouble(Double.NaN))
    assertEquals(42424242, opts.colorMap.get.mapDouble(Double.NaN))
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
    val buffered: MultibandTileLayerRDD[SpaceTimeKey] = p.remove_overlap(p.retileGeneric(tileLayerRDD,224,224,16,16),224,224,16,16)

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

    val outDir = Paths.get("tmp/testWriteMultibandRDDWithGaps/")
    new Directory(outDir.toFile).deepFiles.foreach(_.delete())
    Files.createDirectories(outDir)

    val filename = outDir + "/outFiltered.tif"
    saveRDD(filtered.withContext{_.repartition(layoutCols*layoutRows)},3,filename)
    val result = GeoTiff.readMultiband(filename).raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val croppedOutput = result.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(),croppedOutput.toArray())
  }

  @Test
  def testWriteMultibandRDDWithGapsSeparateAssetPerBand(): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (imageTile: ByteArrayTile, filtered: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandRDDWithGapsSeparateAssetPerBand/")
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val filename = outDir + "/out"
    val options = new GTiffOptions()
    options.separateAssetPerBand = true
    options.addBandTag(0, "DESCRIPTION", "B01")
    options.addBandTag(1, "DESCRIPTION", "B02")
    options.addBandTag(2, "DESCRIPTION", "B03")
    val paths = saveRDD(filtered.withContext {
      _.repartition(layoutCols * layoutRows)
    }, 3, filename, formatOptions = options)
    assertEquals(3, paths.size())

    GeoTiff.readMultiband(outDir.resolve("openEO_B01.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("openEO_B02.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("openEO_B03.tif").toString).raster.tile

    val result = GeoTiff.readMultiband(paths.get(0)).raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val resultWidth = result.band(0).toArrayTile().dimensions.cols
    val croppedOutput = result.band(0).toArrayTile().crop(resultWidth - (6 * 256), 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(), croppedOutput.toArray())
  }

  @Test
  def testWriteMultibandRDDWithGapsFilepathPerBand(): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (imageTile: ByteArrayTile, filtered: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandRDDWithGapsFilepathPerBand/")
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val filename = outDir + "/out"
    val options = new GTiffOptions()
    options.separateAssetPerBand = true

    val filepathPerBand: util.ArrayList[String] = new util.ArrayList[String]()
    filepathPerBand.add("testA/B01.tiff")
    filepathPerBand.add("testA/A/B02.tiff")
    filepathPerBand.add("testB/B03.tiff")
    options.setFilepathPerBand(Some(filepathPerBand))
    options.addBandTag(0, "DESCRIPTION", "B01")
    options.addBandTag(1, "DESCRIPTION", "B02")
    options.addBandTag(2, "DESCRIPTION", "B03")
    val paths = saveRDD(filtered.withContext {
      _.repartition(layoutCols * layoutRows)
    }, 3, filename, formatOptions = options)
    assertEquals(3, paths.size())

    GeoTiff.readMultiband(outDir.resolve("testA/B01.tiff").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("testA/A/B02.tiff").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("testB/B03.tiff").toString).raster.tile

    val result = GeoTiff.readMultiband(paths.get(0)).raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val resultWidth = result.band(0).toArrayTile().dimensions.cols
    val croppedOutput = result.band(0).toArrayTile().crop(resultWidth - (6 * 256), 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(), croppedOutput.toArray())
  }

  @Test
  def testWriteMultibandTemporalRDDWithGaps(): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandTemporalRDDWithGaps/")
    new Directory(outDir.toFile).deepFiles.foreach(_.delete())
    Files.createDirectories(outDir)

    saveRDDTemporal(layer, outDir.toString)
    val result = GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-02Z.tif").toString).raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val croppedOutput = result.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(), croppedOutput.toArray())
    val result2 = GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-03Z.tif").toString).raster.tile
    assertArrayEquals(croppedReference.toArray(), result2.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArray())
  }


  @Test
  def testWriteMultibandTemporalRDDWithGapsSeparateAssetPerBand(): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandTemporalRDDWithGapsSeparateAssetPerBand/")
    new Directory(outDir.toFile).deepFiles.foreach(_.delete())
    Files.createDirectories(outDir)

    val options = new GTiffOptions()
    options.separateAssetPerBand = true
    options.addBandTag(0, "DESCRIPTION", "B01")
    options.addBandTag(1, "DESCRIPTION", "B02")
    options.addBandTag(2, "DESCRIPTION", "B03")
    saveRDDTemporal(layer, outDir.toString, formatOptions = options)

    GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-02Z_B01.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-02Z_B02.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-02Z_B03.tif").toString).raster.tile

    GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-03Z_B01.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-03Z_B02.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-03Z_B03.tif").toString).raster.tile
  }

  @Test
  def testWriteMultibandTemporalHourlyRDDWithGaps(): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerHoursRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/geotiffGapsHourly/")
    new Directory(outDir.toFile).deleteRecursively()
    Files.createDirectories(outDir)

    saveRDDTemporal(layer, outDir.toString)
    val result = GeoTiff.readMultiband(outDir.resolve("openEO_20170101T010000Z.tif").toString).raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val croppedOutput = result.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(), croppedOutput.toArray())
    val result2 = GeoTiff.readMultiband(outDir.resolve("openEO_20170101T020000Z.tif").toString).raster.tile
    assertArrayEquals(croppedReference.toArray(), result2.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArray())
  }

  @Test
  def testWriteMultibandTemporalRDDWithGapsNamed(): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/geotiffGapsNamed/")
    new Directory(outDir.toFile).deleteRecursively()
    Files.createDirectories(outDir)

    val opts = new GTiffOptions()
    opts.setFilenamePrefix("testName")
    saveRDDTemporal(layer, outDir.toString, formatOptions = opts)
    val result = GeoTiff.readMultiband(outDir.resolve("testName_2017-01-02Z.tif").toString).raster.tile

    //crop away the area where data was removed, and check if rest of geotiff is still fine
    val croppedReference = imageTile.crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArrayTile()

    val croppedOutput = result.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256)
    assertArrayEquals(croppedReference.toArray(), croppedOutput.toArray())
    val result2 = GeoTiff.readMultiband(outDir.resolve("testName_2017-01-03Z.tif").toString).raster.tile
    assertArrayEquals(croppedReference.toArray(), result2.band(0).toArrayTile().crop(2 * 256, 0, layoutCols * 256, layoutRows * 256).toArray())
  }

  @Test
  def testSaveSamplesOnlyConsidersPixelsWithinGeometryHourly(): Unit = {
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
      .map(_.toString + "-testName")
      .asJava

    val outDir = Paths.get("tmp/geotiffSampleHourly/")
    new Directory(outDir.toFile).deleteRecursively()
    Files.createDirectories(outDir)

    val ret = saveSamples(tileLayerRDD, outDir.toString, tiltedRectangle, sampleNames,
      DeflateCompression(BEST_COMPRESSION))
    assertTrue(ret.get(0)._2.contains("T"))
  }

  @Test
  def testSaveSamplesOnlyConsidersPixelsWithinGeometry(): Unit = {
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage(layoutCols * 256, layoutRows * 256)
    val imageTile = ByteArrayTile(intImage, layoutCols * 256, layoutRows * 256)

    val now = ZonedDateTime.now()
    val date = ZonedDateTime.of(now.toLocalDate, LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val tileLayerRDD = TileLayerRDDBuilders
      .createSpaceTimeTileLayerRDD(Seq((imageTile, date)), TileLayout(layoutCols, layoutRows, 256, 256),
        ByteConstantNoDataCellType)(WriteRDDToGeotiffTest.sc)
      .withContext(_.mapValues(MultibandTile(_)))

    val geometriesPath = getClass.getResource("/org/openeo/geotrellis/geotiff/ll_ur_polygon.geojson").getPath

    // its extent differs substantially from its shape
    val tiltedRectangle = ProjectedPolygons.fromVectorFile(geometriesPath)

    val sampleNames = tiltedRectangle.polygons.indices
      .map(_.toString + "-testName")
      .asJava

    val outDir = Paths.get("tmp/geotiffSample/")
    new Directory(outDir.toFile).deleteRecursively()
    Files.createDirectories(outDir)

    saveSamples(tileLayerRDD, outDir.toString, tiltedRectangle, sampleNames,
      DeflateCompression(BEST_COMPRESSION))

    val Array(geoTiffPath) = Files.list(outDir).iterator().asScala.toArray // 1 date, 1 polygon
    val raster = GeoTiff.readMultiband(geoTiffPath.toString).raster.mapTile(_.band(0))

    val geometry = {
      val in = Source.fromFile(geometriesPath)
      try GeoJson.parse[GeometryCollection](in.mkString).getGeometryN(0)
      finally in.close()
    }

    // raster extent should be the same as the extent of the input geometry
    assertTrue(raster.extent.equalsExact(geometry.extent, 1.0))

    def rasterValueAt(point: Point): Int = {
      val (col, row) = raster.rasterExtent.mapToGrid(point)
      raster.tile.get(col, row)
    }

    // pixels within input geometry should carry data
    val pointWithinGeometry = geometry.getCentroid
    assertTrue(isData(rasterValueAt(pointWithinGeometry)))

    // pixels outside of geometry should not carry data
    val pointOutsideOfGeometry = {
      val point = LineString(geometry.getCentroid, geometry.extent.southEast).getCentroid
      // sanity checks
      assertTrue(geometry.extent contains point)
      assertFalse(geometry.union() contains point)
      point
    }

    assertFalse(isData(rasterValueAt(pointOutsideOfGeometry)))
  }
}
