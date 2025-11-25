package org.openeo.geotrellis.geotiff

import better.files.File
import better.files.File.apply
import cats.data.NonEmptyList
import geotrellis.layer.{CRSWorldExtent, FloatingLayoutScheme, SpaceTimeKey, SpatialKey, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiff, Tiled}
import geotrellis.raster.render.ColorMap.Options
import geotrellis.raster.render.DoubleColorMap
import geotrellis.raster.resample.Min
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ByteArrayTile, ByteConstantNoDataCellType, ByteConstantTile, CellSize, CellType, ColorMaps, GridBounds, IntArrayTile, MultibandTile, Raster, Tile, TileLayout, UByteArrayTile, isData}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.vector._
import geotrellis.vector.io.json.GeoJson
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.junit.Assert._
import org.junit.Rule
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.rules.TemporaryFolder
import org.openeo.geotrellis.LayerFixtures.loadFeaturesWithArtifactoryMock
import org.openeo.geotrellis.layers.{FileLayerProvider, SplitYearMonthDayPathDateExtractor}
import org.openeo.geotrellis.{LayerFixtures, OpenEOProcesses, ProjectedPolygons, geotiff}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path, Paths}
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util
import java.util.zip.Deflater._
import scala.annotation.meta.getter
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.reflect.io.Directory


object WriteRDDToGeotiffTest{
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[WriteRDDToGeotiffTest])

  var sc: SparkContext = _

  @BeforeAll
  def setupSpark() = {
    sc = {
      val maxFailures = 3
      val conf = new SparkConf().setMaster(f"local[8, $maxFailures]")
        .setAppName(getClass.getSimpleName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName)
        .set("spark.ui.enabled", "true")
        .set("spark.task.maxFailures", maxFailures.toString)
      SparkContext.getOrCreate(conf)
    }
    if (sc.uiWebUrl.isDefined) logger.info("Spark uiWebUrl: " + sc.uiWebUrl.get)
  }

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class WriteRDDToGeotiffTest extends RasterMatchers {

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
  def testWriteRDDConverted(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = UByteArrayTile(intImage, layoutCols * 256, layoutRows * 256)

    val targetCellType: CellType = CellType.fromName("uint16ud244")


    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile),TileLayout(layoutCols,layoutRows,256,256),LatLng)
      .convert(targetCellType)
    val filename = (tempDir / "out.tif").toString()

    saveRDD(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},1,filename,formatOptions = allOverviewOptions)

    val tiff = GeoTiff.readSingleband(filename)
    assertTrue(s"no color map in $filename", tiff.options.colorMap.isDefined)
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
  def testWriteRDD(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = UByteArrayTile(intImage, layoutCols * 256, layoutRows * 256)

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val filename = (tempDir / "out.tif").toString()

    saveRDD(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},1,filename,formatOptions = allOverviewOptions)

    val tiff = GeoTiff.readSingleband(filename)
    assertTrue(s"no color map in $filename", tiff.options.colorMap.isDefined)
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

    val m = Map(0.0 -> 100, 1.0 -> 101, 2.0 -> -10, 3.0 -> 0).mapValues(_ * 3).toMap //.map(identity)
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
  def testWriteRDD_apply_neighborhood(@TempDir outDir: Path): Unit = {
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val tileLayerRDD = LayerFixtures.buildSingleBandSpatioTemporalDataCube(util.Arrays.asList(imageTile),Seq("2017-03-01T00:00:00Z"))

    val filename = outDir + "/openEO_2017-03-01Z.tif"
    val p = new OpenEOProcesses()
    val buffered: MultibandTileLayerRDD[SpaceTimeKey] = p.remove_overlap(p.retileGeneric(tileLayerRDD,224,224,16,16),224,224,16,16)

    val cropBounds = Extent(-115, -65, 5.0, 56)
    saveRDDTemporal(buffered, outDir.toString, cropBounds = Some(cropBounds))

    val croppedRaster: Raster[MultibandTile] = tileLayerRDD.toSpatial().stitch().crop(cropBounds)
    val referenceFile = outDir + "/croppedRaster.tif"
    Files.deleteIfExists(Path.of(referenceFile))
    GeoTiff(croppedRaster,LatLng).write(referenceFile)

    val result = GeoTiff.readMultiband(filename).raster
    val reference = GeoTiff.readMultiband(referenceFile).raster

    assertArrayEquals(reference.tile.band(0).toArray(),result.tile.band(0).toArray())

  }

  @Test
  def testWriteMultibandRDD(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256)

    val secondBand = imageTile.map{x => if(x >= 5 ) 10 else 100 }
    val thirdBand = imageTile.map{x => if(x >= 5 ) 50 else 200 }

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile,secondBand,thirdBand),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val filename = (tempDir / "outRGB.tif").toString()
    saveRDD(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},3,filename)
    val result = GeoTiff.readMultiband(filename).raster.tile
    assertArrayEquals(imageTile.toArray(),result.band(0).toArray())
    assertArrayEquals(secondBand.toArray(),result.band(1).toArray())
    assertArrayEquals(thirdBand.toArray(),result.band(2).toArray())
  }


  @Test
  def testWriteCroppedRDD(@TempDir tempDir: Path): Unit ={
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
    val referenceFile = (tempDir / "croppedRaster.tif").toString()
    GeoTiff(croppedRaster,LatLng).write(referenceFile)
    val filename = (tempDir / "outRGBCropped3.tif").toString()
    saveRDD(tileLayerRDD.withContext{_.repartition(layoutCols*layoutRows)},3,filename,cropBounds = Some(cropBounds))
    val result = GeoTiff.readMultiband(filename).raster
    val reference = GeoTiff.readMultiband(referenceFile).raster

    assertEquals(result.extent,reference.extent)
    assertArrayEquals(reference.tile.band(0).toArray(),result.tile.band(0).toArray())

  }

  @Test
  def testWriteRDDGlobalLayout(@TempDir tempDir: Path): Unit ={
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
    val referenceFile = (tempDir / "croppedRasterGlobalLayout.tif").toString()
    GeoTiff(croppedRaster,LatLng).write(referenceFile)

    val filename = (tempDir / "outCropped.tif").toString()
    saveRDD(tileLayerRDD.withContext{_.repartition(tileLayerRDD.count().toInt)},3,filename,cropBounds = Some(cropBounds))
    val resultRaster = GeoTiff.readMultiband(filename).raster


    val reference = GeoTiff.readMultiband(referenceFile).raster

    assertEquals(resultRaster.extent,reference.extent)
    assertArrayEquals(reference.tile.band(0).toArray(),resultRaster.tile.band(0).toArray())
  }

  @Test
  def testWriteEmptyRdd(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4

    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = ByteArrayTile(intImage,layoutCols*256, layoutRows*256,256.toByte)

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(WriteRDDToGeotiffTest.sc,MultibandTile(imageTile),TileLayout(layoutCols,layoutRows,256,256),LatLng)
    val empty = tileLayerRDD.withContext{_.filter(_ => false)}
    val filename = (tempDir / "outEmpty.tif").toString()
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
    new Directory(outDir.toFile).deepList().foreach(_.delete())
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
    options.overviews = "ALL"
    options.resampleMethod = "min"

    val paths = saveRDD(filtered.withContext {
      _.repartition(layoutCols * layoutRows)
    }, 3, filename, formatOptions = options)
    assertEquals(3, paths.size())

    val tiff = GeoTiff.readMultiband(outDir.resolve("openEO_B01.tif").toString)
    assertEquals(1, tiff.overviews.length)
    tiff.raster.tile
    GeoTiff.readMultiband(outDir.resolve("openEO_B02.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("openEO_B03.tif").toString).raster.tile

    val firstResult = GeoTiff.readMultiband(paths.get(0))
    val result = firstResult.raster.tile
    val resultOverview = firstResult.overviews(0).raster.tile

    assertTilesEqual(result.resample(resultOverview.cols,resultOverview.rows, Min),resultOverview)

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
    options.setOverview("ALL")
    options.setTileSize(128)
    val paths = saveRDD(filtered.withContext {
      _.repartition(layoutCols * layoutRows)
    }, 3, filename, formatOptions = options)
    val expectedPaths = Set(
      outDir + "/testA/A/B02.tiff",
      outDir + "/testA/B01.tiff",
      outDir + "/testB/B03.tiff",
    )
    assertEquals(expectedPaths, paths.asScala.toSet)
    assertEquals(3, paths.size())

    for (path <- expectedPaths){
      val tile = GeoTiff.readMultiband(path)
      assertEquals(1,tile.overviews.size)
      assertEquals(Tiled(128,128),tile.overviews.head.options.storageMethod)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      assertEquals(math.ceil(colSize.toDouble/2).toInt,tile.overviews(0).tile.cols)
      assertEquals(math.ceil(rowSize.toDouble/2).toInt,tile.overviews(0).tile.rows)
    }

    GeoTiff.readMultiband(outDir.resolve("testA/B01.tiff").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("testA/A/B02.tiff").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("testB/B03.tiff").toString).raster.tile
    assert(Path.of(outDir.resolve("testA/B01.tiff").toString + GDALINFO_SUFFIX).exists)

    val result = GeoTiff.readMultiband(paths.asScala.find(_.contains("B01")).get).raster.tile

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
    new Directory(outDir.toFile).deepList().foreach(_.delete())
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
  def testSaveRDDTemporalOverviewResampleMethod(): Unit = {
    val outDir = Paths.get("tmp/testSaveRDDTemporalOverviewResampleMethod/")
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val arrayDim = 16
    val layoutCols = 1
    val layoutRows = 1

    val rangeArray = Array.range(0,layoutCols*layoutRows*arrayDim*arrayDim)
    //change med value in overview0 (0,0)
    rangeArray.update(3,18)
    rangeArray.update(34,19)
    //change average, bilinear and max value in overview0 (0,2)
    rangeArray.update(43,89)
    //change average, bilinear and min value in overview0 (1,2)
    rangeArray.update(88,42)
    // Change near value in overview0 (2,2)
    rangeArray.update(171,187)
    rangeArray.update(187,171)
    // set to noDataValue
    rangeArray.update(195,256)

    val arrayTileCount = IntArrayTile(rangeArray,layoutCols*arrayDim,layoutRows*arrayDim,noDataValue = 256)
    val layer = LayerFixtures.aSpacetimeTileLayerRddArrayTile(arrayTileCount,layoutCols,layoutRows)

    val options = new GTiffOptions()
    options.setOverview("ALL")

    def testValues(resampleMethod: String, expectedValues0:Array[Int], expectedValues1:Array[Int],expectedValue2:Int) = {
      options.setResampleMethod(resampleMethod)
      def reductionsForTest(gridBounds: GridBounds[Int], options: GTiffOptions): List[Int] = {
        List(4, 8, 16)
      }
      saveRDDTemporal(layer, outDir.toString, formatOptions = options, overviewReductions = reductionsForTest)

      val result = GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-02Z.tif").toString)
      assertEquals(3, result.overviews.size)
      val overview0 = result.overviews(0)
      for (i <- 0 until 4; j <- 0 until 4) {
        val n = 4 * i + j
        assertEquals(expectedValues0(n), overview0.tile.band(0).get(j, i))
      }
      val overview1 = result.overviews(1)
      for (i <- 0 until 2; j <- 0 until 2) {
        val n = 2 * i + j
        assertEquals(expectedValues1(n), overview1.tile.band(0).get(j, i))
      }
      val overview2 = result.overviews(2).tile.band(0).get(0, 0)
      assertEquals(expectedValue2, overview2)
    }
    testValues("near"    ,Array(51,55,59,63,115,119,123,127,179,183,171,191,243,247,251,255),Array(119,127,247,255),255)
    testValues("average" ,Array(25,29,36,37, 89, 93, 94,101,153,157,161,165,217,221,225,229),Array( 59, 67,187,195),127)
    testValues("bilinear",Array(26,30,37,38, 90, 94, 95,102,154,158,162,166,218,222,226,230),Array( 60, 68,188,196),128)
    testValues("max"     ,Array(51,55,89,63,115,119,123,127,179,183,187,191,243,247,251,255),Array(119,127,247,255),255)
    testValues("min"     ,Array( 0, 4, 8,12, 64, 68, 42, 76,128,132,136,140,192,196,200,204),Array(  0,  8,128,136),0)
    testValues("med"     ,Array(29,29,33,37, 89, 93, 97,101,153,157,161,165,217,221,225,229),Array( 59, 67,187,195),127)
  }

  @Test
  def testWriteMultibandTemporalRDDWithGapsOverviews(): Unit = {
    val layoutCols = 18
    val layoutRows = 14
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandTemporalRDDWithGapsOverview/")
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val options = new GTiffOptions()
    options.setOverview("ALL")
    saveRDDTemporal(layer, outDir.toString,formatOptions = options)
    val result = GeoTiff.readMultiband(outDir.resolve("openEO_2017-01-02Z.tif").toString)
    assertEquals(3,result.overviews.size)
    val resampled = imageTile.resample(256*layoutCols/2,256*layoutRows/2)
    val resampled0 = resampled.resample(256*layoutCols/2,256*layoutRows/2)
    val overview0 = result.overviews.head.tile.band(0)
    assertEquals((-1,0),overview0.findMinMax)
    for (
      i <- 0 until 256*layoutCols/4;
      j <- 0 until 256*layoutRows/4;
      if (overview0.get(i,j) == -1 || overview0.get(i,j) == 0)
    ) {
      assertEquals(resampled0.get(i,j), overview0.get(i,j))
    }
    val colSize = result.tile.cols
    val rowSize = result.tile.rows
    assertEquals(math.ceil(colSize.toDouble/2).toInt,result.overviews(0).tile.cols)
    assertEquals(math.ceil(rowSize.toDouble/2).toInt,result.overviews(0).tile.rows)
  }


  @Test
  def testWriteMultibandTemporalRDDWithGapsOverwrite(): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandTemporalRDDWithGapsOverwrite/")
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val filename = outDir.resolve("openEO_2017-01-02Z.tif")
    // Emulate a failing executor writing a corrupt file that will be overwritten:
    File(filename).write("This file should be overwritten!")

    saveRDDTemporal(layer, outDir.toString)
    val result = GeoTiff.readMultiband(filename.toString).raster.tile

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
    new Directory(outDir.toFile).deepList().foreach(_.delete())
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
  def testWriteMultibandTemporalRDDWithGapsSeparateAssetPerBandOverview_ALL(): Unit = {
    val layoutCols = 18
    val layoutRows = 14
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandTemporalRDDWithGapsSeparateAssetPerBandOverview/")
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val options = new GTiffOptions()
    options.separateAssetPerBand = true
    options.addBandTag(0, "DESCRIPTION", "B01")
    options.addBandTag(1, "DESCRIPTION", "B02")
    options.addBandTag(2, "DESCRIPTION", "B03")
    options.setOverview("ALL")
    val tiles = saveRDDTemporalAllowAssetPerBandInternal(layer, outDir.toString, formatOptions = options)

    val expectedPaths = List(
      outDir + "/openEO_2017-01-02Z_B01.tif",
      outDir + "/openEO_2017-01-02Z_B02.tif",
      outDir + "/openEO_2017-01-02Z_B03.tif",
      outDir + "/openEO_2017-01-03Z_B01.tif",
      outDir + "/openEO_2017-01-03Z_B02.tif",
      outDir + "/openEO_2017-01-03Z_B03.tif",
    )

    val assets = tiles.asScala.map { case item => item.assets}.toSet
    val paths = assets.foldLeft(List[String]())((temp,asset) => asset.asScala.values.toList.map(_.path)++temp) //assets.flatMap(asset => asset.)
    for (path <- paths){
      assertTrue(expectedPaths.contains(path))
    }
    assertEquals(6,paths.size)

    for (path <- expectedPaths) {
      val tile = GeoTiff.readMultiband(path)
      assertEquals(3,tile.overviews.size)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      assertEquals(math.ceil(colSize.toDouble/2).toInt,tile.overviews(0).tile.cols)
      assertEquals(math.ceil(rowSize.toDouble/2).toInt,tile.overviews(0).tile.rows)
      assertEquals(math.ceil(colSize.toDouble/4).toInt,tile.overviews(1).tile.cols)
      assertEquals(math.ceil(rowSize.toDouble/4).toInt,tile.overviews(1).tile.rows)
      assertEquals(math.ceil(colSize.toDouble/8).toInt,tile.overviews(2).tile.cols)
      assertEquals(math.ceil(rowSize.toDouble/8).toInt,tile.overviews(2).tile.rows)
    }
  }

  @Test
  def testWriteMultibandTemporalRDDWithGapsSeparateAssetPerBandOverview_AUTO(): Unit = {
    val layoutCols = 18
    val layoutRows = 14
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandTemporalRDDWithGapsSeparateAssetPerBandOverview/")
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val options = new GTiffOptions()
    options.separateAssetPerBand = true
    options.addBandTag(0, "DESCRIPTION", "B01")
    options.addBandTag(1, "DESCRIPTION", "B02")
    options.addBandTag(2, "DESCRIPTION", "B03")
    options.setOverview("AUTO")
    val tiles = saveRDDTemporalAllowAssetPerBand(layer, outDir.toString, formatOptions = options)

    val expectedPaths = List(
      outDir + "/openEO_2017-01-02Z_B01.tif",
      outDir + "/openEO_2017-01-02Z_B02.tif",
      outDir + "/openEO_2017-01-02Z_B03.tif",
      outDir + "/openEO_2017-01-03Z_B01.tif",
      outDir + "/openEO_2017-01-03Z_B02.tif",
      outDir + "/openEO_2017-01-03Z_B03.tif",
    )

    val assets = tiles.asScala.map { case item => item.assets}.toSet
    val paths = assets.foldLeft(List[String]())((temp,asset) => asset.asScala.values.toList.map(_.path)++temp) //assets.flatMap(asset => asset.)
    for (path <- paths){
      assertTrue(expectedPaths.contains(path))
    }
    assertEquals(6,paths.size)

    for (path <- expectedPaths) {
      val tile = GeoTiff.readMultiband(path)
      assertEquals(2,tile.overviews.size)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      assertEquals(math.ceil(colSize.toDouble/4).toInt,tile.overviews(0).tile.cols)
      assertEquals(math.ceil(rowSize.toDouble/4).toInt,tile.overviews(0).tile.rows)
      assertEquals(math.ceil(colSize.toDouble/8).toInt,tile.overviews(1).tile.cols)
      assertEquals(math.ceil(rowSize.toDouble/8).toInt,tile.overviews(1).tile.rows)
    }
  }



  @Test
  def testWriteMultibandTemporalRDDWithGapsFilepathPerBand(): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (layer, imageTile) = LayerFixtures.aSpacetimeTileLayerRdd(layoutCols, layoutRows)

    val outDir = Paths.get("tmp/testWriteMultibandTemporalRDDWithGapsFilepathPerBand/")
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val options = new GTiffOptions()
    val filepathPerBand: util.ArrayList[String] = new util.ArrayList[String]()
    filepathPerBand.add("testA/<date>_B01.tif")
    filepathPerBand.add("testA/A/<date>_B02.tif")
    filepathPerBand.add("testB/<date>_B03.tif")
    options.setFilepathPerBand(Some(filepathPerBand))
    options.separateAssetPerBand = true
    options.addBandTag(0, "DESCRIPTION", "B01")
    options.addBandTag(1, "DESCRIPTION", "B02")
    options.addBandTag(2, "DESCRIPTION", "B03")
    saveRDDTemporal(layer, outDir.toString, formatOptions = options)

    GeoTiff.readMultiband(outDir.resolve("testA/2017-01-02Z_B01.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("testA/A/2017-01-02Z_B02.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("testB/2017-01-02Z_B03.tif").toString).raster.tile

    GeoTiff.readMultiband(outDir.resolve("testA/2017-01-03Z_B01.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("testA/A/2017-01-03Z_B02.tif").toString).raster.tile
    GeoTiff.readMultiband(outDir.resolve("testB/2017-01-03Z_B03.tif").toString).raster.tile
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
    assertTrue(ret.get(0).datetime.contains("T"))
  }

  @Test
  def testSaveSamplesWithOptions(@TempDir outDir: Path): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (imageTile: ByteArrayTile, filtered: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(layoutCols, layoutRows)

    val date = ZonedDateTime.of(LocalDate.of(2023, 4, 5), MIDNIGHT, UTC)

    val tileLayerRDD = TileLayerRDDBuilders
      .createSpaceTimeTileLayerRDD(Seq((imageTile, date),(imageTile, date.plusDays(1)),(imageTile, date.plusDays(2))), TileLayout(layoutCols, layoutRows, 256, 256),
        ByteConstantNoDataCellType)(WriteRDDToGeotiffTest.sc)
      .withContext(_.mapValues(MultibandTile(_)))

    val geometriesPath = getClass.getResource("/org/openeo/geotrellis/geotiff/ll_ur_polygon.geojson").getPath

    // its extent differs substantially from its shape
    val tiltedRectangle = ProjectedPolygons.fromVectorFile(geometriesPath)

    val sampleNames = tiltedRectangle.polygons.indices
      .map(_.toString + "-testName")
      .asJava

    val gtiffOptions = new GTiffOptions
    gtiffOptions.setOverview("ALL")
    gtiffOptions.setTileSize(128)

    val tiles = saveSamples(tileLayerRDD, outDir + "/", tiltedRectangle, sampleNames,
      DeflateCompression(BEST_COMPRESSION),gtiffOptions)

    val expectedPaths = List(
      outDir + "/openEO_2023-04-05Z_0-testName.tif",
      outDir + "/openEO_2023-04-06Z_0-testName.tif",
      outDir + "/openEO_2023-04-07Z_0-testName.tif",
    )
    val paths = tiles.asScala.map { case item => item.assets.values().iterator().next().path }.toSet

    for (path <- paths){
      assertTrue(expectedPaths.contains(path))
    }
    assertEquals(3,paths.size)

    for (path <- expectedPaths) {
      val tile = GeoTiff.readMultiband(path)
      assertEquals(1,tile.overviews.size)
      assertEquals(Tiled(128,128),tile.overviews.head.options.storageMethod)
      val colSize = tile.tile.cols
      val rowSize = tile.tile.rows
      assertEquals(math.ceil(colSize.toDouble/2).toInt,tile.overviews(0).tile.cols)
      assertEquals(math.ceil(rowSize.toDouble/2).toInt,tile.overviews(0).tile.rows)
    }

    assertTrue(tiles.get(0).datetime.contains("T"))
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

    val paths = Files.list(outDir).iterator().asScala.toArray // 1 date, 1 polygon
    val geoTiffPath = paths.find(_.toString.endsWith(".tif")).get
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

  @Test
  def testAvoidCroppingAwayNoData(): Unit = {

    val layerProvider = FileLayerProvider(
      loadFeaturesWithArtifactoryMock("/org/openeo/geotrellis/testAvoidCroppingAwayNoData.json"),
      "GLOBAL-MOSAICS",
      openSearchLinkTitles = NonEmptyList.of("VV"),
      rootPath = "/eodata/Global-Mosaics/Sentinel-1",
      CellSize(20, 20),
      SplitYearMonthDayPathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
    )
    val bbox = ProjectedExtent(Extent(466000, 8170000, 509760, 8171000), CRS.fromEpsgCode(32643))
    val layer = layerProvider.readMultibandTileLayer(
      from = ZonedDateTime.of(LocalDate.of(2019, 12, 31), MIDNIGHT, UTC),
      to = ZonedDateTime.of(LocalDate.of(2020, 2, 1), MIDNIGHT, UTC),
      bbox,
      sc = sc,
    )

    val outDir = Paths.get("tmp/testAvoidCroppingAwayNoData/").toAbsolutePath
    new Directory(outDir.toFile).deepList().foreach(_.delete())
    Files.createDirectories(outDir)

    val options = new GTiffOptions()
    options.separateAssetPerBand = true
    options.addBandTag(0, "DESCRIPTION", "VV")
    val paths = saveRDD(layer.toSpatial(layer.keys.collect().head.time), 0, outDir.toString + "/out", formatOptions = options)
    assertEquals(1, paths.size())

    val result = GeoTiff.readMultiband(outDir.resolve("openEO_VV.tif").toString).raster.tile
    val arrayTile = result.band(0).toArrayTile()
    assertEquals(2188, arrayTile.dimensions.cols)
    assertEquals(50, arrayTile.dimensions.rows)
  }

  @Test
  def testSaveStitchWithTileGridsTemporalWithOptions(@TempDir outDir: Path): Unit = {
    val layoutCols = 8
    val layoutRows = 4
    val (_, filtered: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(layoutCols, layoutRows)

    val extent = filtered.metadata.extent
    val cropBounds = Map("xmin" -> extent.xmin, "xmax" -> extent.xmax, "ymin" -> extent.ymin, "ymax" -> extent.ymax).asJava

    val filename = outDir + "/out"
    val options = new GTiffOptions()
    options.separateAssetPerBand = true

    options.setOverview("ALL")
    options.setTileSize(128)
    saveStitched(filtered.withContext {
      _.repartition(layoutCols * layoutRows)
    }, filename, cropBounds, DeflateCompression(6), formatOptions = options)

    val tile = GeoTiff.readMultiband(filename)
    assertEquals(4,tile.overviews.size)
    assertEquals(Tiled(128,128),tile.overviews.head.options.storageMethod)
    assertEquals(896,tile.overviews(0).tile.cols)
    assertEquals(512,tile.overviews(0).tile.rows)
    assertEquals(448,tile.overviews(1).tile.cols)
    assertEquals(256,tile.overviews(1).tile.rows)
    assertEquals(224,tile.overviews(2).tile.cols)
    assertEquals(128,tile.overviews(2).tile.rows)
    assertEquals(112,tile.overviews(3).tile.cols)
    assertEquals(64,tile.overviews(3).tile.rows)
  }

}
