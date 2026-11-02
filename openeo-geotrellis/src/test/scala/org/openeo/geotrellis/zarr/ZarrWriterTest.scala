package org.openeo.geotrellis.zarr
import better.files.File.apply
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.luben.zstd.Zstd
import dev.zarr.zarrjava.v3.{Array => ZArray}
import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{BitArrayTile, ByteArrayTile, DoubleArrayTile, FloatArrayTile, IntArrayTile, MultibandTile, ShortArrayTile, Tile, TileLayout, UByteArrayTile, UShortArrayTile}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.vector.Extent
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.{LayerFixtures, PixelType, zarr}
import org.slf4j.{Logger, LoggerFactory}
import java.nio.file.{Files, Path, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import java.time.ZonedDateTime
import java.util
import scala.jdk.CollectionConverters._
object ZarrWriterTest{
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[ZarrWriterTest])

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
  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class ZarrWriterTest {
  private val mapper = new ObjectMapper()
  private type JMap = util.Map[String, Object]
  private def readJson(path: Path): JMap = {
    assertTrue(Files.exists(path), s"$path missing")
    mapper.readValue(path.toFile, classOf[util.Map[String, Object]])
  }
  private def readNode(root: String, relPath: String): JMap =
    readJson(Paths.get(root, relPath, "zarr.json"))
  private def map(m: JMap, key: String): JMap = m.get(key).asInstanceOf[JMap]
  private def readChunk(root: String, relPath: String, index: Int*): Array[Byte] = {
    val chunkPath = index.foldLeft(Paths.get(root, relPath, "c"))((p, i) => p.resolve(i.toString))
    assertTrue(Files.exists(chunkPath), s"$chunkPath missing")
    val compressed = Files.readAllBytes(chunkPath)
    Zstd.decompress(compressed, Zstd.getFrameContentSize(compressed).toInt)
  }
  /** Chunks that only contain the fill value are not written, so a chunk may legitimately be absent. */
  private def readChunkIfPresent(root: String, relPath: String, index: Int*): Option[Array[Byte]] = {
    val chunkPath = index.foldLeft(Paths.get(root, relPath, "c"))((p, i) => p.resolve(i.toString))
    if (Files.exists(chunkPath)) Some(readChunk(root, relPath, index: _*)) else None
  }
  /** Reads the full array back with zarr-java and compares it cell by cell with the expected raster. */
  private def assertRoundTrip(root: String, relPath: String, expected: Tile): Unit = {
    val array = ZArray.open(Paths.get(root, relPath))
    val data = array.read()
    // element-wise getDouble respects unsigned types (get1DJavaArray(DOUBLE) does not)
    val actual = Array.tabulate(data.getSize.toInt)(i => data.getDouble(i))
    assertArrayEquals(expected.withNoData(None).toArrayDouble(), actual, 0.0, s"$relPath content differs")
  }
  private def assertGroup(node: JMap): Unit = {
    assertEquals(3, node.get("zarr_format"))
    assertEquals("group", node.get("node_type"))
  }
  /** Checks the generic Zarr v3 array metadata and returns it. */
  private def assertArray(node: JMap, shape: util.List[Integer], chunks: util.List[Integer], dims: util.List[String]): JMap = {
    assertEquals(3, node.get("zarr_format"))
    assertEquals("array", node.get("node_type"))
    assertEquals(shape, node.get("shape"))
    assertEquals("regular", map(node, "chunk_grid").get("name"))
    assertEquals(chunks, map(map(node, "chunk_grid"), "configuration").get("chunk_shape"))
    assertEquals("default", map(node, "chunk_key_encoding").get("name"))
    assertEquals("/", map(map(node, "chunk_key_encoding"), "configuration").get("separator"))
    assertTrue(node.containsKey("fill_value"))
    assertEquals(dims, node.get("dimension_names"))
    val codecs = node.get("codecs").asInstanceOf[util.List[JMap]]
    assertEquals(2, codecs.size())
    assertEquals("bytes", codecs.get(0).get("name"))
    val bytesConfig = map(codecs.get(0), "configuration")
    if (bytesConfig != null) assertEquals("little", bytesConfig.get("endian"))
    assertEquals("zstd", codecs.get(1).get("name"))
    assertEquals(3, map(codecs.get(1), "configuration").get("level"))
    // No V2 leftovers
    assertFalse(map(node, "attributes").containsKey("_ARRAY_DIMENSIONS"))
    node
  }
  private def assertDataAttributes(node: JMap, crsCode: String, expectedShape: util.List[Integer], expectedBbox: util.List[_], temporal: Boolean): Unit = {
    val attr = map(node, "attributes")
    val crs = map(attr, "_CRS")
    assertTrue(crs.containsKey("wkt"))
    assertEquals(crsCode, crs.get("code"))
    assertEquals(expectedShape, crs.get("proj:shape"))
    assertEquals(expectedBbox, crs.get("proj:bbox"))
    val extent = map(attr, "extent")
    assertTrue(extent.containsKey("spatial"))
    assertEquals(temporal, extent.containsKey("temporal"))
  }
  private def assertNoV2Files(root: String): Unit = {
    val stream = Files.walk(Paths.get(root))
    try {
      stream.iterator().asScala.foreach { p =>
        val name = p.getFileName.toString
        assertFalse(Set(".zarray", ".zattrs", ".zgroup", ".zmetadata").contains(name), s"Unexpected V2 file $p")
      }
    } finally stream.close()
  }
  private def assertConsolidated(root: String, expectedKeys: Set[String]): Unit = {
    val rootNode = readNode(root, "")
    assertGroup(rootNode)
    val consolidated = map(rootNode, "consolidated_metadata")
    assertEquals("inline", consolidated.get("kind"))
    assertEquals(false, consolidated.get("must_understand"))
    val metadata = map(consolidated, "metadata")
    assertEquals(expectedKeys, metadata.keySet().asScala.toSet)
    expectedKeys.foreach { k => assertEquals(readNode(root, k), metadata.get(k)) }
  }
  private def assertXY(root: String, rows: Int, cols: Int): Unit = {
    val x = assertArray(readNode(root, "x"), util.Arrays.asList(cols), util.Arrays.asList(cols), util.Arrays.asList("x"))
    assertEquals("float64", x.get("data_type"))
    assertEquals(cols * 8, readChunk(root, "x", 0).length)
    val y = assertArray(readNode(root, "y"), util.Arrays.asList(rows), util.Arrays.asList(rows), util.Arrays.asList("y"))
    assertEquals("float64", y.get("data_type"))
    assertEquals(rows * 8, readChunk(root, "y", 0).length)
  }
  private def bytesPerValue(dataType: String): Int = dataType match {
    case "int8" | "uint8" => 1
    case "int16" | "uint16" => 2
    case "int32" | "float32" => 4
    case "int64" | "float64" => 8
  }

  @Test
  def testWriteSingleBandRDD(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4
    def testInput(multibandTile: MultibandTile, filePrefix:String, expectedType: String): Unit = {
      val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(ZarrWriterTest.sc, multibandTile, TileLayout(layoutCols, layoutRows, 256, 256), LatLng)
      val filename = (tempDir / (filePrefix+".zarr")).toString()
      zarr.ZarrWriter.saveZarrGeneric(tileLayerRDD, filename, new ZarrOptions)
      val node = assertArray(readNode(filename, filePrefix), util.Arrays.asList(1024, 2048), util.Arrays.asList(256, 256), util.Arrays.asList("y", "x"))
      assertEquals(expectedType, node.get("data_type"))
      // _FillValue is only written when the cell type defines no-data, and then equals fill_value
      if (multibandTile.cellType.isInstanceOf[geotrellis.raster.NoNoData])
        assertFalse(map(node, "attributes").containsKey("_FillValue"))
      else
        assertEquals(node.get("fill_value"), map(node, "attributes").get("_FillValue"))
      val m = tileLayerRDD.metadata
      assertDataAttributes(node, "EPSG:4326", util.Arrays.asList(1024, 2048),
        util.Arrays.asList(m.extent.xmin, m.extent.ymin, m.extent.xmax, m.extent.ymax), temporal = false)
      assertXY(filename, 1024, 2048)
      for (row <- 0 until layoutRows; col <- 0 until layoutCols) {
        readChunkIfPresent(filename, filePrefix, row, col).foreach(c => assertEquals(256 * 256 * bytesPerValue(expectedType), c.length))
      }
      assertRoundTrip(filename, filePrefix, multibandTile.band(0))
      assertConsolidated(filename, Set(filePrefix, "x", "y"))
      assertNoV2Files(filename)
    }
    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = BitArrayTile(Array.concat(Array.fill[Byte](layoutCols*layoutRows*4096){0},Array.fill[Byte](layoutCols*layoutRows*4096){1}),layoutCols*256, layoutRows*256)
    testInput(MultibandTile(imageTile),"bitOut", "uint8")
    testInput(MultibandTile(ByteArrayTile(intImage,layoutCols*256, layoutRows*256)),"byteOut", "int8")
    testInput(MultibandTile(UByteArrayTile(intImage,layoutCols*256, layoutRows*256)),"uByteOut", "uint8")
    testInput(MultibandTile(FloatArrayTile(intImage.map(_.toFloat),layoutCols*256, layoutRows*256)),"floutOut", "float32")
    testInput(MultibandTile(ShortArrayTile(intImage.map(_.toShort),layoutCols*256, layoutRows*256)),"shortOut", "int16")
    testInput(MultibandTile(UShortArrayTile(intImage.map(_.toShort),layoutCols*256, layoutRows*256)),"uShortOut", "uint16")
    testInput(MultibandTile(DoubleArrayTile(intImage.map(_.toDouble),layoutCols*256, layoutRows*256)),"doubleOut", "float64")
    testInput(MultibandTile(IntArrayTile(intImage.map(_.toInt),layoutCols*256, layoutRows*256)),"intOut", "int32")
  }

  @Test
  def testWriteMultiBandRDD(@TempDir tempDir: Path): Unit ={
    val layoutCols = 8
    val layoutRows = 4
    def testInput(tile: Tile, filePrefix:String): Unit = {
      val secondBand = tile.map { x => if (x >= 5) 10 else 100 }
      val thirdBand = tile.map { x => if (x >= 5) 50 else 200 }
      val zarrOptions = new ZarrOptions
      zarrOptions.setBands(3, new java.util.ArrayList(java.util.Arrays.asList("B01", "B02", "B04")))
      val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(ZarrWriterTest.sc, MultibandTile(tile, secondBand, thirdBand), TileLayout(layoutCols, layoutRows, 256, 256), LatLng)
      val filename = (tempDir / (filePrefix+".zarr")).toString()
      zarr.ZarrWriter.saveZarrGeneric(tileLayerRDD.withContext {
        _.repartition(layoutCols * layoutRows)
      }, filename, zarrOptions)
      val m = tileLayerRDD.metadata
      val expectedBbox = util.Arrays.asList(m.extent.xmin, m.extent.ymin, m.extent.xmax, m.extent.ymax)
      for (band <- Seq("B01", "B02", "B04")) {
        val node = assertArray(readNode(filename, band), util.Arrays.asList(1024, 2048), util.Arrays.asList(256, 256), util.Arrays.asList("y", "x"))
        assertDataAttributes(node, "EPSG:4326", util.Arrays.asList(1024, 2048), expectedBbox, temporal = false)
        val size = bytesPerValue(node.get("data_type").toString)
        for (row <- 0 until layoutRows; col <- 0 until layoutCols) {
          readChunkIfPresent(filename, band, row, col).foreach(c => assertEquals(256 * 256 * size, c.length))
        }
      }
      Seq("B01" -> tile, "B02" -> secondBand, "B04" -> thirdBand).foreach { case (band, expected) =>
        assertRoundTrip(filename, band, expected)
      }
      assertXY(filename, 1024, 2048)
      assertConsolidated(filename, Set("B01", "B02", "B04", "x", "y"))
      assertNoV2Files(filename)
    }
    val intImage = LayerFixtures.createTextImage( layoutCols*256, layoutRows*256)
    val imageTile = BitArrayTile(Array.concat(Array.fill[Byte](layoutCols*layoutRows*4096){0},Array.fill[Byte](layoutCols*layoutRows*4096){1}),layoutCols*256, layoutRows*256)
    testInput(imageTile,"bitOut")
    testInput(ByteArrayTile(intImage,layoutCols*256, layoutRows*256),"byteOut")
    testInput(UByteArrayTile(intImage,layoutCols*256, layoutRows*256),"uByteOut")
    testInput(FloatArrayTile(intImage.map(_.toFloat),layoutCols*256, layoutRows*256),"floutOut")
    testInput(ShortArrayTile(intImage.map(_.toShort),layoutCols*256, layoutRows*256),"shortOut")
    testInput(UShortArrayTile(intImage.map(_.toShort),layoutCols*256, layoutRows*256),"uShortOut")
    testInput(DoubleArrayTile(intImage.map(_.toDouble),layoutCols*256, layoutRows*256),"doubleOut")
    testInput(IntArrayTile(intImage.map(_.toInt),layoutCols*256, layoutRows*256),"intOut")
  }

  @Test
  def testFillValue(@TempDir tempDir: Path): Unit = {
    val tile = IntArrayTile.fill(1, 512, 512).convert(geotrellis.raster.IntUserDefinedNoDataCellType(-1))
    val rdd = TileLayerRDDBuilders.createMultibandTileLayerRDD(ZarrWriterTest.sc, MultibandTile(tile), TileLayout(2, 2, 256, 256), LatLng)
    val filename = (tempDir / "nodata.zarr").toString()
    zarr.ZarrWriter.saveZarrGeneric(rdd, filename, new ZarrOptions)
    val node = readNode(filename, "nodata")
    assertEquals(-1, node.get("fill_value"))
    assertEquals(-1, map(node, "attributes").get("_FillValue"))
  }

  @Test
  def testWriteSpaceTime(@TempDir tempDir:Path):Unit ={
    val date1 = ZonedDateTime.parse("1990-01-02T00:00:00Z")
    val extentTAP3857 = Extent(564389 - 10, 6659413 - 10, 565503 + 10, 6660301 + 10)
    for{pixeltype <- List(PixelType.Bit,PixelType.Byte,PixelType.Int,PixelType.Float,PixelType.Short,PixelType.Double)} {
      for {
        date2 <- List(date1.plusDays(2), date1.plusHours(2))
      } {
        val dates = List(date1, date2)
        val dataCubeContextRDD: MultibandTileLayerRDD[SpaceTimeKey] = LayerFixtures.randomNoiseLayer(
          pixelType = pixeltype,
          extent = extentTAP3857,
          crs = CRS.fromName("EPSG:3857"),
          dates = Some(dates)
        )
        val prefix = pixeltype.toString + "out"
        val filename = (tempDir / (prefix + ".zarr")).toString()
        zarr.ZarrWriter.saveZarr(dataCubeContextRDD, filename, new ZarrOptions)
        val tileRows = dataCubeContextRDD.metadata.tileRows
        val tileCols = dataCubeContextRDD.metadata.tileCols
        val node = assertArray(readNode(filename, prefix), util.Arrays.asList(2, 256, 256), util.Arrays.asList(1, tileRows, tileCols),
          util.Arrays.asList("time", "y", "x"))
        assertDataAttributes(node, "EPSG:3857", util.Arrays.asList(256, 256),
          util.Arrays.asList(extentTAP3857.xmin, extentTAP3857.ymin, extentTAP3857.xmax, extentTAP3857.ymax), temporal = true)
        val time = assertArray(readNode(filename, "time"), util.Arrays.asList(2), util.Arrays.asList(2), util.Arrays.asList("time"))
        assertEquals("int64", time.get("data_type"))
        assertTrue(map(time, "attributes").containsKey("units"))
        assertEquals("milliseconds since 1970-01-01T00:00:00Z", map(time, "attributes").get("units"))
        // _FillValue is only written when the cell type defines no-data
        if (map(node, "attributes").containsKey("_FillValue")) {
          assertEquals(node.get("fill_value"), map(node, "attributes").get("_FillValue"))
        }
        val timeBuf = ByteBuffer.wrap(readChunk(filename, "time", 0)).order(ByteOrder.LITTLE_ENDIAN)
        assertEquals(date1.toInstant.toEpochMilli, timeBuf.getLong)
        assertEquals(date2.toInstant.toEpochMilli, timeBuf.getLong)
        val size = bytesPerValue(node.get("data_type").toString)
        val timeIndex = Map(date1.toInstant.toEpochMilli -> 0, date2.toInstant.toEpochMilli -> 1)
        for (key <- dataCubeContextRDD.keys.collect()) {
          readChunkIfPresent(filename, prefix, timeIndex(key.instant), key.row, key.col)
            .foreach(chunk => assertEquals(tileRows * tileCols * size, chunk.length))
        }
        assertXY(filename, 256, 256)
        assertConsolidated(filename, Set(prefix, "x", "y", "time"))
        assertNoV2Files(filename)
      }
    }
  }
}
