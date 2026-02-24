package org.openeo.geotrellis.netcdf

import com.azavea.gdal.GDALWarp
import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ArrayTile, CellType, DoubleUserDefinedNoDataCellType, FloatConstantNoDataCellType, IntArrayTile, IntUserDefinedNoDataCellType, MultibandTile, Raster, RasterExtent, Tile, TileLayout, UByteUserDefinedNoDataCellType, UShortCellType, UShortUserDefinedNoDataCellType, isData}
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.vector.io.json.GeoJson
import geotrellis.vector.{ProjectedExtent, _}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}
import org.openeo.geotrellis.stac.Item
import org.openeo.geotrellis.{LayerFixtures, ProjectedPolygons, TemporalResolution}
import org.openeo.geotrelliscommon.{ByKeyPartitioner, DataCubeParameters, SparseSpaceTimePartitioner}
import org.slf4j.LoggerFactory
import ucar.nc2.dataset.NetcdfDataset

import java.nio.file.Path
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util
import scala.io.Source
import scala.jdk.CollectionConverters._


object NetCDFRDDWriterTest {
  private var sc: SparkContext = _

  private val logger = LoggerFactory.getLogger(NetCDFRDDWriterTest.getClass)

  @BeforeAll
  def setupSpark(): Unit = {
    // originally geotrellis.spark.util.SparkUtils.createLocalSparkContext
    val conf = SparkUtils.createSparkConf
      .setMaster("local[1]")
      .setAppName(NetCDFRDDWriterTest.getClass.getName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .set("spark.kryo.registrationRequired", "true") // this requires e.g. RasterSource to be registered too
      .set("spark.kryo.registrator", Seq(
        classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName,
        classOf[org.openeo.geotrellis.png.KryoRegistrator].getName) mkString ","
      )

    sc = SparkContext.getOrCreate(conf)
  }

  @AfterAll
  def tearDown(): Unit = try {
    GDALWarp.deinit()
  } catch {
    // triggers error when running locally
    case e: Throwable => logger.error("Ignoring deinit error: " + e.toString)
  }
}

@EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasGdalInstalled")
class NetCDFRDDWriterTest extends RasterMatchers {

  import org.openeo.geotrellis.netcdf.NetCDFRDDWriterTest._

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testWriteSamples(@TempDir temporaryFolder: Path): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val utm31 = CRS.fromEpsgCode(32631)
    val geometriesPath = getClass.getResource("/org/openeo/geotrellis/minimallyOverlappingGeometryCollection.json").getPath
    val polygons = ProjectedPolygons.fromVectorFile(geometriesPath)

    val extent = polygons.polygons.seq.extent
    val bbox = ProjectedExtent(ProjectedExtent(extent, LatLng).reproject(utm31), utm31)
    val polygonsUTM31 = ProjectedPolygons.reproject(polygons, 32631)


    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"
    dcParams.tileSize = 64

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from = date, to = date.plusDays(20), bbox, polygonsUTM31.polygons, utm31, 14, sc = sc, Some(dcParams))
    val partitioner = layer.partitioner.get
    assert(partitioner.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
    val index: PartitionerIndex[SpaceTimeKey] = partitioner.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index
    assert(index.isInstanceOf[SparseSpaceTimePartitioner])
    assert(layer.metadata.tileCols == 64)

    val sampleNames = polygons.polygons.indices.map(_.toString)
    val sampleNameList = new util.ArrayList[String]()
    sampleNames.foreach(sampleNameList.add)

    val targetDir = temporaryFolder.toString

    val sampleFilenames: util.List[String] = assetFileNames(NetCDFRDDWriter.saveSamples(layer, targetDir, polygonsUTM31,
      sampleNameList, new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M",
        "SCENECLASSIFICATION_20M")), Some("prefixTest")))

    val expectedPaths = util.Arrays.asList(s"$targetDir/prefixTest_0.nc", s"$targetDir/prefixTest_1.nc")

    assertEquals(expectedPaths, sampleFilenames)

    // note: tests first geometry only
    val bandName = "TOC-B04_10M"
    val rasterSource = GDALRasterSource(s"""NETCDF:"${expectedPaths.get(0)}":$bandName""")
    val Some(multiBandRaster) = rasterSource.read()
    val raster = multiBandRaster.mapTile(_.band(0)) // first timestamp

    val geometry = {
      val in = Source.fromFile(geometriesPath)
      try GeoJson.parse[GeometryCollection](in.mkString).getGeometryN(0)
      finally in.close()
    }

    // TODO: raster extent should be the same as the extent of the input geometries

    def rasterValueAt(point: Point): Int = {
      val reprojectedPoint = point.reproject(LatLng, rasterSource.crs)
      val (col, row) = raster.rasterExtent.mapToGrid(reprojectedPoint)
      raster.tile.get(col, row)
    }

    // pixels within input geometries should carry data
    val pointWithinGeometry = geometry.getCentroid
    assertTrue(isData(rasterValueAt(pointWithinGeometry)))

    val pointOutsideOfGeometry = {
      val point = Point(3.251151, 50.977251)
      // sanity checks
      assertTrue(geometry.extent contains point)
      assertFalse(geometry.union() contains point)
      point
    }

    assertFalse(isData(rasterValueAt(pointOutsideOfGeometry)))
  }

  @Test
  def testWriteSamplesItems(): Unit = {
    val polygon0 = MultiPolygon(
      Polygon(
        (-180.0, -90.0),
        (-180.0, 90.0),
        (180.0, 90.0),
        (180.0, -90.0),
        (-180.0, -90.0),
      ),
    )

    def testStatistics(arrayTile: ArrayTile, expectedStatistics: util.HashMap[String, Any] = null, polygon: Geometry = polygon0, expectedShape: Array[Int] = Array(512, 512), addStatistics: Boolean = true): Unit = {
      val layer = LayerFixtures.aSpacetimeTileLayerRddArrayTile(arrayTile, 1, 1, nbDates = 5)
      val polygons = ProjectedPolygons(polygon, CRS.fromEpsgCode(4326))
      val sampleNames = polygons.polygons.indices.map(_.toString)

      val samples = NetCDFRDDWriter.saveSamples(
        layer,
        "/tmp",
        polygons = polygons,
        sampleNames = new util.ArrayList(sampleNames.asJava),
        bandNames = new util.ArrayList(util.Arrays.asList("B04", "B03", "B02")),
        dimensionNames = null,
        attributes = null,
        bandsMetadata = null,
        addBandsStatistics = addStatistics,
        filenamePrefix = Some("prefixTest"),
      )

      assertEquals(1, samples.size())
      val sample = samples.get(0)
      val assets = sample.assets
      assertEquals(1, assets.size())
      val metadata = assets.get("openEO").metadata
      assertEquals(LatLng.epsgCode.get, metadata.get("proj:epsg"))
      assertArrayEquals(expectedShape, metadata.get("proj:shape").asInstanceOf[Array[Int]])
      val bbox = polygon.extent match {
        case Extent(-18.0, 30.0, 18.0, 60) => Array(-18.281254492187486, 29.8828091796875, 18.28124449218752, 60.1171808203125)
        case extent => Array(extent.xmin, extent.ymin, extent.xmax, extent.ymax)
      }
      assertArrayEquals(bbox, metadata.get("proj:bbox").asInstanceOf[Array[Double]], 0.01)
      val bands = metadata.get("bands").asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
      assertEquals(3, bands.size())
      bands.forEach(band => {
        assertTrue(band.containsKey("name"))
        assertEquals(addStatistics, band.containsKey("statistics"))
        val statistics = band.getOrDefault("statistics", null).asInstanceOf[util.HashMap[String, Number]]
        assertEquals(expectedStatistics, statistics)
      })
    }

    val polygon1 = MultiPolygon(
      Polygon(
        (-18.0, 30.0),
        (-18.0, 60.0),
        (18.0, 60.0),
        (18.0, 30.0),
        (-18.0, 30.0),
      ),
    )
    val arrayDim = 512
    val arrayTile0 = IntArrayTile(Array.fill(arrayDim * arrayDim / 4)(0) ++ Array.fill(arrayDim * arrayDim / 2)(30) ++ Array.fill(arrayDim * arrayDim / 4)(256), arrayDim, arrayDim, noDataValue = 256)
    testStatistics(arrayTile = arrayTile0, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 75, "minimum", 0.0, "maximum", 30.0, "mean", 20.0))) // , "stddev", 14.142135623730951
    val arrayTile1 = IntArrayTile(Array.fill(arrayDim * arrayDim)(256), arrayDim, arrayDim, noDataValue = 256)
    val imageTile1 = arrayTile1.convert(DoubleUserDefinedNoDataCellType(256)).mutable
    testStatistics(arrayTile = imageTile1, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 0.0)))
    val arrayTile2 = IntArrayTile(Array.fill(arrayDim * arrayDim / 2)(256) ++ Array.fill(arrayDim * arrayDim / 8)(30) ++ Array.fill(arrayDim * arrayDim / 8)(10) ++ Array.fill(arrayDim * arrayDim / 4)(256), arrayDim, arrayDim, noDataValue = 256)
    testStatistics(arrayTile = arrayTile2, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 25, "minimum", 10.0, "maximum", 30.0, "mean", 20.0))) // , "stddev", 10
    testStatistics(arrayTile = arrayTile0, addStatistics = false)
    testStatistics(arrayTile = arrayTile0, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 100, "minimum", 0.0, "maximum", 30.0, "mean", 15.0)), polygon = polygon1, expectedShape = Array(86, 52)) // , "stddev", 15.0
    testStatistics(arrayTile = arrayTile2, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 0.0)), polygon = polygon1, expectedShape = Array(86, 52))
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testWriteSamplesWithGlobalBoundsBuffer(@TempDir temporaryFolder: Path): Unit = {
    val utm30 = CRS.fromEpsgCode(32630)

    // Use recent year, as the tested Sentinel 2 collection ony keeps track of 2 years.
    val year = LocalDate.now().getYear - 1
    val startDate = ZonedDateTime.of(LocalDate.of(year, 7, 1), MIDNIGHT, UTC)
    val endDate = ZonedDateTime.of(LocalDate.of(year, 7, 15), MIDNIGHT, UTC)

    val polygon1 = new Extent(-0.6, 60.0, -0.597, 60.003).toPolygon()
    val polygon2 = new Extent(-0.6, 61.0, -0.597, 61.003).toPolygon()
    val polygon3 = new Extent(-0.6, 62.0, -0.597, 62.003).toPolygon()

    val polygon1_nativecrs = polygon1.reproject(CRS.fromEpsgCode(4326), utm30)
    val polygon2_nativecrs = polygon2.reproject(CRS.fromEpsgCode(4326), utm30)
    val polygon3_nativecrs = polygon3.reproject(CRS.fromEpsgCode(4326), utm30)
    val polySeq = List(MultiPolygon(polygon1_nativecrs), MultiPolygon(polygon2_nativecrs), MultiPolygon(polygon3_nativecrs)).toArray
    val polygons = ProjectedPolygons(polySeq, CRS.fromEpsgCode(32630))

    val extent = polygons.polygons.seq.extent
    val bbox = ProjectedExtent(extent, utm30)

    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"
    dcParams.tileSize = 256
    dcParams.setPartitionerIndexReduction(8)
    dcParams.setPartitionerTemporalResolution("ByDay")
    dcParams.setGlobalExtent(-0.6, 60.0, -0.597, 62.003, "EPSG:4326")
    val zoom = 0
    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from = startDate, to = endDate, bbox, polygons.polygons, utm30, zoom, sc = sc, Some(dcParams))

    val sampleNames = polygons.polygons.indices.map(_.toString)
    val sampleNameList = new util.ArrayList[String]()
    sampleNames.foreach(sampleNameList.add)
    val bandNames = new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"))

    val targetDir = temporaryFolder.toString

    val sampleFilenames: util.List[String] = assetFileNames(NetCDFRDDWriter.saveSamples(
      layer, targetDir, polygons, sampleNameList, bandNames
    ))

    val raster1: Raster[MultibandTile] = GDALRasterSource(s"""NETCDF:${sampleFilenames.get(0)}:TOC-B04_10M""").read().get
    val raster2: Raster[MultibandTile] = GDALRasterSource(s"""NETCDF:${sampleFilenames.get(1)}:TOC-B04_10M""").read().get

    // Compare raster extents.
    //assert(raster1.extent.width == 2560.0)
    //assert(raster1.extent.height == 2 * 2560.0)
    //assert(raster2.extent.width == 2560.0)
    //assert(raster2.extent.height == 2560.0)
    val bands = raster1.tile.bands.filter(!_.isNoDataTile)
    assert(bands.size >= 3) // There should be at least 3 dates

    for (band <- bands) {
      // Ensure there is data within the polygon on this observation.
      assert(band.mask(raster1.extent, polygon1_nativecrs).toArray().exists(p => p != -2147483648))
    }
  }

  @Test
  def testKeyPartitioner(): Unit = {
    val splits = (0 to 30).map(_.toString).toArray
    val p = new ByKeyPartitioner(splits)
    assertEquals(0, p.getPartition("0"))
    assertEquals(1, p.getPartition("1"))
    assertEquals(2, p.getPartition("2"))
    assertEquals(3, p.getPartition("3"))
    assertEquals(4, p.getPartition("4"))
    assertEquals(20, p.getPartition("20"))
    assertEquals(30, p.getPartition("30"))
  }

  @Test
  def testWriteSamplesSpatial(): Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val polygons = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/minimallyOverlappingGeometryCollection.json").getPath)

    val extent = polygons.polygons.seq.extent
    val bbox = ProjectedExtent(ProjectedExtent(extent, LatLng).reproject(utm31), utm31)
    val polygonsUTM31 = ProjectedPolygons.reproject(polygons, 32631)


    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val (_, layer: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(20, 10)

    val localLayer = ContextRDD(layer, layer.metadata.copy(extent = bbox.extent, crs = bbox.crs, layout = layer.metadata.layout.copy(extent = bbox.extent)))

    val sampleNames = polygons.polygons.indices.map(_.toString)
    val sampleNameList = new util.ArrayList[String]()
    sampleNames.foreach(sampleNameList.add)

    val samples = NetCDFRDDWriter.saveSamplesSpatial(
        localLayer,
        "/tmp",
        polygonsUTM31,
        sampleNameList,
        new util.ArrayList(util.Arrays.asList("B04", "B03", "B02")),
        null,
        null,
        null,
        Some("prefixTest"),
      ).stream()
      .flatMap { item =>
        item.assets.values().stream().map[(String, Extent)] { asset =>
          (asset.path, item.bbox)
        }
      }
      .collect(util.stream.Collectors.toSet())

    val expectedSamples = Set(
      ("/tmp/prefixTest_0.nc", polygonsUTM31.polygons(0).extent),
      ("/tmp/prefixTest_1.nc", polygonsUTM31.polygons(1).extent),
    )

    assertEquals(expectedSamples.asJava, samples)
  }

  @Test
  def testWriteSamplesSpatialItems(): Unit = {
    val polygon0 = MultiPolygon(
      Polygon(
        (-180.0, -90.0),
        (-180.0, 90.0),
        (180.0, 90.0),
        (180.0, -90.0),
        (-180.0, -90.0),
      ),
    )

    def testStatistics(imageTile: Tile, expectedStatistics: util.HashMap[String, Any] = null, polygon: Geometry = polygon0, expectedShape: Array[Int] = Array(512, 512), addStatistics: Boolean = true): Unit = {
      val polygons = ProjectedPolygons(polygon, CRS.fromEpsgCode(4326))
      val sampleNames = polygons.polygons.indices.map(_.toString)
      val layer = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, MultibandTile(imageTile, imageTile, imageTile), TileLayout(imageTile.cols / 256, imageTile.rows / 256, 256, 256), LatLng)

      val samples = NetCDFRDDWriter.saveSamplesSpatial(
        layer,
        "/tmp",
        polygons = polygons,
        sampleNames = new util.ArrayList(sampleNames.asJava),
        bandNames = new util.ArrayList(util.Arrays.asList("B04", "B03", "B02")),
        dimensionNames = null,
        attributes = null,
        bandsMetadata = null,
        addBandsStatistics = addStatistics,
        filenamePrefix = Some("prefixTest"),
      )

      assertEquals(1, samples.size())
      val sample = samples.get(0)
      val assets = sample.assets
      assertEquals(1, assets.size())
      val metadata = assets.get("openEO").metadata
      assertEquals(LatLng.epsgCode.get, metadata.get("proj:epsg"))
      assertArrayEquals(expectedShape, metadata.get("proj:shape").asInstanceOf[Array[Int]])
      val bbox = polygon.extent match {
        case Extent(-18.0, 30.0, 18.0, 60) => Array(-18.281254492187486, 29.8828091796875, 18.28124449218752, 60.1171808203125)
        case extent => Array(extent.xmin, extent.ymin, extent.xmax, extent.ymax)
      }
      assertArrayEquals(bbox, metadata.get("proj:bbox").asInstanceOf[Array[Double]], 0.01)
      assertTrue(metadata.containsKey("bands"))
      assertTrue(metadata.get("bands").isInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]])
      val bands = metadata.get("bands").asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
      assertEquals(3, bands.size())
      bands.forEach(band => {
        assertTrue(band.containsKey("name"))
        assertEquals(addStatistics, band.containsKey("statistics"))
        val statistics = band.getOrDefault("statistics", null).asInstanceOf[util.HashMap[String, Number]]
        assertEquals(expectedStatistics, statistics)
      })
    }

    val polygon1 = MultiPolygon(
      Polygon(
        (-18.0, 30.0),
        (-18.0, 60.0),
        (18.0, 60.0),
        (18.0, 30.0),
        (-18.0, 30.0),
      ),
    )
    val arrayDim = 512
    val arrayTile0 = IntArrayTile(Array.fill(arrayDim * arrayDim / 4)(0) ++ Array.fill(arrayDim * arrayDim / 2)(30) ++ Array.fill(arrayDim * arrayDim / 4)(256), arrayDim, arrayDim)
    val imageTile0 = arrayTile0.convert(UShortUserDefinedNoDataCellType(256)).mutable
    testStatistics(imageTile = imageTile0, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 75, "minimum", 0.0, "maximum", 30.0, "mean", 20.0))) // , "stddev", 14.142135623730951
    val arrayTile1 = IntArrayTile(Array.fill(arrayDim * arrayDim)(256), arrayDim, arrayDim)
    val imageTile1 = arrayTile1.convert(DoubleUserDefinedNoDataCellType(256)).mutable
    testStatistics(imageTile = imageTile1, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 0.0)))
    val arrayTile2 = IntArrayTile(Array.fill(arrayDim * arrayDim / 2)(256) ++ Array.fill(arrayDim * arrayDim / 8)(30) ++ Array.fill(arrayDim * arrayDim / 8)(10) ++ Array.fill(arrayDim * arrayDim / 4)(256), arrayDim, arrayDim)
    val imageTile2 = arrayTile2.convert(UShortUserDefinedNoDataCellType(256)).mutable
    testStatistics(imageTile = imageTile2, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 25, "minimum", 10.0, "maximum", 30.0, "mean", 20.0))) // , "stddev", 10
    val imageTile3 = arrayTile2.convert(UShortCellType).mutable
    testStatistics(imageTile = imageTile3, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 100, "minimum", 10, "maximum", 256, "mean", 197.0))) // , "stddev", 102.3132444994293
    testStatistics(imageTile = imageTile0, addStatistics = false)
    testStatistics(imageTile = imageTile0, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 100, "minimum", 0.0, "maximum", 30.0, "mean", 15.0)), polygon = polygon1, expectedShape = Array(86, 52)) // , "stddev", 15.0
    val imageTile4 = arrayTile2.convert(UShortUserDefinedNoDataCellType(256)).mutable
    testStatistics(imageTile = imageTile4, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 0.0)), polygon = polygon1, expectedShape = Array(86, 52))
  }

  @Disabled
  @Test
  def testWriteSingleNetCDF(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val utm31 = CRS.fromEpsgCode(32631)


    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31), utm31)

    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(date, date.plusDays(10), bbox, Array(MultiPolygon(bbox.extent.toPolygon())), bbox.crs, 13, sc, datacubeParams = Some(dcParams))


    val sampleFilenames: util.List[String] = assetFileNames(NetCDFRDDWriter.saveSingleNetCDF(layer, "/tmp/stitched.nc", new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M")), null, null, null, 6))
    val expectedPaths = util.Collections.singletonList("/tmp/stitched.nc")

    assertEquals(expectedPaths, sampleFilenames)
  }

  private def assetFileNames(items: util.List[Item]): util.List[String] =
    items.stream()
      .flatMap { item =>
        item.assets.values().stream().map[String] { asset => asset.path }
      }
      .collect(util.stream.Collectors.toList())

  @Test
  def testWriteSingleNetCDFLarge(): Unit = {

    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val (layer, refTile) = LayerFixtures.aSpacetimeTileLayerRdd(20, 20, nbDates = 10)

    val options = new NetCDFOptions
    options.setBandNames(new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M")))
    val sampleFilenames: util.List[String] = assetFileNames(NetCDFRDDWriter.writeRasters(layer, "/tmp/stitched.nc", options))
    val expectedPaths = util.Collections.singletonList("/tmp/stitched.nc")

    assertEquals(expectedPaths, sampleFilenames)
    val ds = NetcdfDataset.openDataset("/tmp/stitched.nc", true, null)
    val b04 = ds.findVariable("TOC-B04_10M")


  }

  @Test
  def testWriteNetCDFAttributes(): Unit = {
    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"
    val options = new NetCDFOptions
    options.setBandNames(new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M")))

    val layerDefault = LayerFixtures.aSpacetimeTileLayerRddShortFillValue(20, 20)
    val sampleFilenames: util.List[String] = assetFileNames(NetCDFRDDWriter.writeRasters(layerDefault, "/tmp/stitched.nc", options))
    val expectedPaths = util.Collections.singletonList("/tmp/stitched.nc")
    assertEquals(expectedPaths, sampleFilenames)

    val ds = NetcdfDataset.openDataset("/tmp/stitched.nc", true, null)
    val b04 = ds.findVariable("TOC-B04_10M")

    assertEquals(2, ds.findDimension("t").getLength)

    val unsigned = b04.findAttributeIgnoreCase("_Unsigned")
    assertEquals("true", unsigned.getValue(0))

    val longName = b04.findAttributeIgnoreCase("long_name")
    assertEquals("TOC-B04_10M", longName.getValue(0))

    val units = b04.findAttributeIgnoreCase("units")
    assertEquals("", units.getValue(0))

    val fillValueDefault = b04.findAttributeIgnoreCase("_fillValue")
    assertEquals(-1.toShort, fillValueDefault.getValue(0))


    val gridMapping = b04.findAttributeIgnoreCase("grid_mapping")
    assertEquals("crs", gridMapping.getValue(0))


    val chunking = b04.findAttributeIgnoreCase("_ChunkSizes")
    assertEquals(1, chunking.getValue(0))
    assertEquals(256, chunking.getValue(1))
    assertEquals(256, chunking.getValue(2))

    assertEquals("t", b04.getDimension(0).getShortName)
    assertEquals("y", b04.getDimension(1).getShortName)
    assertEquals("x", b04.getDimension(2).getShortName)

    assertEquals(2, b04.getShape(0))
    assertEquals(1024, b04.getShape(1))
    assertEquals(1024, b04.getShape(2))

    assertEquals("uint", b04.getDataType.toString)
    assertEquals(4, b04.getElementSize)


    val layerChosen = LayerFixtures.aSpacetimeTileLayerRddShortFillValue(20, 20, fillValue = 9)
    val sampleFilenamesChosen: util.List[String] = assetFileNames(NetCDFRDDWriter.writeRasters(layerChosen, "/tmp/stitched.nc", options))
    assertEquals(expectedPaths, sampleFilenamesChosen)

    val dsChosen = NetcdfDataset.openDataset("/tmp/stitched.nc", true, null)
    val b04Chosen = dsChosen.findVariable("TOC-B04_10M")

    assertEquals(2, dsChosen.findDimension("t").getLength)

    val unsignedChosen = b04Chosen.findAttributeIgnoreCase("_Unsigned")
    assertEquals("true", unsignedChosen.getValue(0))

    val longNameChosen = b04Chosen.findAttributeIgnoreCase("long_name")
    assertEquals("TOC-B04_10M", longNameChosen.getValue(0))
    val unitsChosen = b04Chosen.findAttributeIgnoreCase("units")
    assertEquals("", unitsChosen.getValue(0))

    val fillValueChosen = b04Chosen.findAttributeIgnoreCase("_fillValue")
    assertEquals(9.toShort, fillValueChosen.getValue(0))

    val gridMappingChosen = b04Chosen.findAttributeIgnoreCase("grid_mapping")
    assertEquals("crs", gridMappingChosen.getValue(0))

    val chunkingChosen = b04Chosen.findAttributeIgnoreCase("_ChunkSizes")
    assertEquals(1, chunkingChosen.getValue(0))
    assertEquals(256, chunkingChosen.getValue(1))
    assertEquals(256, chunkingChosen.getValue(2))

    assertEquals("t", b04Chosen.getDimension(0).getShortName)
    assertEquals("y", b04Chosen.getDimension(1).getShortName)
    assertEquals("x", b04Chosen.getDimension(2).getShortName)

    assertEquals(2, b04Chosen.getShape(0))
    assertEquals(1024, b04Chosen.getShape(1))
    assertEquals(1024, b04Chosen.getShape(2))

    assertEquals("uint", b04Chosen.getDataType.toString)
    assertEquals(4, b04Chosen.getElementSize)

  }

  @Test
  def testWriteSingleNetCDFSpatial(): Unit = {

    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val (image, layer) = LayerFixtures.createLayerWithGaps(5, 5)

    val sampleFilenames: util.List[String] = assetFileNames(NetCDFRDDWriter.saveSingleNetCDFSpatial(layer, "/tmp/stitched.nc", new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M")), null, null, null, 6))
    val expectedPaths = util.Collections.singletonList("/tmp/stitched.nc")

    assertEquals(expectedPaths, sampleFilenames)
    val ds = NetcdfDataset.openDataset("/tmp/stitched.nc", true, null)
    val b04 = ds.findVariable("TOC-B04_10M")

    val chunking = b04.findAttributeIgnoreCase("_ChunkSizes")
    assertEquals(256, chunking.getValue(0))
    assertEquals(256, chunking.getValue(1))
    assertEquals("y", b04.getDimension(0).getShortName)
    assertEquals("x", b04.getDimension(1).getShortName)
    val crs = ds.findVariable("x")
    val units = crs.findAttributeIgnoreCase("units")
    assertEquals("degrees_east", units.getStringValue)

  }

  @Test
  def testWriteSingleNetCDFSpatialItem(): Unit = {
    def testStatistics(imageTile: Tile, expectedStatistics: util.HashMap[String, Any] = null, cropBounds: Option[Extent] = None, expectedShape: Array[Int] = Array(512, 512), addStatistics: Boolean = true): Unit = {
      val layer = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, MultibandTile(imageTile, imageTile, imageTile), TileLayout(imageTile.cols / 256, imageTile.rows / 256, 256, 256), LatLng)

      val items = NetCDFRDDWriter.saveSingleNetCDFGeneric(layer,
        "/tmp/stitched.nc",
        bandNames = new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M")),
        dimensionNames = null,
        attributes = null,
        bandsMetadata = null,
        zLevel = 6,
        addBandsStatistics = addStatistics,
        cropBounds = cropBounds
      )
      assertEquals(1, items.size())
      val item = items.get(0)
      assertEquals(1, item.assets.size())
      val asset = item.assets.get("openEO")
      val metadata = asset.metadata
      assertEquals(LatLng.epsgCode.get, metadata.get("proj:epsg"))
      assertArrayEquals(expectedShape, metadata.get("proj:shape").asInstanceOf[Array[Int]])
      val bbox = cropBounds match {
        case Some(Extent(-18.0, 30.0, 18.0, 60)) => Array(-18.281254492187486, 29.8828091796875, 18.28124449218752, 60.1171808203125)
        case Some(extent) => Array(extent.xmin, extent.ymin, extent.xmax, extent.ymax)
        case _ => Array(-180.0, -90.0, 180.0, 90.0)
      }
      assertArrayEquals(bbox, metadata.get("proj:bbox").asInstanceOf[Array[Double]], 0.01)
      val bands = metadata.get("bands").asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
      assertEquals(3, bands.size())
      bands.forEach(band => {
        assertTrue(band.containsKey("name"))
        assertEquals(addStatistics, band.containsKey("statistics"))
        val statistics = band.getOrDefault("statistics", null).asInstanceOf[util.HashMap[String, Number]]
        assertEquals(expectedStatistics, statistics)
      })
    }

    val arrayDim = 512
    val arrayTile0 = IntArrayTile(Array.fill(arrayDim * arrayDim / 4)(0) ++ Array.fill(arrayDim * arrayDim / 2)(30) ++ Array.fill(arrayDim * arrayDim / 4)(256), arrayDim, arrayDim)
    val imageTile0 = arrayTile0.convert(UShortUserDefinedNoDataCellType(256)).mutable
    testStatistics(imageTile = imageTile0, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 75, "minimum", 0.0, "maximum", 30.0, "mean", 20.0))) // , "stddev", 14.142135623730951
    val arrayTile1 = IntArrayTile(Array.fill(arrayDim * arrayDim)(256), arrayDim, arrayDim)
    val imageTile1 = arrayTile1.convert(DoubleUserDefinedNoDataCellType(256)).mutable
    testStatistics(imageTile = imageTile1, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 0.0)))
    val arrayTile2 = IntArrayTile(Array.fill(arrayDim * arrayDim / 2)(256) ++ Array.fill(arrayDim * arrayDim / 8)(30) ++ Array.fill(arrayDim * arrayDim / 8)(10) ++ Array.fill(arrayDim * arrayDim / 4)(256), arrayDim, arrayDim)
    val imageTile2 = arrayTile2.convert(UShortUserDefinedNoDataCellType(256)).mutable
    testStatistics(imageTile = imageTile2, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 25, "minimum", 10.0, "maximum", 30.0, "mean", 20.0))) // , "stddev", 10
    val arrayTile3 = IntArrayTile(Array.fill(arrayDim * arrayDim / 2)(256) ++ Array.fill(arrayDim * arrayDim / 8)(30) ++ Array.fill(arrayDim * arrayDim / 8)(10) ++ Array.fill(arrayDim * arrayDim / 4)(256), arrayDim, arrayDim)
    testStatistics(imageTile = arrayTile3, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 100, "minimum", 10, "maximum", 256, "mean", 197.0))) //  , "stddev", 102.3132444994293
    val imageTile3 = arrayTile3.convert(UShortCellType).mutable
    testStatistics(imageTile = imageTile3, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 100, "minimum", 10, "maximum", 256, "mean", 197.0))) //  , "stddev", 102.3132444994293
    testStatistics(imageTile = imageTile0, addStatistics = false)
    testStatistics(imageTile = imageTile0, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 100, "minimum", 0.0, "maximum", 30.0, "mean", 15.0)), cropBounds = Some(Extent(-18.0, 30.0, 18.0, 60)), expectedShape = Array(86, 52)) // , "stddev", 15.0
    val imageTile4 = arrayTile2.convert(UShortUserDefinedNoDataCellType(256)).mutable
    testStatistics(imageTile = imageTile4, expectedStatistics = new util.HashMap[String, Any](util.Map.of("valid_percent", 0.0)), cropBounds = Some(Extent(-18.0, 30.0, 18.0, 60)), expectedShape = Array(86, 52))
  }

  @Test
  def testWriteSingleNetCDFMultipleSamplesOnADay(): Unit = {
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
      val sampleFilenames: util.List[String] = assetFileNames(NetCDFRDDWriter.saveSingleNetCDF(
        dataCubeContextRDD,
        "tmp/testWriteSingleNetCDFMultipleSamplesOnADay_" + date2.toString.replace(":", "_") + ".nc",
        new util.ArrayList(util.Arrays.asList("band")),
        null, null, null, 6
      ))
      val ds = NetcdfDataset.openDataset(sampleFilenames.get(0), true, null)
      // When the samples are in the same day, they should still be separate
      assertEquals(dates.length, ds.findDimension("t").getLength)
    }
    assertEquals(true, true)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testWriteCGLS(): Unit = {

    val boundingBox = ProjectedExtent(Extent(38.6, 5.7, 41.0, 9.15), LatLng)

    val parameters = new DataCubeParameters()
    parameters.layoutScheme = "FloatingLayoutScheme"

    val layerProvider = LayerFixtures.cglsNDVI300
    val polygons = ProjectedPolygons.fromExtent(boundingBox.extent, "EPSG:4326")
    val layer = layerProvider.datacube(polygons.polygons, polygons.crs, "2019-06-01T10:08:02Z", "2019-06-01T10:08:02Z", util.Collections.emptyMap(), "", parameters).cache()

    val options = new NetCDFOptions
    options.setBandNames(new util.ArrayList(util.Arrays.asList("NDVI")))
    val sampleFilenames: util.List[String] = assetFileNames(NetCDFRDDWriter.writeRasters(layer, "/tmp/cgls_ndvi300.nc", options))

    val referenceTile = GeoTiffRasterSource("https://artifactory.vgt.vito.be/artifactory/testdata-public/cgls_ndvi300.tiff").read().get
    val actualTile = GDALRasterSource("/tmp/cgls_ndvi300.nc").read().get
    //assertRastersEqual(referenceTile,actualTile,1.0)

  }

  @Test
  def testSetupNetCDF(): Unit = {
    def setup(cellType: CellType) = {

      val dimMapping = new util.HashMap[String, String]()
      dimMapping.put("t", "myTimeDim")
      val attributes = new util.HashMap[String, String]()
      attributes.put("title", "my netcdf file")
      val file = NetCDFRDDWriter.setupNetCDF(
        "test.nc",
        RasterExtent(Extent(0, 0, 10, 10), 512, 512),
        Seq(ZonedDateTime.parse("2021-05-01T00:00:00Z"), ZonedDateTime.parse("2021-05-10T00:00:00Z")),
        new util.ArrayList(util.Arrays.asList("b1", "b2")),
        LatLng, cellType, dimMapping,
        TemporalResolution.days,
        attributes, null
      )
      assertEquals("my netcdf file", file.findGlobalAttribute("title").getStringValue())
      assertNotNull(file.findVariable("myTimeDim"))
      assertNotNull(file.findVariable("crs"))
      file.close()
    }

    setup(UByteUserDefinedNoDataCellType(5))
    setup(FloatConstantNoDataCellType)

    //boolean not supported by library
    //setup(BitCellType)
    setup(UShortCellType)
    setup(IntUserDefinedNoDataCellType(255))
  }


  @Test
  def testNetCDFBandAttributes(): Unit = {
    def setup(cellType: CellType) = {

      val dimMapping = new util.HashMap[String, String]()
      dimMapping.put("t", "myTimeDim")
      val attributes = new util.HashMap[String, String]()
      attributes.put("title", "my netcdf file")
      val bandMetadata = new util.HashMap[String, util.Map[String, String]]()
      val metadataB1 = new util.HashMap[String, String]()
      metadataB1.put("SCALE", "1.23")
      metadataB1.put("OFFSET", "4.56")
      bandMetadata.put("b1", metadataB1)
      val file = NetCDFRDDWriter.setupNetCDF(
        "test.nc",
        RasterExtent(Extent(0, 0, 10, 10), 512, 512),
        Seq(ZonedDateTime.parse("2021-05-01T00:00:00Z"), ZonedDateTime.parse("2021-05-10T00:00:00Z")),
        new util.ArrayList(util.Arrays.asList("b1", "b2")),
        LatLng, cellType, dimMapping,
        TemporalResolution.days,
        attributes, bandMetadata
      )
      assertEquals("my netcdf file", file.findGlobalAttribute("title").getStringValue())
      assertNotNull(file.findVariable("myTimeDim"))
      assertNotNull(file.findVariable("crs"))
      assertNotNull(file.findVariable("b1"))
      val b1 = file.findVariable("b1")
      assertNotNull(b1.findAttribute("long_name"))
      assertNotNull(b1.findAttribute("units"))
      assertNotNull(b1.findAttribute("_FillValue"))
      assertNotNull(b1.findAttribute("scale_factor"))
      assertNotNull(b1.findAttribute("add_offset"))
      assertNotNull(b1.findAttribute("grid_mapping"))
      assertNotNull(b1.findAttribute("_ChunkSizes"))
      assertNotNull(file.findVariable("b2"))
      val b2 = file.findVariable("b2")
      assertNotNull(b2.findAttribute("long_name"))
      assertNotNull(b2.findAttribute("units"))
      assertNotNull(b2.findAttribute("_FillValue"))
      assertNull(b2.findAttribute("scale_factor"))
      assertNull(b2.findAttribute("add_offset"))
      assertNotNull(b2.findAttribute("grid_mapping"))
      assertNotNull(b2.findAttribute("_ChunkSizes"))
      file.close()
    }

    setup(UByteUserDefinedNoDataCellType(5))
    setup(FloatConstantNoDataCellType)

    //boolean not supported by library
    //setup(BitCellType)
    setup(UShortCellType)
    setup(IntUserDefinedNoDataCellType(255))
  }
}
