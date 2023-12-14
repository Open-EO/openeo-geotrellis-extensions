package org.openeo.geotrellis.netcdf

import com.azavea.gdal.GDALWarp
import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ByteArrayTile, CellType, FloatConstantNoDataCellType, IntUserDefinedNoDataCellType, MultibandTile, Raster, RasterExtent, UByteUserDefinedNoDataCellType, UShortCellType, isData}
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.util.SparkUtils
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.vector.io.json.GeoJson
import geotrellis.vector.{ProjectedExtent, _}
import org.apache.spark.SparkContext
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.openeo.geotrellis.{LayerFixtures, ProjectedPolygons}
import org.openeo.geotrelliscommon.{ByKeyPartitioner, ConfigurableSpaceTimePartitioner, DataCubeParameters}
import ucar.nc2.dataset.NetcdfDataset

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util
import scala.annotation.meta.getter
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source


object NetCDFRDDWriterTest {
  private var sc: SparkContext = _

  @BeforeClass
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

  @AfterClass
  def tearDown(): Unit = GDALWarp.deinit()
}


class NetCDFRDDWriterTest extends RasterMatchers{
  import org.openeo.geotrellis.netcdf.NetCDFRDDWriterTest._

  @(Rule @getter)
  val temporaryFolder = new TemporaryFolder

  @Test
  def testWriteSamples(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val utm31 = CRS.fromEpsgCode(32631)
    val geometriesPath = getClass.getResource("/org/openeo/geotrellis/minimallyOverlappingGeometryCollection.json").getPath
    val polygons = ProjectedPolygons.fromVectorFile(geometriesPath)

    val extent = polygons.polygons.seq.extent
    val bbox = ProjectedExtent(ProjectedExtent(extent, LatLng).reproject(utm31),utm31)
    val polygonsUTM31 = ProjectedPolygons.reproject(polygons,32631)


    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from = date, to = date.plusDays(20), bbox,polygonsUTM31.polygons,utm31,14, sc = sc,Some(dcParams))
    val partitioner = layer.partitioner.get
    assert(partitioner.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
    val index: PartitionerIndex[SpaceTimeKey] = partitioner.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index
    assert(index.isInstanceOf[ConfigurableSpaceTimePartitioner])
    assert(layer.metadata.tileCols == 128)

    val sampleNames = polygons.polygons.indices.map(_.toString)
    val sampleNameList = new util.ArrayList[String]()
    sampleNames.foreach(sampleNameList.add)

    val targetDir = temporaryFolder.getRoot.toString

    val sampleFilenames: util.List[String] = NetCDFRDDWriter.saveSamples(layer, targetDir, polygonsUTM31,
      sampleNameList, new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M",
        "SCENECLASSIFICATION_20M")),
      Some("prefixTest"))

    val expectedPaths = List(s"$targetDir/prefixTest_0.nc", s"$targetDir/prefixTest_1.nc")

    Assert.assertEquals(sampleFilenames.asScala.groupBy(identity), expectedPaths.groupBy(identity))

    // note: tests first geometry only
    val bandName = "TOC-B04_10M"
    val rasterSource = GDALRasterSource(s"""NETCDF:"${expectedPaths.head}":$bandName""")
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
  def testWriteSamplesWithGlobalBoundsBuffer(): Unit = {
    val utm30 = CRS.fromEpsgCode(32630)
    val startDate = ZonedDateTime.of(LocalDate.of(2021, 12, 1), MIDNIGHT, UTC)
    val endDate = ZonedDateTime.of(LocalDate.of(2021, 12, 31), MIDNIGHT, UTC)

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
    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from=startDate, to=endDate, bbox, polygons.polygons, utm30,zoom, sc = sc, Some(dcParams))

    val sampleNames = polygons.polygons.indices.map(_.toString)
    val sampleNameList = new util.ArrayList[String]()
    sampleNames.foreach(sampleNameList.add)
    val bandNames = new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"))

    val targetDir = temporaryFolder.getRoot.toString

    val sampleFilenames: util.List[String] = NetCDFRDDWriter.saveSamples(
      layer, targetDir, polygons, sampleNameList, bandNames
    )

    val raster1: Raster[MultibandTile] = GDALRasterSource(s"""NETCDF:${sampleFilenames.get(0)}:TOC-B04_10M""").read().get
    val raster2: Raster[MultibandTile] = GDALRasterSource(s"""NETCDF:${sampleFilenames.get(1)}:TOC-B04_10M""").read().get

    // Compare raster extents.
    //assert(raster1.extent.width == 2560.0)
    //assert(raster1.extent.height == 2 * 2560.0)
    //assert(raster2.extent.width == 2560.0)
    //assert(raster2.extent.height == 2560.0)
    val bands = raster1.tile.bands.filter(!_.isNoDataTile)
    assert(bands.size==3) //3 dates

    for(bandIndex:Int <- 0 until 3) {
      // Ensure there is data within the polygon on this observation.
      assert(bands(bandIndex).mask(raster1.extent, polygon1_nativecrs).toArray().exists(p => p != -2147483648))
    }
  }

  @Test
  def testKeyPartitioner():Unit = {
    val splits = (0 to 30).map(_.toString).toArray
    val p = new ByKeyPartitioner(splits)
    Assert.assertEquals(0,p.getPartition("0"))
    Assert.assertEquals(1,p.getPartition("1"))
    Assert.assertEquals(2,p.getPartition("2"))
    Assert.assertEquals(3,p.getPartition("3"))
    Assert.assertEquals(4,p.getPartition("4"))
    Assert.assertEquals(20,p.getPartition("20"))
    Assert.assertEquals(30,p.getPartition("30"))
  }

  @Test
  def testWriteSamplesSpatial(): Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val polygons = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/minimallyOverlappingGeometryCollection.json").getPath)

    val extent = polygons.polygons.seq.extent
    val bbox = ProjectedExtent(ProjectedExtent(extent, LatLng).reproject(utm31),utm31)
    val polygonsUTM31 = ProjectedPolygons.reproject(polygons,32631)


    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val (imageTile: ByteArrayTile, layer: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(20, 10)

    val localLayer = ContextRDD(layer,layer.metadata.copy(extent = bbox.extent,crs=bbox.crs,layout = layer.metadata.layout.copy(extent=bbox.extent)))

    val sampleNames = polygons.polygons.indices.map(_.toString)
    val sampleNameList = new util.ArrayList[String]()
    sampleNames.foreach(sampleNameList.add)

    val sampleFilenames: util.List[String] = NetCDFRDDWriter.saveSamplesSpatial(
      localLayer,
      "/tmp",
      polygonsUTM31,
      sampleNameList,
      new util.ArrayList(util.Arrays.asList("B04", "B03", "B02")),
      null,
      null,
      Some("prefixTest"),
    )
    val expectedPaths = List("/tmp/prefixTest_0.nc", "/tmp/prefixTest_1.nc")

    Assert.assertEquals(sampleFilenames.asScala.groupBy(identity), expectedPaths.groupBy(identity))
  }

  @Ignore
  @Test
  def testWriteSingleNetCDF(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val utm31 = CRS.fromEpsgCode(32631)


    val bbox = ProjectedExtent(ProjectedExtent(Extent(1.95, 50.95, 2.05, 51.05), LatLng).reproject(utm31),utm31)

    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(date,date.plusDays(10),bbox,Array(MultiPolygon(bbox.extent.toPolygon())),bbox.crs,13,sc,datacubeParams = Some(dcParams))


    val sampleFilenames: util.List[String] = NetCDFRDDWriter.saveSingleNetCDF(layer,"/tmp/stitched.nc", new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M")),null,null,6)
    val expectedPaths = List("/tmp/stitched.nc")

    Assert.assertEquals(sampleFilenames.asScala.groupBy(identity), expectedPaths.groupBy(identity))
  }

  @Test
  def testWriteSingleNetCDFLarge(): Unit = {

    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val (layer,refTile) = LayerFixtures.aSpacetimeTileLayerRdd(20,20,nbDates = 10)

    val options = new NetCDFOptions
    options.setBandNames(new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M")))
    val sampleFilenames: util.List[String] = NetCDFRDDWriter.writeRasters(layer,"/tmp/stitched.nc",options)
    val expectedPaths = List("/tmp/stitched.nc")

    Assert.assertEquals(sampleFilenames.asScala.groupBy(identity), expectedPaths.groupBy(identity))
    val ds = NetcdfDataset.openDataset("/tmp/stitched.nc",true,null)
    val b04 = ds.findVariable("TOC-B04_10M")

    Assert.assertEquals(10, ds.findDimension("t").getLength)

    val chunking = b04.findAttributeIgnoreCase("_ChunkSizes")
    Assert.assertEquals(256,chunking.getValue(1))
    Assert.assertEquals(256,chunking.getValue(2))
    Assert.assertEquals("t",b04.getDimension(0).getShortName)
    Assert.assertEquals("y",b04.getDimension(1).getShortName)
    Assert.assertEquals("x",b04.getDimension(2).getShortName)

  }

  @Test
  def testWriteSingleNetCDFSpatial(): Unit = {

    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val (image,layer) = LayerFixtures.createLayerWithGaps(5,5)

    val sampleFilenames: util.List[String] = NetCDFRDDWriter.saveSingleNetCDFSpatial(layer,"/tmp/stitched.nc", new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M")),null,null,6)
    val expectedPaths = List("/tmp/stitched.nc")

    Assert.assertEquals(sampleFilenames.asScala.groupBy(identity), expectedPaths.groupBy(identity))
    val ds = NetcdfDataset.openDataset("/tmp/stitched.nc",true,null)
    val b04 = ds.findVariable("TOC-B04_10M")

    val chunking = b04.findAttributeIgnoreCase("_ChunkSizes")
    Assert.assertEquals(256,chunking.getValue(0))
    Assert.assertEquals(256,chunking.getValue(1))
    Assert.assertEquals("y",b04.getDimension(0).getShortName)
    Assert.assertEquals("x",b04.getDimension(1).getShortName)
    val crs = ds.findVariable("x")
    val units = crs.findAttributeIgnoreCase("units")
    Assert.assertEquals("degrees_east",units.getStringValue)

  }

  @Test
  def testWriteCGLS(): Unit = {

    val boundingBox = ProjectedExtent(Extent(38.6, 5.7, 41.0, 9.15), LatLng)

    val parameters = new DataCubeParameters()
    parameters.layoutScheme = "FloatingLayoutScheme"

    val layerProvider = LayerFixtures.cglsNDVI300
    val polygons = ProjectedPolygons.fromExtent(boundingBox.extent, "EPSG:4326")
    val layer = layerProvider.datacube(polygons.polygons,polygons.crs,"2019-06-01T10:08:02Z", "2019-06-01T10:08:02Z", util.Collections.emptyMap(), "",parameters).cache()

    val options = new NetCDFOptions
    options.setBandNames(new util.ArrayList(util.Arrays.asList("NDVI")))
    val sampleFilenames: util.List[String] = NetCDFRDDWriter.writeRasters(layer,"/tmp/cgls_ndvi300.nc",options)

    val referenceTile = GeoTiffRasterSource("https://artifactory.vgt.vito.be/artifactory/testdata-public/cgls_ndvi300.tiff").read().get
    val actualTile = GDALRasterSource("/tmp/cgls_ndvi300.nc").read().get
    //assertRastersEqual(referenceTile,actualTile,1.0)

  }

  @Test
  def testSetupNetCDF(): Unit = {
    def setup(cellType:CellType) = {

      val dimMapping = new util.HashMap[String, String]()
      dimMapping.put("t","myTimeDim")
      val attributes = new util.HashMap[String, String]()
      attributes.put("title","my netcdf file")
      val file = NetCDFRDDWriter.setupNetCDF("test.nc", RasterExtent(Extent(0, 0, 10, 10), 512, 512),Seq(ZonedDateTime.parse("2021-05-01T00:00:00Z"),ZonedDateTime.parse("2021-05-10T00:00:00Z")),new util.ArrayList(util.Arrays.asList("b1","b2")),LatLng,cellType,dimMapping,attributes)
      Assert.assertEquals("my netcdf file",file.findGlobalAttribute("title").getStringValue())
      Assert.assertNotNull(file.findVariable("myTimeDim"))
      Assert.assertNotNull(file.findVariable("crs"))
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
