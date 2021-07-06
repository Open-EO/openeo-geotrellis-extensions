package org.openeo.geotrellis.netcdf

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellType, FloatConstantNoDataCellType, IntUserDefinedNoDataCellType, RasterExtent, UByteUserDefinedNoDataCellType, UShortCellType}
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{ProjectedExtent, _}
import org.apache.spark.SparkContext
import org.junit.{Assert, BeforeClass, Ignore, Test}
import org.openeo.geotrellis.{LayerFixtures, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.JavaConverters.asScalaBufferConverter


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


}

class NetCDFRDDWriterTest {

  import org.openeo.geotrellis.netcdf.NetCDFRDDWriterTest._

  @Test
  def testWriteSamples(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val utm31 = CRS.fromEpsgCode(32631)
    val polygons = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/minimallyOverlappingGeometryCollection.json").getPath)

    val extent = polygons.polygons.seq.extent
    val bbox = ProjectedExtent(ProjectedExtent(extent, LatLng).reproject(utm31),utm31)
    val polygonsUTM31 = ProjectedPolygons.reproject(polygons,32631)


    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(from = date, to = date.plusDays(20), bbox,polygonsUTM31.polygons,utm31,14, sc = sc,Some(dcParams))



    val sampleNames = polygons.polygons.indices.map(_.toString)
    val sampleNameList = new util.ArrayList[String]()
    sampleNames.foreach(sampleNameList.add)

    val sampleFilenames: util.List[String] = NetCDFRDDWriter.saveSamples(layer,"/tmp",polygonsUTM31,sampleNameList, new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M")))
    val expectedPaths = List("/tmp/openEO_0.nc", "/tmp/openEO_1.nc")

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

    val sampleFilenames: util.List[String] = NetCDFRDDWriter.saveSingleNetCDF(layer,"/tmp/stitched.nc", new util.ArrayList(util.Arrays.asList("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M")),null,null,6)
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
