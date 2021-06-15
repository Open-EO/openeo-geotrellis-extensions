package org.openeo.geotrellis.netcdf

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{ProjectedExtent, _}
import org.apache.spark.SparkContext
import org.junit.{Assert, BeforeClass, Test}
import org.openeo.geotrellis.{LayerFixtures, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters

import scala.collection.JavaConverters.asScalaBufferConverter


object NetCDFRDDWriterTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    // originally geotrellis.spark.util.SparkUtils.createLocalSparkContext
    val conf = SparkUtils.createSparkConf
      .setMaster("local[2]")
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
}
