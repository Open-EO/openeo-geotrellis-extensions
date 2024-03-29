package org.openeo.geotrellis.layers

import com.azavea.gdal.GDALWarp
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, RasterExtent, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType}
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, Disabled, Test}
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.time.ZonedDateTime



object NetCDFCollectionTest {
  @AfterAll
  def tearDown():Unit = {
    GDALWarp.deinit()
    GDALWarp.init(20)
  }
}

class NetCDFCollectionTest {


  @Test
  def testLoadNetCDFCollection():Unit = {
    val osClient = new FixedFeaturesOpenSearchClient()

    val url0 = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/netcdfCollection/openEO_0.nc")
    val url1 = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/netcdfCollection/openEO_1.nc")
    val url2 = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/netcdfCollection/openEO_2.nc")


    val e0 = Extent(615050.0, 5677250.0, 628750.0, 5686540.0)
    val crs = CRS.fromName("EPSG:32631")
    val f = new Feature("openEO_0",
      e0.reproject(crs,LatLng), ZonedDateTime.parse("2021-01-01T00:00:00Z"),
      Array(new Link(url0.toURI, Some("Sentinel2_L2A"), None, Some(List("B01", "B02")))),Some(10.0),None,None,Some(crs),null,
      Some(e0),0.0)
    osClient.addFeature(f)
    val e1 = Extent(636340.0, 5663450.0, 653150.0, 5674120.0)
    osClient.addFeature(new Feature("openEO_1", e1.reproject(crs,LatLng), ZonedDateTime.parse("2021-01-01T00:00:00Z"),Array(new Link(url1.toURI, Some("Sentinel2_L2A"), None, Some(List("B01", "B02")))),Some(10.0),None,None,Some(CRS.fromName("EPSG:32631")),null,Some(e1),0.0))
    val e2 = Extent(604500.0, 5656790.0, 617590.0, 5666820.0)
    osClient.addFeature(new Feature("openEO_2", e2.reproject(crs,LatLng), ZonedDateTime.parse("2021-01-01T00:00:00Z"),Array(new Link(url2.toURI, Some("Sentinel2_L2A"), None, Some(List("B01", "B02")))),Some(10.0),None,None,Some(CRS.fromName("EPSG:32631")),null,Some(e2),0.0))

    val sc = SparkUtils.createLocalSparkContext(
      "local[1]",
      appName = classOf[NetCDFCollectionTest].getName
    )

    val cube = NetCDFCollection.loadCollection(osClient, sc)
    assertEquals(2331,cube.count())
    assertEquals(Extent(603901.4819578232, 5656508.552285681, 653638.1910088382, 5687527.3439567955),cube.metadata.extent)
    assertEquals(crs,cube.metadata.crs)
    assertEquals(ShortUserDefinedNoDataCellType(32767),cube.metadata.cellType)
    assertEquals(CellSize(10.0,10.0),cube.metadata.cellSize)

  }

}
