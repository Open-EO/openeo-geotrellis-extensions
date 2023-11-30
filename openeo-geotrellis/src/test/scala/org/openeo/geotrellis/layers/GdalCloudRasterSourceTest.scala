package org.openeo.geotrellis.layers
import geotrellis.raster.gdal.GDALPath
import geotrellis.vector._
import org.junit.Test

class GdalCloudRasterSourceTest {

  @Test
  def testReadPolygonsAndExtent(): Unit = {
    val cloudPath = "https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240.SAFE/GRANULE/L1C_T35JPM_A028875_20210101T081145/QI_DATA/MSK_CLOUDS_B00.gml"
    val metaDataPath = "https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240.SAFE/GRANULE/L1C_T35JPM_A028875_20210101T081145/MTD_TL.xml"
    val source = GDALCloudRasterSource(cloudPath, metaDataPath, new GDALPath(""))
    val polygons: Seq[Polygon]  = source.readCloudFile()
    assert(polygons.length == 709)

    val dilationDistance = 10000
    val mergedPolygons: Seq[Polygon] = source.getMergedPolygons(dilationDistance)
    val bufferedPolygons = source.readCloudFile().par.map(p => p.buffer(dilationDistance).asInstanceOf[Polygon]).toBuffer

    assert(mergedPolygons.extent.area == bufferedPolygons.extent.area)
    assert(mergedPolygons.extent == bufferedPolygons.extent)
    for (p <- polygons) assert(mergedPolygons.exists(_.covers(p)))

    val extent = source.readExtent()
    assert(extent.width == 109800.0)
    assert(extent.height == 109800.0)
    for (polygon <- polygons) assert(extent.covers(polygon))

    assert(mergedPolygons.exists(_.covers(extent)))
  }
}
