package org.openeo.geotrellis.layers
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.gdal.GDALPath
import geotrellis.vector._
import org.junit.{Ignore, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import java.nio.file.{Files, Paths}

class GdalCloudRasterSourceTest {

  @Test
  def testReadPolygonsAndExtent(): Unit = {
    val cloudPath = "https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240.SAFE/GRANULE/L1C_T35JPM_A028875_20210101T081145/QI_DATA/MSK_CLOUDS_B00.gml"
    val metaDataPath = "https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240.SAFE/GRANULE/L1C_T35JPM_A028875_20210101T081145/MTD_TL.xml"
    val source = GDALCloudRasterSource(cloudPath, metaDataPath, new GDALPath(""))
    val polygons: Seq[Polygon]  = source.readCloudFile()
    //assert(polygons.length == 100)

    val dilationDistance = 10000
    val mergedPolygons: Seq[Polygon] = source.getMergedPolygons(dilationDistance)

    /*
    val json = FeaturesToGeoJson(mergedPolygons.map(_.reproject(CRS.fromEpsgCode(32635),LatLng)).map(Feature(_,()))).toGeoJson()
    Files.write(Paths.get("polygons.geojson"), json.getBytes)

    val json2 = FeaturesToGeoJson(polygons.map(_.reproject(CRS.fromEpsgCode(32635),LatLng)).map(Feature(_,()))).toGeoJson()
    Files.write(Paths.get("polygons_original.geojson"), json2.getBytes)
*/
    val bufferedPolygons = polygons.par.map(p => p.buffer(dilationDistance).asInstanceOf[Polygon]).toBuffer

    assertEquals(mergedPolygons.extent.area, bufferedPolygons.extent.area,0.5)
    assertTrue(mergedPolygons.extent.toPolygon().equalsExact(bufferedPolygons.extent.toPolygon(),0.01) )
    for (p <- polygons) {
      val buffered = p.buffer(-0.5)//use of fixed precision model makes comparisons less exact
      assertTrue(mergedPolygons.exists(_.covers(buffered)),f"no overlap for $p")
    }

    val extent = source.readExtent()
    assertEquals(extent.width, 109800.0, 0.01)
    assertEquals(extent.height, 109800.0, 0.01)
    for (polygon <- polygons) assert(extent.covers(polygon))

    assert(mergedPolygons.exists(_.covers(extent)))
  }
}
