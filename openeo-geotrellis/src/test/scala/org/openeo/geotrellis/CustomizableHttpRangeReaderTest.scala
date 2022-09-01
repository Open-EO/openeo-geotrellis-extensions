package org.openeo.geotrellis

import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.util.RangeReader
import geotrellis.vector.Extent
import org.junit.{Ignore, Test}

import java.net.URI

@Ignore("needs credentials + Terrascope URL doesn't support byte ranges")
class CustomizableHttpRangeReaderTest {
  // private val geoTiffUri = new URI("http://127.0.0.1:8080/S2A_20200105T071301_39RVH_LAI_20M_V200.tif")

  // java.lang.IllegalArgumentException: requirement failed: Server doesn't support ranged byte reads
  private val geoTiffUri = new URI("https://services.terrascope.be/download/Sentinel2/LAI_V2/2020/01/05/S2A_20200105T071301_39RVH_LAI_V200/20M/S2A_20200105T071301_39RVH_LAI_20M_V200.tif")

  @Test
  def test(): Unit = {
    val customizableHttpRangeReaderProvider = new CustomizableHttpRangeReaderProvider()
    assert(customizableHttpRangeReaderProvider.canProcess(geoTiffUri))
    val httpRangeReader = customizableHttpRangeReaderProvider.rangeReader(geoTiffUri)

    val geoTiff = readGeoTiff(httpRangeReader)
    val geoTiffFileName = geoTiffUri.getPath.split("/").last
    geoTiff.write(s"/tmp/$geoTiffFileName")
  }

  private def readGeoTiff(rangeReader: RangeReader): SinglebandGeoTiff = {
    val bytes = rangeReader.readAll()
    SinglebandGeoTiff(bytes)
  }

  @Test
  def fromRasterSource(): Unit = {
    val geoTiffRasterSource = GeoTiffRasterSource(geoTiffUri.toString)
    val subExtent = buffer(geoTiffRasterSource.extent, -0.4)
    val Some(raster) = geoTiffRasterSource.read(subExtent)

    SinglebandGeoTiff(raster.tile.band(0), raster.extent, geoTiffRasterSource.crs).write("/tmp/testRasterSource.tif")
  }

  private def buffer(extent: Extent, relativeDistance: Double): Extent =
    extent.buffer(extent.width * relativeDistance, extent.height * relativeDistance)
}
