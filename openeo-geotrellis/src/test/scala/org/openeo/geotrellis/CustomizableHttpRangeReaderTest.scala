package org.openeo.geotrellis

import geotrellis.raster.RasterSource
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.util.RangeReader
import geotrellis.vector.Extent
import net.jodah.failsafe.FailsafeException
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}
import scalaj.http.HttpStatusException

import java.net.URI
import java.time.Instant

@Ignore("requires a file with Terrascope credentials")
class CustomizableHttpRangeReaderTest {
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

  def getCornerPixelValue(rs: RasterSource): Int = rs.read().get._1.toArrayTile().band(0).get(5, 5)

  @Test(expected = classOf[HttpStatusException], timeout = 10000L)
  def test400Exception(): Unit = {
    // Add '|| r.is4xx' to 'retryableResult' to see what retrying on invalid 400 looks like.
    val url = "http://localhost:8000/echo?response_code=400"
    val tiffRs = GeoTiffRasterSource(url)
    getCornerPixelValue(tiffRs)
  }

  @Test(expected = classOf[FailsafeException], timeout = 5000L)
  def testNonExiestentHostException(): Unit = {
    // Add this to 'retryableException' to what retrying on invalid DNS looks like:
    //    case _: java.net.UnknownHostException => true
    val url = "http://non-existent-dns-jsfdjsldfnsdfndslf.com/img.tif"
    val tiffRs = GeoTiffRasterSource(url)
    getCornerPixelValue(tiffRs)
  }

  @Test(expected = classOf[Exception], timeout = 5000L)
  def testNonExiestentPathException(): Unit = {
    val url = "/data/fake-folder-jsfdjsldfnsdfndslf/img.tif"
    val tiffRs = GeoTiffRasterSource(url)
    getCornerPixelValue(tiffRs)
  }

  @Test
  def testRetry(): Unit = {
    // Run test server with this snippet: https://gist.github.com/EmileSonneveld/67f8a050d5891cb96bb969d634796841
    val url = "http://localhost:8000/shaky?token=rand" + Instant.now()
    // val url = "https://services.terrascope.be/download/AgERA5/2024/20240418/AgERA5_dewpoint-temperature_20240418.tif" // Used to give 429 quickly
    val tiffRs = GeoTiffRasterSource(url)

    // log output:
    //   Attempt 1 failed in context: 'readClippedRange' Scheduled retry in PT20S
    //   Attempt 2 failed in context: 'readClippedRange' Scheduled retry in PT3S
    //   Attempt 3 failed in context: 'readClippedRange' result code: 500
    //   Attempt 4 failed in context: 'readClippedRange' java.net.SocketTimeoutException: Read timed out

    val newValue = getCornerPixelValue(tiffRs)
    assertEquals(-2147483648, newValue)
  }
}
