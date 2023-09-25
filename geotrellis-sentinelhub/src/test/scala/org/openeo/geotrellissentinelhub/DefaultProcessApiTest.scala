package org.openeo.geotrellissentinelhub

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.openeo.geotrellissentinelhub.DefaultProcessApi.{Sentinel1BandNotPresentException, withRetryAfterRetries}
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Duration, Instant, LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util
import scala.math.Ordered.orderingToOrdered

object DefaultProcessApiTest {
  private def time[R](body: => R): (R, Duration) = {
    val start = Instant.now()
    val result = body
    val end = Instant.now()

    (result, Duration.between(start, end))
  }
}

class DefaultProcessApiTest {
  import DefaultProcessApiTest._

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret
  private val authorizer = new MemoizedAuthApiAccessTokenAuthorizer(
    clientId, clientSecret)

  private val processApi = new DefaultProcessApi("https://services.sentinel-hub.com")
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  @Test(expected = classOf[SentinelHubException], timeout = 1000L)
  def testCorruptTileRequestIsNotRetried(): Unit =
    withRetryAfterRetries(context = "testCorruptTileRequestIsNotRetried") {
      val responseBody = """{"error":{"status":500,"reason":"Internal Server Error","message":"java.util.concurrent.ExecutionException: java.lang.IllegalArgumentException: newLimit > capacity: (2808 > 2804)","code":"RENDERER_EXCEPTION"}}"""
      throw new SentinelHubException(message = responseBody, 500, responseHeaders = Map(), responseBody)
    }

  @Test(expected = classOf[SentinelHubException], timeout = 1000L)
  def testBandUnavailableRequestIsNotRetried(): Unit =
    withRetryAfterRetries(context = "testBandUnavailableRequestIsNotRetried") {
      val responseBody = """{"error":{"status":500,"reason":"Internal Server Error","message":"Illegal request to https://sentinel-s1-l1c.s3.amazonaws.com/GRD/2018/11/25/EW/DH/S1B_EW_GRDM_1SDH_20181125T043340_20181125T043419_013756_0197D4_BC1C/measurement/ew-vh.tiff. HTTP Status: 404.","code":"RENDERER_EXCEPTION"}}"""
      throw new SentinelHubException(message = responseBody, 500, responseHeaders = Map(), responseBody)
    }

  @Test(expected = classOf[SentinelHubException], timeout = 1000L)
  def testBandUnavailableWithImprovedErrorMessageRequestIsNotRetried(): Unit =
    withRetryAfterRetries(context = "testBandUnavailableWithImprovedErrorMessageRequestIsNotRetried") {
      val responseBody = """{"error":{"status":400,"reason":"Bad Request","message":"Requested band 'VH' is not present in Sentinel 1 tile 'S1B_EW_GRDM_1SDH_20181125T043340_20181125T043419_013756_0197D4_BC1C' returned by criteria specified in `dataFilter` parameter.","code":"RENDERER_S1_MISSING_POLARIZATION"}}"""
      throw new SentinelHubException(message = responseBody, 400, responseHeaders = Map(), responseBody)
    }

  @Test(timeout = 6000)
  def testRetryAfterHeaderIsRespected(): Unit = {
    val retryAfter = Duration.ofSeconds(5)

    val (_, delay) = time {
      var tooSoon = true

      withRetryAfterRetries(context = "testRetryAfterHeaderIsRespected") {
        val responseBody = ""

        if (tooSoon) {
          tooSoon = false
          throw new SentinelHubException(message = responseBody, 429,
            responseHeaders = Map("Retry-After" -> Seq(retryAfter.getSeconds.toString)), responseBody)
        }
      }
    }

    assertTrue(delay >= retryAfter)
  }

  @Test(timeout = 3000)
  def testSentinel1BandNotPresentException(): Unit = {
    try {
      authorizer.authorized { accessToken =>
        processApi.getTile(datasetId = "sentinel-1-grd", ProjectedExtent(Extent(16.06, 48.06, 16.07, 48.07), LatLng),
          ZonedDateTime.of(LocalDate.of(2017, 3, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC), width = 256, height = 256,
          bandNames = Seq("VV", "HH"), sampleType = SampleType.FLOAT32,
          additionalDataFilters = util.Collections.emptyMap(), processingOptions = util.Collections.emptyMap(),
          accessToken)
      }
    } catch {
      case e: Sentinel1BandNotPresentException =>
        assertTrue(e.getMessage, e.getMessage contains "not present in Sentinel 1 tile")
        assertEquals(e.bandName, "HH", e.bandName)
    }
  }
}
