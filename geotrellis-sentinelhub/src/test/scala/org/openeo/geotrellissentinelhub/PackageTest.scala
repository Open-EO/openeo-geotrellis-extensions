package org.openeo.geotrellissentinelhub

import org.junit.Assert.{assertEquals, fail}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, ZoneOffset}

class PackageTest {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  @Test
  def testEmptyRequestBodyIsRetried(): Unit = {
    def eventuallySucceedingRequest: () => Unit = {
      var emptyBody = true

      () => {
        if (emptyBody) {
          emptyBody = false

          val responseBody = """{"error":{"status":400,"reason":"Bad Request","message":"Request body should be non-empty.","code":"COMMON_BAD_PAYLOAD"}}"""
          throw new SentinelHubException(message = responseBody, 400, responseHeaders = Map(), responseBody)
        }
      }
    }

    // sanity check
    try {
      val request = eventuallySucceedingRequest
      request()
      fail("should have thrown a SentinelHubException")
    } catch {
      case _: SentinelHubException => /* expected */
    }

    val request = eventuallySucceedingRequest

    withRetries(context = "testEmptyRequestBodyIsRetried") {
      request()
    }
  }

  @Test(expected = classOf[SentinelHubException], timeout = 1000L)
  def testCorruptTileRequestIsNotRetried(): Unit =
    withRetries(context = "testCorruptTileRequestIsNotRetried") {
      val responseBody = """{"error":{"status":500,"reason":"Internal Server Error","message":"java.util.concurrent.ExecutionException: java.lang.IllegalArgumentException: newLimit > capacity: (2808 > 2804)","code":"RENDERER_EXCEPTION"}}"""
      throw new SentinelHubException(message = responseBody, 500, responseHeaders = Map(), responseBody)
    }

  @Test(expected = classOf[SentinelHubException], timeout = 1000L)
  def testBandUnavailableRequestIsNotRetried(): Unit =
    withRetries(context = "testBandUnavailableRequestIsNotRetried") {
      val responseBody = """{"error":{"status":500,"reason":"Internal Server Error","message":"Illegal request to https://sentinel-s1-l1c.s3.amazonaws.com/GRD/2018/11/25/EW/DH/S1B_EW_GRDM_1SDH_20181125T043340_20181125T043419_013756_0197D4_BC1C/measurement/ew-vh.tiff. HTTP Status: 404.","code":"RENDERER_EXCEPTION"}}"""
      throw new SentinelHubException(message = responseBody, 500, responseHeaders = Map(), responseBody)
    }

  @Test
  def testSequentialDaysBoundsAreInclusive(): Unit = {
    val from = LocalDate.of(2023, 1, 1).atStartOfDay(ZoneOffset.UTC)
    val to = from plusDays 2

    val expected = Seq(
      from,
      from plusDays 1,
      from plusDays 2)

    val actual = sequentialDays(from, to)

    assertEquals(expected, actual)
  }
}
