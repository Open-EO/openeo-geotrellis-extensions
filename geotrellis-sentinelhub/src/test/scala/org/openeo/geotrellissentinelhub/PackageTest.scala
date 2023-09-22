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
