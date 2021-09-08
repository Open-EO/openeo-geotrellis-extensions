package org.openeo.geotrellissentinelhub

import org.junit.Assert.fail
import org.junit.Test
import org.slf4j.LoggerFactory

class PackageTest {
  private implicit val logger = LoggerFactory.getLogger(getClass)

  @Test
  def testEmptyRequestBodyIsRetried(): Unit = {
    def eventuallySucceedingRequest: () => Unit = {
      var emptyBody = true

      () => {
        if (emptyBody) {
          emptyBody = false

          val responseBody = """{"error":{"status":400,"reason":"Bad Request","message":"Request body should be non-empty.","code":"COMMON_BAD_PAYLOAD"}}"""
          throw new SentinelHubException(message = responseBody, 400, responseBody)
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

    withRetries(attempts = 5, context = "testEmptyRequestBodyIsRetried") {
      request()
    }
  }
}
