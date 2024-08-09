package org.openeo.geotrelliscommon

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrowsExactly, fail}
import org.junit.jupiter.api.{Test, Timeout}

import java.time.Duration

class PackageTest {
  class FailedAttempt extends Exception

  @Test
  def retryForeverNumberOfAttempts(): Unit = {
    var attempts = 0

    try {
      retryForever(delay = Duration.ZERO, retries = 3, onAttemptFailed = _ => attempts += 1) {
        println("attempting...")
        throw new FailedAttempt
      }

      fail("should have thrown a FailedAttempt")
    } catch {
      case _: FailedAttempt =>
    }

    // count the number of failures to get the number of attempts
    assertEquals(3, attempts)
  }

  @Test
  @Timeout(5) // less than RetryForever's delay below
  def retryForeverNoDelayAfterFinalFailure(): Unit =
    assertThrowsExactly(classOf[FailedAttempt], () =>
      retryForever(delay = Duration.ofSeconds(60), retries = 1) {
        println("attempting...")
        throw new FailedAttempt
      })
}
