package org.openeo.geotrellis

import net.jodah.failsafe.{Failsafe, RetryPolicy}
import net.jodah.failsafe.event.ExecutionAttemptedEvent

import java.util
import java.time.temporal.ChronoUnit.SECONDS

package object layers {
  def retryForever[R](maxAttempts: Int = 20, onAttemptFailed: Exception => Unit = _ => ())(f: => R): R = {
    val retryPolicy = new RetryPolicy[R]
      .handle(classOf[Exception]) // will otherwise retry Error
      .withMaxAttempts(maxAttempts)
      .withBackoff(1, 16, SECONDS)
      .onFailedAttempt((attempt: ExecutionAttemptedEvent[R]) =>
        onAttemptFailed(attempt.getLastFailure.asInstanceOf[Exception]))

    Failsafe
      .`with`(util.Collections.singletonList(retryPolicy))
      .get(f _)
  }
}
