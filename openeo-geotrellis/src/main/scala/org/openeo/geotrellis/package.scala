package org.openeo

import java.time.{Duration, Instant}

import org.slf4j.Logger

package object geotrellis {
  def logTiming[R](context: String)(action: => R)(implicit logger: Logger): R = {
    if (logger.isDebugEnabled()) {
      val start = Instant.now()
      logger.debug(s"$context: start")

      try
        action
      finally {
        val end = Instant.now()
        val elapsed = Duration.between(start, end)

        logger.debug(s"$context: end, elapsed $elapsed")
      }
    } else action
  }
}
