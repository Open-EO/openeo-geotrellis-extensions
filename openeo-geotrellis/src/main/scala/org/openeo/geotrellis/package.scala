package org.openeo

import java.time.{Duration, Instant}

package object geotrellis {
  def time[R](action: => R): (R, Duration) = {
    val start = Instant.now()
    val result = action
    val end = Instant.now()

    (result, Duration.between(start, end))
  }
}
