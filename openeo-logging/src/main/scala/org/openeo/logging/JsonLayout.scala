package org.openeo.logging

import org.slf4j.Marker
import org.slf4j.MarkerFactory

object JsonLayout {
  private final val sparkPropagatablePrefix = "mdc."
  private def addPrefix(mdcKey: String): String = sparkPropagatablePrefix + mdcKey

  // Audience: records are user-facing, except DEBUG/TRACE (unless marked USER) and records marked DEVELOPER.
  final val USER: Marker = MarkerFactory.getMarker("USER_FACING")
  final val DEVELOPER: Marker = MarkerFactory.getMarker("DEVELOPER")

  final val UserId = addPrefix("user_id")
  final val RequestId = addPrefix("req_id")
  final val JobId = addPrefix("job_id")
}
