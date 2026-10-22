package org.openeo.logging

import org.slf4j.Marker
import org.slf4j.MarkerFactory

object JsonLayout {
  private final val sparkPropagatablePrefix = "mdc."
  private def addPrefix(mdcKey: String): String = sparkPropagatablePrefix + mdcKey

  final val USER: Marker = MarkerFactory.getMarker("USER_FACING")

  final val UserId = addPrefix("user_id")
  final val RequestId = addPrefix("req_id")
  final val JobId = addPrefix("job_id")
}
