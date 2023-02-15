package org.openeo.logging

object JsonLayout {
  private final val sparkPropagatablePrefix = "mdc."
  private def addPrefix(mdcKey: String): String = sparkPropagatablePrefix + mdcKey

  final val UserId = addPrefix("user_id")
  final val RequestId = addPrefix("req_id")
  final val JobId = addPrefix("job_id")
}
