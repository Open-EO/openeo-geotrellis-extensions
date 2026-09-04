package org.openeo.geotrellis

import org.openeo.logging.JsonLayout
import org.slf4j.MDC

case class RequestContext(requestId: String, userId: String) {
  def apply[R](f: => R): R = {
    if (requestId != null) MDC.put(JsonLayout.RequestId, requestId)
    if (userId != null) MDC.put(JsonLayout.UserId, userId)

    try f
    finally {
      if (requestId != null) MDC.remove(JsonLayout.RequestId)
      if (userId != null) MDC.remove(JsonLayout.UserId)
    }
  }
}

object RequestContext {
  def get = RequestContext(MDC.get(JsonLayout.RequestId), MDC.get(JsonLayout.UserId))
}
