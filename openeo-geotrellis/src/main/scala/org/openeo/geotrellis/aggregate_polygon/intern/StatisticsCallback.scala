package org.openeo.geotrellis.aggregate_polygon.intern

import java.io.IOException
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicBoolean

trait StatisticsCallback[-V] {
  @throws(classOf[IOException])
  def onComputed(date: ZonedDateTime, results: Seq[V]): Unit

  @throws(classOf[IOException])
  def onCompleted(): Unit
}

class CancellationContext(val id: String, val description: String) {
  private val _canceled = new AtomicBoolean(false)

  def cancel(): Unit = _canceled.set(true)
  def canceled: Boolean = _canceled.get()
}
