package org.openeo.geotrelliscommon

import java.util.concurrent.atomic.AtomicReference

import org.slf4j.LoggerFactory

final case class ExecutionMetrics(
  totalStageRuntimeMillis: Long,
  executorAllocationTimeMillis: Long,
  cpuUtilizationRatio: Double,
  totalStageFailures: Int,
  totalTaskFailures: Int
)

object ExecutionMetrics {
  private val logger = LoggerFactory.getLogger(getClass)
  private val current = new AtomicReference(ExecutionMetrics(0L, 0L, 0d, 0, 0))

  def get: ExecutionMetrics = current.get()

  private[openeo] def store(metrics: ExecutionMetrics): Unit = {
    current.set(metrics)
  }

  /** Atomically stores `metrics` and returns the previously stored value. */
  private[openeo] def getAndStore(metrics: ExecutionMetrics): ExecutionMetrics =
    current.getAndSet(metrics)
}
