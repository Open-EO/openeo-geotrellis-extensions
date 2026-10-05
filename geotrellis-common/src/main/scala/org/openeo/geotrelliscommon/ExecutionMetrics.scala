package org.openeo.geotrelliscommon

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicReference

import org.slf4j.LoggerFactory

final case class ExecutionMetrics(
  totalStageRuntimeMillis: Long,
  executorAllocationTimeMillis: Long,
  cpuUtilizationRatio: Double,
  totalStageFailures: Int
)

object ExecutionMetrics {
  private val logger = LoggerFactory.getLogger(getClass)
  private val current = new AtomicReference(ExecutionMetrics(0L, 0L, 0d, 0))

  def get: ExecutionMetrics = current.get()

  def asMap(): Map[String, Any] = {
    logger.debug(s"ExecutionMetrics.asMap() called in JVM process ${ManagementFactory.getRuntimeMXBean.getName}")
    val metrics = current.get()
    if (metrics.totalStageRuntimeMillis == 0) {
      Map.empty
    } else {
      Map(
        "totalStageRuntimeMillis" -> metrics.totalStageRuntimeMillis,
        "executorAllocationTimeMillis" -> metrics.executorAllocationTimeMillis,
        "cpuUtilizationRatio" -> metrics.cpuUtilizationRatio,
        "totalStageFailures" -> metrics.totalStageFailures
      )
    }
  }

  private[openeo] def store(metrics: ExecutionMetrics): Unit = {
    current.set(metrics)
  }

  /** Atomically stores `metrics` and returns the previously stored value. */
  private[openeo] def getAndStore(metrics: ExecutionMetrics): ExecutionMetrics =
    current.getAndSet(metrics)
}
