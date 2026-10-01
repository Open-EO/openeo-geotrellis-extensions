package org.openeo.geotrelliscommon

final case class ExecutionMetrics(
  totalStageRuntimeMillis: Long,
  executorAllocationTimeMillis: Long,
  cpuUtilizationRatio: Double,
  totalStageFailures: Int
)

object ExecutionMetrics {
  @volatile private var current = ExecutionMetrics(0L, 0L, 0d, 0)

  def get: ExecutionMetrics = current

  def asMap(): Map[String, Any] = Map(
    "totalStageRuntimeMillis" -> current.totalStageRuntimeMillis,
    "executorAllocationTimeMillis" -> current.executorAllocationTimeMillis,
    "cpuUtilizationRatio" -> current.cpuUtilizationRatio,
    "totalStageFailures" -> current.totalStageFailures
  )

  private[openeo] def store(metrics: ExecutionMetrics): Unit = {
    current = metrics
  }
}
