package org.openeo.geotrellis

import geotrellis.layer.{EmptyBounds, KeyBounds, SpaceTimeKey, SpatialKey}
import geotrellis.spark.MultibandTileLayerRDD
import org.openeo.geotrelliscommon.{CubeProcessProvider, OpenEOProcess}

/**
 * openEO processes that inject failures, to test recovery of Spark jobs.
 * They are exposed to the Python driver through [[org.openeo.geotrelliscommon.CubeProcessRegistry]].
 */
object FaultInjectionProcesses {

  @OpenEOProcess(
    id = "fail_once",
    description = "Testing only: the first attempt of the Spark task for partition 0 of the stage that evaluates the datacube exits its executor JVM (System.exit), so Spark has to reschedule the tasks and recompute lost shuffle output. In local mode an exception is thrown instead, so the task is retried. The data is passed through unchanged."
  )
  def failOnce(datacube: Object): Object = {
    val processes = new OpenEOProcesses()
    datacube.asInstanceOf[MultibandTileLayerRDD[_]].metadata.bounds match {
      case KeyBounds(_: SpaceTimeKey, _) => processes.failOnce(datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]])
      case KeyBounds(_: SpatialKey, _) => processes.failOnce(datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]])
      case EmptyBounds => datacube
      case bounds => throw new IllegalArgumentException(s"Unsupported key type for fail_once: $bounds")
    }
  }
}

class FaultInjectionProcessesProvider extends CubeProcessProvider {
  def getInstance(): AnyRef = FaultInjectionProcesses
}
