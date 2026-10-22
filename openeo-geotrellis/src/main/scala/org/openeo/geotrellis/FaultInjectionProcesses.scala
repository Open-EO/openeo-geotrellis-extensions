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
    description = "Testing only: the first attempt of the Spark task for the given partition (argument 'partition', default 0) of the stage that evaluates the datacube exits its executor JVM (System.exit), so Spark has to reschedule the tasks and recompute lost shuffle output. In local mode an exception is thrown instead, so the task is retried. The data is passed through unchanged."
  )
  def failOnce(datacube: Object, args: java.util.Map[String, Any]): Object = {
    val partition = Option(args).flatMap(a => Option(a.get("partition"))) match {
      case None => 0
      case Some(n: Number) if n.doubleValue() == n.intValue() => n.intValue()
      case Some(s: String) if s.trim.toIntOption.isDefined => s.trim.toInt
      case Some(other) => throw new IllegalArgumentException(s"fail_once: 'partition' should be an integer, got: $other")
    }
    val processes = new OpenEOProcesses()
    datacube.asInstanceOf[MultibandTileLayerRDD[_]].metadata.bounds match {
      case KeyBounds(_: SpaceTimeKey, _) => processes.failOnce(datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]], partition)
      case KeyBounds(_: SpatialKey, _) => processes.failOnce(datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]], partition)
      case EmptyBounds => datacube
      case bounds => throw new IllegalArgumentException(s"Unsupported key type for fail_once: $bounds")
    }
  }
}

class FaultInjectionProcessesProvider extends CubeProcessProvider {
  def getInstance(): AnyRef = FaultInjectionProcesses
}
