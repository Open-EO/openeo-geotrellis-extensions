package org.openeo.geotrelliscommon

import org.apache.spark.SparkContext
import org.apache.spark.util.DoubleAccumulator
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap

/*
The ScopedMetadataTracker companion object acts as a factory for ScopedMetadataTracker objects in the driver.
These instances are memoized so only the first call will actually create a tracker and its underlying accumulators.

A ScopedMetadataTracker object, along with its accumulators, will be serialized and sent to the executors to update the
tracker's values.

The driver typically first creates an instance and after the Spark task completes, gets the tracker's values; if
necessary (think: /result requests as opposed to batch jobs) he can remove the tracker instance so it's not kept
indefinitely.
 */
object ScopedMetadataTracker {
  // TODO: merge with BatchJobMetadataTracker
  private val logger = LoggerFactory.getLogger(classOf[ScopedMetadataTracker])
  private val trackers = new ConcurrentHashMap[String, ScopedMetadataTracker]

  /*
  Get/create a particular tracker in the driver; The scope corresponds to a certain scope e.g. a request (ID) or a
  batch job (ID).
  */
  def apply(scope: String)(implicit sc: SparkContext): ScopedMetadataTracker =
    trackers.computeIfAbsent(scope, new ScopedMetadataTracker(_)(sc))


  def remove(scope: String): Unit = {
    trackers.remove(scope)
    logger.debug(s"removed ScopedMetadataTracker($scope)")
  }
}

class ScopedMetadataTracker private(val scope: String)(implicit sc: SparkContext) extends Serializable {
  import ScopedMetadataTracker._

  logger.debug(s"creating ScopedMetadataTracker($scope)")

  private val sentinelHubProcessingUnitsCounter: DoubleAccumulator = {
    /*
    An accumulator's name is only for displaying purposes; you can have distinct accumulators with the
    same name and you can't look one up by name either.
     */
    sc.doubleAccumulator(s"$scope Sentinel Hub processing units")
  }

  def addSentinelHubProcessingUnits(amount: Double): Unit = {
    sentinelHubProcessingUnitsCounter.add(amount)
    logger.debug(s"ScopedMetadataTracker($scope) added $amount PUs")
  }

  def sentinelHubProcessingUnits: Double = {
    val amount = sentinelHubProcessingUnitsCounter.value
    logger.debug(s"ScopedMetadataTracker($scope) recorded $amount PUs")
    amount
  }

  override def toString: String = s"${getClass.getName}($scope)"
}
