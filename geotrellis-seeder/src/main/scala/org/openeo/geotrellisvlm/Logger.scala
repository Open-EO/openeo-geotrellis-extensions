package org.openeo.geotrellisvlm

import geotrellis.spark.SpatialKey
import org.slf4j.LoggerFactory

trait Logger {
  def logKey(key: SpatialKey) {}
  def logTile(key: SpatialKey, path: String) {}
  def logNoDataTile(key: SpatialKey) {}
}

class EmptyLogger extends Logger

class VerboseLogger extends Logger {
  private val logger = LoggerFactory.getLogger(classOf[VerboseLogger])
  
  override def logKey(key: SpatialKey) {
    logger.info(s"Treatment of $key")
  }
  
  override def logTile(key: SpatialKey, path: String) {
    logger.info(s"Writing tile with $key to $path")
  }

  override def logNoDataTile(key: SpatialKey) {
    logger.info(s"Not writing tile with $key because it has no data")
  }
}
