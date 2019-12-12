package org.openeo.geotrellisseeder

import java.io.File
import java.lang.System.lineSeparator

import be.vito.eodata.biopar.EOProduct
import geotrellis.layer.SpatialKey
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

trait Logger[T] extends Serializable {
  def cls: Class[T]
  def logger: org.slf4j.Logger = LoggerFactory.getLogger(cls)
  def logKey(key: SpatialKey) {}
  def logTile(key: SpatialKey, path: String) {}
  def logNoDataTile(key: SpatialKey) {}
  def logProducts(products: Iterable[_ <: EOProduct]) {
    val files = products.flatMap(_.getFiles.asScala.map(f => new File(f.getFilename.getPath).getName))
    logger.info(s"Using products -> $lineSeparator${files.toStream.sorted.mkString(lineSeparator)}")
  }
}

case class StandardLogger[T](cls: Class[T]) extends Logger[T]

case class VerboseLogger[T](cls: Class[T]) extends Logger[T] {
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
