package org.openeo.geotrellishealpix

import scala.collection.immutable.{Map, Seq}
import scala.jdk.CollectionConverters._

/** * Scala utilities for JSON-based configuration loading. * Provides conversions from Java collections to Scala collections. */
object ConfigJsonLoader {

  /**   * Convert a Java Map<String, List<String>> to a Scala immutable Map[String, Seq[String]].   *   * @param javaMap the Java map to convert   * @return a Scala immutable Map   */
  def javaMapToScalaMap(
                         javaMap: java.util.Map[String, java.util.List[String]]
                       ): Map[String, Seq[String]] = {
    javaMap.asScala
      .view
      .mapValues(_.asScala.toList)
      .toMap
  }
}
