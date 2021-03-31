package org.openeo.geotrelliscommon

import java.util
import java.util.Collections

class DataCubeParameters {
  var tileSize: Int = 256
  var maskingStrategyParameters: util.Map[String, Object] = Collections.emptyMap()
  var layoutScheme: String = "ZoomedLayoutScheme"

  override def toString = s"DataCubeParameters($tileSize, $maskingStrategyParameters, $layoutScheme)"
}
