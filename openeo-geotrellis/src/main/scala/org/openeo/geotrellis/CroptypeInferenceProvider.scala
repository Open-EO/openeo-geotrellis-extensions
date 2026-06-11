package org.openeo.geotrellis

import org.openeo.geotrelliscommon.CubeProcessProvider

/**
 * SPI provider that registers [[CroptypeInference]] with [[org.openeo.geotrelliscommon.CubeProcessRegistry]].
 *
 * Declared in `META-INF/services/org.openeo.geotrelliscommon.CubeProcessProvider`
 * so it is discovered automatically by [[java.util.ServiceLoader]] — no explicit
 * registration call is needed.
 */
class CroptypeInferenceProvider extends CubeProcessProvider {
  def getInstance(): AnyRef = CroptypeInference
}
