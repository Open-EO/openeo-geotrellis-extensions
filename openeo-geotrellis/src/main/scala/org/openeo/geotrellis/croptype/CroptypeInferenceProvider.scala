package org.openeo.geotrellis.croptype

import org.openeo.geotrelliscommon.CubeProcessProvider

class CroptypeInferenceProvider extends CubeProcessProvider {
  def getInstance(): AnyRef = CroptypeInference
}

class PrestoInferenceProvider extends CubeProcessProvider {
  def getInstance(): AnyRef = PrestoInference
}
