package org.openeo.geotrellis.croptype

import org.openeo.geotrelliscommon.CubeProcessProvider

class CroptypeInferenceProvider extends CubeProcessProvider {
  def getInstance(): AnyRef = CroptypeInference
}

