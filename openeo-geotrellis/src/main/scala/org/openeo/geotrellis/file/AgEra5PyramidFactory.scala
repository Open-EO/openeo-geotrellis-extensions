package org.openeo.geotrellis.file

import org.openeo.geotrellis.layers.AgEra5FileLayerProvider

import java.util
import scala.collection.JavaConverters._

class AgEra5PyramidFactory(dataGlob: String, bandFileMarkers: util.List[String], dateRegex: String)
  extends GlobPyramidFactory(new AgEra5FileLayerProvider(dataGlob, bandFileMarkers.asScala, dateRegex.r))
