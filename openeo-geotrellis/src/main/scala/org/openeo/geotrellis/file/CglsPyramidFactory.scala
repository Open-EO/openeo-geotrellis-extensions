package org.openeo.geotrellis.file

import org.openeo.geotrellis.layers.GlobalNetCdfFileLayerProvider

class CglsPyramidFactory(dataGlob: String, bandName: String, dateRegex: String)
  extends GlobPyramidFactory(new GlobalNetCdfFileLayerProvider(dataGlob, bandName, dateRegex.r))
