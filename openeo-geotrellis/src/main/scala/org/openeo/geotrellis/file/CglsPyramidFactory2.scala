package org.openeo.geotrellis.file

import org.openeo.opensearch.OpenSearchClient
import geotrellis.raster.CellSize
import org.openeo.opensearch.backends.GlobalNetCDFSearchClient

import java.util

class CglsPyramidFactory2(dataGlob: String, dateRegex: String, netcdfVariables: util.List[String], maxSpatialResolution: CellSize) extends Sentinel2PyramidFactory("http://dummy.endpoint.com","",netcdfVariables, "", maxSpatialResolution: CellSize) {
  override def createOpenSearch: OpenSearchClient = {
    new GlobalNetCDFSearchClient(dataGlob,netcdfVariables, dateRegex.r.unanchored)
  }
}

