package org.openeo.geotrellis.file

import geotrellis.raster.CellSize
import org.openeo.opensearch.backends.Agera5SearchClient
import org.openeo.opensearch.OpenSearchClient

import java.util

class AgEra5PyramidFactory2(dataGlob: String, bandFileMarkers: util.List[String], dateRegex: String, maxSpatialResolution: CellSize) extends Sentinel2PyramidFactory("http://dummy.endpoint.com","", bandFileMarkers, "", maxSpatialResolution: CellSize) {
  override def createOpenSearch: OpenSearchClient = {
    new Agera5SearchClient(dataGlob, bandFileMarkers, dateRegex.r.unanchored)
  }
}

