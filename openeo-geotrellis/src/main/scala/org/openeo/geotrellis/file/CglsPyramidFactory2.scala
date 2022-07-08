package org.openeo.geotrellis.file

import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchClient
import cats.data.NonEmptyList
import geotrellis.layer.FloatingLayoutScheme
import geotrellis.raster.CellSize
import org.openeo.geotrellis.layers.{GlobalNetCDFSearchClient, GlobalNetCdfFileLayerProvider, Sentinel5PPathDateExtractor}

import java.util

class CglsPyramidFactory2(dataGlob: String, dateRegex: String,netcdfVariables: util.List[String],  maxSpatialResolution: CellSize) extends Sentinel2PyramidFactory("http://dummy.endpoint.com","",netcdfVariables, "", maxSpatialResolution: CellSize) {
  override def createOpenSearch: OpenSearchClient = {
    new GlobalNetCDFSearchClient(dataGlob,netcdfVariables, dateRegex.r.unanchored)
  }
}

