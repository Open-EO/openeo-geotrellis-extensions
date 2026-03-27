package org.openeo.geotrellis.layers.provider

import geotrellis.raster.RasterSource

trait RasterSourceProvider {

  def canProcess(rasterSourceDefinition: RasterSourceDefinition): Boolean

  def rasterSource(rasterSourceDefinition: RasterSourceDefinition): RasterSource
}
