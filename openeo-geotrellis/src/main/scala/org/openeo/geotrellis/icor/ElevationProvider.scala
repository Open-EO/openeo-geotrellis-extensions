package org.openeo.geotrellis.icor

import geotrellis.raster.Tile
import geotrellis.layer.LayoutDefinition
import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.CRS


/**
 * Base interface that all elevetion-type providers should implement
 * Compute has to return a float tile with values in km
 */
abstract class ElevationProvider {

  def compute(key:SpaceTimeKey, targetCRS:CRS,layoutDefinition:LayoutDefinition): Tile

}
