package org.openeo.geotrellis.icor

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.raster.geotiff.GeoTiffRasterSource

class DEMProvider(path:String="/data/MEP/DEM/DEM_NOAA_Globe/DEM_Globe_CS.tif") {

  def computeDEM(key:SpaceTimeKey, targetCRS:CRS,layoutDefinition:LayoutDefinition): Tile = {
    val targetExtent = layoutDefinition.mapTransform.apply(key)
    val rasterSource = GeoTiffRasterSource(path)
    val raster = rasterSource.reprojectToRegion(targetCRS,layoutDefinition.toRasterExtent()).read(targetExtent)
    raster.get.tile.band(0)
  }

}
