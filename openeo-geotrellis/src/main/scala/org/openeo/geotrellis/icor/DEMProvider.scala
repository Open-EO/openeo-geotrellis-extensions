package org.openeo.geotrellis.icor

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.raster.geotiff.GeoTiffRasterSource

class DEMProvider(layout:LayoutDefinition, crs:CRS,path:String="https://artifactory.vgt.vito.be/auxdata-public/DEM/DEM_Globe_CS.tif") {
  val rasterSource = GeoTiffRasterSource(path).reprojectToRegion(crs,layout.toRasterExtent())

  def computeDEM(key:SpaceTimeKey, targetCRS:CRS,layoutDefinition:LayoutDefinition): Tile = {
    val targetExtent = layoutDefinition.mapTransform.apply(key)
    val raster = rasterSource.read(targetExtent)
    raster.get.tile.band(0)
  }

}
