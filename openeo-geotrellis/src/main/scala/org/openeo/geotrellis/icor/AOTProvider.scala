package org.openeo.geotrellis.icor

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.raster.geotiff.GeoTiffRasterSource

class AOTProvider(basePath:String = "/data/MEP/ECMWF/cams/aod550/%1$tY/%1$tm/CAMS_NRT_aod550_%1$tY%1$tm%1$tdT120000Z.tif") {

  def computeAOT(key:SpaceTimeKey, targetCRS:CRS,layoutDefinition:LayoutDefinition): Tile = {
    val aotPath = basePath.format(key.time)
    val rasterSource = GeoTiffRasterSource(aotPath)

    val targetExtent = layoutDefinition.mapTransform.apply(key)
    val raster = rasterSource.reprojectToRegion(targetCRS,layoutDefinition.toRasterExtent()).read(targetExtent)
    raster.get.tile.band(0)

  }

}
