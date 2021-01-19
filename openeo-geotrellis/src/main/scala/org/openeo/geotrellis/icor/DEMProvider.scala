package org.openeo.geotrellis.icor

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.CRS
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.{FloatConstantNoDataCellType, Tile}

/**
 * Provides DEM values, in kilometers, which is what icor requires.
 *
 * @param layout
 * @param crs
 * @param path
 */
class DEMProvider(layout:LayoutDefinition, crs:CRS,path:String="https://artifactory.vgt.vito.be/auxdata-public/DEM/DEM_Globe_CS.tif") extends ElevationProvider {
  val rasterSource = GeoTiffRasterSource(path).reprojectToRegion(crs,layout.toRasterExtent())

  def compute(key:SpaceTimeKey, targetCRS:CRS,layoutDefinition:LayoutDefinition): Tile = {
    val targetExtent = layoutDefinition.mapTransform.apply(key)
    val raster = rasterSource.read(targetExtent)
    raster.get.tile.band(0).convert(FloatConstantNoDataCellType).localDivide(1000.0)
  }

}
