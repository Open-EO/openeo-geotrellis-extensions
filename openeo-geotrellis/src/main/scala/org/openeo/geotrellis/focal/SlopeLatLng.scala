package org.openeo.geotrellis.focal

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey, SpatialComponent, SpatialKey}
import geotrellis.raster.{CellSize, GridBounds, Tile, ZFactor}
import geotrellis.raster.mapalgebra.focal.Angles.degrees
import geotrellis.raster.mapalgebra.focal.{DoubleArrayTileResult, Neighborhood, TargetCell}
import geotrellis.raster.mapalgebra.focal.hillshade.{SurfacePoint, SurfacePointCalculation}
import geotrellis.vector.Extent
import squants.space.Meters

object SlopeLatLng {

  def apply(layoutDefinition: LayoutDefinition, key: SpatialKey, r: Tile, n: Neighborhood, bounds: Option[GridBounds[Int]], cs: CellSize, target: TargetCell = TargetCell.All): Tile = {
    new SurfacePointCalculation[Tile](r, n, bounds, cs, target)
      with DoubleArrayTileResult
    {
      private val extent: Extent = layoutDefinition.mapTransform.keyToExtent(key)
      val zFactor = ZFactor.forLatLng(Meters).fromExtent(extent)

      def setValue(x: Int, y: Int, s: SurfacePoint): Unit = {
        resultTile.setDouble(x, y, degrees(s.slope(zFactor)))
      }
    }
  }.execute()
}