package org.openeo.geotrellis.focal

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal.DoubleArrayTileResult
import geotrellis.raster.mapalgebra.focal.hillshade.{SurfacePoint, SurfacePointCalculation}


/** Calculates the aspect of each cell in a raster.
 *
 * Aspect is the direction component of a gradient vector. It is the
 * direction in degrees of which direction the maximum change in direction is pointing.
 * It is defined as the directional component of the gradient vector and is the
 * direction of maximum gradient of the surface at a given point. It uses Horn's method
 * for computing aspect.
 *
 * As with slope, aspect is calculated from estimates of the partial derivatives dz / dx and dz / dy.
 *
 * If Aspect operations encounters NoData in its neighborhood, that neighborhood cell well be treated as having
 * the same elevation as the focal cell.
 *
 * Aspect is computed in radians from due north.
 * The expression for aspect is:
 * {{{
 * val aspect = atan2(`dz / dy`, `dz / dx`) - 90
 * }}}
 *
 */
object AspectRadians {

  def apply(tile: Tile, n: Neighborhood, bounds: Option[GridBounds[Int]], cs: CellSize, target: TargetCell = TargetCell.All): Tile = {
    new SurfacePointCalculation[Tile](tile, n, bounds, cs, target)
      with DoubleArrayTileResult
    {
      def setValue(x: Int, y: Int, s: SurfacePoint): Unit = {
        resultTile.setDouble(x, y, s.aspect())
      }
    }
  }.execute()
}