package org.openeo.geotrellis.mapalgebra

import geotrellis.raster.isNoData
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp

/**
 * Gets minimum values.
 *
 * @note   Nodata will be ignored
 */
object MinIgnoreNodata extends LocalTileBinaryOp {
  def combine(z1:Int,z2:Int) =
    if (isNoData(z1)) z2
    else if ( isNoData(z2)) z1
    else math.min(z1,z2)

  def combine(z1:Double,z2:Double) =
    if (isNoData(z1)) z2
    else if ( isNoData(z2)) z1
    else math.min(z1,z2)
}

