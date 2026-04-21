package org.openeo.geotrellis.mapalgebra

import geotrellis.raster.{NODATA, doubleNODATA, isNoData}
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp

/**
 * Operation to compute modulus (remainder) of two values.
 *
 * @note          If either argument is NoData, the result will be NoData.
 */
object Modulo extends LocalTileBinaryOp {
  def combine(z1:Int,z2:Int) =
    if (isNoData(z1) || isNoData(z2)) NODATA
    else z1 % z2

  def combine(z1:Double,z2:Double) =
    if (isNoData(z1) || isNoData(z2)) doubleNODATA
    else z1 % z2
}

