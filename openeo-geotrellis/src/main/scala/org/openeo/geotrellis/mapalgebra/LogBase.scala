package org.openeo.geotrellis.mapalgebra

import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.{NODATA, isNoData}

object LogBase extends LocalTileBinaryOp {
  def combine(x: Int, base: Int) =
    if (isNoData(x) || isNoData(base)) NODATA
    else if (base == 0) NODATA
    else math.log(x).toInt / math.log(base).toInt

  def combine(x: Double, base: Double) =
    math.log(x) / math.log(base)
}