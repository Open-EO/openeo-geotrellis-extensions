package org.openeo.geotrellis.aggregate_polygon.intern

import geotrellis.raster._

import scala.Double.NaN

case class MeanResult(sum: Double, valid: Long, total: Long, scale: Option[Double], offset: Option[Double]) {
  def mean: Double = if (valid == 0) {
    NaN
  } else {
    sum / valid
  }

  def meanPhysical: Double = (scale, offset) match {
    case (Some(s), Some(o)) => mean * s + o
    case _ => mean
  }

  def +(b: MeanResult) = MeanResult(
    sum + b.sum,
    valid + b.valid,
    total + b.total,
    scale.orElse(b.scale),
    offset.orElse(b.offset)
  )
}

object MeanResult {

  def apply(sum: Double, valid: Long, total: Long) = new MeanResult(sum,valid,total,Option.empty,Option.empty)
  def apply(scale: Double, offset: Double): MeanResult = MeanResult(0.0, 0L, 0L, Some(scale), Some(offset))

  def apply(): MeanResult = MeanResult(0.0, 0L, 0L, None, None)

  def fromFullTile(tile: Tile) = {
    var s = 0L
    var c = 0L
    var t = 0L
    tile.foreach((x: Int) => if (isData(x)) {
      s = s + x; c = c + 1; t += 1
    } else {
      t += 1
    })
    MeanResult(s, c, t, None, None)
  }

  def fromFullTileDouble(tile: Tile) = {
    var s = 0.0
    var c = 0L
    var t = 0L
    tile.foreachDouble((x: Double) => if (isData(x)) {
      s = s + x; c = c + 1; t += 1
    } else {
      t += 1
    })
    MeanResult(s, c, t, None, None)
  }
}


