package org.openeo.geotrellis.stac

import geotrellis.raster.CellType

import java.util

case class Asset(path: String, cellType: CellType, bandIndices: util.List[Int] = null, metadata: util.Map[String,Any] = null) {
  def dataType: String = ???
  def nodata: String = ???
}
