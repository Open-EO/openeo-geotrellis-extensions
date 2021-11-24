package org.openeo.geotrellissentinelhub

import geotrellis.raster.{CellType, FloatUserDefinedNoDataCellType, UShortConstantNoDataCellType}

object SampleType extends Enumeration {
  type SampleType = Value

  private[geotrellissentinelhub] case class Val(cellType: CellType) extends super.Val
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val UINT16: SampleType = Val(UShortConstantNoDataCellType)
  val FLOAT32: SampleType = Val(FloatUserDefinedNoDataCellType(0))
}
