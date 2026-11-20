package org.openeo.geotrellis

import geotrellis.raster._

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

package object stac {
  // TODO: "cloud-optimize" is a typo, but existing consumers/tests match on it; fix together with them.
  val GTIFF_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimize"
  val NETCDF_MEDIA_TYPE = "application/x-netcdf"
  val PNG_MEDIA_TYPE = "image/png"
  val ZARR_MEDIA_TYPE = "application/x-zarr"


  def formatDate(date: ZonedDateTime): String = DateTimeFormatter.ISO_INSTANT.format(date.toInstant)

  def dataType(cellType: CellType): String = cellType match {
    case _: BitCells | _: UByteCells => "uint8"
    case _: ByteCells => "int8"
    case _: UShortCells => "uint16"
    case _: ShortCells => "int16"
    case _: IntCells => "int32"
    case _: FloatCells => "float32"
    case _: DoubleCells => "float64"
    case _ => throw new IllegalArgumentException(s"Unsupported GeoTrellis cell type: $cellType")
  }

  def nodata(cellType: CellType): java.lang.Number = cellType match {
    case ByteConstantNoDataCellType => java.lang.Byte.valueOf(ByteConstantNoDataCellType.noDataValue)
    case ct: ByteUserDefinedNoDataCellType => java.lang.Byte.valueOf(ct.noDataValue)
    case UByteConstantNoDataCellType => java.lang.Integer.valueOf(0)
    case ct: UByteUserDefinedNoDataCellType => java.lang.Integer.valueOf(ct.noDataValue & 0xff)
    case ShortConstantNoDataCellType => java.lang.Short.valueOf(ShortConstantNoDataCellType.noDataValue)
    case ct: ShortUserDefinedNoDataCellType => java.lang.Short.valueOf(ct.noDataValue)
    case UShortConstantNoDataCellType => java.lang.Integer.valueOf(0)
    case ct: UShortUserDefinedNoDataCellType => java.lang.Integer.valueOf(ct.noDataValue & 0xffff)
    case IntConstantNoDataCellType => java.lang.Integer.valueOf(IntConstantNoDataCellType.noDataValue)
    case ct: IntUserDefinedNoDataCellType => java.lang.Integer.valueOf(ct.noDataValue)
    case FloatConstantNoDataCellType => java.lang.Float.valueOf(FloatConstantNoDataCellType.noDataValue)
    case ct: FloatUserDefinedNoDataCellType => java.lang.Float.valueOf(ct.noDataValue)
    case DoubleConstantNoDataCellType => java.lang.Double.valueOf(DoubleConstantNoDataCellType.noDataValue)
    case ct: DoubleUserDefinedNoDataCellType => java.lang.Double.valueOf(ct.noDataValue)
    case BitCellType | ByteCellType | UByteCellType | ShortCellType | UShortCellType |
         IntCellType | FloatCellType | DoubleCellType => null
    case _ => throw new IllegalArgumentException(s"Unsupported GeoTrellis cell type: $cellType")
  }
}
