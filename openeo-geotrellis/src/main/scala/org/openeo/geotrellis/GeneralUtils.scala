package org.openeo.geotrellis

import geotrellis.layer.LayoutDefinition
import geotrellis.proj4.CRS
import geotrellis.raster.{BitCellType, BitCells, ByteCellType, ByteCells, ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, CellType, DoubleCellType, DoubleCells, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatCellType, FloatCells, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntCellType, IntCells, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, NODATA, ShortCellType, ShortCells, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, TileLayout, UByteCellType, UByteCells, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortCellType, UShortCells, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType, byteNODATA, doubleNODATA, floatNODATA, shortNODATA, ubyteNODATA, ushortNODATA}

object GeneralUtils {

  def toSigned(cellType: CellType): CellType = {
    cellType match {
      case UByteCellType => ByteCellType
      case UByteConstantNoDataCellType => ByteConstantNoDataCellType
      case UByteUserDefinedNoDataCellType(noDataValue) => ByteUserDefinedNoDataCellType(noDataValue)
      case UShortCellType => ShortCellType
      case UShortConstantNoDataCellType => ShortConstantNoDataCellType
      case UShortUserDefinedNoDataCellType(noDataValue) => ShortUserDefinedNoDataCellType(noDataValue)
      case FloatConstantNoDataCellType => cellType
      case ShortConstantNoDataCellType => cellType
      case BitCellType => cellType
      case ByteConstantNoDataCellType => cellType
      case ByteCellType => cellType
      case ByteUserDefinedNoDataCellType(_) => cellType
      case ShortCellType => cellType
      case ShortUserDefinedNoDataCellType(_) => cellType
      case IntConstantNoDataCellType => cellType
      case IntCellType => cellType
      case IntUserDefinedNoDataCellType(_) => cellType
      case FloatCellType => cellType
      case FloatUserDefinedNoDataCellType(_) => cellType
      case DoubleConstantNoDataCellType => cellType
      case DoubleCellType => cellType
      case DoubleUserDefinedNoDataCellType(_) => cellType
      case _ => throw new IllegalArgumentException("Cannot convert to unsigned equivalent: '" + cellType.getClass.getName + "'.")
    }
  }

  def cellTypeUnion(a:CellType,b:CellType):CellType = {
    if (a.bits < b.bits)
      b
    else if (a.bits > b.bits)
      a
    else if (a.isFloatingPoint && !b.isFloatingPoint)
      a
    else if(isUnSigned(a) != isUnSigned(b) ) {
      if(a.bits==8) {
        ShortConstantNoDataCellType
      }else if(a.isFloatingPoint || b.isFloatingPoint){
        Seq(a,b).maxBy(_.bits)
      }else{
        IntConstantNoDataCellType
      }
    }
    else
      b
  }

  def cellTypeUnionWithNoData(leftCellType:CellType, rightCellType:CellType):CellType = {
    def getNodataMaxMin(cellType:CellType):(Option[Double],Double,Double) = {
      cellType match {
        case BitCellType => (None, 1,0)
        case ByteCellType => (None, Byte.MaxValue, Byte.MinValue)
        case UByteCellType => (None, 255, 0)
        case ShortCellType => (None, Short.MaxValue, Short.MinValue)
        case UShortCellType => (None, 65535, 0)
        case IntCellType => (None, Int.MaxValue, Int.MinValue)
        case FloatCellType => (None, Float.MaxValue, Float.MinValue)
        case DoubleCellType => (None, Double.MaxValue, Double.MinValue)
        case ByteConstantNoDataCellType => (Some(byteNODATA), Byte.MaxValue, Byte.MinValue)
        case UByteConstantNoDataCellType => (Some(ubyteNODATA), 255, 0)
        case ShortConstantNoDataCellType => (Some(shortNODATA), Short.MaxValue, Short.MinValue)
        case UShortConstantNoDataCellType => (Some(ushortNODATA), 65535, 0)
        case IntConstantNoDataCellType => (Some(NODATA), Int.MaxValue, Int.MinValue)
        case FloatConstantNoDataCellType => (Some(floatNODATA), Float.MaxValue, Float.MinValue)
        case DoubleConstantNoDataCellType => (Some(doubleNODATA), Double.MaxValue, Double.MinValue)
        case ct: ByteUserDefinedNoDataCellType => (Some(ct.noDataValue), Byte.MaxValue, Byte.MinValue)
        case ct: UByteUserDefinedNoDataCellType => (Some(ct.widenedNoData.asInt), 255, 0)
        case ct: ShortUserDefinedNoDataCellType => (Some(ct.noDataValue), Short.MaxValue, Short.MinValue)
        case ct: UShortUserDefinedNoDataCellType => (Some(ct.widenedNoData.asInt), 65535, 0)
        case ct: IntUserDefinedNoDataCellType => (Some(ct.noDataValue), Int.MaxValue, Int.MinValue)
        case ct: FloatUserDefinedNoDataCellType => (Some(ct.noDataValue), Float.MaxValue, Float.MinValue)
        case ct: DoubleUserDefinedNoDataCellType => (Some(ct.noDataValue), Double.MaxValue, Double.MinValue)
      }
    }

    def upgradeCellTypes(dataType: CellType): CellType = {
      dataType match {
        case _: BitCells => ByteConstantNoDataCellType
        case _: ByteCells => ShortUserDefinedNoDataCellType(Short.MaxValue)
        case _: UByteCells => ShortUserDefinedNoDataCellType(Short.MaxValue)
        case _: ShortCells => IntUserDefinedNoDataCellType(Int.MaxValue)
        case _: UShortCells => IntUserDefinedNoDataCellType(Int.MaxValue)
        case _: IntCells => FloatConstantNoDataCellType
        case _: FloatCells => DoubleConstantNoDataCellType
        case _: DoubleCells => DoubleConstantNoDataCellType
      }
    }

    val dataType = cellTypeUnion(leftCellType,rightCellType)
    val (maybeNodataLeft, maxLeft, minLeft) = getNodataMaxMin(leftCellType)
    val (maybeNodataRight, maxRight, minRight) = getNodataMaxMin(rightCellType)

    if (maybeNodataLeft.isEmpty || maybeNodataRight.isEmpty){
      if (maybeNodataLeft.isDefined) {
        val nodataLeft = maybeNodataLeft.get
        if (maxRight < nodataLeft) {
          dataType.withNoData(Some(nodataLeft))
        }
        else if (minRight > nodataLeft){
          dataType.withNoData(Some(nodataLeft))
        }
        else if (nodataLeft.isNaN){
          dataType.withDefaultNoData()
        }
        else if (isUnSigned(leftCellType) || isUnSigned(rightCellType)) {
          if (leftCellType.bits >= rightCellType.bits){
            upgradeCellTypes(leftCellType)
          }else {
            upgradeCellTypes(rightCellType)
          }
        } else {
          upgradeCellTypes(dataType)
        }
      } else if (maybeNodataRight.isDefined) {
        val nodataRight = maybeNodataRight.get
        if (maxLeft < nodataRight) {
          dataType.withNoData(Some(nodataRight))
        }
        else if (minLeft > nodataRight){
          dataType.withNoData(Some(nodataRight))
        }
        else if (nodataRight.isNaN){
          dataType.withDefaultNoData()
        }
        else {
          if (isUnSigned(leftCellType) || isUnSigned(rightCellType)) {
            if (leftCellType.bits >= rightCellType.bits){
              upgradeCellTypes(leftCellType)
            }else {
              upgradeCellTypes(rightCellType)
            }
          } else {
            upgradeCellTypes(dataType)
          }
        }
      } else dataType
    }
    else {
      val nodataLeft = maybeNodataLeft.get
      val nodataRight = maybeNodataRight.get

      if (nodataLeft.isNaN || nodataRight.isNaN) {
        if (dataType.isInstanceOf[FloatCells]) {
          FloatConstantNoDataCellType
        } else {
          DoubleConstantNoDataCellType
        }
      }
      else {
        if (nodataLeft == nodataRight) {
          dataType.withNoData(maybeNodataLeft)
        } else if (nodataLeft > nodataRight) {
          if (maxRight < nodataLeft) {
            dataType.withNoData(Some(nodataLeft))
          }
          else if (minLeft > nodataRight ){
            dataType.withNoData(Some(nodataRight))
          } else {
            if (isUnSigned(leftCellType) || isUnSigned(rightCellType)) {
              if (leftCellType.bits >= rightCellType.bits){
                upgradeCellTypes(leftCellType)
              }else {
                upgradeCellTypes(rightCellType)
              }
            } else {
              upgradeCellTypes(dataType)
            }
          }
        } else { // nodataRight > nodataLeft
          if (maxLeft < nodataRight) {
            dataType.withNoData(Some(nodataRight))
          }
          else if (minRight > nodataLeft ){
            dataType.withNoData(Some(nodataLeft))
          } else {
            if (isUnSigned(leftCellType) || isUnSigned(rightCellType)) {
              if (leftCellType.bits >= rightCellType.bits){
                upgradeCellTypes(leftCellType)
              }else {
                upgradeCellTypes(rightCellType)
              }
            } else {
              upgradeCellTypes(dataType)
            }
          }
        }
      }
    }
  }

  private def isUnSigned(a:CellType): Boolean = {
    a match{
      case x:UByteCells => true
      case x:UShortCells => true
      case _ => false
    }
  }

  def layoutMerged(layoutLeft:LayoutDefinition, layoutRight:LayoutDefinition, crsLeft: CRS, crsRight: CRS): LayoutDefinition = {
    if (layoutLeft == layoutRight & crsLeft == crsRight) layoutLeft
    else {
      val reprojectedLayoutRight = layoutRight.extent.reproject(crsRight, crsLeft)
      val combinedExtent = reprojectedLayoutRight.combine(layoutLeft.extent)
      val mappedLayout = layoutLeft.mapTransform.apply(combinedExtent)
      val tileLayout = TileLayout(mappedLayout.width, mappedLayout.height, layoutLeft.tileCols, layoutLeft.tileRows)
      LayoutDefinition(combinedExtent, tileLayout)
    }
  }

}
