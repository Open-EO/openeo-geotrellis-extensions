package org.openeo.geotrellis

import geotrellis.layer.LayoutDefinition
import geotrellis.proj4.CRS
import geotrellis.raster.{BitCellType, BitCells, BitConstantTile, ByteCellType, ByteCells, ByteConstantNoDataCellType, ByteConstantTile, ByteUserDefinedNoDataCellType, CellType, ConstantTile, DoubleCellType, DoubleCells, DoubleConstantNoDataCellType, DoubleConstantTile, DoubleUserDefinedNoDataCellType, FloatCellType, FloatCells, FloatConstantNoDataCellType, FloatConstantTile, FloatUserDefinedNoDataCellType, IntCellType, IntCells, IntConstantNoDataCellType, IntConstantTile, IntUserDefinedNoDataCellType, NODATA, ShortCellType, ShortCells, ShortConstantNoDataCellType, ShortConstantTile, ShortUserDefinedNoDataCellType, Tile, TileLayout, UByteCellType, UByteCells, UByteConstantNoDataCellType, UByteConstantTile, UByteUserDefinedNoDataCellType, UShortCellType, UShortCells, UShortConstantNoDataCellType, UShortConstantTile, UShortUserDefinedNoDataCellType, byteNODATA, d2f, doubleNODATA, floatNODATA, i2b, i2s, i2us, shortNODATA, ubyteNODATA, ushortNODATA}
import geotrellis.vector.Extent
import org.slf4j.LoggerFactory

object GeneralUtils {

  val logger = LoggerFactory.getLogger(GeneralUtils.getClass)

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
    def getNodataAndMax(cellType:CellType):(Option[Double],Double) = {
      cellType match {
        case BitCellType => (None, 1)
        case ByteCellType => (None, Byte.MaxValue)
        case UByteCellType => (None, 255)
        case ShortCellType => (None, Short.MaxValue)
        case UShortCellType => (None, 65535)
        case IntCellType => (None, Int.MaxValue)
        case FloatCellType => (None, Float.MaxValue)
        case DoubleCellType => (None, Double.MaxValue)
        case ByteConstantNoDataCellType => (Some(byteNODATA), Byte.MaxValue)
        case UByteConstantNoDataCellType => (Some(ubyteNODATA), 255)
        case ShortConstantNoDataCellType => (Some(shortNODATA), Short.MaxValue)
        case UShortConstantNoDataCellType => (Some(ushortNODATA), 65535)
        case IntConstantNoDataCellType => (Some(NODATA), Int.MaxValue)
        case FloatConstantNoDataCellType => (Some(floatNODATA), Float.MaxValue)
        case DoubleConstantNoDataCellType => (Some(doubleNODATA), Double.MaxValue)
        case ct: ByteUserDefinedNoDataCellType => (Some(ct.noDataValue), Byte.MaxValue)
        case ct: UByteUserDefinedNoDataCellType => (Some(ct.widenedNoData.asInt), 255)
        case ct: ShortUserDefinedNoDataCellType => (Some(ct.noDataValue), Short.MaxValue)
        case ct: UShortUserDefinedNoDataCellType => (Some(ct.widenedNoData.asInt), 65535)
        case ct: IntUserDefinedNoDataCellType => (Some(ct.noDataValue), Int.MaxValue)
        case ct: FloatUserDefinedNoDataCellType => (Some(ct.noDataValue), Float.MaxValue)
        case ct: DoubleUserDefinedNoDataCellType => (Some(ct.noDataValue), Double.MaxValue)
      }
    }

    def upgradeCellTypes(dataType: CellType): CellType = {
      dataType match {
        case _: BitCells => ByteConstantNoDataCellType
        case _: ByteCells => ShortUserDefinedNoDataCellType(Short.MaxValue)
        case _: UByteCells => ShortUserDefinedNoDataCellType(Short.MaxValue)
        case _: ShortCells => IntUserDefinedNoDataCellType(Int.MaxValue)
        case _: UShortCells => IntUserDefinedNoDataCellType(Int.MaxValue)
        case _: IntCells => DoubleConstantNoDataCellType
        case _: FloatCells => DoubleConstantNoDataCellType
        case _: DoubleCells => DoubleConstantNoDataCellType
      }
    }

    val dataType = cellTypeUnion(leftCellType,rightCellType)
    val (maybeNodataLeft,maxLeft) = getNodataAndMax(leftCellType)
    val (maybeNodataRight,maxRight) = getNodataAndMax(rightCellType)

    if (maybeNodataLeft.isEmpty || maybeNodataRight.isEmpty){
      if (maybeNodataLeft.isDefined) {
        val nodataLeft = maybeNodataLeft.get
        if (maxRight < nodataLeft) {
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


  def saveConvertTile(tile:Tile, newType: CellType): Tile = {
    if (tile.isInstanceOf[ConstantTile]){
      val constantTile = tile.asInstanceOf[ConstantTile]
      if (constantTile.isNoDataTile) {
        newType match {
          case BitCellType => BitConstantTile(0, tile.cols, tile.rows)
          case ByteCellType => ByteConstantTile(byteNODATA, tile.cols, tile.rows)
          case UByteCellType => UByteConstantTile(ubyteNODATA, tile.cols, tile.rows)
          case ShortCellType => ShortConstantTile(shortNODATA, tile.cols, tile.rows)
          case UShortCellType => UShortConstantTile(ushortNODATA, tile.cols, tile.rows)
          case IntCellType => IntConstantTile(NODATA, tile.cols, tile.rows)
          case FloatCellType => FloatConstantTile(floatNODATA, tile.cols, tile.rows)
          case DoubleCellType => DoubleConstantTile(doubleNODATA, tile.cols, tile.rows)
          case ByteConstantNoDataCellType => ByteConstantTile(byteNODATA, tile.cols, tile.rows)
          case UByteConstantNoDataCellType => UByteConstantTile(ubyteNODATA, tile.cols, tile.rows)
          case ShortConstantNoDataCellType => ShortConstantTile(shortNODATA, tile.cols, tile.rows)
          case UShortConstantNoDataCellType => UShortConstantTile(ushortNODATA, tile.cols, tile.rows)
          case IntConstantNoDataCellType => IntConstantTile(NODATA, tile.cols, tile.rows)
          case FloatConstantNoDataCellType => FloatConstantTile(floatNODATA, tile.cols, tile.rows)
          case DoubleConstantNoDataCellType => DoubleConstantTile(doubleNODATA, tile.cols, tile.rows)
          case ct: ByteUserDefinedNoDataCellType => ByteConstantTile(ct.noDataValue, tile.cols, tile.rows, ct)
          case ct: UByteUserDefinedNoDataCellType => UByteConstantTile(ct.noDataValue, tile.cols, tile.rows, ct)
          case ct: ShortUserDefinedNoDataCellType => ShortConstantTile(ct.noDataValue, tile.cols, tile.rows, ct)
          case ct: UShortUserDefinedNoDataCellType => UShortConstantTile(ct.noDataValue, tile.cols, tile.rows, ct)
          case ct: IntUserDefinedNoDataCellType => IntConstantTile(ct.noDataValue, tile.cols, tile.rows, ct)
          case ct: FloatUserDefinedNoDataCellType => FloatConstantTile(ct.noDataValue, tile.cols, tile.rows, ct)
          case ct: DoubleUserDefinedNoDataCellType => DoubleConstantTile(ct.noDataValue, tile.cols, tile.rows, ct)
          case _ => throw new IllegalArgumentException("Cannot convert to unsigned equivalent: '" + newType.getClass.getName + "'.")
        }
      }
      else {
        constantTile.convert(newType)
      }
    }
    else {
      tile.convert(newType)
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

  private def crossesAntimeridian(bbox: Extent): Boolean = {
    bbox.xmin < -180 || bbox.xmax > 180
  }

  def fixBboxLargerThanWorld(bbox: Extent): Extent = {
    if (crossesAntimeridian(bbox)) {
      if (bbox.width>360) {
        cropXDimensionBboxLargerThanWorld(bbox)
      } else {
        bbox
      }
    } else {
      bbox
    }
  }
  // returns an extent that is cropped to the world extent if the input extent is larger than the world extent
  // the bbox has to be CRS EPSG:4326
  def cropXDimensionBboxLargerThanWorld(bbox: Extent): Extent = {
    Extent(
      math.max(bbox.xmin, -180),
      bbox.ymin,
      math.min(bbox.xmax, 180),
      bbox.ymax
    )
  }

}
