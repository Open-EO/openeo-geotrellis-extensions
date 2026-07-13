package org.openeo.geotrellis

import geotrellis.layer.LayoutDefinition
import geotrellis.proj4.CRS
import geotrellis.raster.{BitCellType, BitCells, ByteCellType, ByteCells, ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, CellType, DoubleCellType, DoubleCells, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatCellType, FloatCells, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntCellType, IntCells, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, NODATA, ShortCellType, ShortCells, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, TileLayout, UByteCellType, UByteCells, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortCellType, UShortCells, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType, byteNODATA, doubleNODATA, floatNODATA, shortNODATA, ubyteNODATA, ushortNODATA}
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

  def layoutMerged(layoutLeft:LayoutDefinition, layoutRight:LayoutDefinition, crsLeft: CRS, crsRight: CRS): LayoutDefinition = {
    if (layoutLeft == layoutRight & crsLeft == crsRight) layoutLeft
    else {
      val reprojectedExtentRight = layoutRight.extent.reproject(crsRight, crsLeft)
      val result = layoutDefinitionMergeWithEqualCellSize(layoutLeft, reprojectedExtentRight)
      logger.info(s"layoutMerged: layoutLeft=$layoutLeft, layoutRight=$layoutRight, crsLeft=$crsLeft, crsRight=$crsRight, result layout =$result")
      result
    }
  }

  private def layoutDefinitionMergeWithEqualCellSize(layoutLeft:LayoutDefinition, extentRight:Extent): LayoutDefinition = {
    val combinedExtent = extentRight.extent.combine(layoutLeft.extent)
    
    val ratioWidth = combinedExtent.width / layoutLeft.extent.width
    val ratioTileWidth = ratioWidth*layoutLeft.layoutCols
    val newLayoutCols = Math.ceil(ratioTileWidth)
    val xMax = if (math.abs(ratioTileWidth - math.round(ratioTileWidth)) > 1e-6){
      combinedExtent.xmin + newLayoutCols/layoutLeft.layoutCols * layoutLeft.extent.width
    } else combinedExtent.xmax

    val ratioHeight = combinedExtent.height / layoutLeft.extent.height
    val ratioTileHeight = ratioHeight*layoutLeft.layoutRows
    val newLayoutRows = Math.ceil(ratioTileHeight)
    val yMax = if (math.abs(ratioTileHeight - math.round(ratioTileHeight)) > 1e-6){
      combinedExtent.ymin + newLayoutRows/layoutLeft.layoutRows * layoutLeft.extent.height
    } else combinedExtent.ymax


    val tileLayout = TileLayout(newLayoutCols.toInt, newLayoutRows.toInt, layoutLeft.tileCols, layoutLeft.tileRows)
    LayoutDefinition(Extent(combinedExtent.xmin,combinedExtent.ymin,xMax, yMax), tileLayout)
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
