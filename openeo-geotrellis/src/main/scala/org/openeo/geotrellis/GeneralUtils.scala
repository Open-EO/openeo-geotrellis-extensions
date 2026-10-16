package org.openeo.geotrellis

import geotrellis.layer.{Boundable, Bounds, EmptyBounds, KeyBounds, LayoutDefinition, SpatialComponent, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.GridBounds
import geotrellis.raster.{BitCellType, BitCells, ByteCellType, ByteCells, ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, CellType, ConstantTile, DoubleCellType, DoubleCells, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatCellType, FloatCells, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntCellType, IntCells, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, NODATA, ShortCellType, ShortCells, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, Tile, TileLayout, UByteCellType, UByteCells, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortCellType, UShortCells, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType, byteNODATA, doubleNODATA, floatNODATA, isData, isNoData, shortNODATA, ubyteNODATA, ushortNODATA}
import geotrellis.vector.Extent
import org.slf4j.LoggerFactory
import java.util
import scala.reflect.{ClassTag, classTag}

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
    if(isUnSigned(a) != isUnSigned(b) ){
      if (a.bits == b.bits) {
        if (a.bits == 8) {
          ShortConstantNoDataCellType
        } else {
          IntConstantNoDataCellType
        }
      }
      else if (a.bits < b.bits) {
          if (isUnSigned(b)){
            upgradeCellTypes(b)
          }else b
      } else {
          if (isUnSigned(a)){
            upgradeCellTypes(a)
          }else a
      }
    }else {
      if (a.bits < b.bits)
        b
      else if (a.bits > b.bits)
        a
      else if (a.isFloatingPoint && !b.isFloatingPoint)
        a
      else
        b
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

  def cellTypeUnionWithNoData(leftCellType:CellType, rightCellType:CellType):CellType = {


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
      case x:BitCells => true
      case x:UByteCells => true
      case x:UShortCells => true
      case _ => false
    }
  }


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

  /**
   * Works around geotrellis issue.
   * https://github.com/locationtech/geotrellis/issues/3525
   */
  def safeConvert(tile: Tile,ct:CellType): Tile = {
    if(tile.isInstanceOf[ConstantTile] && tile.getDouble(0,0).isNaN ){
      EmptyMultibandTile.empty(ct, tile.cols, tile.rows)
    }else{
      tile.convert(ct)
    }
  }

  def extentMerged(extentLeft:Extent, extentRight:Extent, crsLeft: CRS, crsRight: CRS): Extent = {
    if (extentLeft == extentRight && crsLeft == crsRight) extentLeft
    else {
      val reprojectedExtentRight =
        if (crsLeft != crsRight) {
          extentRight.reproject(crsRight, crsLeft)
        } else extentRight
      extentLeft.combine(reprojectedExtentRight)
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

  /**
   * Translates key bounds expressed in `sourceLayout` (and `sourceCrs`) into the key numbering of `targetLayout`
   * (and `targetCrs`). Non-spatial components of the keys (e.g. time) are preserved.
   * The result is clamped to the target layout; EmptyBounds is returned if there is no overlap.
   */
  def boundsInLayout[K: SpatialComponent](bounds: Bounds[K], sourceLayout: LayoutDefinition, sourceCrs: CRS,
                                          targetLayout: LayoutDefinition, targetCrs: CRS): Bounds[K] = {
    bounds match {
      case EmptyBounds => EmptyBounds
      case kb: KeyBounds[K] =>
        if (sourceLayout == targetLayout && sourceCrs == targetCrs) kb
        else {
          val sourceExtent = sourceLayout.mapTransform(kb.toGridBounds())
          val reprojected =
            if (sourceCrs == targetCrs) sourceExtent
            else sourceExtent.reproject(sourceCrs, targetCrs)
          // shrink slightly to avoid picking up neighbouring tiles due to floating point errors on tile edges
          val dx = targetLayout.cellwidth / 4
          val dy = targetLayout.cellheight / 4
          val targetExtent =
            if (reprojected.width > 2 * dx && reprojected.height > 2 * dy)
              Extent(reprojected.xmin + dx, reprojected.ymin + dy, reprojected.xmax - dx, reprojected.ymax - dy)
            else reprojected
          val layoutGrid = GridBounds(0, 0, targetLayout.layoutCols - 1, targetLayout.layoutRows - 1)
          targetLayout.mapTransform.extentToBounds(targetExtent).intersection(layoutGrid) match {
            case Some(gb) =>
              kb.setSpatialBounds(KeyBounds(SpatialKey(gb.colMin, gb.rowMin), SpatialKey(gb.colMax, gb.rowMax)))
            case None => EmptyBounds
          }
        }
    }
  }

  /**
   * Retains only the spatial component of key bounds.
   */
  def toSpatialBounds[K: SpatialComponent](bounds: Bounds[K]): Bounds[SpatialKey] = {
    bounds match {
      case EmptyBounds => EmptyBounds
      case kb: KeyBounds[K] =>
        val spatial = implicitly[SpatialComponent[K]]
        KeyBounds(spatial.get(kb.minKey), spatial.get(kb.maxKey))
    }
  }

  /**
   * Metadata of the result of merge_cubes.
   *
   * Mergeable fields cover the union of both cubes:
   *  - layout and extent are combined,
   *  - spatial bounds are combined (expressed in the key numbering of the merged layout),
   *  - if both cubes have the same key type, the non-spatial bounds (e.g. time) are combined as well; otherwise they are
   *    taken from `left` (e.g. a spatial cube has no temporal bounds to contribute),
   *  - the cell type is the union of both cell types.
   *
   * Non-mergeable fields (crs, cell size, tile size, grid origin) are taken from the dominant cube: `left`, or `right` if
   * `rightIsDominant` is set.
   */
  def mergedMetadata[K: SpatialComponent: Boundable: ClassTag, R: SpatialComponent: ClassTag](
      left: TileLayerMetadata[K], right: TileLayerMetadata[R], rightIsDominant: Boolean = false): TileLayerMetadata[K] = {
    val (dominant, other) = if (rightIsDominant) (right, left) else (left, right)
    val crs = dominant.crs
    val layout = layoutMerged(dominant.layout, other.layout, dominant.crs, other.crs)
    val extent = extentMerged(dominant.extent, other.extent, dominant.crs, other.crs)

    val leftBounds = boundsInLayout(left.bounds, left.layout, left.crs, layout, crs)
    val rightBounds = boundsInLayout(right.bounds, right.layout, right.crs, layout, crs)
    val sameKeyType = classTag[K] == classTag[R]
    val bounds: Bounds[K] =
      if (sameKeyType) leftBounds.combine(rightBounds.asInstanceOf[Bounds[K]])
      else leftBounds match {
        case kb: KeyBounds[K] => toSpatialBounds(kb).combine(toSpatialBounds(rightBounds)) match {
          case spatial: KeyBounds[SpatialKey] => kb.setSpatialBounds(spatial)
          case EmptyBounds => kb
        }
        // without left bounds, the non-spatial components of the keys are unknown
        case EmptyBounds => EmptyBounds
      }

    TileLayerMetadata(cellTypeUnionWithNoData(left.cellType, right.cellType), layout, extent, crs, bounds)
  }

  private def layoutDefinitionMergeWithEqualCellSize(layoutLeft:LayoutDefinition, extentRight:Extent): LayoutDefinition = {
    val combinedExtent = extentRight.combine(layoutLeft.extent)
    val gridCombined = layoutLeft.mapTransform(combinedExtent)
    val extentFinal = layoutLeft.mapTransform(gridCombined)
    val tileLayout = TileLayout(gridCombined.width, gridCombined.height, layoutLeft.tileCols, layoutLeft.tileRows)
    LayoutDefinition(extentFinal, tileLayout)
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

  type Stats = (Double, Double, Double, Double, Int, Int) // min, max, sum, powerSum, validCount, totalCount

  def computeStatsTile(tile:Tile): Stats = {
    val (tempMin, tempMax, tempSum, tempPowerSum, tempValidCount, totalCount) = tile.cellType match {
      case _: FloatCells => statsDouble(tile)
      case _: DoubleCells => statsDouble(tile)
      case _: BitCells => statsInt(tile)
      case _: ByteCells => statsInt(tile)
      case _: UByteCells => statsInt(tile)
      case _: ShortCells => statsInt(tile)
      case _: UShortCells => statsInt(tile)
      case _: IntCells => statsInt(tile)
    }
    (tempMin, tempMax, tempSum, tempPowerSum, tempValidCount, totalCount)
  }

  def statsDouble(tile: Tile): Stats = {
    var zmin = Double.NaN
    var zmax = Double.NaN
    var sum = 0.0
    var powerSum = 0.0
    var validCount = 0
    var totalCount = 0
    val nodata = getNodataMaxMin(cellType = tile.cellType)._1
    tile.foreachDouble { z =>
      totalCount += 1
      if (isData(z) && (nodata.isEmpty || z != nodata.get)) {
        validCount+=1
        sum += z
        powerSum += Math.pow(z,2)
        if(isNoData(zmin)) {
          zmin = z
          zmax = z
        } else {
          zmin = math.min(zmin, z)
          zmax = math.max(zmax, z)
        }
      }
    }
    (zmin,zmax,sum,powerSum,validCount,totalCount)
  }
  
  def statsInt(tile:Tile): Stats = {
    var zmin = Int.MaxValue
    var zmax = Int.MinValue
    var sum = 0L
    var powerSum = 0.0
    var validCount = 0
    var totalCount = 0
    val nodata = getNodataMaxMin(cellType = tile.cellType)._1

    tile.foreach { z =>
      totalCount+= 1
      if (isData(z) && (nodata.isEmpty || z != nodata.get)) {
        validCount +=1
        zmin = math.min(zmin, z)
        zmax = math.max(zmax, z)
        sum += z
        powerSum += Math.pow(z,2)
      }
    }
    (zmin,zmax,sum.toDouble,powerSum,validCount,totalCount)
  }

  def combineStats(existingStats: Stats, curStats: Stats): Stats = {
    val (existingMin, existingMax, existingSum, existingPowerSum, existingValidCount, existingTotalCount) = existingStats
    if (existingTotalCount == 0) return curStats
    if (existingValidCount == 0) return curStats.copy(_6 = existingStats._6 + curStats._6)

    val (tempMin, tempMax, tempSum, tempPowerSum, tempValidCount, totalCount) = curStats
    if (totalCount == 0) return existingStats
    if (tempValidCount == 0) return existingStats.copy(_6 = existingStats._6 + curStats._6)

    val newMin = math.min(existingMin, tempMin)
    val newMax = math.max(existingMax, tempMax)
    val newSum = existingSum + tempSum
    val newPowerSum = existingPowerSum + tempPowerSum
    val newValidCount = existingValidCount + tempValidCount
    val newTotalCount = existingTotalCount + totalCount
    (newMin, newMax, newSum, newPowerSum, newValidCount, newTotalCount)
  }

  def convertStatsToMap(stats: Stats): util.HashMap[String,Any] = {
    val (min, max, sum, powerSum, validCount, totalCount) = stats
    if (validCount == 0) {
      new util.HashMap[String, Any](util.Map.of("valid_percent", 0.0))
    } else {
      val stddev = Math.sqrt(powerSum / validCount - Math.pow(sum / validCount, 2))
      new util.HashMap[String, Any](util.Map.of("maximum", max, "minimum", min, "mean", sum / validCount, "stddev", stddev, "valid_percent", validCount.toDouble / totalCount * 100))
    }
  }
}
