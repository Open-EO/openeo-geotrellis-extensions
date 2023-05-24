package org.openeo.geotrellis.tile

import geotrellis.macros.{DoubleTileMapper, DoubleTileVisitor, IntTileMapper, IntTileVisitor}
import geotrellis.raster.crop.Crop
import geotrellis.raster.{ArrayTile, CellType, ConstantTile, GridBounds, MutableArrayTile, Tile}

object ResampledTile {
  def apply(tile: Tile, sourceBounds: GridBounds[Long], targetBounds: GridBounds[Long]): ResampledTile = {
    ResampledTile(tile, sourceBounds.width, sourceBounds.height, targetBounds.width, targetBounds.height)
  }
}

/**
 * ResampledTile allows e.g. low resolution arrays to be accessed as if they were of a higher resolution.
 * For example, the original tile can be in 20x20m per pixel, but we can access it as 10x10m per pixel.
 */
case class ResampledTile(tile: Tile, sourceCols: Long, sourceRows: Long, targetCols: Long, targetRows: Long) extends Tile {

  private val colsMultiplier: Double = targetCols.toDouble / sourceCols.toDouble
  private val rowsMultiplier: Double = targetRows.toDouble / sourceRows.toDouble

  def cropAndConvert(bounds: GridBounds[Int], targetCellType: CellType): ResampledTile = {
    val newSourceCols: Int = Math.floor(bounds.width / targetCols * sourceCols).toInt
    val newSourceRows: Int = Math.floor(bounds.height / targetRows * sourceRows).toInt
    val newSourceTile: Tile = tile.crop(newSourceCols, newSourceRows).convert(targetCellType)
    ResampledTile(newSourceTile, newSourceCols, newSourceRows, bounds.width, bounds.height)
  }

  private def withTile(tile: Tile): ResampledTile = ResampledTile(tile, sourceCols, sourceRows, targetCols, targetRows)

  override def withNoData(noDataValue: Option[Double]): Tile = withTile(tile.withNoData(noDataValue))

  override def toArrayTile(): ArrayTile = tile.toArrayTile()

  override def toBytes(): Array[Byte] = tile.toBytes()

  override def cellType: CellType = tile.cellType

  override def cols: Int = targetCols.toInt

  override def rows: Int = targetRows.toInt

  override def mutable: MutableArrayTile = tile.resample(cols, rows).mutable

  override def convert(cellType: CellType): Tile = withTile(tile.convert(cellType))

  override def interpretAs(newCellType: CellType): Tile = withTile(tile.interpretAs(newCellType))

  override def get(col: Int, row: Int): Int = tile.get((col / colsMultiplier).floor.toInt, (row / rowsMultiplier).floor.toInt)

  override def getDouble(col: Int, row: Int): Double = tile.getDouble((col / colsMultiplier).floor.toInt, (row / rowsMultiplier).floor.toInt)

  override def toArray(): Array[Int] = {
    val array = Array.ofDim[Int](cols * rows)
    var row = 0
    var i = 0 // Faster than calculating (row * cols) + col each iteration.
    while (row < rows) {
      var col = 0
      while (col < cols) {
        array(i) = get(col, row)
        i += 1
        col += 1
      }
      row += 1
    }
    array
  }

  override def toArrayDouble(): Array[Double] = {
    val array = Array.ofDim[Double](cols * rows)
    var row = 0
    var i = 0
    while (row < rows) {
      var col = 0
      while (col < cols) {
        array(i) = getDouble(col, row)
        i += 1
        col += 1
      }
      row += 1
    }
    array
  }

  override def foreach(f: Int => Unit): Unit = tile.foreach(f)

  override def foreachDouble(f: Double => Unit): Unit = tile.foreachDouble(f)

  override def map(f: Int => Int): Tile = tile.map(f)

  override def combine(r2: Tile)(f: (Int, Int) => Int): Tile = tile.combine(r2)(f)

  override def mapDouble(f: Double => Double): Tile = tile.mapDouble(f)

  override def combineDouble(r2: Tile)(f: (Double, Double) => Double): Tile = tile.combineDouble(r2)(f)

  override def mapIntMapper(mapper: IntTileMapper): Tile = tile.mapIntMapper(mapper)

  override def mapDoubleMapper(mapper: DoubleTileMapper): Tile = tile.mapDoubleMapper(mapper)

  override def foreachIntVisitor(visitor: IntTileVisitor): Unit = tile.foreachIntVisitor(visitor)

  override def foreachDoubleVisitor(visitor: DoubleTileVisitor): Unit = tile.foreachDoubleVisitor(visitor)

}