package org.openeo.geotrellis

import geotrellis.raster._

object EmptyMultibandTile {

  def empty(t: CellType, cols: Int, rows: Int): Tile = {
    t match {
      case _: BitCells => BitConstantTile(false, cols, rows)
      case ct: ByteUserDefinedNoDataCellType => ByteConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: ByteConstantNoDataCellType.type => ByteConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: UByteUserDefinedNoDataCellType => UByteConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: UByteConstantNoDataCellType.type => UByteConstantTile(ct.noDataValue, cols, rows, ct)

      case ct: ShortUserDefinedNoDataCellType => ShortConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: ShortConstantNoDataCellType.type => ShortConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: UShortUserDefinedNoDataCellType => UShortConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: UShortConstantNoDataCellType.type => UShortConstantTile(ct.noDataValue, cols, rows, ct)

      case ct: IntUserDefinedNoDataCellType => IntConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: IntConstantNoDataCellType.type => IntConstantTile(ct.noDataValue, cols, rows, ct)
      // No uint32

      case ct: FloatUserDefinedNoDataCellType => FloatConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: FloatConstantNoDataCellType.type => FloatConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: DoubleUserDefinedNoDataCellType => DoubleConstantTile(ct.noDataValue, cols, rows, ct)
      case ct: DoubleConstantNoDataCellType.type => DoubleConstantTile(ct.noDataValue, cols, rows, ct)
      case _: NoNoData =>
        throw new IllegalArgumentException("Need NoDataHandling for empty tile. CellType: " + t)
      case _ =>
        throw new IllegalArgumentException("Unhandled CellType: " + t)
    }
  }

}

class EmptyMultibandTile(val cols:Int, val rows:Int, val cellType:CellType, val bandCount:Int = 0 ) extends MultibandTile with MacroMultibandCombiners {

  override def band(bandIndex: Int): Tile = EmptyMultibandTile.empty(cellType, cols, rows)

  override def bands: Vector[Tile] = Vector.fill(bandCount)(EmptyMultibandTile.empty(cellType, cols, rows))

  override def subsetBands(bandSequence: Seq[Int]): MultibandTile = new EmptyMultibandTile(cols,rows,cellType,bandSequence.size)

  override def convert(newCellType: CellType): MultibandTile = new EmptyMultibandTile(cols,rows,newCellType,bandCount)

  override def withNoData(noDataValue: Option[Double]): MultibandTile = new EmptyMultibandTile(cols,rows,cellType.withNoData(noDataValue ),bandCount)

  override def interpretAs(newCellType: CellType): MultibandTile = new EmptyMultibandTile(cols,rows,newCellType,bandCount)

  override def map(subset: Seq[Int])(f: (Int, Int) => Int): MultibandTile = this

  override def mapDouble(subset: Seq[Int])(f: (Int, Double) => Double): MultibandTile = this

  override def map(f: (Int, Int) => Int): MultibandTile = this

  override def mapDouble(f: (Int, Double) => Double): MultibandTile = this

  override def map(b0: Int)(f: Int => Int): MultibandTile = this

  override def mapDouble(b0: Int)(f: Double => Double): MultibandTile = this

  override def foreach(f: (Int, Int) => Unit): Unit = f(0,NODATA)

  override def foreachDouble(f: (Int, Double) => Unit): Unit = f(0,Double.NaN)

  override def foreach(b0: Int)(f: Int => Unit): Unit = f(NODATA)

  override def foreachDouble(b0: Int)(f: Double => Unit): Unit = f(Double.NaN)

  override def foreach(f: Array[Int] => Unit): Unit = {
    f(Array[Int](NODATA))
  }

  override def foreachDouble(f: Array[Double] => Unit): Unit = f(Array[Double](Double.NaN))

  override def combine(subset: Seq[Int])(f: Seq[Int] => Int): Tile = ???

  override def combineDouble(subset: Seq[Int])(f: Seq[Double] => Double): Tile = ???

  override def combine(f: Array[Int] => Int): Tile = ???

  override def combine(b0: Int, b1: Int)(f: (Int, Int) => Int): Tile = ???

  override def combineDouble(f: Array[Double] => Double): Tile = ???

  override def combineDouble(b0: Int, b1: Int)(f: (Double, Double) => Double): Tile = ???

  override def toArrayTile(): ArrayMultibandTile = ???
}
