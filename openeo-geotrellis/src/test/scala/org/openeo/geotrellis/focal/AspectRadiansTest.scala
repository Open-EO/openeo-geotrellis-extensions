package org.openeo.geotrellis.focal

import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.{CellSize, DoubleArrayTile, IntArrayTile, IntCellType, IntCells, IntConstantTile}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AspectRadiansTest {

  @Test
  def flatSurface(): Unit = {
    val inTile = IntConstantTile(5, 100, 100, IntCells withNoData (None))
    val outTile = AspectRadians.apply(inTile, Square(1), None, CellSize(5, 5))
    assertEquals(Double.NaN, outTile.getDouble(10, 10))
  }

  @Test
  def west(): Unit = {
    val values: Array[Int] = Range.inclusive(0, 100*100).map(i => Math.floor(i / 100).toInt).toArray
    val inTile = IntArrayTile(values, 100, 100)
    val outTile = AspectRadians.apply(inTile, Square(1), None, CellSize(5, 5))
    assertEquals(Math.PI/2, outTile.getDouble(10, 10))
  }

  @Test
  def south(): Unit = {
    val values: Array[Int] = Range.inclusive(0, 100*100).map(i => Math.floor(i % 100).toInt).toArray
    val inTile = IntArrayTile(values, 100, 100)
    val outTile = AspectRadians.apply(inTile, Square(1), None, CellSize(5, 5))
    assertEquals(Math.PI, outTile.getDouble(10, 10))
  }


  @Test
  def east(): Unit = {
    val values: Array[Int] = Range.inclusive(0, 100*100).map(i => 100 - Math.floor(i / 100).toInt).toArray
    val inTile = IntArrayTile(values, 100, 100)

    val outTile = AspectRadians.apply(inTile, Square(1), None, CellSize(5, 5))
    assertEquals(3*Math.PI/2, outTile.getDouble(10, 10))
  }

  @Test
  def north(): Unit = {
    val values: Array[Int] = Range.inclusive(0, 100*100).map(i => 100 - Math.floor(i % 100).toInt).toArray
    val inTile = IntArrayTile(values, 100, 100)
    val outTile = AspectRadians.apply(inTile, Square(1), None, CellSize(5, 5))
    assertEquals(0, outTile.getDouble(10, 10))
  }
}
