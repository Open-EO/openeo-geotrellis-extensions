package org.openeo.geotrellis.focal

import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.{CellSize, IntArrayTile, IntCells, IntConstantTile}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SlopeRadiansTest {

  @Test
  def flatSurface(): Unit = {
    val inTile = IntConstantTile(5, 100, 100, IntCells withNoData (None))
    val outTile = SlopeRadians.apply(inTile, Square(1), None, CellSize(5, 5), 5)
    assertEquals(0, outTile.getDouble(10, 10))
  }

  @Test
  def west45degrees(): Unit = {
    val values: Array[Int] = Range.inclusive(0, 100*100).map(i => Math.floor(i / 100).toInt).toArray
    val inTile = IntArrayTile(values, 100, 100)
    val outTile = SlopeRadians.apply(inTile, Square(1), None, CellSize(5, 5), 5)
    assertEquals(Math.PI/4, outTile.getDouble(10, 10))
  }

  @Test
  def west60degrees(): Unit = {
    val values: Array[Int] = Range.inclusive(0, 100*100).map(i => Math.floor(i / 100).toInt).toArray
    val inTile = IntArrayTile(values, 100, 100)
    val outTile = SlopeRadians.apply(inTile, Square(1), None, CellSize(5, 5), 5*Math.tan(Math.PI/3))
    assertEquals(Math.PI/3, outTile.getDouble(10, 10))
  }

  @Test
  def east45degrees(): Unit = {
    val values: Array[Int] = Range.inclusive(0, 100*100).map(i => 100 - Math.floor(i / 100).toInt).toArray
    val inTile = IntArrayTile(values, 100, 100)
    val outTile = SlopeRadians.apply(inTile, Square(1), None, CellSize(5, 5), 5)
    assertEquals(Math.PI/4, outTile.getDouble(10, 10))
  }

  @Test
  def east60degrees(): Unit = {
    val values: Array[Int] = Range.inclusive(0, 100*100).map(i => 100 - Math.floor(i / 100).toInt).toArray
    val inTile = IntArrayTile(values, 100, 100)
    val outTile = SlopeRadians.apply(inTile, Square(1), None, CellSize(5, 5), 5*Math.tan(Math.PI/3))
    assertEquals(Math.PI/3, outTile.getDouble(10, 10))
  }

}
