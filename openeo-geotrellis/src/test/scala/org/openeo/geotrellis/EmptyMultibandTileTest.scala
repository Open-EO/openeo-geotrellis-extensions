package org.openeo.geotrellis

import geotrellis.raster.CellType.constantNoDataCellTypes
import geotrellis.raster._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.scalatest.Assertions.assertThrows

import scala.collection.JavaConverters._

object EmptyMultibandTileTest {
  def cellTypeParams: java.lang.Iterable[Array[CellType]] = {
    val list = new java.util.ArrayList[Array[CellType]]()
    list.add(Array[CellType](UShortUserDefinedNoDataCellType(11)))
    list.add(Array[CellType](IntUserDefinedNoDataCellType(12)))
    list.add(Array[CellType](UByteUserDefinedNoDataCellType(12)))
    list.add(Array[CellType](FloatUserDefinedNoDataCellType(12)))

    list.addAll(constantNoDataCellTypes.map(c => Array[CellType](c)).asJavaCollection)
    list
  }

  def noNoDataCellTypeParams: java.lang.Iterable[Array[CellType]] = {
    // Based on geotrellis.raster.CellType.noNoDataCellTypes
    val noNoDataCellTypes = Seq(
      // BitCellType, // Has special case.
      ByteCellType,
      UByteCellType,
      ShortCellType,
      UShortCellType,
      IntCellType,
      FloatCellType,
      DoubleCellType
    )

    val list = new java.util.ArrayList[Array[CellType]]()
    list.addAll(noNoDataCellTypes.map(c => Array[CellType](c)).asJavaCollection)
    list
  }
}

class EmptyMultibandTileTest() {

  @ParameterizedTest
  @MethodSource(Array("cellTypeParams"))
  def testCreateEmpty(ct: CellType): Unit = {
    val tile = EmptyMultibandTile.empty(ct, 10, 10)
    assertTrue(tile.isNoDataTile)
  }

  @ParameterizedTest
  @MethodSource(Array("noNoDataCellTypeParams"))
  def testCreateEmptyNoNoData(ct: CellType): Unit = {
    assertThrows[IllegalArgumentException](EmptyMultibandTile.empty(ct, 10, 10))
  }

  @ParameterizedTest
  @MethodSource(Array("cellTypeParams"))
  def testCreate(ct: CellType): Unit = {
    val emptyMultibandTile: MultibandTile = new EmptyMultibandTile(10, 10, ct, 3)
    assertEquals(emptyMultibandTile.bandCount, 3)
    assertEquals(emptyMultibandTile.bands.size, 3)
    for (band <- emptyMultibandTile.bands) {
      assertTrue(band.isNoDataTile)
    }
  }
}
