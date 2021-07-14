package org.openeo.geotrellis

import java.util

import geotrellis.raster.CellType.constantNoDataCellTypes
import geotrellis.raster.{CellType, FloatUserDefinedNoDataCellType, IntUserDefinedNoDataCellType, NODATA, UByteUserDefinedNoDataCellType, UShortUserDefinedNoDataCellType}
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._

object EmptyMultibandTileTest {
  @Parameters(name = "CellType: {0}") def data: java.lang.Iterable[Array[CellType]] = {
    val list = new util.ArrayList[Array[CellType]]()
    list.add(Array[CellType](UShortUserDefinedNoDataCellType(11)))
    list.add(Array[CellType](IntUserDefinedNoDataCellType(12)))
    list.add(Array[CellType](UByteUserDefinedNoDataCellType(12)))
    list.add(Array[CellType](FloatUserDefinedNoDataCellType(12)))

    list.addAll(constantNoDataCellTypes.map(c => Array[CellType](c)).asJavaCollection)
    list
  }
}

@RunWith(classOf[Parameterized])
class EmptyMultibandTileTest(ct:CellType) {

  //@Parameterized.Parameter(value = 0) var ct: CellType = IntConstantNoDataCellType



  @Test
  def testCreateEmpty(): Unit = {
    val tile = EmptyMultibandTile.empty(ct, 10, 10)
    assertEquals(NODATA,tile.get(0,0))
  }

}
