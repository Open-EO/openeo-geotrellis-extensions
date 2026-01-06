package org.openeo.geotrellis

import geotrellis.raster.CellType.constantNoDataCellTypes
import geotrellis.raster.{CellType, DoubleCellType, FloatCellType, FloatUserDefinedNoDataCellType, IntUserDefinedNoDataCellType, MultibandTile, NODATA, UByteUserDefinedNoDataCellType, UShortUserDefinedNoDataCellType}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.stream.{Stream => JStream}
import scala.jdk.CollectionConverters._

object EmptyMultibandTileTest {

  def data: JStream[Arguments] = JStream.concat(JStream.of(
    Arguments.of(UShortUserDefinedNoDataCellType(11)),
    Arguments.of(IntUserDefinedNoDataCellType(12)),
    Arguments.of(UByteUserDefinedNoDataCellType(12)),
    Arguments.of(FloatUserDefinedNoDataCellType(12)),
    Arguments.of(DoubleCellType),
    Arguments.of(FloatCellType)), constantNoDataCellTypes.toList.asJava.stream().map(c => Arguments.of(c))
  )
}



class EmptyMultibandTileTest() {

  @ParameterizedTest
  @MethodSource(value = Array("data"))
  def testCreateEmpty(ct: CellType): Unit = {
    val tile = EmptyMultibandTile.empty(ct, 10, 10)
    assertEquals(NODATA, tile.get(0, 0))
  }

  @ParameterizedTest
  @MethodSource(value = Array("data"))
  def testCreate(ct: CellType): Unit = {
    val emptyMultibandTile: MultibandTile = new EmptyMultibandTile(10, 10, ct, 3)
    assertEquals(emptyMultibandTile.bandCount, 3)
    assertEquals(emptyMultibandTile.bands.size, 3)
    for (band <- emptyMultibandTile.bands) {
      // Test the corners:
      assertEquals(NODATA, band.get(0, 0))
      assertEquals(NODATA, band.get(0, band.rows - 1))
      assertEquals(NODATA, band.get(band.cols - 1, 0))
      assertEquals(NODATA, band.get(band.cols - 1, band.rows - 1))
    }
  }
}
