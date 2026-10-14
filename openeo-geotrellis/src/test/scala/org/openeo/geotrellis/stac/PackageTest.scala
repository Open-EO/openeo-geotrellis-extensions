package org.openeo.geotrellis.stac

import geotrellis.raster._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.stream.{Stream => JStream}

object PackageTest {
  def mapsCellTypesToStacDataTypesParams: JStream[Arguments] = JStream.of(
    arguments(BitCellType, "uint8"),
    arguments(ByteConstantNoDataCellType, "int8"),
    arguments(ByteCellType, "int8"),
    arguments(ByteUserDefinedNoDataCellType(3), "int8"),
    arguments(UByteConstantNoDataCellType, "uint8"),
    arguments(UByteCellType, "uint8"),
    arguments(UByteUserDefinedNoDataCellType(3), "uint8"),
    arguments(ShortConstantNoDataCellType, "int16"),
    arguments(ShortCellType, "int16"),
    arguments(ShortUserDefinedNoDataCellType(3), "int16"),
    arguments(UShortConstantNoDataCellType, "uint16"),
    arguments(UShortCellType, "uint16"),
    arguments(UShortUserDefinedNoDataCellType(3), "uint16"),
    arguments(IntConstantNoDataCellType, "int32"),
    arguments(IntCellType, "int32"),
    arguments(IntUserDefinedNoDataCellType(3), "int32"),
    arguments(FloatConstantNoDataCellType, "float32"),
    arguments(FloatCellType, "float32"),
    arguments(FloatUserDefinedNoDataCellType(3.0f), "float32"),
    arguments(DoubleConstantNoDataCellType, "float64"),
    arguments(DoubleCellType, "float64"),
    arguments(DoubleUserDefinedNoDataCellType(3.0), "float64")
  )
}

class PackageTest {
  @ParameterizedTest
  @MethodSource(Array("mapsCellTypesToStacDataTypesParams"))
  def mapsCellTypesToStacDataTypes(cellType: CellType, expected: String): Unit = {
    assertEquals(expected, Asset("asset", cellType).dataType)
  }

  @Test
  def exposesNodataOnlyForCellTypesThatDefineIt(): Unit = {
    assertEquals(ByteConstantNoDataCellType.noDataValue, Asset("asset", ByteConstantNoDataCellType).nodata)
    assertEquals(3.toByte, Asset("asset", ByteUserDefinedNoDataCellType(3)).nodata)
    assertEquals(255, Asset("asset", UByteUserDefinedNoDataCellType(-1)).nodata)
    assertEquals(65535, Asset("asset", UShortUserDefinedNoDataCellType(-1)).nodata)
    assertTrue(Asset("asset", FloatConstantNoDataCellType).nodata.floatValue().isNaN)
    assertTrue(Asset("asset", DoubleConstantNoDataCellType).nodata.doubleValue().isNaN)
    assertNull(Asset("asset", ByteCellType).nodata)
    assertNull(Asset("asset", BitCellType).nodata)
  }
}
