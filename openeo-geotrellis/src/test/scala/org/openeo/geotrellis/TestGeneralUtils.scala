package org.openeo.geotrellis

import geotrellis.raster.{BitCellType, ByteCellType, ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, DoubleCellType, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatCellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, ShortCellType, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, UByteCellType, UByteUserDefinedNoDataCellType, UShortCellType, UShortUserDefinedNoDataCellType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrowsExactly, assertTrue}
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.GeneralUtils.{cellTypeUnion, cellTypeUnionWithNoData, toSigned}

class TestGeneralUtils {

  @Test
  def testToSigned(): Unit = {
    assertEquals(ByteCellType, toSigned(UByteCellType))
    assertEquals(ByteUserDefinedNoDataCellType(42), toSigned(UByteUserDefinedNoDataCellType(42)))
    assertEquals(FloatUserDefinedNoDataCellType(42), toSigned(FloatUserDefinedNoDataCellType(42)))
    assertEquals(ByteUserDefinedNoDataCellType(42), toSigned(ByteUserDefinedNoDataCellType(42)))
  }


  @Test
  def testCellTypeUnion(): Unit = {
    assertEquals(BitCellType, cellTypeUnion(BitCellType, BitCellType))
    assertEquals(ByteCellType, cellTypeUnion(BitCellType, ByteCellType))
    assertEquals(DoubleCellType, cellTypeUnion(BitCellType, DoubleCellType))

    assertEquals(ByteCellType, cellTypeUnion(ByteCellType, BitCellType))
    assertEquals(ByteCellType, cellTypeUnion(ByteCellType, ByteCellType))
    assertEquals(ShortConstantNoDataCellType, cellTypeUnion(ByteCellType, UByteCellType))
    assertEquals(ShortCellType, cellTypeUnion(ByteCellType, ShortCellType))
    assertEquals(FloatCellType, cellTypeUnion(ByteCellType, FloatCellType))

    assertEquals(UByteCellType, cellTypeUnion(UByteCellType, BitCellType))
    assertEquals(ShortConstantNoDataCellType, cellTypeUnion(UByteCellType, ByteCellType))
    assertEquals(UByteCellType, cellTypeUnion(UByteCellType, UByteCellType))
    assertEquals(UShortCellType, cellTypeUnion(UByteCellType, UShortCellType))
    assertEquals(DoubleCellType, cellTypeUnion(UByteCellType, DoubleCellType))

    assertEquals(ShortCellType, cellTypeUnion(ShortCellType, UByteCellType))
    assertEquals(ShortCellType, cellTypeUnion(ShortCellType, ShortCellType))
    assertEquals(IntConstantNoDataCellType, cellTypeUnion(ShortCellType, UShortCellType))
    assertEquals(DoubleCellType, cellTypeUnion(ShortCellType, DoubleCellType))

    assertEquals(UShortCellType, cellTypeUnion(UShortCellType, BitCellType))
    assertEquals(IntConstantNoDataCellType, cellTypeUnion(UShortCellType, ShortCellType))
    assertEquals(UShortCellType, cellTypeUnion(UShortCellType, UShortCellType))
    assertEquals(IntCellType, cellTypeUnion(UShortCellType, IntCellType))

    assertEquals(IntCellType, cellTypeUnion(IntCellType, ByteCellType))
    assertEquals(IntCellType, cellTypeUnion(IntCellType, IntCellType))
    assertEquals(FloatCellType, cellTypeUnion(IntCellType, FloatCellType))

    assertEquals(FloatCellType, cellTypeUnion(FloatCellType, IntCellType))
    assertEquals(FloatCellType, cellTypeUnion(FloatCellType, FloatCellType))
    assertEquals(DoubleCellType, cellTypeUnion(FloatCellType, DoubleCellType))

    assertEquals(DoubleCellType, cellTypeUnion(DoubleCellType, UByteCellType))
    assertEquals(DoubleCellType, cellTypeUnion(DoubleCellType, ShortCellType))
    assertEquals(DoubleCellType, cellTypeUnion(DoubleCellType, FloatCellType))
    assertEquals(DoubleCellType, cellTypeUnion(DoubleCellType, DoubleCellType))
  }

  @Test
  def testCellTypeUnionWithNoData(): Unit = {
    // both without nodata
    assertEquals(ShortConstantNoDataCellType, cellTypeUnionWithNoData(ByteCellType, UByteCellType))
    assertEquals(IntCellType, cellTypeUnionWithNoData(ShortCellType, IntCellType))
    assertEquals(DoubleCellType, cellTypeUnionWithNoData(FloatCellType, DoubleCellType))

    // only left side has nodata value
    assertEquals(ShortUserDefinedNoDataCellType(350), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(350), ByteCellType))
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(FloatConstantNoDataCellType, ByteCellType))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(ByteConstantNoDataCellType, ByteCellType))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(9), ByteCellType))
    assertEquals(DoubleConstantNoDataCellType, cellTypeUnionWithNoData(FloatUserDefinedNoDataCellType(2.5f), IntCellType))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UShortUserDefinedNoDataCellType(10), ByteCellType))
    assertEquals(UShortUserDefinedNoDataCellType(200), cellTypeUnionWithNoData(UShortUserDefinedNoDataCellType(200), ByteCellType))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(UByteUserDefinedNoDataCellType(10), UByteCellType))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UByteUserDefinedNoDataCellType(10), UShortCellType))

    // only right side has nodata value
    assertEquals(ShortUserDefinedNoDataCellType(350), cellTypeUnionWithNoData(ByteCellType, ShortUserDefinedNoDataCellType(350)))
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(ByteCellType, FloatConstantNoDataCellType))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(ByteCellType, ByteConstantNoDataCellType))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ByteCellType, ShortUserDefinedNoDataCellType(9)))
    assertEquals(DoubleConstantNoDataCellType, cellTypeUnionWithNoData(IntCellType, FloatUserDefinedNoDataCellType(2.5f)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ByteCellType, UShortUserDefinedNoDataCellType(10)))
    assertEquals(UShortUserDefinedNoDataCellType(200), cellTypeUnionWithNoData(ByteCellType, UShortUserDefinedNoDataCellType(200)))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(UByteCellType, UByteUserDefinedNoDataCellType(10)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UShortCellType, UByteUserDefinedNoDataCellType(10)))

    // both have nodata, equal nodata
    assertEquals(ByteUserDefinedNoDataCellType(7), cellTypeUnionWithNoData(ByteUserDefinedNoDataCellType(7), ByteUserDefinedNoDataCellType(7)))
    assertEquals(ShortUserDefinedNoDataCellType(7), cellTypeUnionWithNoData(UByteUserDefinedNoDataCellType(7), ByteUserDefinedNoDataCellType(7)))
    assertEquals(ShortUserDefinedNoDataCellType(7), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(7), UByteUserDefinedNoDataCellType(7)))
    assertEquals(IntConstantNoDataCellType, cellTypeUnionWithNoData(IntConstantNoDataCellType, IntUserDefinedNoDataCellType(Int.MinValue)))
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(FloatConstantNoDataCellType, FloatConstantNoDataCellType))

    // both have nodata, one is NaN
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType(3.0f)))
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(FloatUserDefinedNoDataCellType(3.0f), FloatConstantNoDataCellType))
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(FloatConstantNoDataCellType, ByteUserDefinedNoDataCellType(3)))
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(ByteUserDefinedNoDataCellType(3), FloatConstantNoDataCellType))
    assertEquals(DoubleConstantNoDataCellType, cellTypeUnionWithNoData(IntConstantNoDataCellType, DoubleConstantNoDataCellType))
    assertEquals(DoubleConstantNoDataCellType, cellTypeUnionWithNoData(DoubleUserDefinedNoDataCellType(658), DoubleConstantNoDataCellType))
    assertEquals(DoubleConstantNoDataCellType, cellTypeUnionWithNoData(DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType(658)))

    // both have nodata, and nodataLeft > nodataRight
    assertEquals(ShortUserDefinedNoDataCellType(280), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(280),ByteUserDefinedNoDataCellType(5)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UShortUserDefinedNoDataCellType(10), ShortUserDefinedNoDataCellType(5)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(100), ShortUserDefinedNoDataCellType(5)))
    assertEquals(DoubleUserDefinedNoDataCellType(1e40), cellTypeUnionWithNoData(DoubleUserDefinedNoDataCellType(1e40), FloatUserDefinedNoDataCellType(3.0f)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UShortUserDefinedNoDataCellType(10), ShortUserDefinedNoDataCellType(5)))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(UByteUserDefinedNoDataCellType(256.toByte), ByteUserDefinedNoDataCellType(5)))

    // both have nodata,  and nodataLeft < nodataRight
    assertEquals(ShortUserDefinedNoDataCellType(280), cellTypeUnionWithNoData(ByteUserDefinedNoDataCellType(5), ShortUserDefinedNoDataCellType(280)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(5), UShortUserDefinedNoDataCellType(10)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(5), ShortUserDefinedNoDataCellType(100)))
    assertEquals(DoubleUserDefinedNoDataCellType(1e40), cellTypeUnionWithNoData(FloatUserDefinedNoDataCellType(3.0f), DoubleUserDefinedNoDataCellType(1e40)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(5), UShortUserDefinedNoDataCellType(10)))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(ByteUserDefinedNoDataCellType(5), UByteUserDefinedNoDataCellType(256.toByte)))

  }


}
