package org.openeo.geotrellis

import geotrellis.raster.{BitCellType, ByteArrayTile, ByteCellType, ByteConstantNoDataCellType, ByteConstantTile, ByteUserDefinedNoDataArrayTile, ByteUserDefinedNoDataCellType, CellType, DoubleCellType, DoubleConstantNoDataCellType, DoubleConstantTile, DoubleUserDefinedNoDataArrayTile, DoubleUserDefinedNoDataCellType, FloatCellType, FloatConstantNoDataCellType, FloatConstantTile, FloatUserDefinedNoDataArrayTile, FloatUserDefinedNoDataCellType, IntArrayTile, IntCellType, IntConstantNoDataCellType, IntConstantTile, IntUserDefinedNoDataArrayTile, IntUserDefinedNoDataCellType, ShortArrayTile, ShortCellType, ShortConstantNoDataCellType, ShortConstantTile, ShortUserDefinedNoDataArrayTile, ShortUserDefinedNoDataCellType, Tile, UByteArrayTile, UByteCellType, UByteConstantNoDataCellType, UByteConstantTile, UByteUserDefinedNoDataArrayTile, UByteUserDefinedNoDataCellType, UShortArrayTile, UShortCellType, UShortConstantNoDataCellType, UShortConstantTile, UShortUserDefinedNoDataArrayTile, UShortUserDefinedNoDataCellType}
import org.junit.jupiter.api.Assertions.assertEquals
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


  @Test
  def testSaveConvert(): Unit = {
    val rows = 8
    val cols = 8
    val tileSize = rows * cols

    def getArrayInt(tile: Tile): Array[Int] = {
      tile match {
        case b: ByteArrayTile => b.array.map(_.toInt)
        case ub: UByteArrayTile => ub.array.map(_.toInt)
        case s: ShortArrayTile => s.array.map(_.toInt)
        case us: UShortArrayTile => us.array.map(_.toInt)
        case i: IntArrayTile => i.array
        case c: ByteConstantTile => Array.fill[Int](c.cols * c.rows)(c.v)
        case c: UByteConstantTile => Array.fill[Int](c.cols * c.rows)(c.v)
        case c: ShortConstantTile => Array.fill[Int](c.cols * c.rows)(c.v)
        case c: UShortConstantTile => Array.fill[Int](c.cols * c.rows)(c.v)
        case c: IntConstantTile => Array.fill[Int](c.cols * c.rows)(c.v)
      }
    }

    def checkValuesInt(array: Tile, newCellType: CellType, noData: Option[(Int,Int)] = None): Unit = {
      val result = GeneralUtils.safeConvert(array, newCellType)
      assertEquals(newCellType, result.cellType)
      assertEquals(array.isNoDataTile, result.isNoDataTile)


      for (col <- 0 until array.cols; row <- 0 until array.rows) {
        val originalValue = getArrayInt(array).apply(col + row * array.cols)
        val convertedValue = getArrayInt(result).apply(col + row * array.cols)
        noData match {
          case Some((noDataOriginal,noDataNew)) if originalValue == noDataOriginal =>
            assertEquals(noDataNew, convertedValue, s"Expected NoData for cell ($col, $row), but got $convertedValue")
          case Some(_) =>
            assertEquals(originalValue, convertedValue, s"Expected $originalValue for cell ($col, $row), but got $convertedValue")
          case None =>
            assertEquals(originalValue, convertedValue, s"Expected $originalValue for cell ($col, $row), but got $convertedValue")
        }
      }
    }


    def getArrayDouble(tile: Tile): Array[Double] = {
      tile match {
        case f: FloatUserDefinedNoDataArrayTile => f.array.map(_.toDouble)
        case d: DoubleUserDefinedNoDataArrayTile => d.array
        case f: FloatConstantTile => Array.fill[Double](f.cols * f.rows)(f.v)
        case d: DoubleConstantTile => Array.fill[Double](d.cols * d.rows)(d.v)
      }
    }
    def checkValuesDouble(array: Tile, newCellType: CellType, noData: Option[(Double, Double)] = None): Unit = {
      val result = GeneralUtils.safeConvert(array, newCellType)
      assertEquals(newCellType, result.cellType)
      assertEquals(array.isNoDataTile, result.isNoDataTile)


      for (col <- 0 until array.cols; row <- 0 until array.rows) {
        val originalValue = getArrayDouble(array).apply(col + row * array.cols)
        val convertedValue = getArrayDouble(result).apply(col + row * array.cols)
        noData match {
          case Some((noDataOriginal, noDataNew)) if originalValue == noDataOriginal =>
            assertEquals(noDataNew, convertedValue, s"Expected NoData for cell ($col, $row), but got $convertedValue")
          case Some(_) =>
            assertEquals(originalValue, convertedValue, s"Expected $originalValue for cell ($col, $row), but got $convertedValue")
          case None =>
            assertEquals(originalValue, convertedValue, s"Expected $originalValue for cell ($col, $row), but got $convertedValue")
        }
      }
    }

    val arrByte = Array.concat(Array.fill[Byte](tileSize/2)(0), Array.fill[Byte](tileSize/2)(5))
    val arrayTileByteUD5 = ByteUserDefinedNoDataArrayTile(arrByte, cols, rows, ByteUserDefinedNoDataCellType(5))
    checkValuesInt(arrayTileByteUD5, ByteUserDefinedNoDataCellType(10), Some(5.toByte, 10.toByte))

    val arrUByte = Array.concat(Array.fill[Byte](tileSize/2)(0), Array.fill[Byte](tileSize/2)(5))
    val arrayTileUByteUD5 = UByteUserDefinedNoDataArrayTile(arrUByte, cols, rows, UByteUserDefinedNoDataCellType(5))
    checkValuesInt(arrayTileUByteUD5, UByteUserDefinedNoDataCellType(10), Some(5.toByte, 10.toByte))

    val arrShort = Array.concat(Array.fill[Short](tileSize/2)(0), Array.fill[Short](tileSize/2)(5))
    val arrayTileShortUD5 = ShortUserDefinedNoDataArrayTile(arrShort, cols, rows, ShortUserDefinedNoDataCellType(5))
    checkValuesInt(arrayTileShortUD5, ShortUserDefinedNoDataCellType(10), Some(5.toShort, 10.toShort))

    val arrInt = Array.concat(Array.fill[Int](32)(0), Array.fill[Int](32)(5))
    val arrayTileIntUD5 = IntUserDefinedNoDataArrayTile(arrInt, 8, 8, IntUserDefinedNoDataCellType(5))
    checkValuesInt(arrayTileIntUD5, IntUserDefinedNoDataCellType(10), Some(5.toShort, 10.toShort))

    val arrFloat = Array.concat(Array.fill[Float](32)(0.0f), Array.fill[Float](32)(5.0f))
    val arrayTileFloatUD5 = FloatUserDefinedNoDataArrayTile(arrFloat, 8, 8, FloatUserDefinedNoDataCellType(5.0f))
    checkValuesDouble(arrayTileFloatUD5, FloatUserDefinedNoDataCellType(10.0f), Some(5.0f, 10.0f))

    val arrDouble = Array.concat(Array.fill[Double](32)(0.0), Array.fill[Double](32)(5.0))
    val arrayTileDoubleUD5 = DoubleUserDefinedNoDataArrayTile(arrDouble, 8, 8, DoubleUserDefinedNoDataCellType(5.0))
    checkValuesDouble(arrayTileDoubleUD5, DoubleUserDefinedNoDataCellType(10.0), Some(5.0, 10.0))



    val constantTileByte = ByteConstantTile(5, cols, rows, ByteCellType)
    checkValuesInt(constantTileByte, UByteUserDefinedNoDataCellType(10))
    val constantTileByteCND = ByteConstantTile(5, cols, rows, ByteConstantNoDataCellType)
    checkValuesInt(constantTileByteCND, UByteUserDefinedNoDataCellType(10), Some(ByteConstantNoDataCellType.noDataValue, 10.toByte))
    val constantTileByteNoData = ByteConstantTile(5, cols, rows, ByteUserDefinedNoDataCellType(5))
    checkValuesInt(constantTileByteNoData, ByteUserDefinedNoDataCellType(10), Some(5.toByte, 10.toByte))

    val constantTileUByte = UByteConstantTile(5, cols, rows, UByteCellType)
    checkValuesInt(constantTileUByte, UByteUserDefinedNoDataCellType(10))
    val constantTileUByteCND = UByteConstantTile(5, cols, rows, UByteConstantNoDataCellType)
    checkValuesInt(constantTileUByteCND, UByteUserDefinedNoDataCellType(10), Some(UByteConstantNoDataCellType.noDataValue, 10.toByte))
    val constantTileUByteNoData = UByteConstantTile(5, cols, rows, UByteUserDefinedNoDataCellType(5))
    checkValuesInt(constantTileUByteNoData, UByteUserDefinedNoDataCellType(10), Some(5.toByte, 10.toByte))

    val constantTileShort = ShortConstantTile(5, cols, rows, ShortCellType)
    checkValuesInt(constantTileShort,  ShortUserDefinedNoDataCellType(10))
    val constantTileShortCND = ShortConstantTile(5, cols, rows, ShortConstantNoDataCellType)
    checkValuesInt(constantTileShortCND, ShortUserDefinedNoDataCellType(10), Some(ShortConstantNoDataCellType.noDataValue, 10.toShort))
    val constantTileShortNoData = ShortConstantTile(5, cols, rows, ShortUserDefinedNoDataCellType(5))
    checkValuesInt(constantTileShortNoData, ShortUserDefinedNoDataCellType(10), Some(5.toShort, 10.toShort))

    val constantTileUShort = UShortConstantTile(5, cols, rows, UShortCellType)
    checkValuesInt(constantTileUShort, UShortUserDefinedNoDataCellType(10))
    val constantTileUShortCND = UShortConstantTile(5, cols, rows, UShortConstantNoDataCellType)
    checkValuesInt(constantTileUShortCND, UShortUserDefinedNoDataCellType(10), Some(UShortConstantNoDataCellType.noDataValue, 10.toShort))
    val constantTileUShortNoData = UShortConstantTile(5, cols, rows, UShortUserDefinedNoDataCellType(5))
    checkValuesInt(constantTileUShortNoData, UShortUserDefinedNoDataCellType(10), Some(5.toShort, 10.toShort))

    val constantTileInt = IntConstantTile(5, cols, rows, IntCellType)
    checkValuesInt(constantTileInt, IntUserDefinedNoDataCellType(10))
    val constantTileIntCND = IntConstantTile(5, cols, rows, IntConstantNoDataCellType)
    checkValuesInt(constantTileIntCND, IntUserDefinedNoDataCellType(10), Some(IntConstantNoDataCellType.noDataValue, 10))
    val constantTileIntNoData = IntConstantTile(5, cols, rows, IntUserDefinedNoDataCellType(5))
    checkValuesInt(constantTileIntNoData, IntUserDefinedNoDataCellType(10), Some(5, 10))

    val constantTileFloat = FloatConstantTile(5.0f, cols, rows, FloatCellType)
    checkValuesDouble(constantTileFloat, FloatUserDefinedNoDataCellType(10.0f))
    val constantTileFloatCND = FloatConstantTile(5.0f, cols, rows, FloatConstantNoDataCellType)
    checkValuesDouble(constantTileFloatCND, FloatUserDefinedNoDataCellType(10), Some(FloatConstantNoDataCellType.noDataValue, 10.0f))
    val constantTileFloatNoData = FloatConstantTile(5.0f, cols, rows, FloatUserDefinedNoDataCellType(5.0f))
    checkValuesDouble(constantTileFloatNoData, FloatUserDefinedNoDataCellType(10.0f), Some(5.0f, 10.0f))

    val constantTileDouble = DoubleConstantTile(5.0, cols, rows, DoubleCellType)
    checkValuesDouble(constantTileDouble, DoubleUserDefinedNoDataCellType(10.0))
    val constantTileDoubleCND = DoubleConstantTile(5.0, cols, rows, DoubleConstantNoDataCellType)
    checkValuesDouble(constantTileDoubleCND, DoubleUserDefinedNoDataCellType(10), Some(DoubleConstantNoDataCellType.noDataValue, 10.0))
    val constantTileDoubleNoData = DoubleConstantTile(5.0, cols, rows, DoubleUserDefinedNoDataCellType(5.0))
    checkValuesDouble(constantTileDoubleNoData, DoubleUserDefinedNoDataCellType(10.0), Some(5.0, 10.0))
  }


}
