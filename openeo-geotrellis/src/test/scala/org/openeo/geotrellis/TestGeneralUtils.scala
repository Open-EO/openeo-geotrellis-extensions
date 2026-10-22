package org.openeo.geotrellis

import geotrellis.raster.{BitCellType, ByteArrayTile, ByteCellType, ByteConstantNoDataCellType, ByteConstantTile, ByteUserDefinedNoDataArrayTile, ByteUserDefinedNoDataCellType, CellType, DoubleCellType, DoubleConstantNoDataCellType, DoubleConstantTile, DoubleUserDefinedNoDataArrayTile, DoubleUserDefinedNoDataCellType, FloatCellType, FloatConstantNoDataCellType, FloatConstantTile, FloatUserDefinedNoDataArrayTile, FloatUserDefinedNoDataCellType, IntArrayTile, IntCellType, IntConstantNoDataCellType, IntConstantTile, IntUserDefinedNoDataArrayTile, IntUserDefinedNoDataCellType, ShortArrayTile, ShortCellType, ShortConstantNoDataCellType, ShortConstantTile, ShortUserDefinedNoDataArrayTile, ShortUserDefinedNoDataCellType, Tile, UByteArrayTile, UByteCellType, UByteConstantNoDataCellType, UByteConstantTile, UByteUserDefinedNoDataArrayTile, UByteUserDefinedNoDataCellType, UShortArrayTile, UShortCellType, UShortConstantNoDataCellType, UShortConstantTile, UShortUserDefinedNoDataArrayTile, UShortUserDefinedNoDataCellType}
import org.junit.jupiter.api.Assertions.assertEquals
import geotrellis.layer.LayoutDefinition
import geotrellis.proj4.CRS
import geotrellis.raster.{BitCellType, ByteCellType, ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, DoubleCellType, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatCellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, ShortCellType, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, TileLayout, UByteCellType, UByteUserDefinedNoDataCellType, UShortCellType, UShortUserDefinedNoDataCellType}
import geotrellis.raster.{BitArrayTile, MultibandTile}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrowsExactly, assertTrue}
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.GeneralUtils.{cellTypeUnion, cellTypeUnionWithNoData, toSigned, widenWithNoData, widenedCellTypeWithNoData}

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
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnion(UShortCellType, ByteCellType))
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
    assertEquals(ShortUserDefinedNoDataCellType(-350), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(-350), ByteCellType))
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(FloatConstantNoDataCellType, ByteCellType))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(ByteConstantNoDataCellType, ByteCellType))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(9), ByteCellType))
    assertEquals(DoubleConstantNoDataCellType, cellTypeUnionWithNoData(FloatUserDefinedNoDataCellType(2.5f), IntCellType))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UShortUserDefinedNoDataCellType(10), ByteCellType))
    assertEquals(IntUserDefinedNoDataCellType(200), cellTypeUnionWithNoData(UShortUserDefinedNoDataCellType(200), ByteCellType))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(UByteUserDefinedNoDataCellType(10), UByteCellType))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UByteUserDefinedNoDataCellType(10), UShortCellType))

    // only right side has nodata value
    assertEquals(ShortUserDefinedNoDataCellType(350), cellTypeUnionWithNoData(ByteCellType, ShortUserDefinedNoDataCellType(350)))
    assertEquals(ShortUserDefinedNoDataCellType(-350), cellTypeUnionWithNoData(ByteCellType, ShortUserDefinedNoDataCellType(-350)))
    assertEquals(FloatConstantNoDataCellType, cellTypeUnionWithNoData(ByteCellType, FloatConstantNoDataCellType))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(ByteCellType, ByteConstantNoDataCellType))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ByteCellType, ShortUserDefinedNoDataCellType(9)))
    assertEquals(DoubleConstantNoDataCellType, cellTypeUnionWithNoData(IntCellType, FloatUserDefinedNoDataCellType(2.5f)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ByteCellType, UShortUserDefinedNoDataCellType(10)))
    assertEquals(IntUserDefinedNoDataCellType(200), cellTypeUnionWithNoData(ByteCellType, UShortUserDefinedNoDataCellType(200)))
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
    assertEquals(ShortUserDefinedNoDataCellType(-280), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(-280),ByteUserDefinedNoDataCellType(5)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UShortUserDefinedNoDataCellType(10), ShortUserDefinedNoDataCellType(5)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(100), ShortUserDefinedNoDataCellType(5)))
    assertEquals(DoubleUserDefinedNoDataCellType(1e40), cellTypeUnionWithNoData(DoubleUserDefinedNoDataCellType(1e40), FloatUserDefinedNoDataCellType(3.0f)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(UShortUserDefinedNoDataCellType(10), ShortUserDefinedNoDataCellType(5)))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(UByteUserDefinedNoDataCellType(256.toByte), ByteUserDefinedNoDataCellType(5)))

    // both have nodata,  and nodataLeft < nodataRight
    assertEquals(ShortUserDefinedNoDataCellType(280), cellTypeUnionWithNoData(ByteUserDefinedNoDataCellType(5), ShortUserDefinedNoDataCellType(280)))
    assertEquals(ShortUserDefinedNoDataCellType(-280), cellTypeUnionWithNoData(ByteUserDefinedNoDataCellType(5), ShortUserDefinedNoDataCellType(-280)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(5), UShortUserDefinedNoDataCellType(10)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(5), ShortUserDefinedNoDataCellType(100)))
    assertEquals(DoubleUserDefinedNoDataCellType(1e40), cellTypeUnionWithNoData(FloatUserDefinedNoDataCellType(3.0f), DoubleUserDefinedNoDataCellType(1e40)))
    assertEquals(IntUserDefinedNoDataCellType(Int.MaxValue), cellTypeUnionWithNoData(ShortUserDefinedNoDataCellType(5), UShortUserDefinedNoDataCellType(10)))
    assertEquals(ShortUserDefinedNoDataCellType(Short.MaxValue), cellTypeUnionWithNoData(ByteUserDefinedNoDataCellType(5), UByteUserDefinedNoDataCellType(256.toByte)))

  }

  @Test
  def testLayoutMergedSimple(): Unit = {
    val extent1 = Extent(0, 0, 3, 3)
    val tileLayout1 = TileLayout(3, 3, 128, 128)
    val layoutDefinition1= LayoutDefinition(extent1, tileLayout1)
    val extent2 = Extent(0, 0, 4, 4)
    val tileLayout2 = TileLayout(2, 2, 256, 256)
    val layoutDefinition2= LayoutDefinition(extent2, tileLayout2)

    val crs = CRS.fromEpsgCode(32631)
    val merged = GeneralUtils.layoutMerged(layoutDefinition1, layoutDefinition2, crs, crs)
    assertEquals(TileLayout(4, 4, 128, 128), merged.tileLayout)
    assertEquals(layoutDefinition1.cellSize, merged.cellSize)
    assertEquals(Extent(0, 0, 4, 4), merged.extent)
  }

  @Test
  def testLayoutMergedNeedsBiggerExtent(): Unit = {
    val extent1 = Extent(5, 10, 10, 20)
    val tileLayout1 = TileLayout(2, 4, 128, 128)
    val layoutDefinition1= LayoutDefinition(extent1, tileLayout1)
    val extent2 = Extent(0, 0, 10, 10)
    val tileLayout2 = TileLayout(1, 1, 256, 256)
    val layoutDefinition2= LayoutDefinition(extent2, tileLayout2)

    val crs = CRS.fromEpsgCode(32631)
    val merged = GeneralUtils.layoutMerged(layoutDefinition1, layoutDefinition2, crs, crs)
    assertEquals(layoutDefinition1.cellSize, merged.cellSize)
    assertEquals(TileLayout(4, 8, 128, 128), merged.tileLayout)
    assertEquals(Extent(0, 0, 10, 20), merged.extent)
  }

  @Test
  def testBoundsInMergedLayout(): Unit = {
    import geotrellis.layer.{KeyBounds, SpaceTimeKey}
    val layoutLeft = LayoutDefinition(Extent(5, 10, 10, 20), TileLayout(2, 4, 128, 128))
    val layoutRight = LayoutDefinition(Extent(0, 0, 10, 10), TileLayout(1, 1, 256, 256))
    val crs = CRS.fromEpsgCode(32631)
    val merged = GeneralUtils.layoutMerged(layoutLeft, layoutRight, crs, crs)

    val leftBounds = KeyBounds(SpaceTimeKey(0, 0, 1000L), SpaceTimeKey(1, 3, 2000L))
    val rightBounds = KeyBounds(SpaceTimeKey(0, 0, 1500L), SpaceTimeKey(0, 0, 3000L))

    val leftInMerged = GeneralUtils.boundsInLayout(leftBounds, layoutLeft, crs, merged, crs)
    val rightInMerged = GeneralUtils.boundsInLayout(rightBounds, layoutRight, crs, merged, crs)

    // left keys are shifted by 2 columns, time is preserved
    assertEquals(KeyBounds(SpaceTimeKey(2, 0, 1000L), SpaceTimeKey(3, 3, 2000L)), leftInMerged)
    // the single right tile covers the bottom half of the merged layout
    assertEquals(KeyBounds(SpaceTimeKey(0, 4, 1500L), SpaceTimeKey(3, 7, 3000L)), rightInMerged)
    assertEquals(KeyBounds(SpaceTimeKey(0, 0, 1000L), SpaceTimeKey(3, 7, 3000L)), leftInMerged.combine(rightInMerged))

    // unchanged layout keeps the bounds as-is
    assertEquals(leftBounds, GeneralUtils.boundsInLayout(leftBounds, layoutLeft, crs, layoutLeft, crs))
  }

  private object MergedMetadataFixture {
    import geotrellis.layer.{KeyBounds, SpaceTimeKey, SpatialKey, TileLayerMetadata}
    val crs: CRS = CRS.fromEpsgCode(32631)
    // left: fine resolution (5/256), right: coarse resolution (10/256)
    val layoutLeft: LayoutDefinition = LayoutDefinition(Extent(5, 10, 10, 20), TileLayout(2, 4, 128, 128))
    val layoutRight: LayoutDefinition = LayoutDefinition(Extent(0, 0, 10, 10), TileLayout(1, 1, 256, 256))
    val left: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(ByteConstantNoDataCellType, layoutLeft, layoutLeft.extent, crs,
      KeyBounds(SpaceTimeKey(0, 0, 1000L), SpaceTimeKey(1, 3, 2000L)))
    val rightSpaceTime: TileLayerMetadata[SpaceTimeKey] = TileLayerMetadata(FloatConstantNoDataCellType, layoutRight, layoutRight.extent, crs,
      KeyBounds(SpaceTimeKey(0, 0, 1500L), SpaceTimeKey(0, 0, 3000L)))
    val rightSpatial: TileLayerMetadata[SpatialKey] = TileLayerMetadata(FloatConstantNoDataCellType, layoutRight, layoutRight.extent, crs,
      KeyBounds(SpatialKey(0, 0), SpatialKey(0, 0)))
  }

  @Test
  def testMergedMetadataSameKeyType(): Unit = {
    import geotrellis.layer.{KeyBounds, SpaceTimeKey}
    import MergedMetadataFixture._
    val merged = GeneralUtils.mergedMetadata(left, rightSpaceTime)
    // non-mergeable fields from the left cube
    assertEquals(crs, merged.crs)
    assertEquals(layoutLeft.cellSize, merged.cellSize)
    assertEquals(128, merged.tileCols)
    // mergeable fields are the union
    assertEquals(Extent(0, 0, 10, 20), merged.extent)
    assertEquals(TileLayout(4, 8, 128, 128), merged.tileLayout)
    assertEquals(KeyBounds(SpaceTimeKey(0, 0, 1000L), SpaceTimeKey(3, 7, 3000L)), merged.bounds)
    assertEquals(FloatConstantNoDataCellType, merged.cellType)
  }

  @Test
  def testMergedMetadataSpaceTimeSpatial(): Unit = {
    import geotrellis.layer.{KeyBounds, SpaceTimeKey}
    import MergedMetadataFixture._
    val merged = GeneralUtils.mergedMetadata(left, rightSpatial)
    assertEquals(layoutLeft.cellSize, merged.cellSize)
    assertEquals(Extent(0, 0, 10, 20), merged.extent)
    // spatial bounds are the union, temporal bounds come from the spacetime cube
    assertEquals(KeyBounds(SpaceTimeKey(0, 0, 1000L), SpaceTimeKey(3, 7, 2000L)), merged.bounds)
  }

  @Test
  def testMergedMetadataRightDominant(): Unit = {
    import geotrellis.layer.{KeyBounds, SpaceTimeKey}
    import MergedMetadataFixture._
    val merged = GeneralUtils.mergedMetadata(left, rightSpatial, rightIsDominant = true)
    // non-mergeable fields from the right cube
    assertEquals(layoutRight.cellSize, merged.cellSize)
    assertEquals(256, merged.tileCols)
    assertEquals(TileLayout(1, 2, 256, 256), merged.tileLayout)
    assertEquals(Extent(0, 0, 10, 20), merged.extent)
    assertEquals(KeyBounds(SpaceTimeKey(0, 0, 1000L), SpaceTimeKey(0, 1, 2000L)), merged.bounds)
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



  /**
   * Raw (NoNoData) integral cell types are widened to the next larger cell type with a NODATA value outside of their
   * original range of values (int32 has no larger integral cell type so it becomes float64). Raw floating point cell
   * types keep their size and get NaN as NODATA. Cell types that already have a NODATA value are returned as is.
   */
  @Test
  def testWidenedCellTypeWithNoData(): Unit = {
    assertEquals(ByteConstantNoDataCellType, widenedCellTypeWithNoData(BitCellType))
    assertEquals(ShortConstantNoDataCellType, widenedCellTypeWithNoData(ByteCellType))
    assertEquals(ShortConstantNoDataCellType, widenedCellTypeWithNoData(UByteCellType))
    assertEquals(IntConstantNoDataCellType, widenedCellTypeWithNoData(ShortCellType))
    assertEquals(IntConstantNoDataCellType, widenedCellTypeWithNoData(UShortCellType))
    assertEquals(DoubleConstantNoDataCellType, widenedCellTypeWithNoData(IntCellType))
    assertEquals(FloatConstantNoDataCellType, widenedCellTypeWithNoData(FloatCellType))
    assertEquals(DoubleConstantNoDataCellType, widenedCellTypeWithNoData(DoubleCellType))

    assertEquals(UByteConstantNoDataCellType, widenedCellTypeWithNoData(UByteConstantNoDataCellType))
    assertEquals(UShortUserDefinedNoDataCellType(42), widenedCellTypeWithNoData(UShortUserDefinedNoDataCellType(42)))
    assertEquals(IntConstantNoDataCellType, widenedCellTypeWithNoData(IntConstantNoDataCellType))
  }

  /**
   * Widening a raw tile should yield the cell type of [[widenedCellTypeWithNoData]] with all of its original values
   * remaining valid data.
   *
   * Each tile contains the extremes of its cell type's range, which are the values most likely to be mistaken for
   * NODATA or to be mangled by the conversion: e.g. 0 (formerly treated as NODATA), unsigned maxima (stored as -1) and
   * Int.MinValue (GeoTrellis' NODATA value for int32, which is why int32 is special-cased). Constant tiles are included
   * because they implement convert separately from array tiles.
   *
   * A tile that already has a NODATA value should be returned as is.
   */
  @Test
  def testWidenWithNoData(): Unit = {
    def assertPreserved(tile: Tile): Unit = {
      val converted = widenWithNoData(MultibandTile(tile)).band(0)

      assertEquals(widenedCellTypeWithNoData(tile.cellType), converted.cellType)
      assertTrue(converted.cellType.isInstanceOf[geotrellis.raster.HasNoData[_]])
      assertEquals(tile.toArrayDouble().toSeq, converted.toArrayDouble().toSeq)
      assertTrue(converted.toArrayDouble().forall(d => geotrellis.raster.isData(d)))
    }

    assertPreserved(BitArrayTile(Array[Byte](0x02), 2, 1))
    assertPreserved(ByteArrayTile(Array[Byte](Byte.MinValue, 0, Byte.MaxValue), 3, 1, ByteCellType))
    assertPreserved(UByteArrayTile(Array[Byte](0, 1, -1), 3, 1, UByteCellType))
    assertPreserved(ShortArrayTile(Array[Short](Short.MinValue, 0, Short.MaxValue), 3, 1, ShortCellType))
    assertPreserved(UShortArrayTile(Array[Short](0, 1, -1), 3, 1, UShortCellType))
    assertPreserved(IntArrayTile(Array(Int.MinValue, 0, Int.MaxValue), 3, 1, IntCellType))
    assertPreserved(UShortConstantTile(0, 2, 2, UShortCellType))
    assertPreserved(UByteConstantTile(0, 2, 2, UByteCellType))

    val alreadyNoData = MultibandTile(UShortArrayTile(Array[Short](0, 1), 2, 1, UShortConstantNoDataCellType))
    assertTrue(widenWithNoData(alreadyNoData) eq alreadyNoData)
  }
}
