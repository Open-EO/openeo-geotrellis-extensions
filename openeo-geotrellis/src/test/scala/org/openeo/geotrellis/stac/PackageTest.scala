package org.openeo.geotrellis.stac

import geotrellis.raster._
import geotrellis.vector.Extent
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
    assertEquals(expected, asset(cellType).dataType)
  }

  private def asset(cellType: CellType): Asset = Asset("asset", cellType, GTIFF_MEDIA_TYPE)

  @Test
  def exposesNodataOnlyForCellTypesThatDefineIt(): Unit = {
    assertEquals(ByteConstantNoDataCellType.noDataValue, asset(ByteConstantNoDataCellType).nodata)
    assertEquals(3.toByte, asset(ByteUserDefinedNoDataCellType(3)).nodata)
    assertEquals(255, asset(UByteUserDefinedNoDataCellType(-1)).nodata)
    assertEquals(65535, asset(UShortUserDefinedNoDataCellType(-1)).nodata)
    assertTrue(asset(FloatConstantNoDataCellType).nodata.floatValue().isNaN)
    assertTrue(asset(DoubleConstantNoDataCellType).nodata.doubleValue().isNaN)
    assertNull(asset(ByteCellType).nodata)
    assertNull(asset(BitCellType).nodata)
  }

  @Test
  def itemIsStacFeatureInLatLng(): Unit = {
    val item = Item("id", datetime = null, bbox = Extent(500000, 5000000, 600000, 5100000),
      assets = java.util.Collections.singletonMap("openEO", asset(FloatCellType)), crs = geotrellis.proj4.CRS.fromEpsgCode(32631),
      startDatetime = "2020-01-01T00:00:00Z", endDatetime = "2020-01-10T00:00:00Z")
    val stac = item.toStacMap
    assertEquals("Feature", stac.get("type"))
    val props = stac.get("properties").asInstanceOf[java.util.Map[String, Any]]
    assertTrue(props.containsKey("datetime"))
    assertNull(props.get("datetime"))
    assertEquals("2020-01-01T00:00:00Z", props.get("start_datetime"))
    assertEquals("2020-01-10T00:00:00Z", props.get("end_datetime"))
    val bbox = stac.get("bbox").asInstanceOf[java.util.List[Double]]
    assertTrue(bbox.get(0) > -1.0 && bbox.get(0) < 5.0)
  }

  private def itemProperties(startDatetime: String, endDatetime: String): java.util.Map[String, Any] =
    Item("id", datetime = null, bbox = Extent(0, 0, 1, 1),
      assets = java.util.Collections.singletonMap("openEO", asset(FloatCellType)), crs = geotrellis.proj4.LatLng,
      startDatetime = startDatetime, endDatetime = endDatetime).properties

  @Test
  def itemWithoutDatesHasNullDatetime(): Unit = {
    val props = itemProperties(null, null)
    assertTrue(props.containsKey("datetime"))
    assertNull(props.get("datetime"))
    assertFalse(props.containsKey("start_datetime"))
    assertFalse(props.containsKey("end_datetime"))
  }

  @Test
  def itemWithHalfOpenExtentUsesKnownDate(): Unit = {
    assertEquals("2020-01-01T00:00:00Z", itemProperties("2020-01-01T00:00:00Z", null).get("datetime"))
    assertEquals("2020-01-10T00:00:00Z", itemProperties(null, "2020-01-10T00:00:00Z").get("datetime"))
    assertFalse(itemProperties("2020-01-01T00:00:00Z", null).containsKey("start_datetime"))
  }

  @Test
  def itemWithSingleDateUsesItAsDatetime(): Unit = {
    val props = itemProperties("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z")
    assertEquals("2020-01-01T00:00:00Z", props.get("datetime"))
    assertFalse(props.containsKey("start_datetime"))
  }
}
