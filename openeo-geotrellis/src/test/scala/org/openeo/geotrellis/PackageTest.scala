package org.openeo.geotrellis

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{ByteCellType, ByteUserDefinedNoDataCellType, FloatUserDefinedNoDataCellType, UByteCellType, UByteUserDefinedNoDataCellType}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.geotrellis.layers.FileLayerProvider
import org.slf4j.{Logger, LoggerFactory}

object PackageTest {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[FileLayerProvider])

  def testHealthCheckExtentParamsOk: java.util.stream.Stream[Arguments] = java.util.Arrays.stream(Array(
    arguments(ProjectedExtent(Extent(40, 40, 50, 50), LatLng)),
    arguments(ProjectedExtent(Extent(11000, 40, 22000, 50), CRS.fromName("EPSG:32631"))),
  ))
  def testHealthCheckExtentParamsNok: java.util.stream.Stream[Arguments] = java.util.Arrays.stream(Array(
    arguments(ProjectedExtent(Extent(-400, 40, -300, 50), LatLng)),
    arguments(ProjectedExtent(Extent(5000111, 40, 5000222, 50), CRS.fromName("EPSG:32631"))),
  ))
}

class PackageTest {

  import PackageTest._

  @Test
  def testToSigned(): Unit = {
    assertEquals(ByteCellType, toSigned(UByteCellType))
    assertEquals(ByteUserDefinedNoDataCellType(42), toSigned(UByteUserDefinedNoDataCellType(42)))
    assertEquals(FloatUserDefinedNoDataCellType(42), toSigned(FloatUserDefinedNoDataCellType(42)))
    assertEquals(ByteUserDefinedNoDataCellType(42), toSigned(ByteUserDefinedNoDataCellType(42)))
  }

  @ParameterizedTest
  @MethodSource(Array("testHealthCheckExtentParamsOk"))
  def testHealthCheckExtentOk(projectedExtent: ProjectedExtent): Unit = {
    assert(healthCheckExtent(projectedExtent))
  }
  @ParameterizedTest
  @MethodSource(Array("testHealthCheckExtentParamsNok"))
  def testHealthCheckExtentNok(projectedExtent: ProjectedExtent): Unit = {
    assertFalse(healthCheckExtent(projectedExtent))
  }
}
