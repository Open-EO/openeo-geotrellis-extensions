package org.openeo.geotrellis

import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{ByteCellType, ByteUserDefinedNoDataCellType, FloatUserDefinedNoDataCellType, UByteCellType, UByteUserDefinedNoDataCellType}
import org.junit.Assert._
import org.openeo.geotrellis.geotiff._

import java.nio.file.{Files, Path}
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

  @Test
  def testFileMove(): Unit = {
    val refFile = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/Sentinel2FileLayerProvider_multiband_reference_average.tif")
    val refTiff = GeoTiff.readMultiband(refFile.getPath)
    val p = Path.of(f"tmp/testFileMove/")
    Files.createDirectories(p)

    (1 to 20).foreach { i =>
      val dst = Path.of(p + f"/$i.tif")
      // Limit the amount of parallel jobs to avoid getting over the max retries
      (1 to 4).par.foreach { _ =>
        writeGeoTiff(refTiff, dst.toString, gtiffOptions = None)
      }
      assertTrue(Files.exists(dst))
      val refTiff2 = GeoTiff.readMultiband(dst.toString)
      assertEquals(refTiff2.cellSize, refTiff.cellSize)
    }
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
