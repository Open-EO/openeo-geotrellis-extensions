package org.openeo.geotrellis

import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.proj4.{CRS, LatLng, Sinusoidal, WebMercator}
import geotrellis.raster.{ByteCellType, ByteUserDefinedNoDataCellType, FloatUserDefinedNoDataCellType, UByteCellType, UByteUserDefinedNoDataCellType}
import org.openeo.geotrellis.geotiff._

import java.nio.file.{Files, Path}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
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
    arguments(ProjectedExtent(Extent(3134600, 3977500, 3134601, 3977501), CRS.fromName("EPSG:3035"))),
    arguments(ProjectedExtent(Extent(565400, 6660100, 565401, 6660101), CRS.fromName("EPSG:3857"))),
  ))

  def testHealthCheckExtentParamsNok: java.util.stream.Stream[Arguments] = java.util.Arrays.stream(Array(
    arguments(ProjectedExtent(Extent(-400, 40, -300, 50), LatLng)),
    arguments(ProjectedExtent(Extent(5000111, 40, 5000222, 50), CRS.fromName("EPSG:32631"))),
    arguments(ProjectedExtent(Extent(6441000, 13573000, 6441001, 13573001), CRS.fromName("EPSG:3035"))),
    arguments(ProjectedExtent(Extent(99000111, 99000111, 99000222, 99000222), CRS.fromName("EPSG:3857"))),
  ))

  def testIsExtentValidInCrsBelgiumParameters: java.util.stream.Stream[Arguments] = java.util.Arrays.stream(Array(
    arguments(CRS.fromName("EPSG:32630")),
    arguments(CRS.fromName("EPSG:32631")),
    arguments(CRS.fromName("EPSG:32632")),
    arguments(CRS.fromName("EPSG:3035")),
    arguments(CRS.fromName("EPSG:31370")), // "Belgian Lambert 72"
    arguments(LatLng),
  ))

  def testIsExtentValidInCrsAntimeridianParameters: java.util.stream.Stream[Arguments] = java.util.Arrays.stream(Array(
    arguments(CRS.fromName("EPSG:32601")),
    arguments(CRS.fromName("EPSG:32660")),
    arguments(LatLng),
  ))

  def testIsExtentValidInCrsSouthParameters: java.util.stream.Stream[Arguments] = java.util.Arrays.stream(Array(
    arguments(CRS.fromName("EPSG:32734")),
    arguments(CRS.fromName("EPSG:32735")),
    arguments(LatLng),
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

  @ParameterizedTest
  @MethodSource(Array("testIsExtentValidInCrsBelgiumParameters"))
  def testIsExtentValidInCrsBelgium(crs: CRS): Unit = {
    val extentBelgium: ProjectedExtent = ProjectedExtent(Extent(2.5, 49.5, 6.5, 51.5), CRS.fromName("EPSG:4326"))
    val extent = safeReproject(extentBelgium, crs)
    assertTrue(isExtentValidInCrs(extent, CRS.fromName("EPSG:32630"))) // europe
    assertTrue(isExtentValidInCrs(extent, CRS.fromName("EPSG:32631"))) // europe
    assertTrue(isExtentValidInCrs(extent, CRS.fromName("EPSG:32632"))) // europe
    assertTrue(isExtentValidInCrs(extent, CRS.fromName("EPSG:3035"))) // europe LAEA
    assertTrue(isExtentValidInCrs(extent, LatLng))
    assertTrue(isExtentValidInCrs(extent, Sinusoidal))
    assertTrue(isExtentValidInCrs(extent, WebMercator))

    assertFalse(isExtentValidInCrs(extent, CRS.fromName("EPSG:32601"))) // utm antimeridian
    assertFalse(isExtentValidInCrs(extent, CRS.fromName("EPSG:32660"))) // utm antimeridian
  }

  @ParameterizedTest
  @MethodSource(Array("testIsExtentValidInCrsAntimeridianParameters"))
  def testIsExtentValidInCrsAntimeridian(crs: CRS): Unit = {
    val extentAntimeridian = ProjectedExtent(Extent(179, 70, 185, 71), LatLng)
    val extent = safeReproject(extentAntimeridian, crs)
    assertTrue(isExtentValidInCrs(extent, CRS.fromName("EPSG:32601"))) // utm antimeridian
    assertTrue(isExtentValidInCrs(extent, CRS.fromName("EPSG:32660"))) // utm antimeridian
    assertTrue(isExtentValidInCrs(extent, LatLng))
    // assertTrue(isExtentValidInCrs(extent, WebMercator)) // Not every CRS has explicit antimerisian wrapping.

    assertFalse(isExtentValidInCrs(extent, CRS.fromName("EPSG:32631"))) // europe
    assertFalse(isExtentValidInCrs(extent, CRS.fromName("EPSG:3035"))) // europe LAEA

    // An extent at the other side of the world is surprisingly accurate in Belgian Lambert 72
    // assertFalse(isExtentValidInCrs(extent, CRS.fromName("EPSG:31370")))
  }

  @ParameterizedTest
  @MethodSource(Array("testIsExtentValidInCrsSouthParameters"))
  def testIsExtentValidInCrsSouth(crs: CRS): Unit = {
    val extentSouthAfrica = ProjectedExtent(Extent(10, -40, 40, -20), LatLng)
    val extent = safeReproject(extentSouthAfrica, crs)
    assertTrue(isExtentValidInCrs(extent, CRS.fromName("EPSG:32734")))
    assertTrue(isExtentValidInCrs(extent, CRS.fromName("EPSG:32735")))
    assertTrue(isExtentValidInCrs(extent, LatLng))
    assertTrue(isExtentValidInCrs(extent, Sinusoidal))
    assertTrue(isExtentValidInCrs(extent, WebMercator))

    assertFalse(isExtentValidInCrs(extent, CRS.fromName("EPSG:32601"))) // utm antimeridian
    assertFalse(isExtentValidInCrs(extent, CRS.fromName("EPSG:32660"))) // utm antimeridian

    // An extent at the other side of the world is surprisingly accurate in Belgian Lambert 72
    // assertFalse(isExtentValidInCrs(extent, CRS.fromName("EPSG:31370")))
  }
}
