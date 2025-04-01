package org.openeo.geotrellis.geotiff

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.io.geotiff.{GeoTiff, Tiled}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{Disabled, Test}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.io.IOException
import java.nio.file.{Files, Path, Paths}
import java.util.stream.{Stream => JStream}

object PackageTest {
  def testConvertToCogParameters: JStream[Arguments] = JStream.of(
    Arguments.of("/org/openeo/geotrellis/cgls_ndvi300.tif", LatLng),
    Arguments.of("/org/openeo/geotrellis/Sentinel2FileLayerProvider_multiband_reference_average.tif", WebMercator),
  )
}

class PackageTest {

  @Test
  def testEmbedGdalMetadata(@TempDir tempDir: Path): Unit = {
    val geotiffCopy = tempDir.resolve("copy.tif")
    Files.copy(getClass.getResourceAsStream("/org/openeo/geotrellis/cgls_ndvi300.tif"), geotiffCopy)

    assertTrue(processingSoftware(geotiffCopy).isEmpty)

    val gdalMetadataXml =
      <GDALMetadata>
        <Item name="PROCESSING_SOFTWARE">0.45.0a1</Item>
        <Item name="DESCRIPTION" sample="0">CO</Item>
      </GDALMetadata>

    embedGdalMetadata(geotiffCopy, gdalMetadataXml)

    assertEquals(Some("0.45.0a1"), processingSoftware(geotiffCopy))
  }

  @Disabled("quick fix for https://github.com/Open-EO/openeo-geotrellis-extensions/issues/345")
  @Test
  def testEmbedGdalMetadataFails(): Unit = {
    val e = assertThrows(classOf[IOException], () =>
      embedGdalMetadata(geotiffPath = Paths.get("doesnotexist.tif"), <GdalMetadata />)
    )

    assertTrue(e.getMessage contains "doesnotexist.tif: No such file or directory")
  }

  private def processingSoftware(geotiff: Path): Option[String] =
    GeoTiff.readSingleband(geotiff.toString).tags.headTags.get("PROCESSING_SOFTWARE")

  @ParameterizedTest
  @MethodSource(Array("testConvertToCogParameters"))
  def testConvertToCog(geoTiffResource: String, expectedCrs: CRS, @TempDir tempDir: Path): Unit = {
    val geotiffCopy = tempDir.resolve("copy.tif")
    Files.copy(getClass.getResourceAsStream(geoTiffResource), geotiffCopy)
    val originalFilePermissions = Files.getPosixFilePermissions(geotiffCopy)
    val targetBlockSize = 128

    val tiffBefore = GeoTiff.readMultiband(geotiffCopy.toString)

    assertEquals(expectedCrs, tiffBefore.crs)
    assertNotEquals(Tiled(targetBlockSize, targetBlockSize), tiffBefore.options.storageMethod)

    convertToCog(geotiffCopy, tiffBefore.bandCount, blockSize = targetBlockSize)
    assertEquals(originalFilePermissions, Files.getPosixFilePermissions(geotiffCopy))

    val tiffAfter = GeoTiff.readMultiband(geotiffCopy.toString)

    assertEquals(expectedCrs, tiffAfter.crs)
    assertEquals(Tiled(targetBlockSize, targetBlockSize), tiffAfter.options.storageMethod)
    assertEquals(tiffBefore.getOverviewsCount, tiffAfter.getOverviewsCount)
    // additional COG checks here
  }
}
