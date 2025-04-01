package org.openeo.geotrellis.geotiff

import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.{GeoTiff, Tiled}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{Disabled, Test}
import org.junit.jupiter.api.io.TempDir

import java.io.IOException
import java.nio.file.{Files, Path, Paths}

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

  @Test
  def testConvertToCog(@TempDir tempDir: Path): Unit = {
    val geotiffCopy = tempDir.resolve("copy.tif")
    Files.copy(getClass.getResourceAsStream("/org/openeo/geotrellis/cgls_ndvi300.tif"), geotiffCopy)
    val originalFilePermissions = Files.getPosixFilePermissions(geotiffCopy)

    val tiffBefore = GeoTiff.readSingleband(geotiffCopy.toString)

    assertEquals(LatLng, tiffBefore.crs)
    assertEquals(Tiled(512, 512), tiffBefore.options.storageMethod)

    convertToCog(geotiffCopy, tiffBefore.bandCount, blockSize = 256)
    assertEquals(originalFilePermissions, Files.getPosixFilePermissions(geotiffCopy))

    val tiffAfter = GeoTiff.readSingleband(geotiffCopy.toString)

    assertEquals(LatLng, tiffAfter.crs)
    assertEquals(Tiled(256, 256), tiffAfter.options.storageMethod)
    assertEquals(tiffBefore.getOverviewsCount, tiffAfter.getOverviewsCount)
    // additional COG checks here
  }
}
