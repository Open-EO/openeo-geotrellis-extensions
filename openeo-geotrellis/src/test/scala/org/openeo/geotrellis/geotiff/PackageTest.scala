package org.openeo.geotrellis.geotiff

import geotrellis.raster.io.geotiff.MultibandGeoTiff
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
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

  @Test
  def testEmbedGdalMetadataFails(): Unit = {
    val e = assertThrows(classOf[IOException], () =>
      embedGdalMetadata(geotiffPath = Paths.get("doesnotexist.tif"), <GdalMetadata />)
    )

    assertTrue(e.getMessage contains "doesnotexist.tif: No such file or directory")
  }

  private def processingSoftware(geotiff: Path): Option[String] =
    MultibandGeoTiff(geotiff.toString).tags.headTags.get("PROCESSING_SOFTWARE")
}
