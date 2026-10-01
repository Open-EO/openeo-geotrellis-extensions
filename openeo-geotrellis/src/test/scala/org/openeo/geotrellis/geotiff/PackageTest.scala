package org.openeo.geotrellis.geotiff

import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.{Files, Paths}

class PackageTest {

  @Test
  def testIsClassicTiff(): Unit = {
    val geoTiff = Paths.get(
      Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/cgls_ndvi300.tif").getPath
    )

    assertTrue(isClassicTiff(geoTiff), geoTiff.toString)
  }

  @Test
  def testIsNotClassicTiff(): Unit = {
    val someFile = Paths.get(
      Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/GeometryCollection.json").getPath
    )

    assertTrue(Files.exists(someFile), someFile.toString)
    assertFalse(isClassicTiff(someFile), someFile.toString)
  }
}
