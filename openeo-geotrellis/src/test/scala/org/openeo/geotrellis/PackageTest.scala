package org.openeo.geotrellis

import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ByteCellType, ByteUserDefinedNoDataCellType, FloatUserDefinedNoDataCellType, UByteCellType, UByteUserDefinedNoDataCellType}
import org.junit.Assert._
import org.junit.Test
import org.openeo.geotrellis.geotiff._

import java.nio.file.{Files, Path}

class PackageTest {
  @Test
  def testToSigned(): Unit = {
    assertEquals(ByteCellType, toSigned(UByteCellType))
    assertEquals(ByteUserDefinedNoDataCellType(42), toSigned(UByteUserDefinedNoDataCellType(42)))
    assertEquals(FloatUserDefinedNoDataCellType(42), toSigned(FloatUserDefinedNoDataCellType(42)))
    assertEquals(ByteUserDefinedNoDataCellType(42), toSigned(ByteUserDefinedNoDataCellType(42)))
  }

  @Test
  def testFileMove(): Unit = {
    val refFile = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/Sentinel2FileLayerProvider_multiband_reference.tif")
    val refTiff = GeoTiff.readMultiband(refFile.getPath)
    val p = Path.of(f"tmp/testFileMove/")
    Files.createDirectories(p)

    (1 to 20).foreach { i =>
      val dst = Path.of(p + f"/$i.tif")
      // Limit the amount of parallel jobs to avoid getting over the max retries
      (1 to 4).par.foreach { _ =>
        writeGeoTiff(refTiff, dst.toString)
        assertTrue(Files.exists(dst))
      }
      val refTiff2 = GeoTiff.readMultiband(dst.toString)
      assertEquals(refTiff2.cellSize, refTiff.cellSize)
    }
  }
}
