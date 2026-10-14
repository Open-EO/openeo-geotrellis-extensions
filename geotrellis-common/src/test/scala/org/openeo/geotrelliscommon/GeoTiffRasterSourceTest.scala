package org.openeo.geotrelliscommon

import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path}

object GeoTiffRasterSourceTest {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[GeoTiffRasterSourceTest])
}

class GeoTiffRasterSourceTest {

  @Test
  def readZStdCompressedTif():Unit = {
    val source = GeoTiffRasterSource(GeoTiffPath.toGeoTiffDataPath("../testdata/org/openeo/geotrelliscommon/GeoTiffRasterSourceTest/zstd_predictor2.tif"))
    val raster = source.read().get
    assertEquals(1, raster.tile.bandCount)
    assertEquals(4000, raster.tile.rows)
    assertEquals(4000, raster.tile.cols)
    assertEquals((0, 128), raster.tile.band(0).findMinMax)
    assertEquals(2, raster.tile.band(0).get(5, 5))
  }
}
