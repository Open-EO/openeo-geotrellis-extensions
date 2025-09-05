package org.openeo.geotrelliscommon

import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.slf4j.{Logger, LoggerFactory}

object GeoTiffRasterSourceTest {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[GeoTiffRasterSourceTest])
}

class GeoTiffRasterSourceTest {

  @Test
  def readZStdCompressedTif():Unit = {
    val source = GeoTiffRasterSource(GeoTiffPath.toGeoTiffDataPath("https://artifactory.vgt.vito.be/artifactory/testdata-public/openeo/geotrellis_extrensions/zstd_predictor2.tif"))
    val raster = source.read().get
    assertEquals(1, raster.tile.bandCount)
    assertEquals(4000, raster.tile.rows)
    assertEquals(4000, raster.tile.cols)
  }
}
