package org.openeo.geotrellissentinelhub

import geotrellis.vector._
import org.junit.Test

class ElasticsearchTilingGridRepositoryTest {
  private val tilingGridRepository = new ElasticsearchTilingGridRepository("https://es-apps-dev.vgt.vito.be:443")

  @Test
  def getGeometry(): Unit = {
    val geometry = tilingGridRepository.getGeometry("sentinel-hub-tiling-grid-1", "31UDS_7_2")
    println(geometry.toGeoJson())
  }
}
