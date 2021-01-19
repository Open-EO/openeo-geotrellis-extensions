package org.openeo.geotrellis.layers

import geotrellis.raster.CellSize
import org.junit.Test
import org.mockito.IdiomaticMockito

import java.net.URL
import java.nio.file.Paths

class FileLayerProviderTest extends IdiomaticMockito {

  @Test
  def cache(): Unit = {
    val rootPath = Paths.get("/path/to/products")
    val pathDateExtractor = mock[PathDateExtractor]

    def fileLayerProvider = new FileLayerProvider( // important: multiple instances like in openeo-geopyspark-driver
      openSearchEndpoint = new URL("http://oscars-01.vgt.vito.be:8080"),
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      openSearchLinkTitle = "FAPAR_10M",
      rootPath.toString,
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor
    )

    fileLayerProvider.loadMetadata(sc = null)
    fileLayerProvider.loadMetadata(sc = null)

    // should only be called once, not when the cache is hit
    pathDateExtractor.extractDates(rootPath) wasCalled once
  }
}
