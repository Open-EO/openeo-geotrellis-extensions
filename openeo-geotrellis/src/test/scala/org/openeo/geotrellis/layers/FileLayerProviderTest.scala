package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.ZoomedLayoutScheme
import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import org.junit.Assert.assertSame
import org.junit.Test

import java.net.URL
import java.nio.file.Paths

class FileLayerProviderTest {

  @Test
  def cache(): Unit = {
    val rootPath = Paths.get("/data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1")

    def fileLayerProvider = new FileLayerProvider( // important: multiple instances like in openeo-geopyspark-driver
      openSearchEndpoint = new URL("http://oscars-01.vgt.vito.be:8080"),
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1",
      NonEmptyList.one("NO2"),
      rootPath.toString,
      maxSpatialResolution = CellSize(0.05, 0.05),
      new Sentinel5PPathDateExtractor(maxDepth = 3),
      layoutScheme = ZoomedLayoutScheme(LatLng)
    )

    val metadataCall1 = fileLayerProvider.loadMetadata(sc = null)
    val metadataCall2 = fileLayerProvider.loadMetadata(sc = null)
    assertSame(metadataCall1, metadataCall2)
  }
}
