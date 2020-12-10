package org.openeo.geotrellis.layers

import java.net.URL
import java.time.LocalDate
import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Test
import org.openeo.geotrellis.layers.OpenSearch.{logger, requestCounter}
import org.openeo.geotrellis.layers.OpenSearchResponses.FeatureCollection
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpStatusException}

import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import scala.annotation.tailrec

class OpenSearchTest {

  @Test
  def testOscars(): Unit = {
    val openSearch = OpenSearch.oscars(new URL("http://oscars-01.vgt.vito.be:8080"))

    val features = openSearch.getProducts(
      collectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      from = LocalDate.of(2019, 10, 3),
      to = LocalDate.of(2020, 1, 2),
      bbox = ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806),
        LatLng),
      correlationId = "hello"
    )

    println(s"got ${features.size} features")
  }

  @Test
  def testCreo(): Unit = {
    val openSearch = OpenSearch.creo()

    val features = openSearch.getProducts(
      collectionId = "Sentinel2",
      processingLevel = "LEVEL2A",
      from = LocalDate.of(2020, 10, 1),
      to = LocalDate.of(2020, 10, 5),
      bbox = ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806),
        LatLng),
      correlationId = "hello"
    )

    println(s"got ${features.size} features")
  }
}
