package org.openeo.geotrellis.layers

import java.net.URL
import java.time.{LocalDate, ZonedDateTime}

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class OpenSearchTest {

  @Test
  def testOscars(): Unit = {
    val openSearch = new OscarsOpenSearch(new URL("http://oscars-01.vgt.vito.be:8080"))

    val features = openSearch.getProducts(
      collectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      from = LocalDate.of(2019, 10, 3),
      to = LocalDate.of(2020, 1, 2),
      bbox = ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806),
        LatLng),
      correlationId = "hello"
    )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
  }

  @Test
  def testCreo(): Unit = {
    val openSearch = CreoOpenSearch

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
    assertTrue(features.nonEmpty)
    val aFeature = features(0)
    val band02 = aFeature.links.filter(_.title.get.contains("B02_10m"))
    assertTrue(band02.nonEmpty)
  }


  @Test
  def testCreoSentinel1(): Unit = {
    val openSearch = CreoOpenSearch

    val features = openSearch.getProducts(
      collectionId = "Sentinel1",

      from = LocalDate.of(2020, 10, 1),
      to = LocalDate.of(2020, 10, 5),
      bbox = ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806),
        LatLng),
      correlationId = "hello"
    )

    println(s"got ${features.size} features")
    val unique: mutable.Set[(Extent,ZonedDateTime)] = mutable.Set()
    assertTrue(features.nonEmpty)
    features.foreach(f => {
      assertTrue(f.id.contains("GRD"))
      val extent_timestamp = (f.bbox,f.nominalDate)
      unique add extent_timestamp
    })
    assertEquals(features.size,unique.size)
  }

  @Test
  def testSTAC(): Unit = {
    val openSearch = new STACOpenSearch()

    val features = openSearch.getProducts(
      collectionId = "sentinel-s2-l2a-cogs",
      processingLevel = "LEVEL2A",
      from = LocalDate.of(2020, 10, 1),
      to = LocalDate.of(2020, 10, 5),
      bbox = ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806),
        LatLng),
      correlationId = "hello"
    )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
    assertEquals(15,features.length)
  }
}
