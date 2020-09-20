package org.openeo.geotrellis.layers

import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.{LocalDate, OffsetTime, ZonedDateTime}

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.OscarsResponses.{Feature, FeatureCollection}
import scalaj.http.Http

import scala.collection.Map

object Oscars {
  def apply(endpoint: URL = new URL("https://services.terrascope.be/catalogue")) = new Oscars(endpoint)
}

class Oscars(endpoint: URL) {
  def getProducts(collectionId: String, from: LocalDate, to: LocalDate, bbox: ProjectedExtent, attributeValues: Map[String, Any] = Map()): Seq[Feature] = {
    val endOfDay = OffsetTime.of(23, 59, 59, 999999999, UTC)

    val start = from.atStartOfDay(UTC)
    val end = to.atTime(endOfDay).toZonedDateTime

    getProducts(collectionId, start, end, bbox, attributeValues)
  }

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent, attributeValues: Map[String, Any]): Seq[Feature] = {
    def from(startIndex: Int): Seq[Feature] = {
      val FeatureCollection(itemsPerPage, features) = getProducts(collectionId, start, end, bbox, attributeValues, startIndex)
      if (itemsPerPage <= 0) Seq() else features ++ from(startIndex + itemsPerPage)
    }

    from(startIndex = 1)
  }

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent): Seq[Feature] =
    getProducts(collectionId, start, end, bbox, Map[String, Any]())

  private def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent, attributeValues: Map[String, Any], startIndex: Int): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val getProducts = Http(s"$endpoint/products")
      .param("collection", collectionId)
      .param("start", start format ISO_INSTANT)
      .param("end", end format ISO_INSTANT)
      .param("bbox", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortKeys", "title") // paging requires deterministic order
      .param("startIndex", startIndex.toString)
      .param("accessedFrom", "MEP") // get direct access links instead of download urls
      .params(attributeValues.mapValues(_.toString).toSeq)

    val response = getProducts.asString
    if(response.isError) {
      println("Error while invoking Oscars request: " + getProducts.urlBuilder(getProducts))
    }
    val json = response.throwError.body
    FeatureCollection.parse(json)
  }

  def getCollections: Seq[Feature] = {
    val getCollections = Http(s"$endpoint/collections")

    val json = getCollections.asString.throwError.body
    FeatureCollection.parse(json).features
  }
}