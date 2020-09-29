package org.openeo.geotrellis.layers

import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.{LocalDate, OffsetTime, ZonedDateTime}
import java.util.concurrent.TimeUnit

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.OscarsResponses.{Feature, FeatureCollection}
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpRequest, HttpStatusException}
import java.util.concurrent.TimeUnit.SECONDS

import scala.collection.Map

object Oscars {
  private val logger = LoggerFactory.getLogger(classOf[Oscars])

  def apply(endpoint: URL = new URL("http://oscars-01.vgt.vito.be:8080")) = new Oscars(endpoint)
}

class Oscars(endpoint: URL) {
  import Oscars._

  def getProducts(collectionId: String, from: LocalDate, to: LocalDate, bbox: ProjectedExtent,
                  correlationId: String = "", attributeValues: Map[String, Any] = Map()): Seq[Feature] = {
    val endOfDay = OffsetTime.of(23, 59, 59, 999999999, UTC)

    val start = from.atStartOfDay(UTC)
    val end = to.atTime(endOfDay).toZonedDateTime

    getProducts(collectionId, start, end, bbox, correlationId, attributeValues)
  }

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                  correlationId: String, attributeValues: Map[String, Any]): Seq[Feature] = {
    def from(startIndex: Int): Seq[Feature] = {
      val FeatureCollection(itemsPerPage, features) = getProducts(collectionId, start, end, bbox, correlationId, attributeValues, startIndex)
      if (itemsPerPage <= 0) Seq() else features ++ from(startIndex + itemsPerPage)
    }

    from(startIndex = 1)
  }

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent, correlationId: String): Seq[Feature] =
    getProducts(collectionId, start, end, bbox, correlationId, Map[String, Any]())

  private def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                          correlationId: String, attributeValues: Map[String, Any], startIndex: Int): FeatureCollection = {
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
      .param("clientId", correlationId)

    val json = withRetry { execute(getProducts) }
    FeatureCollection.parse(json)
  }

  def getCollections(correlationId: String = ""): Seq[Feature] = {
    val getCollections = Http(s"$endpoint/collections")
      .param("clientId", correlationId)

    val json = withRetry { execute(getCollections) }
    FeatureCollection.parse(json).features
  }

  private def execute(request: HttpRequest): String = {
    val url = request.urlBuilder(request)
    val response = request.asString

    if (response.isError) {
      logger.error(s"Error while invoking Oscars request: $url")
    }

    val json = response.throwError.body // note: the HttpStatusException's message doesn't include the response body

    if (json.trim.isEmpty) {
      throw new IllegalStateException(s"$url returned an empty body")
    }

    json
  }

  private def withRetry[R](action: => R): R = withRetry(attempts = 5, initialDelay = (5, SECONDS)) { action }

  private def withRetry[R](attempts: Int, initialDelay: (Long, TimeUnit))(action: => R): R = {
    require(attempts > 0)

    val (amount, timeUnit) = initialDelay

    try action
    catch {
      case _: HttpStatusException if attempts > 0 =>
        timeUnit.sleep(amount)
        withRetry(attempts - 1, (amount * 2, timeUnit)) { action }
      case e: IllegalStateException if e.getMessage.endsWith("returned an empty body") && attempts > 0 =>
        timeUnit.sleep(amount)
        withRetry(attempts - 1, (amount * 2, timeUnit)) { action }
    }
  }
}
