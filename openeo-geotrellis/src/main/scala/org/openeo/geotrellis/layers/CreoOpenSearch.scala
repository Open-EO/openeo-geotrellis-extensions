package org.openeo.geotrellis.layers

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.OpenSearchResponses._
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpStatusException}

import java.io.IOException
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.{LocalDate, OffsetTime, ZonedDateTime}
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.collection.Map

object CreoOpenSearch {
  private val logger = LoggerFactory.getLogger(classOf[CreoOpenSearch])
  private val requestCounter = new AtomicLong

  val collections = "https://finder.creodias.eu/resto/collections.json"
  val collection = "https://finder.creodias.eu/resto/api/collections/#COLLECTION#/search.json"

  def apply() = new CreoOpenSearch()
}

class CreoOpenSearch {
  import CreoOpenSearch._

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
      if (itemsPerPage <= 0) Seq() else features ++ from(startIndex + 1)
    }

    from(startIndex = 1)
  }

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent, correlationId: String): Seq[Feature] =
    getProducts(collectionId, start, end, bbox, correlationId, Map[String, Any]())

  private def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                          correlationId: String, attributeValues: Map[String, Any], page: Int): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val getProducts = http(collection.replace("#COLLECTION#", collectionId))
      .param("processingLevel", "LEVEL2A")
      .param("startDate", start format ISO_INSTANT)
      .param("completionDate", end format ISO_INSTANT)
      .param("box", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortParam", "startDate") // paging requires deterministic order
      .param("sortOrder", "ascending")
      .param("page", page.toString)
      .param("maxRecords", "10")
      .param("status", "all")
      .param("dataset", "ESA-DATASET")
      .params(attributeValues.mapValues(_.toString).toSeq)

    val json = withRetries { execute(getProducts) }
    CreoFeatureCollection.parse(json)
  }

  def getCollections(correlationId: String = ""): Seq[CreoCollection] = {
    val getCollections = http(collections)
      .option(HttpOptions.followRedirects(true))
      .param("clientId", clientId(correlationId))

    val json = withRetries { execute(getCollections) }
    CreoCollections.parse(json).collections
  }

  private def http(url: String): HttpRequest = Http(url).option(HttpOptions.followRedirects(true))

  private def clientId(correlationId: String): String = {
    if (correlationId.isEmpty) correlationId
    else {
      val count = requestCounter.getAndIncrement()
      s"${correlationId}_$count"
    }
  }

  private def execute(request: HttpRequest): String = {
    val url = request.urlBuilder(request)
    val response = request.asString

    logger.debug(s"$url returned ${response.code}")

    val json = response.throwError.body // note: the HttpStatusException's message doesn't include the response body

    if (json.trim.isEmpty) {
      throw new IOException(s"$url returned an empty body")
    }

    json
  }

  private def withRetries[R](action: => R): R = {
    @tailrec
    def attempt[R](retries: Int, delay: (Long, TimeUnit))(action: => R): R = {
      val (amount, timeUnit) = delay

      def retryable(e: Exception): Boolean = retries > 0 && (e match {
        case h: HttpStatusException if h.code >= 500 => true
        case e: IOException if e.getMessage.endsWith("returned an empty body") =>
          logger.warn(s"encountered empty body: retrying within $amount $timeUnit", e)
          true
        case _ => false
      })

      try
        return action
      catch {
        case e: Exception if retryable(e) => Unit
      }

      timeUnit.sleep(amount)
      attempt(retries - 1, (amount * 2, timeUnit)) { action }
    }

    attempt(retries = 4, delay = (5, SECONDS)) { action }
  }
}
