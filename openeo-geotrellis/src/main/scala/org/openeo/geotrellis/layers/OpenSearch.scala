package org.openeo.geotrellis.layers

import java.io.IOException
import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, OffsetTime, ZonedDateTime}
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicLong

import geotrellis.vector.ProjectedExtent
import org.openeo.geotrellis.layers.OpenSearchResponses.{Feature, FeatureCollection}
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpStatusException}

import scala.annotation.tailrec
import scala.collection.Map

object OpenSearch {
  private val logger = LoggerFactory.getLogger(classOf[OpenSearch])
  private val requestCounter = new AtomicLong

  def apply(endpoint:URL):OpenSearch = {
    endpoint.toString match {
      case s if s.contains("creo") => CreoOpenSearch
      case s if s.contains("aws") => new STACOpenSearch(endpoint)
      case _ => new OscarsOpenSearch(endpoint)
    }

  }

}

abstract class OpenSearch {
  import OpenSearch._

  def getProducts(collectionId: String, from: LocalDate, to: LocalDate, bbox: ProjectedExtent,
                  processingLevel: String = "", attributeValues: Map[String, Any] = Map(),
                  correlationId: String = ""): Seq[Feature] = {
    val endOfDay = OffsetTime.of(23, 59, 59, 999999999, UTC)

    val start = from.atStartOfDay(UTC)
    val end = to.atTime(endOfDay).toZonedDateTime

    getProducts(collectionId, start, end, bbox, processingLevel, attributeValues, correlationId=correlationId)
  }

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                  processingLevel: String, attributeValues: Map[String, Any],
                  correlationId: String): Seq[Feature]

  def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                  processingLevel: String,
                  correlationId: String): Seq[Feature] =
    getProducts(collectionId, start, end, bbox, processingLevel, Map[String, Any](), correlationId=correlationId)

  protected def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                            processingLevel: String,  attributeValues: Map[String, Any], startIndex: Int,
                            correlationId: String): FeatureCollection

  def getCollections(correlationId: String = ""): Seq[Feature]

  protected def http(url: String): HttpRequest = Http(url).option(HttpOptions.followRedirects(true))

  protected def clientId(correlationId: String): String = {
    if (correlationId.isEmpty) correlationId
    else {
      val count = requestCounter.getAndIncrement()
      s"${correlationId}_$count"
    }
  }

  protected def execute(request: HttpRequest): String = {
    val url = request.urlBuilder(request)
    val response = request.asString

    logger.debug(s"$url returned ${response.code}")

    val json = response.throwError.body // note: the HttpStatusException's message doesn't include the response body

    if (json.trim.isEmpty) {
      throw new IOException(s"$url returned an empty body")
    }

    json
  }

  protected def withRetries[R](action: => R): R = {
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
