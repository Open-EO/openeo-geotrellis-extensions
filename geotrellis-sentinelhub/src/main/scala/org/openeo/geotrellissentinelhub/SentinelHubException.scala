package org.openeo.geotrellissentinelhub

import _root_.io.circe.generic.auto._
import cats.syntax.either._
import org.openeo.geotrelliscommon.CirceException.decode
import scalaj.http.{HttpRequest, HttpResponse}

object SentinelHubException {
  def apply(request: HttpRequest, requestBody: String, textResponse: HttpResponse[String]): SentinelHubException =
    apply(request, requestBody, textResponse.code, textResponse.headers, textResponse.body)

  def apply(request: HttpRequest, requestBody: String, statusCode: Int, responseHeaders: collection.Map[String, Seq[String]],
            responseBody: String): SentinelHubException = {
    if (statusCode == 400 && responseBody.contains(""""RENDERER_S1_MISSING_POLARIZATION""""))
      return new Sentinel1BandNotPresentException(request, requestBody, statusCode, responseHeaders, responseBody)

    new SentinelHubException(request, requestBody, statusCode, responseHeaders, responseBody)
  }

  def unapply(e: SentinelHubException): Option[(String, Int, collection.Map[String, Seq[String]], String)] =
    Some((e.message, e.statusCode, e.responseHeaders, e.responseBody))

  private def formatMessage(request: HttpRequest, requestBody: String,
                            responseHeaders: collection.Map[String, Seq[String]], responseBody: String) = {
    val queryString = request.urlBuilder(request)
    val statusLine = responseHeaders.get("Status").flatMap(_.headOption).getOrElse("UNKNOWN")

    s"""Sentinel Hub returned an error
       |response: $statusLine with body: $responseBody
       |request: ${request.method} $queryString with (possibly abbreviated) body: ${requestBody.slice(0,20000)}..."""
      .stripMargin
  }

  case class SentinelHubError(message: String)
  case class SentinelHubErrorResponse(error: SentinelHubError)
}

class SentinelHubException(private val message: String, val statusCode: Int,
                           val responseHeaders: collection.Map[String, Seq[String]], val responseBody: String)
  extends RuntimeException(message) {

  def this(request: HttpRequest, requestBody: String, statusCode: Int,
           responseHeaders: collection.Map[String, Seq[String]], responseBody: String) = {
    this(SentinelHubException.formatMessage(request, requestBody, responseHeaders, responseBody), statusCode,
      responseHeaders, responseBody)
  }
}

object Sentinel1BandNotPresentException {
  private val bandNotPresentPattern = "Requested band '(.+)' is not present in Sentinel 1 tile .+".r
}

class Sentinel1BandNotPresentException(request: HttpRequest, requestBody: String, statusCode: Int,
                                       responseHeaders: collection.Map[String, Seq[String]],
                                       responseBody: String)
  extends SentinelHubException(request, requestBody, statusCode, responseHeaders, responseBody) {

  import Sentinel1BandNotPresentException._

  val missingBandName: String = {
    val sentinelHubError = decode[SentinelHubException.SentinelHubErrorResponse](responseBody).valueOr(throw _)

    sentinelHubError.error.message match {
      case bandNotPresentPattern(bandName) => bandName
    }
  }
}
